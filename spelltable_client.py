# spelltable_client.py
import os
import logging
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse, parse_qs

import httpx

logger = logging.getLogger(__name__)

# ---------- Config from env ----------

WIZARDS_ROOT = os.getenv("WIZARDS_ROOT", "https://account.wizards.com")
SPELLTABLE_ROOT = os.getenv(
    "SPELLTABLE_ROOT",
    "https://spelltable-api-prod.wizards.com",
)
SPELLTABLE_CLIENT_ID = os.getenv("SPELLTABLE_CLIENT_ID")
SPELLTABLE_AUTH_REDIRECT = os.getenv(
    "SPELLTABLE_AUTH_REDIRECT",
    "https://spelltable.wizards.com/auth-callback",
)
SPELLTABLE_API_KEY = os.getenv("SPELLTABLE_API_KEY")

# Single WotC account for SpellTable auth
SPELLTABLE_USER = os.getenv("SPELLTABLE_USER")
SPELLTABLE_PASS = os.getenv("SPELLTABLE_PASS")

TIMEOUT_S = 3
RETRY_ATTEMPTS = 2


@dataclass
class TokenData:
    access_token: str
    refresh_token: str
    expires_at: datetime


# Simple in-memory token cache for this process
_token_data: Optional[TokenData] = None
_token_lock = asyncio.Lock()


class SpellTableAuthError(RuntimeError):
    pass


# ---------- Low-level helpers ----------

async def _get_csrf(client: httpx.AsyncClient) -> str:
    resp = await client.get(f"{WIZARDS_ROOT}/login")
    resp.raise_for_status()
    csrf_token = client.cookies.get("_csrf")
    if not csrf_token:
        raise SpellTableAuthError("No CSRF token in login response")
    return csrf_token


async def _login(
    client: httpx.AsyncClient,
    username: str,
    password: str,
    csrf: str,
) -> None:
    url = f"{WIZARDS_ROOT}/api/login"
    payload = {
        "username": username,
        "password": password,
        "referringClientID": SPELLTABLE_CLIENT_ID,
        "remember": False,
        "_csrf": csrf,
    }
    resp = await client.post(url, json=payload)
    resp.raise_for_status()


async def _client_info(client: httpx.AsyncClient, csrf: str) -> None:
    url = f"{WIZARDS_ROOT}/api/client"
    payload = {
        "clientID": SPELLTABLE_CLIENT_ID,
        "language": "en-US",
        "_csrf": csrf,
    }
    resp = await client.post(url, json=payload)
    resp.raise_for_status()


async def _authorize(client: httpx.AsyncClient, csrf: str) -> str:
    url = f"{WIZARDS_ROOT}/api/authorize"
    payload = {
        "clientInput": {
            "clientID": SPELLTABLE_CLIENT_ID,
            "redirectURI": SPELLTABLE_AUTH_REDIRECT,
            "scope": "email",
            "state": "",
            "version": "2",
        },
        "_csrf": csrf,
    }
    resp = await client.post(url, json=payload, follow_redirects=False)
    resp.raise_for_status()

    data = resp.json()
    redirect_target = data.get("data", {}).get("redirect_target")
    if not redirect_target:
        raise SpellTableAuthError("No redirect_target in authorize response")

    parsed = urlparse(redirect_target)
    code = parse_qs(parsed.query).get("code", [None])[0]
    if not code:
        raise SpellTableAuthError("No auth code in authorize redirect")
    return code


async def _exchange_code(client: httpx.AsyncClient, code: str) -> TokenData:
    if not SPELLTABLE_API_KEY:
        raise SpellTableAuthError("SPELLTABLE_API_KEY is not configured")

    url = f"{SPELLTABLE_ROOT}/prod/exchangeCode"
    headers = {"x-api-key": SPELLTABLE_API_KEY}
    payload = {"code": code}
    resp = await client.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    return TokenData(
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=data["expires_in"]),
    )


async def _refresh_access_token(
    client: httpx.AsyncClient,
    refresh_token: str,
) -> Optional[TokenData]:
    if not SPELLTABLE_API_KEY:
        raise SpellTableAuthError("SPELLTABLE_API_KEY is not configured")

    url = f"{SPELLTABLE_ROOT}/prod/refreshToken"
    headers = {"x-api-key": SPELLTABLE_API_KEY}
    try:
        resp = await client.post(url, headers=headers, json={"refreshToken": refresh_token})
        resp.raise_for_status()
        data = resp.json()
        return TokenData(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=data["expires_in"]),
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("SpellTable token refresh failed: %s", exc, exc_info=True)
        return None


async def _get_access_token(client: httpx.AsyncClient) -> str:
    """
    Returns a valid SpellTable access token, using a cached token if possible.
    """
    global _token_data

    if not SPELLTABLE_CLIENT_ID:
        raise SpellTableAuthError("SPELLTABLE_CLIENT_ID is not configured")
    if not SPELLTABLE_USER or not SPELLTABLE_PASS:
        raise SpellTableAuthError("SPELLTABLE_USER / SPELLTABLE_PASS not configured")

    now = datetime.now(timezone.utc)

    # 1) valid cached token
    if _token_data and _token_data.access_token and _token_data.expires_at > now:
        return _token_data.access_token

    # 2) try refresh if we have a refresh token
    if _token_data and _token_data.refresh_token:
        refreshed = await _refresh_access_token(client, _token_data.refresh_token)
        if refreshed:
            _token_data = refreshed
            return refreshed.access_token

    # 3) full login flow
    csrf = await _get_csrf(client)
    await _login(client, SPELLTABLE_USER, SPELLTABLE_PASS, csrf)
    await _client_info(client, csrf)
    code = await _authorize(client, csrf)
    new_token = await _exchange_code(client, code)
    _token_data = new_token
    return new_token.access_token


# ---------- Public entrypoint ----------

async def create_spelltable_game(
    *,
    game_name: str = "Discord LFG",
    format_name: str = "Commander",
    is_public: bool = False,
) -> str:
    """
    Create a SpellTable game and return its join URL.

    Env vars expected:
      - WIZARDS_ROOT (optional, default: account.wizards.com)
      - SPELLTABLE_ROOT (optional, default: spelltable-api-prod.wizards.com)
      - SPELLTABLE_CLIENT_ID
      - SPELLTABLE_AUTH_REDIRECT
      - SPELLTABLE_API_KEY
      - SPELLTABLE_USER
      - SPELLTABLE_PASS
    """
    timeout = httpx.Timeout(TIMEOUT_S, connect=TIMEOUT_S, read=TIMEOUT_S, write=TIMEOUT_S)

    async with _token_lock, httpx.AsyncClient(timeout=timeout) as client:
        token = await _get_access_token(client)

        url = f"{SPELLTABLE_ROOT}/prod/createGame"
        headers = {"x-api-key": SPELLTABLE_API_KEY}
        payload = {
            "token": token,
            "name": game_name,
            "description": "",
            "format": format_name,   # "Commander" etc – see SpellTableGameTypes in SpellBot
            "isPublic": is_public,
            "tags": {},
        }

        for attempt in range(RETRY_ATTEMPTS):
            try:
                resp = await client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
                game_id = data["id"]
                return f"https://spelltable.wizards.com/game/{game_id}"
            except Exception as exc:
                logger.warning(
                    "SpellTable createGame failed (attempt %s/%s): %s",
                    attempt + 1,
                    RETRY_ATTEMPTS,
                    exc,
                    exc_info=True,
                )
                if attempt == RETRY_ATTEMPTS - 1:
                    raise SpellTableAuthError(
                        "Failed to create SpellTable game after several attempts."
                    ) from exc

    # should never get here
    raise SpellTableAuthError("Unexpected error while creating SpellTable game")
