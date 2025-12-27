# spelltable_client.py
import os
import logging
import asyncio
import re
import unicodedata
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ---------- Config from env ----------

SPELLTABLE_PROXY_URL = os.getenv("SPELLTABLE_PROXY_URL", "").rstrip("/")

# Optional header-based auth, if they ever give you one
SPELLTABLE_PROXY_AUTH_HEADER = os.getenv("SPELLTABLE_PROXY_AUTH_HEADER", "")
SPELLTABLE_PROXY_AUTH_VALUE = os.getenv("SPELLTABLE_PROXY_AUTH_VALUE", "")

# Request timeout + retries
TIMEOUT_S = float(os.getenv("SPELLTABLE_TIMEOUT_SECONDS", "5"))
RETRY_ATTEMPTS = int(os.getenv("SPELLTABLE_RETRY_ATTEMPTS", "2"))

# Simple process-local rate limit so we don't spam his API
MIN_INTERVAL_SECONDS = float(os.getenv("SPELLTABLE_MIN_INTERVAL_SECONDS", "2.0"))
_rate_lock = asyncio.Lock()
_last_call: float = 0.0


class SpellTableAuthError(RuntimeError):
    """Raised when we fail to talk to the SpellTable proxy API."""


async def _respect_rate_limit() -> None:
    """Ensure at least MIN_INTERVAL_SECONDS between calls."""
    global _last_call
    loop = asyncio.get_running_loop()

    async with _rate_lock:
        now = loop.time()
        elapsed = now - _last_call
        wait = MIN_INTERVAL_SECONDS - elapsed
        if wait > 0:
            await asyncio.sleep(wait)
        _last_call = loop.time()


def _slugify_name(name: str) -> str:
    """
    Turn arbitrary game name into ASCII slug
    """
    if not name:
        return "commander-game"
    # Normalize and strip accents / unicode
    n = unicodedata.normalize("NFKD", name)
    n = n.encode("ascii", "ignore").decode("ascii")
    # Replace non-alnum with '-'
    n = re.sub(r"[^a-zA-Z0-9]+", "-", n)
    n = n.strip("-")
    return n or "commander-game"


# ---------- Public entrypoint ----------

async def create_spelltable_game(
    *,
    game_name: str = "Discord LFG",
    format_name: str = "Commander",  # kept for compatibility, not used by API
    is_public: bool = False,         # kept for compatibility, not used by API
) -> str:
    """
    Create a SpellTable game via the external proxy API and return its join URL.

    Env vars expected:
      - SPELLTABLE_PROXY_URL 
      - SPELLTABLE_PROXY_AUTH_HEADER (optional)
      - SPELLTABLE_PROXY_AUTH_VALUE  (optional)
    """
    if not SPELLTABLE_PROXY_URL:
        raise SpellTableAuthError(
            "SPELLTABLE_PROXY_URL is not configured "
        )

    timeout = httpx.Timeout(TIMEOUT_S, connect=TIMEOUT_S, read=TIMEOUT_S, write=TIMEOUT_S)

    # Sanitize name for their API – avoid unicode punctuation etc.
    safe_name = _slugify_name(game_name)
    params = {"name": safe_name}

    headers: dict = {}
    if SPELLTABLE_PROXY_AUTH_HEADER and SPELLTABLE_PROXY_AUTH_VALUE:
        headers[SPELLTABLE_PROXY_AUTH_HEADER] = SPELLTABLE_PROXY_AUTH_VALUE

    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(RETRY_ATTEMPTS):
            try:
                await _respect_rate_limit()

                resp = await client.get(
                    SPELLTABLE_PROXY_URL,
                    params=params,
                    headers=headers or None,
                )

                # Log status + body on non-200 for debugging
                if resp.status_code >= 400:
                    body_snippet = resp.text[:500]
                    logger.warning(
                        "SpellTable proxy non-200 response "
                        "(attempt %s/%s): %s %s | body=%r",
                        attempt + 1,
                        RETRY_ATTEMPTS,
                        resp.status_code,
                        resp.reason_phrase,
                        body_snippet,
                    )
                    resp.raise_for_status()

                try:
                    data = resp.json()
                except Exception as exc:
                    body_snippet = resp.text[:500]
                    raise SpellTableAuthError(
                        f"SpellTable proxy did not return JSON: {body_snippet!r}"
                    ) from exc

                link = data.get("link")
                if not isinstance(link, str) or not link:
                    raise SpellTableAuthError(
                        f"SpellTable proxy response missing 'link': {data!r}"
                    )

                logger.info(
                    "SpellTable proxy created game successfully: name=%r slug=%r link=%r",
                    game_name,
                    safe_name,
                    link,
                )
                return link

            except Exception as exc:
                logger.warning(
                    "SpellTable proxy create failed (attempt %s/%s): %s",
                    attempt + 1,
                    RETRY_ATTEMPTS,
                    exc,
                    exc_info=True,
                )
                if attempt == RETRY_ATTEMPTS - 1:
                    raise SpellTableAuthError(
                        "Failed to create SpellTable game via proxy after several attempts."
                    ) from exc

    # should never be reached
    raise SpellTableAuthError("Unexpected error while creating SpellTable game via proxy")
