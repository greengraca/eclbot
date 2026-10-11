# convoke_client.py
# Convoke game creation. Mirrors lexicalunit/spellbot src/spellbot/integrations/convoke.py.
import os
from typing import List, Optional, Tuple

import httpx

from utils.logger import log_ok, log_warn

# ---------- Config from env ----------

CONVOKE_ROOT = os.getenv("CONVOKE_ROOT", "https://api.convoke.games/api").rstrip("/")
CONVOKE_API_KEY = os.getenv("CONVOKE_API_KEY", "").strip()

TIMEOUT_S = float(os.getenv("CONVOKE_TIMEOUT_SECONDS", "5"))
RETRY_ATTEMPTS = int(os.getenv("CONVOKE_RETRY_ATTEMPTS", "2"))


class ConvokeError(RuntimeError):
    """Raised when we fail to create a Convoke game."""


def convoke_enabled() -> bool:
    return bool(CONVOKE_API_KEY)


def _is_terminal_client_error(exc: Exception) -> bool:
    """4xx (except 408/429) won't succeed on retry — bad key or bad payload."""
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return 400 <= code < 500 and code not in (408, 429)
    return False


# ---------- Public entrypoint ----------

async def create_convoke_game(
    *,
    game_name: str,
    guild_id: int,
    channel_id: int,
    players: List[Tuple[int, str]],
    seats: int = 4,
) -> Tuple[str, Optional[str]]:
    """
    Create a private Commander game on Convoke and return (url, password).

    `players` is [(discord_id, display_name), ...]. Password is None for
    no-password lobbies (the default for private games today).
    """
    if not CONVOKE_API_KEY:
        raise ConvokeError("CONVOKE_API_KEY is not configured")

    payload = {
        "isPublic": False,
        "name": game_name,
        "seatLimit": int(seats),
        "format": "commander",
        "discordGuild": str(guild_id),
        "discordChannel": str(channel_id),
        "discordPlayers": [{"id": str(uid), "name": name} for uid, name in players],
        "language": "en",
    }
    headers = {"user-agent": "eclbot", "x-api-key": CONVOKE_API_KEY}
    timeout = httpx.Timeout(TIMEOUT_S, connect=TIMEOUT_S, read=TIMEOUT_S, write=TIMEOUT_S)

    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(RETRY_ATTEMPTS):
            try:
                resp = await client.post(f"{CONVOKE_ROOT}/game/create-game", json=payload, headers=headers)
                if resp.status_code >= 400:
                    log_warn(
                        f"[convoke] non-2xx response (attempt {attempt + 1}/{RETRY_ATTEMPTS}): "
                        f"{resp.status_code} {resp.reason_phrase} | body={resp.text[:500]!r}"
                    )
                    resp.raise_for_status()

                data = resp.json()
                url = data.get("url") if isinstance(data, dict) else None
                if not isinstance(url, str) or not url:
                    raise ConvokeError(f"Convoke response missing 'url': {str(data)[:500]!r}")

                password = data.get("password") or None
                log_ok(f"[convoke] created game: name={game_name!r} url={url!r}")
                return url, password

            except Exception as exc:
                log_warn(f"[convoke] create failed (attempt {attempt + 1}/{RETRY_ATTEMPTS}): {exc}")
                if attempt == RETRY_ATTEMPTS - 1 or _is_terminal_client_error(exc):
                    raise ConvokeError("Failed to create Convoke game.") from exc

    raise ConvokeError("Unexpected error while creating Convoke game")
