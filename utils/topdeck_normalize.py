# utils/topdeck_normalize.py
"""Shared normalization helpers for TopDeck data (Discord handles, timestamps)."""
from __future__ import annotations

import re
from typing import Optional

# TopDeck emits Start/End as either seconds or milliseconds depending on the
# endpoint. Anything above this is milliseconds (1e10 s == year 2286).
MS_THRESHOLD = 10_000_000_000


def normalize_ts(ts) -> Optional[float]:
    """Normalize a TopDeck timestamp to **seconds**, accepting seconds or milliseconds.

    Every persisted `start_ts` must go through this. A raw millisecond value
    (~1.78e12) compares greater than any seconds-based cutoff (~1.78e9), which
    silently makes time-window filters — notably the Top 16 "game after day N"
    recency check — match rows they should exclude.
    """
    if ts is None:
        return None
    try:
        x = float(ts)
    except (TypeError, ValueError):
        return None
    return x / 1000.0 if x > MS_THRESHOLD else x


def norm_handle(s: str) -> str:
    """Lowercase, strip everything except a-z and 0-9."""
    return re.sub(r"[^a-z0-9]", "", s.lower()) if isinstance(s, str) else ""


def normalize_topdeck_discord(discord_raw: str) -> str:
    """Normalize a TopDeck 'discord' field for matching against Discord usernames.

    Handles formats like:
    - 'Zerox#1234'       -> 'zerox'
    - '@Zerox'           -> 'zerox'
    - 'Zerox (Zerox)'    -> 'zerox'
    - 'Zerox some stuff' -> 'zerox'
    """
    if not discord_raw:
        return ""
    s = str(discord_raw).strip()

    # strip leading @
    if s.startswith("@"):
        s = s[1:]

    # keep only first token (before space or paren)
    s = re.split(r"[\s(]", s, 1)[0]

    # strip old-style discriminator (#1234)
    if "#" in s:
        s = s.split("#", 1)[0]

    return norm_handle(s)
