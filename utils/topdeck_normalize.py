# utils/topdeck_normalize.py
"""Shared Discord handle normalization for TopDeck data."""
from __future__ import annotations

import re


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
