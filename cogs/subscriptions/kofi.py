# cogs/subscriptions/kofi.py
"""Ko-fi webhook parsing and helper functions."""

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from utils.settings import LISBON_TZ


def compute_one_time_window(when_lisbon: datetime, days: int) -> tuple[datetime, datetime]:
    """Return (starts_at_utc, expires_at_utc) for a Ko-fi one-time pass."""
    if when_lisbon.tzinfo is None:
        when_lisbon = when_lisbon.replace(tzinfo=LISBON_TZ)
    starts = when_lisbon.astimezone(timezone.utc)
    expires = starts + timedelta(days=max(1, int(days or 30)))
    return starts, expires


def extract_discord_user_id(payload: Dict[str, Any]) -> Optional[int]:
    """Best-effort mapping from Ko-fi payload -> Discord user id."""
    duid = str(payload.get("discord_userid") or payload.get("discord_user_id") or "").strip()
    if duid.isdigit():
        return int(duid)

    msg = str(payload.get("message") or "")
    m = re.search(r"<@!?(\d{15,25})>", msg)
    if m:
        return int(m.group(1))
    m2 = re.search(r"\b(\d{15,25})\b", msg)
    if m2:
        return int(m2.group(1))
    return None


def extract_json_from_message_content(content: str) -> Optional[Dict[str, Any]]:
    """Supports ```json ...``` or raw JSON."""
    if not content:
        return None
    m = re.search(r"```json\s*([\s\S]+?)\s*```", content)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            return None
    content = content.strip()
    if content.startswith("{") and content.endswith("}"):
        try:
            return json.loads(content)
        except Exception:
            return None
    return None
