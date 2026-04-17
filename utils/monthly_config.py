# utils/monthly_config.py
"""
DB-backed monthly config reader for the ECL bot.

Reads from ecl_monthly_config collection (shared with dashboard).
Falls back to env vars if no DB document exists.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import aiohttp

from db import ecl_monthly_config
from utils.dates import LISBON_TZ, month_key
from utils.logger import log_warn
from utils.settings import GUILD_ID

from datetime import datetime

# Env var fallbacks
_ENV_BRACKET_ID = (os.getenv("TOPDECK_BRACKET_ID") or "").strip()
_ENV_NEXT_BRACKET_ID = (os.getenv("NEXT_MONTH_TOPDECK_BRACKET_ID") or "").strip()
_ENV_MOSTGAMES_IMAGE = (os.getenv("MOSTGAMES_PRIZE_IMAGE_URL") or "").strip()
_ENV_DASHBOARD_URL = (os.getenv("DASHBOARD_BASE_URL") or "").rstrip("/")

# In-memory cache: { month_key: (doc_or_None, expiry_ts) }
_cache: Dict[str, tuple[Optional[Dict[str, Any]], float]] = {}
# Separate cache for mostgames image URLs (resolved via dashboard HTTP)
_mostgames_cache: Dict[str, tuple[str, float]] = {}
_CACHE_TTL = 60  # seconds


def clear_cache() -> None:
    """Clear the in-memory config cache (call after writes)."""
    _cache.clear()
    _mostgames_cache.clear()


def _now_lisbon() -> datetime:
    return datetime.now(LISBON_TZ)


def _current_month() -> str:
    return month_key(_now_lisbon())


async def _get_config(month: str) -> Optional[Dict[str, Any]]:
    """Fetch config from DB with caching."""
    cached = _cache.get(month)
    if cached and time.monotonic() < cached[1]:
        return cached[0]

    doc = await ecl_monthly_config.find_one(
        {"guild_id": str(GUILD_ID), "month": month}
    )

    _cache[month] = (doc, time.monotonic() + _CACHE_TTL)
    return doc


async def get_monthly_config(month: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get the full config document for a month.

    Args:
        month: "YYYY-MM" key. Defaults to current Lisbon month.
    """
    if month is None:
        month = _current_month()
    return await _get_config(month)


async def get_bracket_id(month: Optional[str] = None) -> str:
    """
    Get the TopDeck bracket ID for a month.

    Resolution:
      1. ecl_monthly_config.bracket_id  (set via dashboard)
      2. TOPDECK_BRACKET_ID env var     (fallback)

    Args:
        month: "YYYY-MM" key. Defaults to current Lisbon month.
    """
    if month is None:
        month = _current_month()

    config = await _get_config(month)
    if config and config.get("bracket_id"):
        return config["bracket_id"]

    return _ENV_BRACKET_ID


async def get_next_month_bracket_id() -> str:
    """
    Get the bracket ID for the next month.

    Resolution:
      1. ecl_monthly_config.bracket_id for next month
      2. NEXT_MONTH_TOPDECK_BRACKET_ID env var
    """
    from utils.dates import add_months
    next_month = add_months(_current_month(), 1)

    config = await _get_config(next_month)
    if config and config.get("bracket_id"):
        return config["bracket_id"]

    return _ENV_NEXT_BRACKET_ID


async def get_mostgames_image(month: Optional[str] = None) -> str:
    """
    Get the most games prize image URL for a month.

    Resolution:
      1. Dashboard API /api/prizes/most-games/image (canonical — resolves
         ecl_monthly_config override, dashboard_prizes.image_url, or a fresh
         presigned URL for dashboard_prizes.r2_key).
      2. MOSTGAMES_PRIZE_IMAGE_URL env var (fallback when dashboard unreachable).

    Args:
        month: "YYYY-MM" key. Defaults to current Lisbon month.
    """
    if month is None:
        month = _current_month()

    cached = _mostgames_cache.get(month)
    if cached and time.monotonic() < cached[1]:
        return cached[0]

    url = ""
    if _ENV_DASHBOARD_URL:
        api_url = f"{_ENV_DASHBOARD_URL}/api/prizes/most-games/image?month={month}"
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_url) as resp:
                    if resp.status == 200:
                        payload = await resp.json()
                        resolved = (payload.get("data") or {}).get("url")
                        if resolved:
                            url = resolved
                    else:
                        log_warn(
                            f"[monthly_config] mostgames image HTTP {resp.status} for {month}"
                        )
        except Exception as e:
            log_warn(
                f"[monthly_config] mostgames image fetch failed: {type(e).__name__}: {e}"
            )

    if not url:
        url = _ENV_MOSTGAMES_IMAGE

    if url:
        _mostgames_cache[month] = (url, time.monotonic() + _CACHE_TTL)

    return url


async def get_join_channel_id(month: Optional[str] = None) -> Optional[str]:
    """
    Get the join channel Discord ID for a month.
    Returns None if not configured.
    """
    if month is None:
        month = _current_month()

    config = await _get_config(month)
    if config and config.get("join_channel_id"):
        return config["join_channel_id"]
    return None
