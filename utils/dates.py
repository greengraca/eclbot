# utils/dates.py
"""
Consolidated date/month utilities for the ECL bot.

This module centralizes all date-related helpers that were previously
duplicated across subscriptions_cog, debug_cog, and join_league_cog.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Tuple
from zoneinfo import ZoneInfo

# The canonical timezone for all ECL operations
LISBON_TZ = ZoneInfo("Europe/Lisbon")


def month_key(dt: datetime) -> str:
    """
    Return the month key in 'YYYY-MM' format for the given datetime.
    
    Args:
        dt: Any datetime (timezone-aware or naive).
        
    Returns:
        String in format 'YYYY-MM' (e.g., '2026-01').
    """
    return f"{dt.year:04d}-{dt.month:02d}"


def add_months(mk: str, n: int) -> str:
    """
    Add (or subtract) n months from a month key.
    
    Args:
        mk: Month key in 'YYYY-MM' format.
        n: Number of months to add (negative to subtract).
        
    Returns:
        New month key in 'YYYY-MM' format.
        
    Examples:
        >>> add_months("2026-01", 1)
        '2026-02'
        >>> add_months("2026-01", -1)
        '2025-12'
        >>> add_months("2026-11", 3)
        '2027-02'
    """
    y, m = mk.split("-")
    y_i, m_i = int(y), int(m)
    m_i += n
    
    while m_i > 12:
        y_i += 1
        m_i -= 12
    while m_i < 1:
        y_i -= 1
        m_i += 12
        
    return f"{y_i:04d}-{m_i:02d}"


def month_bounds(mk: str) -> Tuple[datetime, datetime]:
    """
    Return the start and end boundaries of a month in Lisbon timezone.
    
    Args:
        mk: Month key in 'YYYY-MM' format.
        
    Returns:
        Tuple of (start, end_exclusive) where:
        - start: First moment of the month (00:00:00 on day 1)
        - end_exclusive: First moment of the NEXT month
        
    Note:
        Both datetimes are timezone-aware (Europe/Lisbon).
    """
    y, m = mk.split("-")
    start = datetime(int(y), int(m), 1, 0, 0, 0, tzinfo=LISBON_TZ)
    
    end_mk = add_months(mk, 1)
    y2, m2 = end_mk.split("-")
    end = datetime(int(y2), int(m2), 1, 0, 0, 0, tzinfo=LISBON_TZ)
    
    return start, end


def league_close_at(mk: str) -> datetime:
    """
    Return the league close time for a given month.
    
    The league closes at 19:00 Lisbon time on the last day of the month.
    
    Args:
        mk: Month key in 'YYYY-MM' format.
        
    Returns:
        Datetime of league close (timezone-aware, Europe/Lisbon).
    """
    _, end = month_bounds(mk)  # end is first day of next month @ 00:00 Lisbon
    last_day = (end - timedelta(days=1)).astimezone(LISBON_TZ)
    return datetime(
        last_day.year, last_day.month, last_day.day,
        19, 0, 0,
        tzinfo=LISBON_TZ
    )


def last_day_of_month(dt: datetime) -> datetime:
    """
    Return midnight on the last day of the month containing dt.
    
    Args:
        dt: Any datetime.
        
    Returns:
        Datetime at 00:00:00 on the last day of the month (Lisbon timezone).
    """
    mk = month_key(dt)
    _, end = month_bounds(mk)
    return (end - timedelta(days=1)).astimezone(LISBON_TZ)


def month_end_inclusive(mk: str) -> datetime:
    """
    Return the last second of a month in Lisbon timezone.
    
    Args:
        mk: Month key in 'YYYY-MM' format.
        
    Returns:
        Datetime at 23:59:59 on the last day of the month.
    """
    _, end = month_bounds(mk)
    return (end - timedelta(seconds=1)).astimezone(LISBON_TZ)


def month_label(mk: str) -> str:
    """
    Return a human-readable label for a month key.
    
    Args:
        mk: Month key in 'YYYY-MM' format.
        
    Returns:
        String like 'January 2026'.
    """
    try:
        y, m = mk.split("-")
        dt = datetime(int(y), int(m), 1, tzinfo=LISBON_TZ)
        return dt.strftime("%B %Y")
    except Exception:
        return mk


def parse_month_from_text(text: str) -> str | None:
    """
    Extract a 'YYYY-MM' pattern from text.
    
    Args:
        text: Any string that might contain a month key.
        
    Returns:
        The first 'YYYY-MM' match found, or None.
    """
    import re
    m = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])\b", text or "")
    return m.group(0) if m else None


def looks_like_month(s: str) -> bool:
    """
    Check if a string looks like a valid month key.
    
    Args:
        s: String to check.
        
    Returns:
        True if s matches 'YYYY-MM' pattern with valid month (01-12).
    """
    import re
    return bool(re.match(r"^20\d{2}-(0[1-9]|1[0-2])$", (s or "").strip()))


def now_lisbon() -> datetime:
    """
    Return the current datetime in Lisbon timezone.
    
    Returns:
        Timezone-aware datetime in Europe/Lisbon.
    """
    return datetime.now(LISBON_TZ)


def current_month_key() -> str:
    """
    Return the month key for the current month in Lisbon timezone.
    
    Returns:
        String in 'YYYY-MM' format.
    """
    return month_key(now_lisbon())
