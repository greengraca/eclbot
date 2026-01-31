# utils/settings.py
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Set

# Import and re-export LISBON_TZ from dates for backwards compatibility
from utils.dates import LISBON_TZ

TOPDECK_BRACKET_ID = (os.getenv("TOPDECK_BRACKET_ID") or "").strip()
NEXT_MONTH_TOPDECK_BRACKET_ID = (os.getenv("NEXT_MONTH_TOPDECK_BRACKET_ID") or "").strip()
FIREBASE_ID_TOKEN = os.getenv("FIREBASE_ID_TOKEN")  # can be None

def env_int(name: str, default: int = 0) -> int:
    try:
        return int((os.getenv(name) or "").strip())
    except Exception:
        return default

def env_float(name: str, default: float = 0.0) -> float:
    try:
        return float((os.getenv(name) or "").strip())
    except Exception:
        return default

def env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if raw in ("1", "true", "yes", "y", "on"):
        return True
    if raw in ("0", "false", "no", "n", "off"):
        return False
    return default

def parse_int_set(csv: str) -> Set[int]:
    out: Set[int] = set()
    for part in re.split(r"[\s,]+", (csv or "").strip()):
        if part.isdigit():
            out.add(int(part))
    return out

DEFAULT_SUBS_ENFORCEMENT_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=LISBON_TZ)

@dataclass(frozen=True)
class SubsConfig:
    guild_id: int
    ecl_role_id: int
    ecl_mod_role_id: int
    dm_optin_role_id: int

    patreon_role_ids: Set[int]
    kofi_role_ids: Set[int]
    free_entry_role_ids: Set[int]

    kofi_inbox_channel_id: int
    kofi_verify_token: str
    entitlement_cutoff_day: int
    kofi_one_time_days: int
    enforcement_start: datetime

    dm_enabled: bool
    dm_concurrency: int
    dm_sleep_seconds: float
    log_channel_id: int

    # delete_kofi_inbox_messages: bool

    top16_role_id: int
    top16_min_online_games: int
    top16_min_online_games_no_recency: int  # >= this many games = no recency needed
    top16_recency_after_day: int  # games after this day of month count as "recent"
    top16_min_total_games: int
    topcut_close_pts: int

    kofi_url: str
    patreon_url: str
    embed_thumbnail_url: str
    embed_color: int

def load_subs_config() -> SubsConfig:
    enforcement_raw = (os.getenv("SUBS_ENFORCEMENT_START") or "").strip()
    enforcement_start = DEFAULT_SUBS_ENFORCEMENT_START

    if enforcement_raw:
        try:
            enforcement_start = datetime.fromisoformat(enforcement_raw.replace("Z", "+00:00"))
        except Exception:
            try:
                y, m, d = enforcement_raw.split("-")
                enforcement_start = datetime(int(y), int(m), int(d), 0, 0, 0)
            except Exception:
                enforcement_start = DEFAULT_SUBS_ENFORCEMENT_START

    if enforcement_start.tzinfo is None:
        enforcement_start = enforcement_start.replace(tzinfo=LISBON_TZ)
    enforcement_start = enforcement_start.astimezone(LISBON_TZ)

    return SubsConfig(
        guild_id=env_int("GUILD_ID", 0),
        ecl_role_id=env_int("ECL_ROLE", 0),
        ecl_mod_role_id=env_int("ECL_MOD_ROLE_ID", 0),
        dm_optin_role_id=env_int("DM_OPTIN_ROLE_ID", 0),

        patreon_role_ids=parse_int_set(os.getenv("PATREON_ROLE_IDS", "")),
        kofi_role_ids=parse_int_set(os.getenv("KOFI_ROLE_IDS", "")),
        free_entry_role_ids=parse_int_set(os.getenv("FREE_ENTRY_ROLE_IDS", "")),

        kofi_inbox_channel_id=env_int("KOFI_INBOX_CHANNEL_ID", 0),
        kofi_verify_token=(os.getenv("KOFI_VERIFY_TOKEN") or "").strip(),
        entitlement_cutoff_day=env_int("SUBS_CUTOFF_DAY", 23),
        kofi_one_time_days=max(1, env_int("KOFI_ONE_TIME_DAYS", 30)),
        enforcement_start=enforcement_start,

        dm_enabled=env_bool("SUBS_DM_ENABLED", True),
        dm_concurrency=max(1, env_int("SUBS_DM_CONCURRENCY", 5)),
        dm_sleep_seconds=max(0.0, env_float("SUBS_DM_SLEEP_SECONDS", 0.8)),
        log_channel_id=env_int("SUBS_LOG_CHANNEL_ID", 0),

        top16_role_id=env_int("TOP16_ROLE_ID", 0),
        top16_min_online_games=env_int("TOP16_MIN_ONLINE_GAMES", 10),
        top16_min_online_games_no_recency=env_int("TOP16_MIN_ONLINE_GAMES_NO_RECENCY", 20),
        top16_recency_after_day=env_int("TOP16_RECENCY_AFTER_DAY", 20),
        top16_min_total_games=env_int("TOP16_MIN_TOTAL_GAMES", 10),
        topcut_close_pts=env_int("TOPCUT_CLOSE_PTS", 250),

        kofi_url=(os.getenv("SUBS_KOFI_URL") or "").strip(),
        patreon_url=(os.getenv("SUBS_PATREON_URL") or "").strip(),
        embed_thumbnail_url=(os.getenv("LFG_EMBED_ICON_URL") or "").strip(),
        embed_color=env_int("SUBS_EMBED_COLOR", 0x2ECC71),
    )


SUBS = load_subs_config()
