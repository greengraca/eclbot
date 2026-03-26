# online_games_store.py
from __future__ import annotations

import calendar
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import List, Dict, Optional

from db import online_games
from utils.logger import log_warn


@dataclass
class OnlineGameRecord:
    """
    One doc per TopDeck match we care about.

    - (season, tid) identify the match in a TopDeck "season".
    - start_ts: unix timestamp in seconds (float) if known.
    - entrant_ids: TopDeck entrant IDs (ints).
    - topdeck_uids: TopDeck player UIDs (strings) for the 4 players.
    - online: True = SpellTable/online, False = in-person.
    """
    season: int
    tid: int
    start_ts: Optional[float]
    entrant_ids: List[int]
    topdeck_uids: List[str]
    online: bool


def _doc_to_record(doc: dict) -> OnlineGameRecord:
    # Back-compat just in case (you’re dropping the collection anyway)
    uids = doc.get("topdeck_uids")
    if uids is None:
        uids = doc.get("discord_ids") or []

    return OnlineGameRecord(
        season=int(doc.get("season") or 0),
        tid=int(doc.get("tid") or 0),
        start_ts=doc.get("start_ts"),
        entrant_ids=list(doc.get("entrant_ids") or []),
        topdeck_uids=[str(x) for x in (uids or []) if str(x).strip()],
        online=bool(doc.get("online", True)),
    )


async def upsert_record(bracket_id: str, year: int, month: int, record: OnlineGameRecord) -> None:
    bid = str(bracket_id)
    y = int(year)
    m = int(month)

    filt = {
        "bracket_id": bid,
        "year": y,
        "month": m,
        "season": int(record.season),
        "tid": int(record.tid),
    }

    doc = asdict(record)
    doc.update({"bracket_id": bid, "year": y, "month": m, "updated_at": datetime.now(timezone.utc)})

    await online_games.update_one(filt, {"$set": doc}, upsert=True)


async def get_record(bracket_id: str, year: int, month: int, season: int, tid: int) -> Optional[OnlineGameRecord]:
    doc = await online_games.find_one(
        {
            "bracket_id": str(bracket_id),
            "year": int(year),
            "month": int(month),
            "season": int(season),
            "tid": int(tid),
        },
        projection={"_id": 0},
    )
    if not doc:
        return None
    return _doc_to_record(doc)


async def count_online_games_by_topdeck_uid(
    bracket_id: str,
    year: int,
    month: int,
    *,
    online_only: bool = True,
) -> Dict[str, int]:
    """
    Return {topdeck_uid -> number of games} for the month.

    Counts each (season, tid) at most once because the collection is unique on that key.
    """
    match: Dict[str, object] = {"bracket_id": str(bracket_id), "year": int(year), "month": int(month)}
    if online_only:
        match["online"] = True

    pipeline = [
        {"$match": match},
        {"$unwind": "$topdeck_uids"},
        {"$group": {"_id": "$topdeck_uids", "count": {"$sum": 1}}},
    ]

    out: Dict[str, int] = {}
    async for row in online_games.aggregate(pipeline):
        try:
            k = str(row["_id"]).strip()
            if not k:
                continue
            out[k] = int(row.get("count") or 0)
        except Exception as e:
            log_warn(f"[online_games] Aggregation row parse error: {type(e).__name__}: {e}")
            continue
    return out


async def has_recent_game_by_topdeck_uid(
    bracket_id: str,
    year: int,
    month: int,
    uids: List[str],
    after_day: int = 20,
    *,
    online_only: bool = True,
) -> Dict[str, bool]:
    """
    Check if each TopDeck UID has at least one game after the specified day of the month.
    
    Args:
        bracket_id: TopDeck bracket ID
        year: Year
        month: Month
        uids: List of TopDeck UIDs to check
        after_day: Day of month (games on or after this day count as "recent")
        online_only: Only count online games
    
    Returns:
        Dict mapping each UID to True/False
    """
    if not uids:
        return {}
    
    # Calculate the timestamp for the start of after_day (clamped to month length)
    max_day = calendar.monthrange(year, month)[1]
    clamped_day = min(after_day, max_day)
    cutoff_dt = datetime(year, month, clamped_day, 0, 0, 0, tzinfo=timezone.utc)
    cutoff_ts = cutoff_dt.timestamp()
    
    match: Dict[str, object] = {
        "bracket_id": str(bracket_id),
        "year": int(year),
        "month": int(month),
        "start_ts": {"$gte": cutoff_ts},
    }
    if online_only:
        match["online"] = True
    
    # Find all games after the cutoff and collect which UIDs participated
    pipeline = [
        {"$match": match},
        {"$unwind": "$topdeck_uids"},
        {"$group": {"_id": "$topdeck_uids"}},
    ]
    
    uids_with_recent: set = set()
    async for row in online_games.aggregate(pipeline):
        try:
            k = str(row["_id"]).strip()
            if k:
                uids_with_recent.add(k)
        except Exception as e:
            log_warn(f"[online_games] Recency check row parse error: {type(e).__name__}: {e}")
            continue
    
    # Build result dict for all requested UIDs
    return {uid: (uid in uids_with_recent) for uid in uids}
