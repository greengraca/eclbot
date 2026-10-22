# online_games_store.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import List, Dict, Optional

from db import online_games
from utils.logger import log_warn
from utils.topdeck_normalize import normalize_ts


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
    # Single choke point for the start_ts unit contract: callers may hand us raw
    # TopDeck values (milliseconds) or already-normalized seconds.
    doc["start_ts"] = normalize_ts(doc.get("start_ts"))
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
