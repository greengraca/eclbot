# online_games_store.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import List, Dict, Optional

from db import online_games


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


async def load_index(bracket_id: str, year: int, month: int) -> List[OnlineGameRecord]:
    cur = online_games.find(
        {"bracket_id": str(bracket_id), "year": int(year), "month": int(month)}
    )
    docs = await cur.to_list(length=None)
    return [_doc_to_record(d) for d in docs]


async def save_index(bracket_id: str, year: int, month: int, records: List[OnlineGameRecord]) -> None:
    bid = str(bracket_id)
    y = int(year)
    m = int(month)

    await online_games.delete_many({"bracket_id": bid, "year": y, "month": m})

    if not records:
        return

    now = datetime.now(timezone.utc)
    docs = []
    for r in records:
        d = asdict(r)
        d.update({"bracket_id": bid, "year": y, "month": m, "updated_at": now})
        docs.append(d)

    await online_games.insert_many(docs, ordered=False)


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
        except Exception:
            continue
    return out


async def count_online_games_by_topdeck_uid_str(
    bracket_id: str,
    year: int,
    month: int,
    *,
    online_only: bool = True,
) -> Dict[str, int]:
    # already str keys, but keep the old call-site style
    return await count_online_games_by_topdeck_uid(bracket_id, year, month, online_only=online_only)


# ---- OPTIONAL: keep legacy names so nothing else explodes ----
# If you truly want “rename everywhere”, you can delete these aliases after you update all call sites.
count_online_games_by_discord_str = count_online_games_by_topdeck_uid_str
