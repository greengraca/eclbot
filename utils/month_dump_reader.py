"""Historical month-dump reader for graphs.

Reassembles chunked dumps from MongoDB and computes per-player stats
across multiple months.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from db import topdeck_month_dump_runs, topdeck_month_dump_chunks, topdeck_pods
from topdeck_fetch import Match, _compute_standings, _is_valid_completed_match
from utils.dates import LISBON_TZ


async def get_historical_months(bracket_id: str) -> List[Dict[str, Any]]:
    """Return distinct (bracket_id, month) pairs with latest run, sorted ascending."""
    pipeline = [
        {"$match": {"bracket_id": bracket_id}},
        {"$sort": {"created_at": -1}},
        {
            "$group": {
                "_id": {"bracket_id": "$bracket_id", "month": "$month"},
                "latest_run_id": {"$first": "$_id"},
                "created_at": {"$first": "$created_at"},
            }
        },
        {"$sort": {"_id.month": 1}},
    ]
    results = []
    async for doc in topdeck_month_dump_runs.aggregate(pipeline):
        results.append({
            "bracket_id": doc["_id"]["bracket_id"],
            "month": doc["_id"]["month"],
            "run_doc_id": doc["latest_run_id"],
            "created_at": doc["created_at"],
        })
    return results


async def reassemble_month_dump(run_doc_id) -> Optional[Dict[str, Any]]:
    """Load chunks for a run and reassemble the full payload."""
    chunks = []
    async for chunk in topdeck_month_dump_chunks.find(
        {"run_doc_id": run_doc_id}
    ).sort("chunk_index", 1):
        chunks.append(chunk["data"])

    if not chunks:
        return None

    try:
        return json.loads("".join(chunks))
    except (json.JSONDecodeError, TypeError):
        return None


def compute_player_month_stats(dump: Dict[str, Any], target_uid: str) -> Optional[Dict[str, Any]]:
    """Compute stats for a single player from a dump payload. Runs synchronously (CPU)."""
    e2u = dump.get("entrant_to_uid")
    if not e2u:
        return None

    # Invert: uid -> entrant_id
    uid_to_entrant: Dict[str, int] = {}
    for eid_str, uid in e2u.items():
        if uid == target_uid:
            uid_to_entrant[uid] = int(eid_str)

    target_entrant = uid_to_entrant.get(target_uid)
    if target_entrant is None:
        return None

    # Rebuild Match objects
    raw_matches = dump.get("matches", [])
    matches: List[Match] = []
    for rm in raw_matches:
        matches.append(Match(
            season=int(rm.get("season", 0)),
            id=int(rm.get("table", 0)),
            start=rm.get("start"),
            end=rm.get("end"),
            es=list(rm.get("es", [])),
            winner=rm.get("winner"),
            raw=rm.get("raw", {}),
        ))

    # Gather all entrant IDs
    all_entrant_ids = set()
    for eid_str in e2u:
        all_entrant_ids.add(int(eid_str))
    for m in matches:
        for eid in m.es:
            all_entrant_ids.add(eid)

    points, stats, win_pct, _ = _compute_standings(matches, all_entrant_ids)

    s = stats.get(target_entrant, {"games": 0, "wins": 0, "draws": 0, "losses": 0})
    games = int(s.get("games", 0))

    # Compute rank among non-dropped players (all players in dump, sorted by pts)
    ranked = sorted(
        all_entrant_ids,
        key=lambda eid: (-float(points.get(eid, 0)), -int(stats.get(eid, {}).get("games", 0))),
    )
    # Only rank players who have games
    ranked_with_games = [eid for eid in ranked if int(stats.get(eid, {}).get("games", 0)) > 0]
    rank = None
    for i, eid in enumerate(ranked_with_games, start=1):
        if eid == target_entrant:
            rank = i
            break

    return {
        "month": dump.get("month", ""),
        "pts": float(points.get(target_entrant, 0)),
        "rank": rank,
        "games": games,
        "wins": int(s.get("wins", 0)),
        "losses": int(s.get("losses", 0)),
        "draws": int(s.get("draws", 0)),
        "win_pct": float(win_pct.get(target_entrant, 0.0)),
    }


async def get_player_history(
    target_uid: str,
    bracket_id: str,
    max_months: int = 12,
) -> List[Dict[str, Any]]:
    """Get per-month stats for a player across historical dumps."""
    months = await get_historical_months(bracket_id)
    months = months[-max_months:]

    sem = asyncio.Semaphore(3)

    async def _process_month(month_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        async with sem:
            dump = await reassemble_month_dump(month_info["run_doc_id"])
            if dump is None:
                return None
            return await asyncio.to_thread(compute_player_month_stats, dump, target_uid)

    tasks = [_process_month(m) for m in months]
    results = await asyncio.gather(*tasks)

    history = [r for r in results if r is not None]
    return history


async def get_daily_games(
    bracket_id: str,
    month_str: str,
    entrant_id: int,
) -> Dict[int, Dict[str, int]]:
    """Bucket games by day-of-month for a specific entrant from topdeck_pods."""
    try:
        y, m = month_str.split("-")
        year, month = int(y), int(m)
    except Exception:
        return {}

    daily: Dict[int, Dict[str, int]] = {}

    async for pod in topdeck_pods.find({
        "bracket_id": bracket_id,
        "year": year,
        "month": month,
    }):
        entrants = pod.get("entrants", [])
        entrant_ids_in_pod = []
        for e in entrants:
            if isinstance(e, dict):
                eid = e.get("id") or e.get("uid")
                if eid is not None:
                    entrant_ids_in_pod.append(int(eid))
            elif isinstance(e, (int, float)):
                entrant_ids_in_pod.append(int(e))

        if entrant_id not in entrant_ids_in_pod:
            continue

        start_ts = pod.get("start_ts")
        if start_ts is None:
            continue

        try:
            dt = datetime.fromtimestamp(float(start_ts), tz=timezone.utc).astimezone(LISBON_TZ)
        except Exception:
            continue

        day = dt.day
        if day not in daily:
            daily[day] = {"wins": 0, "losses": 0, "draws": 0}

        winner = pod.get("winner")
        if winner == "_DRAW_":
            daily[day]["draws"] += 1
        elif winner is not None:
            try:
                if int(winner) == entrant_id:
                    daily[day]["wins"] += 1
                else:
                    daily[day]["losses"] += 1
            except (TypeError, ValueError):
                pass

    return daily
