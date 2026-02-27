"""Historical month-dump reader + current-month daily stats for graphs.

Reassembles chunked dumps from MongoDB and computes per-player stats
across multiple months. Also provides current-month daily breakdowns
using the live TopDeck match cache.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from db import topdeck_month_dump_runs, topdeck_month_dump_chunks, topdeck_pods
from topdeck_fetch import (
    Match,
    START_POINTS,
    WAGER_RATE,
    _compute_standings,
    _is_valid_completed_match,
    get_cached_matches,
    _fetch_league_data_full,
)
from utils.dates import LISBON_TZ, current_month_key
from utils.logger import log_sync, log_warn


# ---------------------------------------------------------------------------
# Historical dump helpers
# ---------------------------------------------------------------------------

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
    log_sync(f"[graphs] get_historical_months bracket={bracket_id}: found {len(results)} months")
    return results


async def reassemble_month_dump(run_doc_id) -> Optional[Dict[str, Any]]:
    """Load chunks for a run and reassemble the full payload."""
    chunks = []
    async for chunk in topdeck_month_dump_chunks.find(
        {"run_doc_id": run_doc_id}
    ).sort("chunk_index", 1):
        chunks.append(chunk["data"])

    if not chunks:
        log_warn(f"[graphs] reassemble_month_dump: no chunks for run_doc_id={run_doc_id}")
        return None

    try:
        payload = json.loads("".join(chunks))
        log_sync(f"[graphs] reassemble_month_dump: OK, month={payload.get('month')}, "
                 f"matches={len(payload.get('matches', []))}")
        return payload
    except (json.JSONDecodeError, TypeError) as e:
        log_warn(f"[graphs] reassemble_month_dump: JSON parse error: {e}")
        return None


def compute_player_month_stats(dump: Dict[str, Any], target_uid: str) -> Optional[Dict[str, Any]]:
    """Compute stats for a single player from a dump payload. Runs synchronously (CPU)."""
    e2u = dump.get("entrant_to_uid")
    if not e2u:
        return None

    # Invert: uid -> entrant_id
    target_entrant = None
    for eid_str, uid in e2u.items():
        if uid == target_uid:
            target_entrant = int(eid_str)
            break

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

    # Compute rank among players who have games, sorted by pts
    ranked_with_games = sorted(
        [eid for eid in all_entrant_ids if int(stats.get(eid, {}).get("games", 0)) > 0],
        key=lambda eid: (-float(points.get(eid, 0)), -int(stats.get(eid, {}).get("games", 0))),
    )
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

    log_sync(f"[graphs] get_player_history uid={target_uid}: processing {len(months)} months")

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
    log_sync(f"[graphs] get_player_history uid={target_uid}: got stats for {len(history)}/{len(months)} months")
    return history


# ---------------------------------------------------------------------------
# Current-month daily data (uses live cached matches, NOT topdeck_pods)
# ---------------------------------------------------------------------------

def _normalize_ts(ts) -> Optional[float]:
    """Normalize a timestamp (ms vs s)."""
    if ts is None:
        return None
    try:
        x = float(ts)
    except Exception:
        return None
    return x / 1000.0 if x > 10_000_000_000 else x


def _get_current_month_matches(
    matches: List[Match],
    entrant_to_uid: Dict[int, Any],
) -> Tuple[List[Match], float, float]:
    """Filter matches to the current month window (Lisbon TZ)."""
    mk = current_month_key()
    y, m = mk.split("-")
    year, month = int(y), int(m)

    start_local = datetime(year, month, 1, 0, 0, 0, tzinfo=LISBON_TZ)
    start_ts = start_local.astimezone(timezone.utc).timestamp()

    if month == 12:
        end_local = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=LISBON_TZ)
    else:
        end_local = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=LISBON_TZ)
    end_ts = end_local.astimezone(timezone.utc).timestamp()

    filtered = []
    for match in matches:
        if not _is_valid_completed_match(match):
            continue
        ts = _normalize_ts(match.start)
        if ts is None:
            continue
        if start_ts <= ts < end_ts:
            filtered.append(match)

    filtered.sort(key=lambda m: (_normalize_ts(m.start) or 0.0, m.season, m.id))
    return filtered, start_ts, end_ts


async def get_live_matches(
    bracket_id: str,
    firebase_id_token: Optional[str] = None,
) -> Tuple[List[Match], Dict[int, Any]]:
    """Get matches from cache or fresh fetch."""
    cached = get_cached_matches(bracket_id, firebase_id_token)
    if cached is not None:
        matches, entrant_to_uid, _ = cached
        log_sync(f"[graphs] get_live_matches: using cache, {len(matches)} matches")
        return matches, entrant_to_uid

    log_sync("[graphs] get_live_matches: cache miss, fetching fresh data")
    _, matches, entrant_to_uid, _ = await _fetch_league_data_full(bracket_id, firebase_id_token)
    log_sync(f"[graphs] get_live_matches: fetched {len(matches)} matches")
    return matches, entrant_to_uid


def get_daily_activity_from_matches(
    matches: List[Match],
    target_entrant_id: int,
) -> Dict[int, Dict[str, int]]:
    """Bucket wins/losses/draws by day-of-month from match objects."""
    daily: Dict[int, Dict[str, int]] = {}
    matched_count = 0

    for m in matches:
        if target_entrant_id not in m.es:
            continue

        matched_count += 1
        ts = _normalize_ts(m.start)
        if ts is None:
            continue

        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(LISBON_TZ)
        day = dt.day

        if day not in daily:
            daily[day] = {"wins": 0, "losses": 0, "draws": 0}

        if m.winner == "_DRAW_":
            daily[day]["draws"] += 1
        elif m.winner is not None:
            try:
                if int(m.winner) == target_entrant_id:
                    daily[day]["wins"] += 1
                else:
                    daily[day]["losses"] += 1
            except (TypeError, ValueError):
                pass

    log_sync(f"[graphs] get_daily_activity_from_matches: entrant={target_entrant_id}, "
             f"matched={matched_count} matches, {len(daily)} days with games")
    return daily


def compute_daily_progression(
    month_matches: List[Match],
    all_entrant_ids: set,
    target_entrant_id: int,
) -> List[Dict[str, Any]]:
    """Compute day-by-day cumulative points, win rate, and rank.

    Returns a list of dicts sorted by day, each with:
        {day, pts, rank, games, wins, losses, draws, win_pct}

    Processes matches chronologically, computing standings after each day.
    """
    # Group matches by Lisbon day
    day_to_matches: Dict[int, List[Match]] = defaultdict(list)
    for m in month_matches:
        ts = _normalize_ts(m.start)
        if ts is None:
            continue
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(LISBON_TZ)
        day_to_matches[dt.day].append(m)

    if not day_to_matches:
        log_sync(f"[graphs] compute_daily_progression: no matches bucketed for entrant={target_entrant_id}")
        return []

    # Process day by day, accumulating matches
    sorted_days = sorted(day_to_matches.keys())
    cumulative_matches: List[Match] = []
    progression: List[Dict[str, Any]] = []

    for day in sorted_days:
        cumulative_matches.extend(day_to_matches[day])

        points, stats, win_pct, _ = _compute_standings(cumulative_matches, all_entrant_ids)

        s = stats.get(target_entrant_id, {"games": 0, "wins": 0, "draws": 0, "losses": 0})
        games = int(s.get("games", 0))

        # Rank among players with games
        ranked = sorted(
            [eid for eid in all_entrant_ids if int(stats.get(eid, {}).get("games", 0)) > 0],
            key=lambda eid: (-float(points.get(eid, 0)), -int(stats.get(eid, {}).get("games", 0))),
        )
        rank = None
        for i, eid in enumerate(ranked, start=1):
            if eid == target_entrant_id:
                rank = i
                break

        progression.append({
            "day": day,
            "pts": float(points.get(target_entrant_id, START_POINTS)),
            "rank": rank,
            "games": games,
            "wins": int(s.get("wins", 0)),
            "losses": int(s.get("losses", 0)),
            "draws": int(s.get("draws", 0)),
            "win_pct": float(win_pct.get(target_entrant_id, 0.0)),
        })

    log_sync(f"[graphs] compute_daily_progression: entrant={target_entrant_id}, "
             f"{len(progression)} days, final pts={progression[-1]['pts']:.0f}")
    return progression


# ---------------------------------------------------------------------------
# Legacy pod-based daily (kept as fallback)
# ---------------------------------------------------------------------------

async def get_daily_games(
    bracket_id: str,
    month_str: str,
    entrant_id: int,
) -> Dict[int, Dict[str, int]]:
    """Bucket games by day-of-month for a specific entrant from topdeck_pods.

    NOTE: This only works if a dump has been run for the month. For current
    month data, prefer get_daily_activity_from_matches() with live matches.
    """
    try:
        y, m = month_str.split("-")
        year, month = int(y), int(m)
    except Exception:
        log_warn(f"[graphs] get_daily_games: invalid month_str={month_str!r}")
        return {}

    log_sync(f"[graphs] get_daily_games: querying topdeck_pods "
             f"bracket={bracket_id} year={year} month={month} entrant={entrant_id}")

    daily: Dict[int, Dict[str, int]] = {}
    total_pods = 0
    matched_pods = 0

    async for pod in topdeck_pods.find({
        "bracket_id": bracket_id,
        "year": year,
        "month": month,
    }):
        total_pods += 1
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

        matched_pods += 1
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

    log_sync(f"[graphs] get_daily_games: total_pods={total_pods}, matched={matched_pods}, "
             f"days_with_games={len(daily)}")
    return daily
