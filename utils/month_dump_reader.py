"""Historical month-dump reader + current-month daily stats for graphs.

Reassembles chunked dumps from MongoDB and computes per-player stats
across multiple months. Also provides current-month daily breakdowns
using the live TopDeck match cache.

Caching strategy (all module-level, bounded):
  - _DUMP_CACHE: reassembled JSON payloads, keyed by (bracket_id, month, run_id),
    max 12 entries, 1-hour TTL. Historical dumps are immutable once saved.
  - _STANDINGS_CACHE: computed standings tuples, keyed by (bracket_id, month),
    max 12 entries, 1-hour TTL.
  - _E2U_MODULE_CACHE: entrant_to_uid API results, keyed by bracket_id,
    2-hour TTL. No lock needed (single event loop).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import OrderedDict, defaultdict
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
    _get_shared_session,
    _fetch_json,
    _get_firestore_doc_url,
    _parse_tournament_fields,
    _extract_entrant_to_uid,
)
from utils.dates import LISBON_TZ, current_month_key
from utils.logger import log_sync, log_warn


# ---------------------------------------------------------------------------
# Module-level caches
# ---------------------------------------------------------------------------

_DUMP_CACHE_MAX = 12
_DUMP_CACHE_TTL = 3600  # 1 hour
_DUMP_CACHE: OrderedDict[Tuple[str, str, str], Tuple[Dict[str, Any], float]] = OrderedDict()
_DUMP_CACHE_LOCK = asyncio.Lock()

_STANDINGS_CACHE_MAX = 12
_STANDINGS_CACHE_TTL = 3600  # 1 hour
# Value: (points, stats_without_opponents, win_pct, month_str, all_entrant_ids, timestamp)
_STANDINGS_CACHE: OrderedDict[
    Tuple[str, str],
    Tuple[Dict[int, float], Dict[int, Dict[str, int]], Dict[int, float], str, set, float],
] = OrderedDict()
_STANDINGS_CACHE_LOCK = asyncio.Lock()

_E2U_CACHE_TTL = 7200  # 2 hours
_E2U_MODULE_CACHE: Dict[str, Tuple[Dict[int, str], float]] = {}


# ---------------------------------------------------------------------------
# Historical dump helpers
# ---------------------------------------------------------------------------

async def get_historical_months(
    bracket_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return distinct (bracket_id, month) pairs with latest run, sorted ascending.

    If bracket_id is None, searches across ALL brackets (needed because the
    league bracket_id changes each month/season).

    Tries topdeck_month_dump_runs first. If that collection is empty,
    falls back to discovering months directly from topdeck_month_dump_chunks.
    """
    match_filter = {"bracket_id": bracket_id} if bracket_id else {}

    # --- Try runs collection first (preferred: has metadata) ---
    runs_count = await topdeck_month_dump_runs.count_documents(match_filter)
    if runs_count > 0:
        pipeline = [
            *([ {"$match": match_filter} ] if match_filter else []),
            {"$sort": {"created_at": -1}},
            {
                "$group": {
                    "_id": {"bracket_id": "$bracket_id", "month": "$month"},
                    "latest_run_id": {"$first": "$_id"},
                    "run_id": {"$first": "$run_id"},
                    "created_at": {"$first": "$created_at"},
                    "chunk_count": {"$first": "$chunk_count"},
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
                "run_id": doc.get("run_id"),
                "chunk_count": doc.get("chunk_count"),
                "source": "runs",
            })
        log_sync(f"[graphs] get_historical_months bracket={bracket_id!r}: "
                 f"found {len(results)} months from runs collection")
        return results

    # --- Fallback: discover months from chunks collection directly ---
    chunks_count = await topdeck_month_dump_chunks.count_documents(match_filter)
    if chunks_count == 0:
        sample = await topdeck_month_dump_chunks.find_one({}, {"bracket_id": 1})
        sample_bid = sample.get("bracket_id", "???") if sample else "(empty collection)"
        log_warn(f"[graphs] get_historical_months: 0 docs in both runs and chunks "
                 f"for bracket={bracket_id!r} (sample chunk bracket_id={sample_bid!r})")
        return []

    log_sync(f"[graphs] get_historical_months: runs empty, falling back to chunks "
             f"({chunks_count} chunk docs for bracket={bracket_id!r})")

    pipeline = [
        *([ {"$match": match_filter} ] if match_filter else []),
        {"$sort": {"created_at": -1}},
        {
            "$group": {
                "_id": {"bracket_id": "$bracket_id", "month": "$month", "run_id": "$run_id"},
                "created_at": {"$first": "$created_at"},
            }
        },
        {"$sort": {"created_at": -1}},
        {
            "$group": {
                "_id": {"bracket_id": "$_id.bracket_id", "month": "$_id.month"},
                "run_id": {"$first": "$_id.run_id"},
                "created_at": {"$first": "$created_at"},
            }
        },
        {"$sort": {"_id.month": 1}},
    ]
    results = []
    async for doc in topdeck_month_dump_chunks.aggregate(pipeline):
        results.append({
            "bracket_id": doc["_id"]["bracket_id"],
            "month": doc["_id"]["month"],
            "run_id": doc.get("run_id"),
            "run_doc_id": None,  # no runs doc available
            "source": "chunks",
        })
    log_sync(f"[graphs] get_historical_months bracket={bracket_id!r}: "
             f"found {len(results)} months from chunks fallback")
    return results


async def reassemble_month_dump(month_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Load chunks for a month and reassemble the full payload.

    Uses a module-level LRU cache (max 12 entries, 1-hour TTL) since
    historical dumps are immutable once saved.

    Supports both:
      - run_doc_id lookup (when discovered from runs collection)
      - bracket_id + month + run_id lookup (when discovered from chunks)
    """
    run_doc_id = month_info.get("run_doc_id")
    run_id = month_info.get("run_id")
    bracket_id = month_info.get("bracket_id")
    month = month_info.get("month")
    expected_chunks = month_info.get("chunk_count")

    # Build cache key — use bracket_id + month + run_id for stable keying
    cache_key = (bracket_id or "", month or "", str(run_id or run_doc_id or ""))

    # Check cache
    async with _DUMP_CACHE_LOCK:
        if cache_key in _DUMP_CACHE:
            payload, cached_at = _DUMP_CACHE[cache_key]
            if time.monotonic() - cached_at < _DUMP_CACHE_TTL:
                _DUMP_CACHE.move_to_end(cache_key)
                log_sync(f"[graphs] reassemble_month_dump: cache HIT for "
                         f"bracket={bracket_id} month={month}")
                return payload
            else:
                del _DUMP_CACHE[cache_key]

    # Build query
    if run_doc_id is not None:
        query = {"run_doc_id": run_doc_id}
        desc = f"run_doc_id={run_doc_id}"
    elif run_id and bracket_id and month:
        query = {"bracket_id": bracket_id, "month": month, "run_id": run_id}
        desc = f"bracket={bracket_id} month={month} run_id={run_id}"
    elif bracket_id and month:
        # Last resort: get latest chunks for this bracket+month
        query = {"bracket_id": bracket_id, "month": month}
        desc = f"bracket={bracket_id} month={month} (latest)"
    else:
        log_warn(f"[graphs] reassemble_month_dump: insufficient info: {month_info}")
        return None

    chunks = []
    async for chunk in topdeck_month_dump_chunks.find(query).sort("chunk_index", 1):
        chunks.append(chunk["data"])

    if not chunks:
        log_warn(f"[graphs] reassemble_month_dump: no chunks for {desc}")
        return None

    if expected_chunks is not None and len(chunks) != expected_chunks:
        log_warn(f"[graphs] reassemble_month_dump: chunk count mismatch for {desc}: "
                 f"expected={expected_chunks}, actual={len(chunks)}")

    try:
        payload = json.loads("".join(chunks))
        log_sync(f"[graphs] reassemble_month_dump: OK ({desc}), month={payload.get('month')}, "
                 f"matches={len(payload.get('matches', []))}, "
                 f"has_entrant_to_uid={'entrant_to_uid' in payload}")
    except (json.JSONDecodeError, TypeError) as e:
        log_warn(f"[graphs] reassemble_month_dump: JSON parse error for {desc}: {e}")
        return None

    # Store in cache
    async with _DUMP_CACHE_LOCK:
        _DUMP_CACHE[cache_key] = (payload, time.monotonic())
        _DUMP_CACHE.move_to_end(cache_key)
        while len(_DUMP_CACHE) > _DUMP_CACHE_MAX:
            _DUMP_CACHE.popitem(last=False)

    return payload


def _rebuild_matches_from_dump(dump: Dict[str, Any]) -> List[Match]:
    """Rebuild Match objects from a dump payload."""
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
    return matches


def _gather_all_entrant_ids(e2u: Dict[str, str], matches: List[Match]) -> set:
    """Gather all entrant IDs from entrant_to_uid mapping and matches."""
    all_entrant_ids = set()
    for eid_str in e2u:
        all_entrant_ids.add(int(eid_str))
    for m in matches:
        for eid in m.es:
            all_entrant_ids.add(eid)
    return all_entrant_ids


def _compute_standings_cacheable(
    matches: List[Match], all_entrant_ids: set
) -> Tuple[Dict[int, float], Dict[int, Dict[str, int]], Dict[int, float]]:
    """Compute standings and return a cache-friendly tuple (no opponent sets)."""
    points, stats, win_pct, _ = _compute_standings(matches, all_entrant_ids)
    # Strip opponent sets to save memory in cache
    stats_clean = {}
    for eid, s in stats.items():
        stats_clean[eid] = {
            "games": s["games"],
            "wins": s["wins"],
            "draws": s["draws"],
            "losses": s["losses"],
        }
    return points, stats_clean, win_pct


def _extract_player_stats(
    points: Dict[int, float],
    stats: Dict[int, Dict[str, int]],
    win_pct: Dict[int, float],
    all_entrant_ids: set,
    target_entrant: int,
    month_str: str,
) -> Dict[str, Any]:
    """Extract a single player's stats from pre-computed standings."""
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
        "month": month_str,
        "pts": float(points.get(target_entrant, 0)),
        "rank": rank,
        "games": games,
        "wins": int(s.get("wins", 0)),
        "losses": int(s.get("losses", 0)),
        "draws": int(s.get("draws", 0)),
        "win_pct": float(win_pct.get(target_entrant, 0.0)),
    }


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

    matches = _rebuild_matches_from_dump(dump)
    all_entrant_ids = _gather_all_entrant_ids(e2u, matches)
    points, stats, win_pct = _compute_standings_cacheable(matches, all_entrant_ids)

    return _extract_player_stats(
        points, stats, win_pct, all_entrant_ids,
        target_entrant, dump.get("month", ""),
    )


def _compute_player_month_stats_from_cached(
    points: Dict[int, float],
    stats: Dict[int, Dict[str, int]],
    win_pct: Dict[int, float],
    all_entrant_ids: set,
    month_str: str,
    e2u: Dict[str, str],
    target_uid: str,
) -> Optional[Dict[str, Any]]:
    """Extract player stats from pre-computed (cached) standings."""
    target_entrant = None
    for eid_str, uid in e2u.items():
        if uid == target_uid:
            target_entrant = int(eid_str)
            break

    if target_entrant is None:
        return None

    return _extract_player_stats(
        points, stats, win_pct, all_entrant_ids, target_entrant, month_str,
    )


async def _fetch_entrant_to_uid_for_bracket(
    bracket_id: str,
    firebase_id_token: Optional[str] = None,
) -> Dict[int, str]:
    """Fetch entrant_to_uid mapping from Firestore for a given bracket.

    Used as a fallback when historical dumps are missing the mapping.
    """
    try:
        doc_url = _get_firestore_doc_url(bracket_id)
        session = _get_shared_session()
        doc = await _fetch_json(session, doc_url, token=firebase_id_token)
        fields = _parse_tournament_fields(doc)
        e2u = _extract_entrant_to_uid(fields)
        log_sync(f"[graphs] _fetch_entrant_to_uid_for_bracket: bracket={bracket_id}, "
                 f"got {len(e2u)} mappings")
        return e2u
    except Exception as e:
        log_warn(f"[graphs] _fetch_entrant_to_uid_for_bracket: bracket={bracket_id}: "
                 f"{type(e).__name__}: {e}")
        return {}


def _get_cached_e2u(bracket_id: str) -> Optional[Dict[int, str]]:
    """Check module-level e2u cache. Returns mapping or None if miss/expired."""
    entry = _E2U_MODULE_CACHE.get(bracket_id)
    if entry is None:
        return None
    mapping, cached_at = entry
    if time.monotonic() - cached_at >= _E2U_CACHE_TTL:
        del _E2U_MODULE_CACHE[bracket_id]
        return None
    return mapping


def _set_cached_e2u(bracket_id: str, mapping: Dict[int, str]) -> None:
    """Store e2u mapping in module-level cache."""
    _E2U_MODULE_CACHE[bracket_id] = (mapping, time.monotonic())


async def get_player_history(
    target_uid: str,
    bracket_id: Optional[str] = None,
    max_months: int = 12,
    firebase_id_token: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get per-month stats for a player across historical dumps.

    If bracket_id is None, searches across all brackets (the league
    bracket_id changes each season/month).

    Returns a list of per-month stat dicts. Also logs a summary if any
    months failed to load.
    """
    months = await get_historical_months(bracket_id)
    months = months[-max_months:]

    log_sync(f"[graphs] get_player_history uid={target_uid} bracket={bracket_id!r}: "
             f"processing {len(months)} months")

    sem = asyncio.Semaphore(3)
    failed_months: List[str] = []

    async def _process_month(month_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        async with sem:
            m_bracket = month_info.get("bracket_id", "")
            m_month = month_info.get("month", "")

            dump = await reassemble_month_dump(month_info)
            if dump is None:
                failed_months.append(m_month)
                return None

            # If dump is missing entrant_to_uid, try module cache then API
            if not dump.get("entrant_to_uid"):
                bid = dump.get("bracket_id") or m_bracket
                if bid:
                    cached_e2u = _get_cached_e2u(bid)
                    if cached_e2u is not None:
                        e2u = cached_e2u
                        log_sync(f"[graphs] e2u module cache HIT for bracket={bid}")
                    else:
                        e2u = await _fetch_entrant_to_uid_for_bracket(
                            bid, firebase_id_token
                        )
                        if e2u:
                            _set_cached_e2u(bid, e2u)
                    if e2u:
                        dump["entrant_to_uid"] = {str(k): v for k, v in e2u.items()}

            e2u = dump.get("entrant_to_uid")
            if not e2u:
                failed_months.append(m_month)
                return None

            # Check standings cache
            standings_key = (m_bracket, m_month)
            async with _STANDINGS_CACHE_LOCK:
                if standings_key in _STANDINGS_CACHE:
                    cached = _STANDINGS_CACHE[standings_key]
                    pts, sts, wpct, month_str, all_eids, cached_at = cached
                    if time.monotonic() - cached_at < _STANDINGS_CACHE_TTL:
                        _STANDINGS_CACHE.move_to_end(standings_key)
                        log_sync(f"[graphs] standings cache HIT for {standings_key}")
                        return _compute_player_month_stats_from_cached(
                            pts, sts, wpct, all_eids, month_str, e2u, target_uid,
                        )
                    else:
                        del _STANDINGS_CACHE[standings_key]

            # Compute standings (CPU-heavy)
            def _compute():
                matches = _rebuild_matches_from_dump(dump)
                all_eids = _gather_all_entrant_ids(e2u, matches)
                pts, sts, wpct = _compute_standings_cacheable(matches, all_eids)
                return pts, sts, wpct, all_eids

            pts, sts, wpct, all_eids = await asyncio.to_thread(_compute)

            # Cache standings
            month_str = dump.get("month", m_month)
            async with _STANDINGS_CACHE_LOCK:
                _STANDINGS_CACHE[standings_key] = (
                    pts, sts, wpct, month_str, all_eids, time.monotonic(),
                )
                _STANDINGS_CACHE.move_to_end(standings_key)
                while len(_STANDINGS_CACHE) > _STANDINGS_CACHE_MAX:
                    _STANDINGS_CACHE.popitem(last=False)

            return _compute_player_month_stats_from_cached(
                pts, sts, wpct, all_eids, month_str, e2u, target_uid,
            )

    tasks = [_process_month(m) for m in months]
    results = await asyncio.gather(*tasks)

    history = [r for r in results if r is not None]

    if failed_months:
        log_warn(f"[graphs] get_player_history uid={target_uid}: "
                 f"{len(failed_months)} months failed to load: {failed_months}")

    log_sync(f"[graphs] get_player_history uid={target_uid}: "
             f"got stats for {len(history)}/{len(months)} months"
             f"{f' ({len(failed_months)} failed)' if failed_months else ''}")
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

    Uses incremental computation: a single pass through matches accumulating
    running totals, with snapshots at the end of each day. This is O(matches)
    instead of the previous O(days * matches) approach.
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

    # Initialize running state for all entrants
    points: Dict[int, float] = {eid: float(START_POINTS) for eid in all_entrant_ids}
    stats: Dict[int, Dict[str, int]] = {
        eid: {"games": 0, "wins": 0, "draws": 0, "losses": 0}
        for eid in all_entrant_ids
    }

    sorted_days = sorted(day_to_matches.keys())
    progression: List[Dict[str, Any]] = []

    for day in sorted_days:
        # Process each match in this day incrementally
        for m in day_to_matches[day]:
            if not _is_valid_completed_match(m):
                continue

            # Ensure all match entrants are registered
            for eid in m.es:
                if eid not in points:
                    points[eid] = float(START_POINTS)
                    stats[eid] = {"games": 0, "wins": 0, "draws": 0, "losses": 0}

            # Float staking (same logic as _compute_standings)
            stakes = [{"eid": eid, "stake": points[eid] * WAGER_RATE} for eid in m.es]
            pot = sum(x["stake"] for x in stakes)

            for s in stakes:
                points[s["eid"]] -= s["stake"]

            if m.winner == "_DRAW_":
                share = pot / len(m.es)
                for eid in m.es:
                    points[eid] += share
                    stats[eid]["games"] += 1
                    stats[eid]["draws"] += 1
            else:
                try:
                    winner_eid = int(m.winner)
                except (TypeError, ValueError):
                    # Reverse the stakes since we're skipping this match
                    for s in stakes:
                        points[s["eid"]] += s["stake"]
                    continue

                points[winner_eid] = points.get(winner_eid, START_POINTS) + pot

                for eid in m.es:
                    stats[eid]["games"] += 1
                    if eid == winner_eid:
                        stats[eid]["wins"] += 1
                    else:
                        stats[eid]["losses"] += 1

        # Snapshot at end of day
        target_stats = stats.get(target_entrant_id, {"games": 0, "wins": 0, "draws": 0, "losses": 0})
        games = target_stats["games"]
        win_pct = (target_stats["wins"] / games) if games > 0 else 0.0

        # Rank ALL entrants (including 0-game) to match TopDeck.gg
        ranked = sorted(
            points.keys(),
            key=lambda eid: (-points.get(eid, 0), -stats.get(eid, {}).get("games", 0)),
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
            "wins": target_stats["wins"],
            "losses": target_stats["losses"],
            "draws": target_stats["draws"],
            "win_pct": win_pct,
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
