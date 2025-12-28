# topdeck_fetch.py
import os
import re
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Iterable,
    Tuple,
    Awaitable,
    Callable,
)

import aiohttp

START_POINTS: float = 1000.0
WAGER_RATE: float = 0.07  # 7% of current points staked each game

# Shared cache TTL (minutes) for all TopDeck league fetches
TOPDECK_CACHE_MINUTES = int(os.getenv("TOPDECK_CACHE_MINUTES", "30"))

FIRESTORE_DOC_URL_TEMPLATE = os.getenv("FIRESTORE_DOC_URL_TEMPLATE", "").strip()


def _get_firestore_doc_url(bracket_id: str) -> str:
    if not FIRESTORE_DOC_URL_TEMPLATE:
        raise RuntimeError(
            "FIRESTORE_DOC_URL_TEMPLATE is not set. "
            "Set it in your environment (e.g. .env) and include {bracket_id}."
        )

    if "{bracket_id}" in FIRESTORE_DOC_URL_TEMPLATE:
        return FIRESTORE_DOC_URL_TEMPLATE.format(bracket_id=bracket_id)

    # fallback: append if user forgot the placeholder
    return FIRESTORE_DOC_URL_TEMPLATE.rstrip("/") + f"/{bracket_id}"


@dataclass
class Match:
    season: int
    id: int
    start: Optional[float]
    end: Optional[float]
    es: List[int]
    winner: Any
    raw: Dict[str, Any]


@dataclass
class PlayerRow:
    entrant_id: int
    uid: Optional[str]
    name: str
    discord: str
    pts: float
    win_pct: float
    ow_pct: float
    games: int
    wins: int
    draws: int
    losses: int
    dropped: bool
    dropped_at: Optional[float]


@dataclass
class InProgressPod:
    season: int
    table: int
    start: Optional[float]
    entrant_ids: List[int]
    entrant_uids: List[Optional[str]]
    entrant_names: List[str]
    entrant_discords: List[str]
    entrant_discords_norm: List[str]


# --------- HTTP + Firestore helpers ---------


async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    token: Optional[str] = None,
) -> Any:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    async with session.get(url, headers=headers or None) as res:
        if res.status != 200:
            text = await res.text()
            snippet = text[:600]
            raise RuntimeError(f"{res.status} {res.reason} for {url}\n{snippet}")
        return await res.json()


def _fs_value_to_py(v: Any) -> Any:
    """Firestore Value -> Python value (same logic as your JS helper)."""
    if v is None:
        return None
    if "nullValue" in v:
        return None
    if "stringValue" in v:
        return v["stringValue"]
    if "booleanValue" in v:
        return bool(v["booleanValue"])
    if "integerValue" in v:
        try:
            return int(v["integerValue"])
        except (ValueError, TypeError):
            return None
    if "doubleValue" in v:
        try:
            return float(v["doubleValue"])
        except (ValueError, TypeError):
            return None
    if "arrayValue" in v:
        vals = v["arrayValue"].get("values", []) or []
        return [_fs_value_to_py(x) for x in vals]
    if "mapValue" in v:
        fields = v["mapValue"].get("fields", {}) or {}
        return {k: _fs_value_to_py(fv) for k, fv in fields.items()}
    return v


def _parse_tournament_fields(doc_json: Dict[str, Any]) -> Dict[str, Any]:
    fields = doc_json.get("fields", {}) or {}
    out: Dict[str, Any] = {}
    for k, fv in fields.items():
        out[k] = _fs_value_to_py(fv)
    return out


def _extract_entrant_to_uid(fields: Dict[str, Any]) -> Dict[int, str]:
    entrant_to_uid: Dict[int, str] = {}
    for k, v in fields.items():
        m = re.match(r"^E(\d+):P1$", k)
        if not m:
            continue
        entrant_id = int(m.group(1))
        if isinstance(v, str) and v:
            entrant_to_uid[entrant_id] = v
    return entrant_to_uid


def _extract_matches_all_seasons(fields: Dict[str, Any]) -> List[Match]:
    matches: List[Match] = []
    for k, v in fields.items():
        m = re.match(r"^S(\d+):T(\d+)$", k)
        if not m:
            continue
        if not isinstance(v, dict):
            continue

        season = int(m.group(1))
        mid = int(m.group(2))

        start = v.get("Start")
        start = float(start) if isinstance(start, (int, float)) else None

        end = v.get("End")
        end = float(end) if isinstance(end, (int, float)) else None

        es_raw = v.get("Es")
        if isinstance(es_raw, list):
            es: List[int] = []
            for x in es_raw:
                if isinstance(x, int):
                    es.append(x)
                elif isinstance(x, float):
                    es.append(int(x))
                elif isinstance(x, str) and x.isdigit():
                    es.append(int(x))
        else:
            es = []

        winner = v.get("Winner", None)

        matches.append(
            Match(
                season=season,
                id=mid,
                start=start,
                end=end,
                es=es,
                winner=winner,
                raw=v,
            )
        )

    matches.sort(key=lambda m: ((m.start or 0.0), m.season, m.id))
    return matches


def _is_valid_completed_match(m: Match) -> bool:
    if not isinstance(m.es, list) or len(m.es) < 2:
        return False
    if isinstance(m.raw, dict) and m.raw.get("Mute") is True:
        return False
    if not isinstance(m.end, (int, float)):
        return False
    if isinstance(m.winner, (int, float)):
        return True
    if m.winner == "_DRAW_":
        return True
    return False


def _is_in_progress_match(m: Match) -> bool:
    if isinstance(m.raw, dict) and m.raw.get("Mute") is True:
        return False

    started = isinstance(m.start, (int, float))
    ended = isinstance(m.end, (int, float))
    has_result = isinstance(m.winner, (int, float)) or m.winner == "_DRAW_"

    return started and not ended and not has_result


def _norm_handle_basic(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower()) if isinstance(s, str) else ""


# def normalize_topdeck_discord(discord_raw: str) -> str:
#     """
#     Take the TopDeck 'discord' field and turn it into something that
#     should match the real Discord username, same idea as the JS helper.
#     """
#     if not discord_raw:
#         return ""
#     s = str(discord_raw).strip()
#     for sep in (" ", "("):
#         if sep in s:
#             s = s.split(sep, 1)[0]
#     if "#" in s:
#         s = s.split("#", 1)[0]
#     return _norm_handle_basic(s)

def normalize_topdeck_discord(discord_raw: str) -> str:
    if not discord_raw:
        return ""
    s = str(discord_raw).strip()

    # handle "@name"
    if s.startswith("@"):
        s = s[1:]

    # keep only first token (same as before)
    for sep in (" ", "("):
        if sep in s:
            s = s.split(sep, 1)[0]

    # IMPORTANT: do NOT strip "#1234"
    # letting _norm_handle_basic remove the "#" but keep digits:
    # "name#1234" -> "name1234"
    return _norm_handle_basic(s)



def _compute_standings(
    matches: Iterable[Match],
    entrant_ids: Iterable[int],
):
    points: Dict[int, float] = {}
    stats: Dict[int, Dict[str, Any]] = {}

    # init players
    for eid in entrant_ids:
        points[eid] = float(START_POINTS)
        stats[eid] = {
            "games": 0,
            "wins": 0,
            "draws": 0,
            "losses": 0,
            "opponents": set(),  # type: ignore[assignment]
        }

    for m in matches:
        if not _is_valid_completed_match(m):
            continue

        # ensure everyone is registered
        for eid in m.es:
            points.setdefault(eid, float(START_POINTS))
            stats.setdefault(
                eid,
                {
                    "games": 0,
                    "wins": 0,
                    "draws": 0,
                    "losses": 0,
                    "opponents": set(),
                },
            )

        # opponent tracking
        for eid in m.es:
            s = stats[eid]
            opps = s["opponents"]
            for opp in m.es:
                if opp != eid:
                    opps.add(opp)

        # float staking
        stakes = [{"eid": eid, "stake": points[eid] * WAGER_RATE} for eid in m.es]
        pot = sum(x["stake"] for x in stakes)

        for s in stakes:
            points[s["eid"]] -= s["stake"]

        if m.winner == "_DRAW_":
            share = pot / len(m.es)
            for eid in m.es:
                points[eid] += share
                st = stats[eid]
                st["games"] += 1
                st["draws"] += 1
        else:
            try:
                winner_eid = int(m.winner)
            except (TypeError, ValueError):
                continue

            points[winner_eid] = points.get(winner_eid, START_POINTS) + pot

            for eid in m.es:
                st = stats[eid]
                st["games"] += 1
                if eid == winner_eid:
                    st["wins"] += 1
                else:
                    st["losses"] += 1

    # win% = wins / games
    win_pct: Dict[int, float] = {}
    for eid, st in stats.items():
        g = st["games"]
        win_pct[eid] = (st["wins"] / g) if g else 0.0

    # OW% = avg win% of unique opponents
    ow_pct: Dict[int, float] = {}
    for eid, st in stats.items():
        opps = list(st["opponents"])
        if not opps:
            ow_pct[eid] = 0.0
            continue
        avg = sum(win_pct.get(opp, 0.0) for opp in opps) / len(opps)
        ow_pct[eid] = avg

    return points, stats, win_pct, ow_pct


def _extract_drop_state(fields: Dict[str, Any]):
    latest_drop: Dict[int, float] = {}
    latest_undrop: Dict[int, float] = {}

    for k, v in fields.items():
        m = re.match(r"^E(\d+):D:Drop(\d+)$", k)
        if m:
            entrant_id = int(m.group(1))
            try:
                ts = float(v)
            except (TypeError, ValueError):
                continue
            prev = latest_drop.get(entrant_id)
            if prev is None or ts > prev:
                latest_drop[entrant_id] = ts
            continue

        m = re.match(r"^E(\d+):D:Undrop(\d+)$", k)
        if m:
            entrant_id = int(m.group(1))
            try:
                ts = float(v)
            except (TypeError, ValueError):
                continue
            prev = latest_undrop.get(entrant_id)
            if prev is None or ts > prev:
                latest_undrop[entrant_id] = ts

    is_dropped: Dict[int, bool] = {}
    dropped_at: Dict[int, float] = {}

    all_ids = set(list(latest_drop.keys()) + list(latest_undrop.keys()))
    for eid in all_ids:
        d = latest_drop.get(eid)
        u = latest_undrop.get(eid)
        currently_dropped = (d is not None) and (u is None or d > u)
        is_dropped[eid] = currently_dropped
        if currently_dropped and d is not None:
            dropped_at[eid] = d

    return {
        "is_dropped": is_dropped,
        "dropped_at": dropped_at,
        "latest_drop": latest_drop,
        "latest_undrop": latest_undrop,
    }


# --------- Main league fetch ---------


async def get_league_rows(
    bracket_id: str,
    firebase_id_token: Optional[str] = None,
) -> List[PlayerRow]:
    """
    Fetch players + tournament doc and compute league rows,
    same logic as the JS script.
    """
    if not bracket_id:
        raise RuntimeError("bracket_id is required")

    players_url = f"https://topdeck.gg/PublicPData/{bracket_id}"
    doc_url = _get_firestore_doc_url(bracket_id)

    async with aiohttp.ClientSession() as session:
        players = await _fetch_json(session, players_url, token=None)
        doc = await _fetch_json(session, doc_url, token=firebase_id_token)

    fields = _parse_tournament_fields(doc)
    drop_state = _extract_drop_state(fields)
    entrant_to_uid = _extract_entrant_to_uid(fields)
    matches = _extract_matches_all_seasons(fields)

    entrant_ids = set(entrant_to_uid.keys())
    for m in matches:
        for eid in m.es:
            entrant_ids.add(eid)

    # ✅ CPU-heavy: run off the event loop to avoid freezing slash commands
    points, stats, win_pct, ow_pct = await asyncio.to_thread(
        _compute_standings, matches, entrant_ids
    )

    rows: List[PlayerRow] = []

    for eid in entrant_ids:
        uid = entrant_to_uid.get(eid)
        p = None

        if uid is not None:
            if isinstance(players, dict):
                p = players.get(uid)
            elif isinstance(players, list):
                try:
                    idx = int(uid)
                    if 0 <= idx < len(players):
                        p = players[idx]
                except (TypeError, ValueError):
                    p = None

        s = stats.get(eid, {"games": 0, "wins": 0, "draws": 0, "losses": 0})
        dropped = bool(drop_state["is_dropped"].get(eid, False))
        dropped_at = drop_state["dropped_at"].get(eid)

        rows.append(
            PlayerRow(
                entrant_id=eid,
                uid=uid,
                name=(p.get("name") if isinstance(p, dict) else None) or uid or "(unknown)",
                discord=(p.get("discord") if isinstance(p, dict) else "") or "",
                pts=float(points.get(eid, START_POINTS)),
                win_pct=float(win_pct.get(eid, 0.0)),
                ow_pct=float(ow_pct.get(eid, 0.0)),
                games=int(s.get("games", 0)),
                wins=int(s.get("wins", 0)),
                draws=int(s.get("draws", 0)),
                losses=int(s.get("losses", 0)),
                dropped=dropped,
                dropped_at=float(dropped_at) if dropped_at is not None else None,
            )
        )

    # same ordering as in JS: active first, then pts, OW%, win%
    rows.sort(
        key=lambda r: (
            r.dropped,       # False < True → active first
            -r.pts,
            -r.ow_pct,
            -r.win_pct,
        )
    )

    return rows


async def get_in_progress_pods(
    bracket_id: str,
    firebase_id_token: Optional[str] = None,
) -> List[InProgressPod]:
    """
    Fetch TopDeck data for this bracket and return only pods that are
    currently in progress (started, not ended, no winner / _DRAW_).

    This is the Python equivalent of your JS listInProgressPods().
    """
    if not bracket_id:
        raise RuntimeError("bracket_id is required")

    players_url = f"https://topdeck.gg/PublicPData/{bracket_id}"
    doc_url = _get_firestore_doc_url(bracket_id)

    async with aiohttp.ClientSession() as session:
        players = await _fetch_json(session, players_url, token=None)
        doc = await _fetch_json(session, doc_url, token=firebase_id_token)

    fields = _parse_tournament_fields(doc)
    entrant_to_uid = _extract_entrant_to_uid(fields)
    matches = _extract_matches_all_seasons(fields)

    # Normalize players into uid -> dict with name/discord
    player_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(players, dict):
        for uid, pdata in players.items():
            if isinstance(pdata, dict):
                player_map[str(uid)] = pdata
    elif isinstance(players, list):
        for idx, pdata in enumerate(players):
            if isinstance(pdata, dict):
                player_map[str(idx)] = pdata

    pods: List[InProgressPod] = []

    for m in matches:
        if not _is_in_progress_match(m):
            continue

        entrant_ids = list(m.es or [])
        uids: List[Optional[str]] = []
        names: List[str] = []
        discords: List[str] = []
        discords_norm: List[str] = []

        for eid in entrant_ids:
            uid = entrant_to_uid.get(eid)
            uids.append(uid)

            pdata = player_map.get(str(uid)) or {}
            name = (pdata.get("name") if isinstance(pdata, dict) else None) or uid or f"E{eid}"
            names.append(str(name))

            d_raw = (pdata.get("discord") if isinstance(pdata, dict) else "") or ""
            discords.append(str(d_raw))
            discords_norm.append(normalize_topdeck_discord(str(d_raw)))

        pods.append(
            InProgressPod(
                season=m.season,
                table=m.id,
                start=float(m.start) if isinstance(m.start, (int, float)) else None,
                entrant_ids=entrant_ids,
                entrant_uids=uids,
                entrant_names=names,
                entrant_discords=discords,
                entrant_discords_norm=discords_norm,
            )
        )

    return pods


# --------- Shared cached wrapper ---------


# --------- Derived caches (computed from cached rows) ---------

# handle -> (pts, games) choosing the *best* row per handle
_TOPDECK_HANDLE_BEST_CACHE: Dict[Tuple[str, str], Tuple[Dict[str, Tuple[float, int]], datetime]] = {}

_TOPDECK_CACHE: Dict[Tuple[str, str], Tuple[List[PlayerRow], datetime]] = {}
_TOPDECK_CACHE_TTL = timedelta(minutes=TOPDECK_CACHE_MINUTES)

# Optional async callback run whenever we do a *real* TopDeck fetch.
_TOPDECK_CACHE_MISS_HOOK: Optional[Callable[[], Awaitable[None]]] = None


def register_topdeck_cache_miss_hook(hook: Callable[[], Awaitable[None]]) -> None:
    """
    Register an async callback that will be awaited every time
    get_league_rows_cached performs a real TopDeck fetch
    (cache miss or expired).

    Used by topdeck_online_sync to auto-refresh online stats.
    """
    global _TOPDECK_CACHE_MISS_HOOK
    _TOPDECK_CACHE_MISS_HOOK = hook


async def get_league_rows_cached(
    bracket_id: str,
    firebase_id_token: Optional[str] = None,
    *,
    force_refresh: bool = False,
) -> Tuple[List[PlayerRow], datetime]:
    """
    Cached wrapper around get_league_rows.

    - Respects TOPDECK_CACHE_MINUTES env var.
    - Cache key is (bracket_id, firebase_id_token or '').
    - Returns (rows, fetched_at).
    """
    if not bracket_id:
        raise RuntimeError("bracket_id is required")

    now = datetime.now(timezone.utc)
    key = (bracket_id, firebase_id_token or "")

    if not force_refresh and key in _TOPDECK_CACHE:
        rows, cached_at = _TOPDECK_CACHE[key]
        if now - cached_at < _TOPDECK_CACHE_TTL:
            return rows, cached_at

    # ✅ Cache miss: DO NOT block the event loop awaiting a hook
    if _TOPDECK_CACHE_MISS_HOOK is not None:
        try:
            asyncio.create_task(_TOPDECK_CACHE_MISS_HOOK())
        except Exception as e:
            print(
                "[topdeck] Warning: cache-miss hook scheduling failed "
                f"{type(e).__name__}: {e}"
            )

    print(
        f"[topdeck] Fetching fresh TopDeck data from API for bracket "
        f"{bracket_id!r} (cache miss or expired)."
    )

    rows = await get_league_rows(bracket_id, firebase_id_token)
    _TOPDECK_CACHE[key] = (rows, now)
    # invalidate derived caches for this key
    _TOPDECK_HANDLE_BEST_CACHE.pop(key, None)
    return rows, now


# --------- Derived helpers ---------


def build_handle_to_best(rows: List[PlayerRow]) -> Dict[str, Tuple[float, int]]:
    """Build a mapping: normalized discord handle -> (points, games).

    If a handle appears multiple times, prefer the row with:
      1) higher games, then
      2) higher points.
    """
    out: Dict[str, Tuple[float, int]] = {}
    for row in rows:
        handle = normalize_topdeck_discord(getattr(row, 'discord', None))
        if not handle:
            continue
        pts = float(getattr(row, 'pts', 0.0) or 0.0)
        games = int(getattr(row, 'games', 0) or 0)
        existing = out.get(handle)
        if existing is None or games > existing[1] or (games == existing[1] and pts > existing[0]):
            out[handle] = (pts, games)
    return out


async def get_handle_to_best_cached(
    bracket_id: str,
    firebase_id_token: Optional[str] = None,
    *,
    force_refresh: bool = False,
) -> Tuple[Dict[str, Tuple[float, int]], datetime]:
    """Return (handle_to_best, fetched_at) using the shared TopDeck cache.

    The mapping is cached per (bracket_id, token) and invalidated whenever
    get_league_rows_cached fetches fresh data for the same key.
    """
    rows, fetched_at = await get_league_rows_cached(
        bracket_id,
        firebase_id_token,
        force_refresh=force_refresh,
    )

    key = (bracket_id, firebase_id_token or '')
    cached = _TOPDECK_HANDLE_BEST_CACHE.get(key)
    if cached is not None:
        mapping, cached_at = cached
        # Only reuse if it was built from the same rows snapshot time
        if cached_at == fetched_at:
            return mapping, fetched_at

    mapping = build_handle_to_best(rows)
    _TOPDECK_HANDLE_BEST_CACHE[key] = (mapping, fetched_at)
    return mapping, fetched_at
