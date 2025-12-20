# topdeck_fetch.py
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Iterable

import aiohttp

START_POINTS: float = 1000.0
WAGER_RATE: float = 0.07  # 7% of current points staked each game

# Firestore URL template (env)
# Example in .env:
# FIRESTORE_DOC_URL_TEMPLATE="https://firestore.googleapis.com/v1/projects/eminence-1b40b/databases/(default)/documents/tournaments/{bracket_id}"
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
                # these come back as ints usually, but just in case they're strings
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
                # malformed winner, ignore match
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

    points, stats, win_pct, ow_pct = _compute_standings(matches, entrant_ids)

    rows: List[PlayerRow] = []

    for eid in entrant_ids:
        uid = entrant_to_uid.get(eid)
        p = None

        if uid is not None:
            if isinstance(players, dict):
                p = players.get(uid)
            elif isinstance(players, list):
                # uid may be numeric string; try index
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
