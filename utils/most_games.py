# utils/most_games.py
"""Most Games raffle selection (no I/O). Shared by /mostgames and /stats.

Candidates are the Top 5 by games among non-dropped players with at least
MOST_GAMES_MIN_POINTS points. The points filter applies first, so a sub-800
grinder is skipped and the next qualified player moves up. Ties on games break
by points. Finalists forfeit later (at the draw), not here.

Mirrors eclDashboard/lib/most-games.ts; keep the two in step.
Truth table:  python verify_most_games.py
"""
from __future__ import annotations

from typing import List, Tuple

MOST_GAMES_TOP_N = 5
MOST_GAMES_MIN_POINTS = 800


def _pts(r) -> float:
    return float(getattr(r, "pts", 0.0) or 0.0)


def _games(r) -> int:
    return int(getattr(r, "games", 0) or 0)


def _key(r) -> str:
    uid = (getattr(r, "uid", None) or "").strip()
    return uid if uid else (getattr(r, "name", "") or "").strip().lower()


def meets_min_points(r) -> bool:
    return _pts(r) >= MOST_GAMES_MIN_POINTS


def most_games_top(rows: List, top_n: int = MOST_GAMES_TOP_N) -> List:
    """Raffle candidates: non-dropped, >= min points, sorted by games desc then pts desc."""
    eligible = [r for r in rows if not getattr(r, "dropped", False) and meets_min_points(r)]
    return sorted(eligible, key=lambda r: (-_games(r), -_pts(r)))[:top_n]


def contender_status(rows: List, target, top_n: int = MOST_GAMES_TOP_N) -> Tuple[str, int]:
    """Where `target` stands for the raffle.

    Returns one of:
      ("dropped", 0)       — dropped players are never eligible
      ("below_points", 0)  — under MOST_GAMES_MIN_POINTS, whatever their games
      ("no_top", 0)        — nobody qualifies yet
      ("in", position)     — 1-based position in the Top N
      ("out", games)       — games needed to reach the cutoff (0 = tied, out on points)
    """
    if getattr(target, "dropped", False):
        return ("dropped", 0)
    if not meets_min_points(target):
        return ("below_points", 0)

    top = most_games_top(rows, top_n)
    if not top:
        return ("no_top", 0)

    tkey = _key(target)
    for i, r in enumerate(top, start=1):
        if tkey and _key(r) == tkey:
            return ("in", i)

    return ("out", max(0, _games(top[-1]) - _games(target)))
