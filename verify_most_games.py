"""Truth-table check for the Most Games raffle selection.

Raffle candidates are the Top 5 by games among non-dropped players with at least
800 points. The points filter applies *first*: a sub-800 grinder is skipped and
the next qualified player moves up.

Mirrors eclDashboard/scripts/verify-most-games.ts; keep the two in step.

Run: python verify_most_games.py
"""
from topdeck_fetch import PlayerRow
from utils.most_games import MOST_GAMES_MIN_POINTS, contender_status, most_games_top

FAILURES = 0


def check(label: str, got, want) -> None:
    global FAILURES
    if got != want:
        print(f"FAIL {label}: got {got!r}, want {want!r}")
        FAILURES += 1


def row(uid: str, games: int, pts: float, *, dropped: bool = False) -> PlayerRow:
    return PlayerRow(
        entrant_id=0, uid=uid, name=uid.title(), discord="", pts=pts,
        win_pct=0.0, ow_pct=0.0, games=games, wins=0, draws=0, losses=0,
        dropped=dropped, dropped_at=None,
    )


def uids(rows) -> list:
    return [r.uid for r in rows]


check("threshold is 800", MOST_GAMES_MIN_POINTS, 800)

# Aug-2026 shape: the five biggest grinders are all under 800
GRINDERS = [
    row("oriol", 91, 114.8), row("merlin", 76, 700.9), row("dinis", 66, 360.5),
    row("jaime", 58, 709.8), row("cesar", 52, 517.4), row("thorsten", 48, 1046.4),
    row("francisco", 41, 854.1), row("jasper", 41, 819.9), row("tiago", 39, 1245.6),
    row("nikolaos", 39, 1003.8), row("x", 30, 1500.0),
]
check("filters first, then top 5 by games", uids(most_games_top(GRINDERS)),
      ["thorsten", "francisco", "jasper", "tiago", "nikolaos"])

# Boundary: exactly 800 qualifies, 799.99 does not
check("800 inclusive", uids(most_games_top([row("a", 10, 800.0), row("b", 20, 799.99)])), ["a"])

# Dropped players never qualify, whatever their points/games
check("dropped excluded", uids(most_games_top([row("d", 99, 2000.0, dropped=True), row("a", 1, 900.0)])), ["a"])

# Ties on games break by points (desc)
check("tie on games -> higher points first",
      uids(most_games_top([row("lo", 30, 900.0), row("hi", 30, 1200.0), row("top", 31, 850.0)])),
      ["top", "hi", "lo"])

# Fewer than 5 qualifiers -> fewer than 5 (no sub-800 backfill); none -> empty
check("short list is not backfilled",
      uids(most_games_top([row("a", 50, 500.0), row("b", 10, 1000.0), row("c", 5, 950.0)])), ["b", "c"])
check("nobody qualifies -> empty", most_games_top([row("a", 50, 500.0)]), [])

# ---- contender_status: where one player stands ----
ROWS = [row("a", 40, 1000.0), row("b", 30, 900.0), row("c", 20, 850.0), row("d", 15, 820.0),
        row("e", 10, 810.0), row("f", 9, 1100.0), row("tie", 10, 805.0),
        row("grinder", 99, 400.0), row("gone", 50, 1500.0, dropped=True)]
by = {r.uid: r for r in ROWS}

check("in the top -> position", contender_status(ROWS, by["c"]), ("in", 3))
check("qualified but behind -> games needed", contender_status(ROWS, by["f"]), ("out", 1))
check("tied with cutoff, out on points -> 0 needed", contender_status(ROWS, by["tie"]), ("out", 0))
# The bug this rule exists for: lots of games but under 800 must NOT read as "in"
check("sub-800 grinder -> below_points", contender_status(ROWS, by["grinder"]), ("below_points", 0))
check("dropped -> dropped", contender_status(ROWS, by["gone"]), ("dropped", 0))
# Defensive: a qualified target outside an empty Top 5 (only if it's missing from `rows`)
check("no one qualifies -> no_top",
      contender_status([row("a", 5, 700.0), row("b", 3, 900.0, dropped=True)], row("me", 1, 900.0)),
      ("no_top", 0))
check("sub-800 even when no one qualifies -> below_points",
      contender_status([row("a", 5, 700.0)], row("a", 5, 700.0)), ("below_points", 0))

if FAILURES:
    print(f"\n{FAILURES} failure(s)")
    raise SystemExit(1)
print("most_games selection truth-table OK")
