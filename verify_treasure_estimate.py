"""Verification for the treasure-pod month-size estimate (no DB).

The estimate decides where pods are placed: pods live in [10, 92% * estimated_total].
Two failures are covered here.

1. The initial estimate was a fixed `players * TREASURE_GAMES_PER_PLAYER` (env 4.0).
   The real tables-per-player ratio swings 3.00..4.53, so a fixed constant is wrong
   most months. `estimate_total_tables` now takes the median of recent months.

2. The estimate never tracked the month's actual pace. In 2026-09 the league ran
   ~30% below August, so an estimate of 360 put 5 of 8 pods past a finish line of
   ~240 and they sat unreachable until a late pull_in crammed them together.
   `project_total_tables` reads the pace instead, and the recalculation gate now
   also triggers on an unreachable tail rather than only on the nearest pod.

Run:  .venv/Scripts/python verify_treasure_estimate.py
"""
import statistics

from utils.treasure_pods import (
    GAMES_PER_PLAYER_ESTIMATE,
    TREASURE_RATIO_MAX,
    TREASURE_RATIO_MIN,
    estimate_total_tables,
    needs_recalculation,
    project_total_tables,
    tables_per_player_ratio,
)

failures = 0


def check(name, cond):
    global failures
    print(f"{'PASS' if cond else 'FAIL'}  {name}")
    if not cond:
        failures += 1


# Real history: (month, players, final table number). Final table is max(tid) in
# online_games; every completed month agrees with the document count.
HISTORY = [
    ("2026-02", 95, 306),
    ("2026-03", 79, 299),
    ("2026-04", 81, 367),
    ("2026-05", 100, 362),
    ("2026-06", 102, 306),
    ("2026-07", 95, 320),
    ("2026-08", 90, 358),
]
SEPT_PLAYERS, SEPT_ACTUAL = 90, 240  # 2026-09 projected finish

# ── A. ratio selection ────────────────────────────────────────────────────
print("\nA. tables-per-player ratio")
check("A1 falls back to env with no history", tables_per_player_ratio(None) == GAMES_PER_PLAYER_ESTIMATE)
check("A2 falls back to env with a single month", tables_per_player_ratio([3.2]) == GAMES_PER_PLAYER_ESTIMATE)
check("A3 median, not mean (ignores one spike)", tables_per_player_ratio([3.0, 3.4, 4.53]) == 3.4)
check("A4 clamped at the top", tables_per_player_ratio([9.0, 9.0, 9.0]) == TREASURE_RATIO_MAX)
check("A5 clamped at the bottom", tables_per_player_ratio([0.4, 0.4, 0.4]) == TREASURE_RATIO_MIN)
check("A6 ignores zero/None entries", tables_per_player_ratio([0, None, 3.0, 3.4, 4.53]) == 3.4)

# ── B. initial estimate beats the fixed constant ──────────────────────────
print("\nB. initial estimate replayed over history")
old_err, new_err = [], []
for i, (month, players, actual) in enumerate(HISTORY):
    if i < 3:
        continue
    ratios = [t / p for _, p, t in HISTORY[i - 3:i]]
    old = max(int(players * 4.0), 50)
    new = estimate_total_tables(players, ratios)
    old_err.append(abs(old - actual) / actual)
    new_err.append(abs(new - actual) / actual)
    print(f"   {month}  actual {actual:3d}   fixed-4.0 {old:3d} ({(old-actual)/actual:+.0%})"
          f"   history {new:3d} ({(new-actual)/actual:+.0%})")

sept_ratios = [t / p for _, p, t in HISTORY[-3:]]
sept_new = estimate_total_tables(SEPT_PLAYERS, sept_ratios)
print(f"   2026-09  actual {SEPT_ACTUAL:3d}   fixed-4.0 360 (+50%)"
      f"   history {sept_new:3d} ({(sept_new-SEPT_ACTUAL)/SEPT_ACTUAL:+.0%})")
check("B1 mean error improves over the fixed constant",
      statistics.mean(new_err) < statistics.mean(old_err))
check("B2 September estimate improves but still overshoots (pace is the real fix)",
      SEPT_ACTUAL < sept_new < 360)
check("B3 zero players is still safe", estimate_total_tables(0, sept_ratios) == 100)
check("B4 never returns below the floor", estimate_total_tables(1, [2.5, 2.5, 2.5]) >= 50)

# ── C. pace projection ────────────────────────────────────────────────────
print("\nC. pace projection")
check("C1 too early in the month -> keeps the fallback",
      project_total_tables(30, days_elapsed=3.0, days_until_close=26.0, fallback=324) == 324)
check("C2 no tables played -> keeps the fallback",
      project_total_tables(0, days_elapsed=10.0, days_until_close=19.0, fallback=324) == 324)

sept = project_total_tables(139, days_elapsed=15.45, days_until_close=13.56, fallback=324)
print(f"   2026-09 at table 139, day 15.4 -> projects {sept} (actual ~{SEPT_ACTUAL})")
check("C3 September projection lands within 10% of the real finish",
      abs(sept - SEPT_ACTUAL) / SEPT_ACTUAL < 0.10)

aug = project_total_tables(217, days_elapsed=16.0, days_until_close=14.0, fallback=360)
print(f"   2026-08 at table 217, day 16   -> projects {aug} (actual 358)")
check("C4 August projection lands within 10% of the real finish", abs(aug - 358) / 358 < 0.10)
check("C5 never projects below the current table",
      project_total_tables(300, days_elapsed=28.0, days_until_close=0.0, fallback=100) > 300)

# ── D. recalculation gate ─────────────────────────────────────────────────
# The 2026-09 state on ~day 8: league at table 70, pods at 166..293, estimate 360.
# The nearest pod is far ahead but the month has weeks left, so the old gate
# (days_until_close <= 11 only) stayed shut while the tail was already doomed.
print("\nD. recalculation gate")
SEPT_PODS = [166, 192, 207, 242, 249, 272, 293, 256]

fire, mode, proj = needs_recalculation(
    unfired=SEPT_PODS, current_max_table=139, estimated_total=360,
    days_elapsed=15.45, days_until_close=13.56,
)
print(f"   2026-09 day 15.4 -> fire={fire} mode={mode} projected={proj}")
check("D1 fires on the unreachable tail even with >11 days left", fire)
check("D2 spreads forward across the month, does not cram near-term", mode == "forward")
check("D3 corrects the stale estimate downward", proj < 360)

fire2, mode2, _ = needs_recalculation(
    unfired=[242, 249, 272, 293], current_max_table=150, estimated_total=240,
    days_elapsed=24.0, days_until_close=5.0,
)
check("D4 near close with a far nearest pod -> pull_in", fire2 and mode2 == "pull_in")

fire3, _, _ = needs_recalculation(
    unfired=[146, 155, 163, 169, 183, 190, 197, 204], current_max_table=139,
    estimated_total=240, days_elapsed=15.45, days_until_close=13.56,
)
check("D5 the corrected 2026-09 schedule needs no recalculation", not fire3)

fire4, _, _ = needs_recalculation(
    unfired=[], current_max_table=200, estimated_total=240,
    days_elapsed=20.0, days_until_close=9.0,
)
check("D6 nothing unfired -> no recalculation", not fire4)

fire5, _, _ = needs_recalculation(
    unfired=[210], current_max_table=60, estimated_total=240,
    days_elapsed=4.0, days_until_close=25.0,
)
check("D7 early month with no pace signal -> leaves the schedule alone", not fire5)

print()
if failures:
    print(f"{failures} CHECK(S) FAILED")
    raise SystemExit(1)
print("ALL CHECKS PASSED")
