"""Verification for treasure-pod redistribution spacing (no DB).

Reproduces the July-2026 bug where many "skipped" pods were redistributed into a
single ~30-table window (fired 206..235 back-to-back) and asserts the fix spreads
them across the tables the league still has left to play.

Run:  .venv/Scripts/python verify_treasure_spread.py
"""
import random

from utils.treasure_pods import compute_redistribution_tables

failures = 0


def check(name, cond):
    global failures
    print(f"{'PASS' if cond else 'FAIL'}  {name}")
    if not cond:
        failures += 1


def gaps(xs):
    return [xs[i + 1] - xs[i] for i in range(len(xs) - 1)]


def span(xs):
    return (max(xs) - min(xs)) if xs else 0


random.seed(42)

# ── Scenario A: the July bug ───────────────────────────────────────────────
# 6 bring-a-friend pods were skipped (played past without firing); the league is
# at table 205, estimated_total 352, and ~10 days remain in the month.
NAT_END = int(352 * 0.92)  # 323
reps = compute_redistribution_tables(
    count=6, current_max_table=205, estimated_total=352,
    days_until_close=10.0, exclude=set(), mode="forward",
)
print(f"\nA. forward (skipped) -> {reps}  span={span(reps)}  gaps={gaps(reps)}")
check("A1 all after current table 205", all(t > 205 for t in reps))
check("A2 stays within the natural range (<=323)", max(reps) <= NAT_END)
check("A3 spread across the month, not a 30-table cram", span(reps) > 60)
check("A4 no back-to-back pods (every gap >= 4)", min(gaps(reps)) >= 4)
check("A5 all distinct", len(set(reps)) == 6)

# What the OLD code produced (illustrative): 6 uniformly-random tables in a fixed
# [206, 236] window — span <= 30, routinely 1-2 apart. That is the bug.
random.seed(42)
old = sorted(random.randint(206, 236) for _ in range(6))
print(f"   (old behaviour would have been ~{old}  span={span(old)})")

# ── Scenario B: near month-end pull-in ─────────────────────────────────────
# 5 pods scheduled too far ahead; only ~4 days left, league at table 300.
random.seed(7)
reps2 = compute_redistribution_tables(
    count=5, current_max_table=300, estimated_total=352,
    days_until_close=4.0, exclude=set(), mode="pull_in",
)
print(f"\nB. pull_in (near close) -> {reps2}  span={span(reps2)}  gaps={gaps(reps2)}")
check("B1 all after current table 300", all(t > 300 for t in reps2))
check("B2 pulled into near-term reach (<= +20)", max(reps2) - 300 <= 20)
check("B3 all distinct", len(set(reps2)) == 5)

# ── Scenario C: single skipped pod is still handled ────────────────────────
reps3 = compute_redistribution_tables(
    count=1, current_max_table=100, estimated_total=352,
    days_until_close=20.0, exclude=set(), mode="forward",
)
print(f"\nC. single -> {reps3}")
check("C1 one pod produced, after current table", len(reps3) == 1 and reps3[0] > 100)

# ── Scenario D: exclude set is honoured (no cross-type collisions) ──────────
excl = {215, 216, 217, 218}
reps4 = compute_redistribution_tables(
    count=4, current_max_table=205, estimated_total=352,
    days_until_close=10.0, exclude=excl, mode="forward",
)
check("D1 avoids excluded tables", not (set(reps4) & {215, 216, 217, 218}))

# ── Scenario E: tight window under stress still yields distinct tables ──────
# 10 pods, 2 days left (reach 5) — physically cramped, but must stay distinct.
random.seed(1)
reps5 = compute_redistribution_tables(
    count=10, current_max_table=340, estimated_total=352,
    days_until_close=2.0, exclude=set(), mode="pull_in",
)
print(f"\nE. tight pull_in -> {reps5}")
check("E1 produced 10 distinct tables (no duplicates)", len(set(reps5)) == 10)
check("E2 all after current table 340", all(t > 340 for t in reps5))

print()
if failures:
    print(f"{failures} CHECK(S) FAILED")
    raise SystemExit(1)
print("ALL CHECKS PASSED")
