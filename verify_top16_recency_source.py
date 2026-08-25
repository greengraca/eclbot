"""Truth-table check for the TopDeck-sourced Top 16 recency helper.

Recency used to be answered from the `online_games` mirror, which only holds pods
the online-sync managed to record. When that sync stalled (August 2026) the
collection went ~45% short and players with a genuine post-cutoff game were
silently ruled out of the cut. `recent_uids_from_matches` reads TopDeck's own
match list instead — the same data the standings come from.

Mirrors eclDashboard/scripts/verify-top16-recency-source.ts; keep the two in step.

Run: python verify_top16_recency_source.py
"""
from datetime import datetime, timezone

from topdeck_fetch import Match, recent_uids_from_matches

FAILURES = 0


def check(label: str, got: bool, want: bool) -> None:
    global FAILURES
    if got is not want:
        print(f"FAIL {label}: got {got}, want {want}")
        FAILURES += 1


def ts(y, mo, d, h=0, mi=0) -> float:
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp()


def match(uid_ids, start, *, end=1.0, winner=1, mute=False) -> Match:
    return Match(
        season=1, id=1, start=start, end=end, es=list(uid_ids),
        winner=winner, raw={"Mute": True} if mute else {},
    )


YEAR, MONTH = 2026, 8                       # cutoff: 2026-08-20T00:00:00Z
BEFORE = ts(2026, 8, 19, 16, 37)            # Aug 19 — before cutoff
AFTER = ts(2026, 8, 20, 10, 29)             # Aug 20 — after cutoff

# entrant id -> uid
E2U = {
    1: "sec_before", 2: "ms_before", 3: "sec_after", 4: "ms_after", 5: "no_ts",
    6: "muted", 7: "in_progress", 8: "edge",
    10: "pod_a", 11: "pod_b", 12: "pod_c", 13: "pod_d",
}

recent = recent_uids_from_matches([
    match([1, 99], BEFORE),
    match([2, 99], BEFORE * 1000),
    match([3, 99], AFTER),
    match([4, 99], AFTER * 1000),
    match([5, 99], None),
    match([6, 99], AFTER, mute=True),
    match([7, 99], AFTER, end=None, winner=None),
    match([8, 99], ts(2026, 8, 20)),
    match([10, 11, 12, 13], AFTER),
], E2U, YEAR, MONTH)

check("seconds before cutoff -> not recent", "sec_before" in recent, False)
check("MILLISECOND before cutoff -> not recent", "ms_before" in recent, False)
check("seconds after cutoff -> recent", "sec_after" in recent, True)
check("millisecond after cutoff -> recent", "ms_after" in recent, True)
check("null timestamp -> not recent", "no_ts" in recent, False)
check("muted match -> not recent", "muted" in recent, False)
check("in-progress match -> not recent", "in_progress" in recent, False)
check("game exactly at cutoff -> recent", "edge" in recent, True)
check("every player in a pod counts",
      all(u in recent for u in ("pod_a", "pod_b", "pod_c", "pod_d")), True)
check("entrant with no uid mapping is skipped", "" in recent, False)

# after_day past the end of the month clamps to the last day.
feb = recent_uids_from_matches(
    [match([1, 99], ts(2026, 2, 28, 12))], {1: "feb28"}, 2026, 2, after_day=31,
)
check("after_day clamped to month length", "feb28" in feb, True)

check("no matches -> empty set", len(recent_uids_from_matches([], {}, YEAR, MONTH)) == 0, True)

if FAILURES:
    raise SystemExit(f"\n{FAILURES} failure(s)")
print("top16 recency-source truth-table OK")
