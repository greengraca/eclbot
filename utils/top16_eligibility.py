# utils/top16_eligibility.py
"""Pure Top 16 eligibility predicate (no I/O). Shared by /stats, /league, and the
month-end cut. Total-games rule: a player qualifies when not dropped, has at least
`min_total` total games, and either has `no_recency_games`+ total games, or recency
isn't active yet, or has a recent game.

Run the self-check:  python -m utils.top16_eligibility
"""
from __future__ import annotations


def needs_recency_check(total_games: int, min_total: int, no_recency_games: int) -> bool:
    """True when a player is in the band that requires a recent game."""
    return min_total <= total_games < no_recency_games


def is_top16_eligible(
    *,
    dropped: bool,
    total_games: int,
    has_recent: bool,
    recency_active: bool,
    min_total: int,
    no_recency_games: int,
) -> bool:
    """Total-games eligibility verdict."""
    if dropped or total_games < min_total:
        return False
    if total_games >= no_recency_games:
        return True
    if not recency_active:
        return True
    return has_recent


def _self_check() -> None:
    # needs_recency_check: band is [min_total, no_recency_games)
    assert needs_recency_check(10, 10, 20) is True
    assert needs_recency_check(19, 10, 20) is True
    assert needs_recency_check(20, 10, 20) is False
    assert needs_recency_check(9, 10, 20) is False

    def elig(**kw):
        base = dict(min_total=10, no_recency_games=20)
        base.update(kw)
        return is_top16_eligible(**base)

    assert elig(dropped=True, total_games=99, has_recent=True, recency_active=True) is False
    assert elig(dropped=False, total_games=9, has_recent=True, recency_active=True) is False
    assert elig(dropped=False, total_games=10, has_recent=False, recency_active=True) is False
    assert elig(dropped=False, total_games=10, has_recent=True, recency_active=True) is True
    assert elig(dropped=False, total_games=10, has_recent=False, recency_active=False) is True
    assert elig(dropped=False, total_games=20, has_recent=False, recency_active=True) is True
    print("top16_eligibility self-check OK")


if __name__ == "__main__":
    _self_check()
