# utils/top16_eligibility.py
"""Pure Top 16 eligibility predicate (no I/O). Shared by /stats, /league, and the
month-end cut. Total-games rule: a player qualifies when not dropped and has at
least `min_total` total games.

Run the self-check:  python -m utils.top16_eligibility
"""
from __future__ import annotations


def is_top16_eligible(
    *,
    dropped: bool,
    total_games: int,
    min_total: int,
) -> bool:
    """Total-games eligibility verdict."""
    return not dropped and total_games >= min_total


def _self_check() -> None:
    def elig(**kw):
        base = dict(min_total=10)
        base.update(kw)
        return is_top16_eligible(**base)

    assert elig(dropped=True, total_games=99) is False
    assert elig(dropped=False, total_games=9) is False
    assert elig(dropped=False, total_games=10) is True
    assert elig(dropped=False, total_games=20) is True
    print("top16_eligibility self-check OK")


if __name__ == "__main__":
    _self_check()
