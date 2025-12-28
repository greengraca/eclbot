from __future__ import annotations

"""Elo-related helpers for LFG.

Centralizes:
- dynamic window sizing (percentile-based)
- floor computation (expands over time)
- last-seat behaviour
- mapping Discord members -> TopDeck (pts, games)

Keep these helpers mostly pure so cogs stay thin.
"""

import re
import math
from typing import Dict, List, Optional, Tuple

import discord

from topdeck_fetch import (
    get_league_rows_cached,
    normalize_topdeck_discord,
)

from .models import LFGLobby, now_utc


def round_up(value: float, unit: int) -> int:
    unit = max(1, int(unit))
    return int(math.ceil(float(value) / unit) * unit)


def percentile_sorted(vals: List[float], q: float) -> Optional[float]:
    """Return q percentile (0..1) from a pre-sorted list."""
    if not vals:
        return None
    q = max(0.0, min(1.0, float(q)))
    idx = (len(vals) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] + (vals[hi] - vals[lo]) * frac


async def compute_dynamic_window(
    host_elo: float,
    *,
    bracket_id: str,
    firebase_id_token: Optional[str],
    min_games: int,
    base_range_default: int,
    range_step_default: int,
    target_pool_frac: float,
    min_base_range: int,
    step_factor: float,
    min_range_step: int,
    round_to: int,
) -> Tuple[int, int]:
    """Compute (base_range, range_step) using TopDeck percentiles.

    This mirrors the logic previously inside LFGCog._compute_dynamic_window.
    """

    base_range = int(base_range_default)
    range_step = int(range_step_default)

    if not bracket_id:
        return base_range, range_step

    try:
        rows, _ = await get_league_rows_cached(bracket_id, firebase_id_token)
    except Exception:
        return base_range, range_step

    pts = sorted(
        float(r.pts) for r in rows
        if getattr(r, "games", 0) >= int(min_games)
    )

    if len(pts) < 10:
        return base_range, range_step

    q_floor = 1.0 - float(target_pool_frac)
    league_floor = percentile_sorted(pts, q_floor)
    if league_floor is None:
        return base_range, range_step

    desired_base = float(min_base_range)
    if float(host_elo) > float(league_floor):
        desired_base = max(desired_base, float(host_elo) - float(league_floor))

    base_range = round_up(desired_base, int(round_to))

    desired_step = max(float(min_range_step), float(base_range) * float(step_factor))
    range_step = round_up(desired_step, int(round_to))

    return int(base_range), int(range_step)


def max_downward_range(
    lobby: LFGLobby,
    *,
    base_range_default: int,
    range_step_default: int,
    max_steps_default: int,
) -> float:
    base = float(lobby.elo_base_range if lobby.elo_base_range is not None else base_range_default)
    step = float(lobby.elo_range_step if lobby.elo_range_step is not None else range_step_default)
    max_steps = int(getattr(lobby, "elo_max_steps", max_steps_default))
    return float(base + max(0, max_steps - 1) * step)


def current_downward_range(
    lobby: LFGLobby,
    *,
    base_range_default: int,
    range_step_default: int,
    expand_interval_min: int,
    max_steps_default: int,
) -> Optional[float]:
    if not lobby.elo_mode or lobby.host_elo is None:
        return None

    base = float(lobby.elo_base_range if lobby.elo_base_range is not None else base_range_default)
    step_pts = float(lobby.elo_range_step if lobby.elo_range_step is not None else range_step_default)
    cap = float(
        max_downward_range(
            lobby,
            base_range_default=base_range_default,
            range_step_default=range_step_default,
            max_steps_default=max_steps_default,
        )
    )

    elapsed_min = (now_utc() - lobby.created_at).total_seconds() / 60.0
    steps = int(elapsed_min // max(int(expand_interval_min), 1))

    rng = base + steps * step_pts
    rng = min(rng, cap)
    return float(rng)


def base_elo_floor(
    lobby: LFGLobby,
    *,
    base_range_default: int,
    range_step_default: int,
    expand_interval_min: int,
    max_steps_default: int,
) -> Optional[float]:
    if not lobby.elo_mode or lobby.host_elo is None:
        return None
    rng = current_downward_range(
        lobby,
        base_range_default=base_range_default,
        range_step_default=range_step_default,
        expand_interval_min=expand_interval_min,
        max_steps_default=max_steps_default,
    )
    if rng is None:
        return None
    return float(lobby.host_elo) - float(rng)


def relaxed_last_seat_floor(
    lobby: LFGLobby,
    *,
    base_range_default: int,
    range_step_default: int,
    expand_interval_min: int,
    max_steps_default: int,
    last_seat_min_rating: int,
) -> Optional[float]:
    if not lobby.elo_mode or lobby.host_elo is None:
        return None

    base_floor = base_elo_floor(
        lobby,
        base_range_default=base_range_default,
        range_step_default=range_step_default,
        expand_interval_min=expand_interval_min,
        max_steps_default=max_steps_default,
    )
    abs_floor = float(max(0, int(last_seat_min_rating)))
    if base_floor is None:
        return abs_floor

    # More permissive of the two (lower).
    return min(float(base_floor), abs_floor)


def is_last_seat_open(
    lobby: LFGLobby,
    *,
    last_seat_grace_min: int,
) -> bool:
    if not lobby.elo_mode or lobby.host_elo is None:
        return False
    if lobby.remaining_slots() != 1:
        return False
    if lobby.last_seat_open:
        return True
    if lobby.almost_full_at is None:
        return False
    elapsed_min = (now_utc() - lobby.almost_full_at).total_seconds() / 60.0
    return elapsed_min >= float(last_seat_grace_min)


def effective_elo_floor(
    lobby: LFGLobby,
    *,
    base_range_default: int,
    range_step_default: int,
    expand_interval_min: int,
    max_steps_default: int,
    last_seat_grace_min: int,
    last_seat_min_rating: int,
) -> Optional[float]:
    base_floor = base_elo_floor(
        lobby,
        base_range_default=base_range_default,
        range_step_default=range_step_default,
        expand_interval_min=expand_interval_min,
        max_steps_default=max_steps_default,
    )
    if base_floor is None:
        return None

    if is_last_seat_open(lobby, last_seat_grace_min=last_seat_grace_min):
        relaxed = relaxed_last_seat_floor(
            lobby,
            base_range_default=base_range_default,
            range_step_default=range_step_default,
            expand_interval_min=expand_interval_min,
            max_steps_default=max_steps_default,
            last_seat_min_rating=last_seat_min_rating,
        )
        return relaxed if relaxed is not None else base_floor

    return base_floor

def _extract_discord_id(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.strip()
    m = re.search(r"<@!?(\d{15,25})>", t)
    if m:
        return int(m.group(1))
    m2 = re.search(r"\b(\d{15,25})\b", t)
    if m2:
        return int(m2.group(1))
    return None


def member_handle_candidates(member: discord.Member) -> List[str]:
    """Return normalized handle candidates for TopDeck matching (stable order)."""
    cands: List[str] = []

    ordered = [
        getattr(member, "display_name", None),
        getattr(member, "global_name", None),
        getattr(member, "name", None),
    ]

    # add name#discriminator when it exists (older accounts / bots)
    discrim = getattr(member, "discriminator", None)
    if discrim and discrim != "0" and getattr(member, "name", None):
        ordered.append(f"{member.name}#{discrim}")

    for raw in ordered:
        if not raw:
            continue
        h = normalize_topdeck_discord(str(raw))
        if h and h not in cands:
            cands.append(h)

    return cands


def resolve_points_games_from_map(
    member: discord.Member,
    handle_to_best: Dict[str, Tuple[float, int]],
) -> Optional[Tuple[float, int]]:
    for h in member_handle_candidates(member):
        if h in handle_to_best:
            pts, games = handle_to_best[h]
            return float(pts), int(games)
    return None


async def get_member_points_games(
    member: discord.Member,
    *,
    bracket_id: str,
    firebase_id_token: Optional[str],
    force_refresh: bool = False,
) -> Optional[Tuple[float, int]]:
    """
    Safer resolver:
      1) match TopDeck row.discord by Discord ID (mention or raw digits) if present
      2) fallback to handle match, BUT only if it maps to exactly 1 row (unique)
         (if ambiguous, return None instead of guessing)
    """
    if not bracket_id:
        return None

    try:
        rows, _ = await get_league_rows_cached(
            bracket_id,
            firebase_id_token,
            force_refresh=force_refresh,
        )
    except Exception:
        return None

    if not rows:
        return None

    # 1) Strong match: Discord ID
    target_id = int(member.id)
    for r in rows:
        did = _extract_discord_id(getattr(r, "discord", "") or "")
        if did == target_id:
            return float(getattr(r, "pts", 0) or 0), int(getattr(r, "games", 0) or 0)

    # 2) Build handle -> (best pts,games) AND handle -> count
    handle_best: Dict[str, Tuple[float, int]] = {}
    handle_count: Dict[str, int] = {}

    for r in rows:
        h = normalize_topdeck_discord(getattr(r, "discord", "") or "")
        if not h:
            continue

        handle_count[h] = handle_count.get(h, 0) + 1

        pts = float(getattr(r, "pts", 0) or 0)
        games = int(getattr(r, "games", 0) or 0)

        prev = handle_best.get(h)
        if prev is None or pts > prev[0] or (pts == prev[0] and games > prev[1]):
            handle_best[h] = (pts, games)

    # 3) Only accept UNIQUE handle matches (count == 1)
    candidates = member_handle_candidates(member)
    for h in candidates:
        if handle_count.get(h, 0) == 1 and h in handle_best:
            pts, games = handle_best[h]
            return float(pts), int(games)

    # If handle match exists but ambiguous, DO NOT guess
    # (prevents “both players have 771”)
    return None
