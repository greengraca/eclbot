# cogs/stats_cog.py
"""/stats — quick per-player snapshot.

What it shows:
  - TopDeck points/games/record + current rank
  - Online games for the current month
  - Top16 qualification thresholds (min total + min online)
  - Access/entitlement snapshot (current + next month) when SubscriptionsCog is loaded

Default target is the caller; pass an optional member to inspect others.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List

import discord
from discord.ext import commands
from discord import Option

from topdeck_fetch import get_league_rows_cached, PlayerRow
from utils.topdeck_identity import find_row_for_member
from online_games_store import count_online_games_by_topdeck_uid_str, has_recent_game_by_topdeck_uid

from utils.dates import current_month_key, add_months
from utils.settings import GUILD_ID, SUBS, TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN
from utils.topdeck_identity import find_row_for_member
from utils.interactions import safe_ctx_defer, safe_ctx_followup
from utils.mod_check import is_mod


def _pct(x: float) -> str:
    try:
        return f"{float(x) * 100.0:.1f}%"
    except Exception:
        return "—"


def _ts(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    try:
        return int(dt.timestamp())
    except Exception:
        return None


def _fmt_map(conf: str, key: str = "", detail: str = "") -> str:
    conf = (conf or "").strip() or "unknown"
    key = (key or "").strip()
    detail = (detail or "").strip()

    out = f"Map: `{conf}`"
    if key:
        out += f" → `{key}`"
    if detail:
        out += f" ({detail})"
    return out


def _rank_of_row(rows: List[PlayerRow], target: PlayerRow) -> Optional[int]:
    # Rank among non-dropped by points, then games.
    active = [r for r in rows if not getattr(r, "dropped", False)]
    active = sorted(active, key=lambda r: (-float(getattr(r, "pts", 0.0) or 0.0), -int(getattr(r, "games", 0) or 0)))

    tuid = (getattr(target, "uid", None) or "").strip()
    if tuid:
        for i, r in enumerate(active, start=1):
            if (getattr(r, "uid", None) or "").strip() == tuid:
                return i

    # fallback: object identity
    for i, r in enumerate(active, start=1):
        if r is target:
            return i
    return None


def _top16_position(rows: List[PlayerRow], target: PlayerRow) -> Optional[int]:
    """Return 1-based position in the *eligible candidates* ordering, or None."""
    cfg = SUBS
    min_games = int(getattr(cfg, "top16_min_total_games", 0) or 0)

    active = [r for r in rows if (not getattr(r, "dropped", False)) and (int(getattr(r, "games", 0) or 0) >= min_games)]
    active = sorted(active, key=lambda r: (-float(getattr(r, "pts", 0.0) or 0.0), -int(getattr(r, "games", 0) or 0)))

    tuid = (getattr(target, "uid", None) or "").strip()
    if tuid:
        for i, r in enumerate(active, start=1):
            if (getattr(r, "uid", None) or "").strip() == tuid:
                return i
        return None

    for i, r in enumerate(active, start=1):
        if r is target:
            return i
    return None

def _most_games_contender_line(rows: List[PlayerRow], target: PlayerRow, top_n: int = 5) -> str:
    """
    Most-games raffle contender check.
    All non-dropped players are eligible — exclusion only happens if
    a player actually reaches the finals (not tracked here).
    Sorted by games desc, pts desc.
    """
    def _key(r: PlayerRow) -> str:
        uid = (getattr(r, "uid", None) or "").strip()
        return uid if uid else (getattr(r, "name", "") or "").strip().lower()

    tkey = _key(target)

    # All non-dropped players sorted by games desc, pts desc
    eligible = sorted(
        [r for r in rows if not getattr(r, "dropped", False)],
        key=lambda r: (-int(getattr(r, "games", 0) or 0), -float(getattr(r, "pts", 0.0) or 0.0)),
    )
    top = eligible[:top_n]

    if not top:
        return "Most games contender: —"

    # Determine if target is in top list
    in_top = any(_key(r) == tkey for r in top if tkey)
    if in_top:
        pos = next((i for i, r in enumerate(top, start=1) if _key(r) == tkey), None)
        if pos:
            return f"Most games contender: ✅ (#{pos}/{top_n})"
        return "Most games contender: ✅"

    # Not in top list: show games needed to reach last position
    cutoff_games = int(getattr(top[-1], "games", 0) or 0)
    my_games = int(getattr(target, "games", 0) or 0)
    need = max(0, cutoff_games - my_games)
    return f"Most games contender: ❌ (need **{need}** more to reach **{cutoff_games}**)"



class StatsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.slash_command(
        name="stats",
        description="Show ECL/TopDeck stats (defaults to you).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def stats(
        self,
        ctx: discord.ApplicationContext,
        player: Optional[discord.Member] = Option(
            discord.Member,
            "Player to check (defaults to you)",
            required=False,
        ),
    ):
        if ctx.guild is None:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        caller: Optional[discord.Member] = ctx.author if isinstance(ctx.author, discord.Member) else None
        caller_is_mod = is_mod(caller, check_manage_roles=True)

        target: discord.Member = player or ctx.author  # type: ignore

        # Keep it private by default; it includes entitlement info.
        await safe_ctx_defer(ctx, ephemeral=True, label="stats")

        if not TOPDECK_BRACKET_ID:
            await safe_ctx_followup(
                ctx,
                "TOPDECK_BRACKET_ID is not configured.",
                ephemeral=True,
            )
            return

        try:
            rows, fetched_at = await get_league_rows_cached(
                TOPDECK_BRACKET_ID,
                FIREBASE_ID_TOKEN,
                force_refresh=False,
            )
        except Exception as e:
            await safe_ctx_followup(
                ctx,
                f"I couldn't fetch TopDeck right now ({type(e).__name__}).",
                ephemeral=True,
            )
            return

        mk = current_month_key()
        next_mk = add_months(mk, 1)

        # ---- resolve TopDeck row for member ----
        match = find_row_for_member(rows or [], target)
        row: Optional[PlayerRow] = match.row if match else None

        emb = discord.Embed(
            title=f"ECL Stats — {target.display_name}",
            color=int(getattr(SUBS, "embed_color", 0x2ECC71) or 0x2ECC71),
        )

        emb.set_footer(text=f"Bracket {TOPDECK_BRACKET_ID} • {mk}")

        # ---- Access snapshot (current + next month) ----
        cfg = SUBS
        ecl_role = ctx.guild.get_role(int(getattr(cfg, "ecl_role_id", 0) or 0))
        top16_role = ctx.guild.get_role(int(getattr(cfg, "top16_role_id", 0) or 0))
        dm_role = ctx.guild.get_role(int(getattr(cfg, "dm_optin_role_id", 0) or 0))

        has_ecl = bool(ecl_role and ecl_role in target.roles)
        has_top16 = bool(top16_role and top16_role in target.roles)
        has_dm = bool(dm_role and dm_role in target.roles)

        subs_cog = self.bot.get_cog("SubscriptionsCog")
        entitled_now = None
        entitled_now_reason = "Reason: unknown"
        entitled_next = None
        entitled_next_reason = "Reason: unknown"

        if subs_cog is not None and hasattr(subs_cog, "_eligibility"):
            try:
                entitled_now, entitled_now_reason = await subs_cog._eligibility(target, mk)  # type: ignore
            except Exception:
                entitled_now, entitled_now_reason = None, "Reason: error"
            try:
                entitled_next, entitled_next_reason = await subs_cog._eligibility(target, next_mk)  # type: ignore
            except Exception:
                entitled_next, entitled_next_reason = None, "Reason: error"

        access_lines = [
            f"ECL role: {'✅' if has_ecl else '❌'}",
            f"Top16 role: {'✅' if has_top16 else '❌'}",
            f"DM opt-in: {'✅' if has_dm else '❌'}",
        ]
        if entitled_now is not None:
            access_lines.append(f"Entitled ({mk}): {'✅' if entitled_now else '❌'} — {entitled_now_reason}")
        if entitled_next is not None:
            access_lines.append(f"Entitled ({next_mk}): {'✅' if entitled_next else '❌'} — {entitled_next_reason}")
        

        # ---- TopDeck row details ----
        if not row:
            hint = "I couldn't find a TopDeck row for this Discord user.\n"
            hint += "If your TopDeck `discord` field contains your Discord ID/mention, mapping will be perfect."
            if caller_is_mod and match is not None:
                hint += f"\nMatch attempt: conf={match.confidence} key={match.matched_key!r} ({match.detail})"
            emb.add_field(name="TopDeck", value=hint, inline=False)
            await safe_ctx_followup(ctx, embed=emb, ephemeral=True)
            return

        pts = int(round(float(getattr(row, "pts", 0.0) or 0.0)))
        games = int(getattr(row, "games", 0) or 0)
        wins = int(getattr(row, "wins", 0) or 0)
        draws = int(getattr(row, "draws", 0) or 0)
        losses = int(getattr(row, "losses", 0) or 0)
        win_pct = float(getattr(row, "win_pct", 0.0) or 0.0)
        dropped = bool(getattr(row, "dropped", False))

        rank = _rank_of_row(rows or [], row)
        rank_str = f"#{rank}" if rank else "—"

        td_lines = [
            f"Name: **{getattr(row, 'name', '—')}**",
            f"Rank: **{rank_str}**",
            f"Points: **{pts}**",
            f"Games: **{games}**",
            f"Record: **{wins}-{losses}-{draws}**(W-L-D)",
            f"Win%: **{_pct(win_pct)}**",
        ]

        # Gate UID + dropped status to mods only.
        if caller_is_mod:
            td_lines.insert(1, f"UID: `{(getattr(row, 'uid', None) or '—')}`")
            td_lines.append(f"Dropped: {'✅' if dropped else '❌'}")

        # Identity mapping confidence (mods only)
        if caller_is_mod and match is not None:
            td_lines.append(_fmt_map(match.confidence, match.matched_key, match.detail))

        emb.add_field(name="TopDeck", value="\n".join(td_lines), inline=False)

        # ---- Online games for current month ----
        uid = (getattr(row, "uid", None) or "").strip()
        online_count = None
        if uid:
            try:
                y, m = mk.split("-")
                online_counts = await count_online_games_by_topdeck_uid_str(
                    TOPDECK_BRACKET_ID,
                    int(y),
                    int(m),
                    online_only=True,
                )
                online_count = int(online_counts.get(uid, 0) or 0)
            except Exception:
                online_count = None

        min_online = int(getattr(cfg, "top16_min_online_games", 0) or 0)
        min_total = int(getattr(cfg, "top16_min_total_games", 0) or 0)
        meets_total = games >= min_total
        meets_online = (online_count is not None) and (online_count >= min_online)

        top16_pos = _top16_position(rows or [], row)
        in_top16_window = bool(top16_pos and top16_pos <= 16)

        online_lines = []
        if online_count is None:
            online_lines.append("Online games: —")
        else:
            online_lines.append(f"Online games: **{online_count}** (need **{min_online}**) — {'✅' if meets_online else '❌'}")
        online_lines.append(f"Min total games: **{min_total}** — {'✅' if meets_total else '❌'}")
        if top16_pos:
            online_lines.append(f"Top16 window (by pts, min games): **#{top16_pos}** — {'✅' if in_top16_window else '❌'}")
        else:
            online_lines.append("Top16 window (by pts, min games): —")
            
        # ---- Recency check for 10-19 online games ----
        no_recency_threshold = int(getattr(cfg, "top16_min_online_games_no_recency", 20) or 20)
        recency_after_day = int(getattr(cfg, "top16_recency_after_day", 20) or 20)
        meets_recency = True  # default: not needed (20+ games or < min)
        if online_count is not None and min_online <= online_count < no_recency_threshold:
            try:
                y, m = mk.split("-")
                recency_map = await has_recent_game_by_topdeck_uid(
                    TOPDECK_BRACKET_ID, int(y), int(m), [uid],
                    after_day=recency_after_day, online_only=True,
                )
                has_recent = recency_map.get(uid, False)
            except Exception:
                has_recent = None

            if has_recent is False:
                meets_recency = False
                online_lines.append(
                    f"🔴 **Warning:** No online game played after day **{recency_after_day}** — "
                    f"required for eligibility with fewer than **{no_recency_threshold}** online games."
                )
            elif has_recent is True:
                online_lines.append(
                    f"Recency (game after day {recency_after_day}): ✅"
                )

        # ---- Final eligibility verdict ----
        all_eligible = meets_total and meets_online and in_top16_window and meets_recency
        if all_eligible:
            online_lines.append("🟢 **You are on the Top cut this month**")
        else:
            online_lines.append("🔴 **You are not eligible for Top cut this month**")

        emb.add_field(name=f"Eligibility Top16 ({mk})", value="\n".join(online_lines), inline=False)

        # ---- Most Games (separate category) ----
        emb.add_field(
            name=f"Most Games ({mk})",
            value=_most_games_contender_line(rows or [], row, top_n=5),
            inline=False,
        )
        
        # Gate entitlement/access info to mods only.
        if caller_is_mod:
            emb.add_field(name="Access", value="\n".join(access_lines), inline=False)

        await safe_ctx_followup(ctx, embed=emb, ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(StatsCog(bot))
