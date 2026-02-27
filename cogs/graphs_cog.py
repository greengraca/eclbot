"""/graphs — Beautiful player charts sent as Discord image attachments.

Chart types (current month, day-by-day):
  - Season Record        — donut W/L/D
  - Monthly Activity     — stacked bar by day
  - Points & Rank        — dual-axis line, day-by-day within current month
  - Win Rate             — line, day-by-day within current month

All-Time charts (month-by-month from historical dumps):
  - All-Time Points & Rank
  - All-Time Win Rate
"""

from __future__ import annotations

import asyncio
from typing import Dict, List, Optional, Tuple

import discord
from discord.ext import commands
from discord import Option

from topdeck_fetch import Match, PlayerRow, get_league_rows_cached
from utils.topdeck_identity import find_row_for_member
from utils.interactions import safe_ctx_defer, safe_ctx_followup
from utils.settings import GUILD_ID, SUBS, TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN
from utils.dates import current_month_key, month_label as fmt_month_label
from utils.logger import log_sync, log_warn
from utils.month_dump_reader import (
    get_live_matches,
    get_player_history,
    get_daily_activity_from_matches,
    compute_daily_progression,
    _get_current_month_matches,
)
from utils.graph_renderer import (
    render_daily_points_rank,
    render_daily_winrate,
    render_daily_activity,
    render_season_record,
    render_points_rank_alltime,
    render_winrate_alltime,
)


CHART_CHOICES = [
    discord.OptionChoice("Season Record", "record"),
    discord.OptionChoice("Monthly Activity", "activity"),
    discord.OptionChoice("Points & Rank", "points_rank"),
    discord.OptionChoice("Win Rate", "winrate"),
    discord.OptionChoice("All-Time Points & Rank", "points_rank_alltime"),
    discord.OptionChoice("All-Time Win Rate", "winrate_alltime"),
]


def _rank_of_row(rows: List[PlayerRow], target: PlayerRow) -> Optional[int]:
    active = [r for r in rows if not getattr(r, "dropped", False)]
    active = sorted(active, key=lambda r: (-float(getattr(r, "pts", 0.0) or 0.0), -int(getattr(r, "games", 0) or 0)))
    tuid = (getattr(target, "uid", None) or "").strip()
    if tuid:
        for i, r in enumerate(active, start=1):
            if (getattr(r, "uid", None) or "").strip() == tuid:
                return i
    for i, r in enumerate(active, start=1):
        if r is target:
            return i
    return None


class GraphsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.slash_command(
        name="graphs",
        description="Generate a beautiful chart for a player's stats.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def graphs(
        self,
        ctx: discord.ApplicationContext,
        chart: str = Option(
            str,
            "Chart type",
            choices=CHART_CHOICES,
            required=True,
        ),
        player: Optional[discord.Member] = Option(
            discord.Member,
            "Player to chart (defaults to you)",
            required=False,
        ),
    ):
        if ctx.guild is None:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        target: discord.Member = player or ctx.author  # type: ignore
        await safe_ctx_defer(ctx, ephemeral=False, label="graphs")

        if not TOPDECK_BRACKET_ID:
            await safe_ctx_followup(ctx, "TOPDECK_BRACKET_ID is not configured.", ephemeral=True)
            return

        # Fetch live rows
        try:
            rows, _ = await get_league_rows_cached(TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN)
        except Exception as e:
            log_warn(f"[graphs] Failed to fetch league rows: {type(e).__name__}: {e}")
            await safe_ctx_followup(ctx, f"Couldn't fetch TopDeck data ({type(e).__name__}).", ephemeral=True)
            return

        # Resolve target player
        match = find_row_for_member(rows or [], target)
        row: Optional[PlayerRow] = match.row if match else None

        if not row:
            log_sync(f"[graphs] No TopDeck row found for {target.display_name} (id={target.id})")
            await safe_ctx_followup(
                ctx,
                f"I couldn't find a TopDeck profile for **{target.display_name}**. "
                "Make sure your TopDeck discord field contains your Discord ID or username.",
                ephemeral=True,
            )
            return

        games = int(getattr(row, "games", 0) or 0)
        if games == 0:
            await safe_ctx_followup(
                ctx,
                f"**{target.display_name}** hasn't played any games yet this season.",
                ephemeral=True,
            )
            return

        mk = current_month_key()
        ml = fmt_month_label(mk)
        uid = (getattr(row, "uid", None) or "").strip()
        entrant_id = int(getattr(row, "entrant_id", 0) or 0)

        log_sync(f"[graphs] chart={chart} player={target.display_name} uid={uid} "
                 f"entrant_id={entrant_id} games={games}")

        try:
            if chart == "record":
                buf, filename, emb = await self._chart_record(row, target.display_name, ml)
            elif chart == "activity":
                buf, filename, emb = await self._chart_activity(
                    entrant_id, target.display_name, mk, ml)
            elif chart == "points_rank":
                buf, filename, emb = await self._chart_points_rank_daily(
                    rows, row, entrant_id, target.display_name, mk, ml)
            elif chart == "winrate":
                buf, filename, emb = await self._chart_winrate_daily(
                    entrant_id, target.display_name, mk, ml)
            elif chart == "points_rank_alltime":
                buf, filename, emb = await self._chart_points_rank_alltime(
                    rows, row, uid, target.display_name, mk, ml)
            elif chart == "winrate_alltime":
                buf, filename, emb = await self._chart_winrate_alltime(
                    row, uid, target.display_name, mk, ml)
            else:
                await safe_ctx_followup(ctx, "Unknown chart type.", ephemeral=True)
                return
        except Exception as e:
            log_warn(f"[graphs] Error generating chart={chart} for {target.display_name}: "
                     f"{type(e).__name__}: {e}")
            await safe_ctx_followup(
                ctx,
                f"Error generating chart: {type(e).__name__}: {e}",
                ephemeral=True,
            )
            return

        emb.set_image(url=f"attachment://{filename}")
        emb.color = int(getattr(SUBS, "embed_color", 0x2ECC71) or 0x2ECC71)

        thumb_url = getattr(SUBS, "embed_thumbnail_url", "") or ""
        if thumb_url.startswith(("http://", "https://")):
            emb.set_thumbnail(url=thumb_url)

        emb.set_footer(text=f"ECL \u2022 {mk} \u2022 /graphs")

        await safe_ctx_followup(ctx, embed=emb, file=discord.File(buf, filename=filename))

    # ------------------------------------------------------------------
    # Helpers: fetch live matches for current month
    # ------------------------------------------------------------------

    async def _get_month_matches(self, entrant_id: int) -> Tuple[List[Match], set]:
        """Fetch live matches, filter to current month, return (matches, all_entrant_ids)."""
        matches, entrant_to_uid = await get_live_matches(TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN)
        month_matches, _, _ = _get_current_month_matches(matches, entrant_to_uid)

        all_entrant_ids = set(entrant_to_uid.keys())
        for m in month_matches:
            for eid in m.es:
                all_entrant_ids.add(eid)

        log_sync(f"[graphs] _get_month_matches: {len(month_matches)} matches this month, "
                 f"{len(all_entrant_ids)} entrants")
        return month_matches, all_entrant_ids

    # ------------------------------------------------------------------
    # Chart: Season Record (donut)
    # ------------------------------------------------------------------

    async def _chart_record(self, row, name, ml):
        wins = int(getattr(row, "wins", 0) or 0)
        losses = int(getattr(row, "losses", 0) or 0)
        draws = int(getattr(row, "draws", 0) or 0)

        buf = await asyncio.to_thread(render_season_record, wins, losses, draws, name, ml)

        emb = discord.Embed(title=f"\U0001f4ca Season Record \u2014 {name}")
        emb.description = f"**{wins}**W / **{losses}**L / **{draws}**D \u2014 {ml}"
        return buf, "season_record.png", emb

    # ------------------------------------------------------------------
    # Chart: Monthly Activity (stacked bar by day)
    # ------------------------------------------------------------------

    async def _chart_activity(self, entrant_id, name, mk, ml):
        month_matches, _ = await self._get_month_matches(entrant_id)
        daily = get_daily_activity_from_matches(month_matches, entrant_id)

        if not daily:
            log_warn(f"[graphs] _chart_activity: no daily data for entrant={entrant_id}")
            raise ValueError(
                "No game data found for this month. "
                "This player may not have completed any games yet."
            )

        max_day = max(daily.keys())
        days = list(range(1, max_day + 1))
        wins = [daily.get(d, {}).get("wins", 0) for d in days]
        losses = [daily.get(d, {}).get("losses", 0) for d in days]
        draws = [daily.get(d, {}).get("draws", 0) for d in days]

        buf = await asyncio.to_thread(render_daily_activity, days, wins, losses, draws, name, ml)

        total = sum(wins) + sum(losses) + sum(draws)
        active_days = sum(1 for d in days if (daily.get(d, {}).get("wins", 0)
                                               + daily.get(d, {}).get("losses", 0)
                                               + daily.get(d, {}).get("draws", 0)) > 0)
        emb = discord.Embed(title=f"\U0001f4c5 Monthly Activity \u2014 {name}")
        emb.description = f"**{total}** games across **{active_days}** active days \u2014 {ml}"
        return buf, "daily_activity.png", emb

    # ------------------------------------------------------------------
    # Chart: Points & Rank (day-by-day, current month)
    # ------------------------------------------------------------------

    async def _chart_points_rank_daily(self, rows, row, entrant_id, name, mk, ml):
        month_matches, all_entrant_ids = await self._get_month_matches(entrant_id)

        progression = await asyncio.to_thread(
            compute_daily_progression, month_matches, all_entrant_ids, entrant_id
        )

        if not progression:
            log_warn(f"[graphs] _chart_points_rank_daily: no progression for entrant={entrant_id}")
            raise ValueError(
                "No game data found for this month. "
                "This player may not have completed any games yet."
            )

        days = [p["day"] for p in progression]
        points = [p["pts"] for p in progression]
        ranks = [p["rank"] or 1 for p in progression]

        buf = await asyncio.to_thread(render_daily_points_rank, days, points, ranks, name, ml)

        current_pts = int(round(points[-1]))
        current_rank = ranks[-1]
        emb = discord.Embed(title=f"\U0001f4c8 Points & Rank \u2014 {name}")
        emb.description = (
            f"Day-by-day progression for **{ml}**\n"
            f"Current: **{current_pts}** pts \u2022 Rank **#{current_rank}**"
        )
        return buf, "points_rank.png", emb

    # ------------------------------------------------------------------
    # Chart: Win Rate (day-by-day, current month)
    # ------------------------------------------------------------------

    async def _chart_winrate_daily(self, entrant_id, name, mk, ml):
        month_matches, all_entrant_ids = await self._get_month_matches(entrant_id)

        progression = await asyncio.to_thread(
            compute_daily_progression, month_matches, all_entrant_ids, entrant_id
        )

        if not progression:
            log_warn(f"[graphs] _chart_winrate_daily: no progression for entrant={entrant_id}")
            raise ValueError(
                "No game data found for this month. "
                "This player may not have completed any games yet."
            )

        days = [p["day"] for p in progression]
        win_pcts = [p["win_pct"] for p in progression]

        buf = await asyncio.to_thread(render_daily_winrate, days, win_pcts, name, ml)

        current_pct = win_pcts[-1] * 100
        emb = discord.Embed(title=f"\U0001f3af Win Rate \u2014 {name}")
        emb.description = (
            f"Day-by-day cumulative win rate for **{ml}**\n"
            f"Current: **{current_pct:.1f}%**"
        )
        return buf, "win_rate.png", emb

    # ------------------------------------------------------------------
    # Chart: All-Time Points & Rank (month-by-month)
    # ------------------------------------------------------------------

    async def _chart_points_rank_alltime(self, rows, row, uid, name, mk, ml):
        history = []
        if uid:
            history = await get_player_history(uid, firebase_id_token=FIREBASE_ID_TOKEN)

        # Append current month from live data
        rank = _rank_of_row(rows, row)
        current = {
            "month": mk,
            "pts": float(getattr(row, "pts", 0.0) or 0.0),
            "rank": rank or 0,
        }
        history = [h for h in history if h.get("month") != mk]
        history.append(current)

        months = [h["month"] for h in history]
        points = [h["pts"] for h in history]
        ranks = [h.get("rank") or 1 for h in history]

        buf = await asyncio.to_thread(render_points_rank_alltime, months, points, ranks, name)

        emb = discord.Embed(title=f"\U0001f4c8 All-Time Points & Rank \u2014 {name}")
        if len(history) <= 1:
            emb.description = (
                f"Only current month data available ({ml}). "
                "Historical data will appear after month dumps are saved."
            )
        else:
            emb.description = f"Showing **{len(history)}** months of data"
        return buf, "points_rank_alltime.png", emb

    # ------------------------------------------------------------------
    # Chart: All-Time Win Rate (month-by-month)
    # ------------------------------------------------------------------

    async def _chart_winrate_alltime(self, row, uid, name, mk, ml):
        history = []
        if uid:
            history = await get_player_history(uid, firebase_id_token=FIREBASE_ID_TOKEN)

        current = {
            "month": mk,
            "win_pct": float(getattr(row, "win_pct", 0.0) or 0.0),
        }
        history = [h for h in history if h.get("month") != mk]
        history.append(current)

        months = [h["month"] for h in history]
        win_pcts = [h.get("win_pct", 0.0) for h in history]

        buf = await asyncio.to_thread(render_winrate_alltime, months, win_pcts, name)

        current_pct = win_pcts[-1] * 100
        emb = discord.Embed(title=f"\U0001f3af All-Time Win Rate \u2014 {name}")
        if len(history) <= 1:
            emb.description = (
                f"Current win rate: **{current_pct:.1f}%** ({ml}). "
                "Historical data will appear after month dumps are saved."
            )
        else:
            emb.description = (
                f"Showing **{len(history)}** months \u2014 "
                f"Current: **{current_pct:.1f}%**"
            )
        return buf, "win_rate_alltime.png", emb


def setup(bot: commands.Bot):
    bot.add_cog(GraphsCog(bot))
