"""/graphs — Beautiful player charts sent as Discord image attachments.

Chart types:
  - Season Record (donut)
  - Monthly Activity (stacked bar by day)
  - Points & Rank Progression (dual-axis line, multi-month)
  - Win Rate Trend (line, multi-month)
"""

from __future__ import annotations

import asyncio
from typing import List, Optional

import discord
from discord.ext import commands
from discord import Option

from topdeck_fetch import PlayerRow, get_league_rows_cached
from utils.topdeck_identity import find_row_for_member
from utils.interactions import safe_ctx_defer, safe_ctx_followup
from utils.settings import GUILD_ID, SUBS, TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN
from utils.dates import current_month_key, month_label as fmt_month_label
from utils.month_dump_reader import get_player_history, get_daily_games
from utils.graph_renderer import (
    render_points_rank,
    render_daily_activity,
    render_win_rate_trend,
    render_season_record,
)


CHART_CHOICES = [
    discord.OptionChoice("Season Record", "record"),
    discord.OptionChoice("Monthly Activity", "activity"),
    discord.OptionChoice("Points & Rank Progression", "points_rank"),
    discord.OptionChoice("Win Rate Trend", "winrate"),
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

        try:
            rows, _ = await get_league_rows_cached(TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN)
        except Exception as e:
            await safe_ctx_followup(ctx, f"Couldn't fetch TopDeck data ({type(e).__name__}).", ephemeral=True)
            return

        match = find_row_for_member(rows or [], target)
        row: Optional[PlayerRow] = match.row if match else None

        if not row:
            await safe_ctx_followup(
                ctx,
                f"I couldn't find a TopDeck profile for **{target.display_name}**. "
                "Make sure your TopDeck discord field contains your Discord ID or username.",
                ephemeral=True,
            )
            return

        if int(getattr(row, "games", 0) or 0) == 0:
            await safe_ctx_followup(
                ctx,
                f"**{target.display_name}** hasn't played any games yet this season.",
                ephemeral=True,
            )
            return

        mk = current_month_key()
        ml = fmt_month_label(mk)
        uid = (getattr(row, "uid", None) or "").strip()

        try:
            if chart == "record":
                buf, filename, emb = await self._chart_record(row, target.display_name, ml)
            elif chart == "activity":
                buf, filename, emb = await self._chart_activity(row, target.display_name, mk, ml)
            elif chart == "points_rank":
                buf, filename, emb = await self._chart_points_rank(rows, row, uid, target.display_name, mk, ml)
            elif chart == "winrate":
                buf, filename, emb = await self._chart_winrate(rows, row, uid, target.display_name, mk, ml)
            else:
                await safe_ctx_followup(ctx, "Unknown chart type.", ephemeral=True)
                return
        except Exception as e:
            await safe_ctx_followup(ctx, f"Error generating chart: {type(e).__name__}: {e}", ephemeral=True)
            return

        emb.set_image(url=f"attachment://{filename}")
        emb.color = int(getattr(SUBS, "embed_color", 0x2ECC71) or 0x2ECC71)

        thumb_url = getattr(SUBS, "embed_thumbnail_url", "") or ""
        if thumb_url.startswith(("http://", "https://")):
            emb.set_thumbnail(url=thumb_url)

        emb.set_footer(text=f"ECL \u2022 {mk} \u2022 /graphs")

        await safe_ctx_followup(ctx, embed=emb, file=discord.File(buf, filename=filename))

    async def _chart_record(self, row, name, ml):
        wins = int(getattr(row, "wins", 0) or 0)
        losses = int(getattr(row, "losses", 0) or 0)
        draws = int(getattr(row, "draws", 0) or 0)

        buf = await asyncio.to_thread(render_season_record, wins, losses, draws, name, ml)
        filename = "season_record.png"

        emb = discord.Embed(title=f"\U0001f4ca Season Record \u2014 {name}")
        emb.description = f"**{wins}**W / **{losses}**L / **{draws}**D \u2014 {ml}"

        return buf, filename, emb

    async def _chart_activity(self, row, name, mk, ml):
        entrant_id = int(getattr(row, "entrant_id", 0) or 0)

        daily = await get_daily_games(TOPDECK_BRACKET_ID, mk, entrant_id)

        if not daily:
            raise ValueError("No daily game data found for this month.")

        max_day = max(daily.keys())
        days = list(range(1, max_day + 1))
        wins = [daily.get(d, {}).get("wins", 0) for d in days]
        losses = [daily.get(d, {}).get("losses", 0) for d in days]
        draws = [daily.get(d, {}).get("draws", 0) for d in days]

        buf = await asyncio.to_thread(render_daily_activity, days, wins, losses, draws, name, ml)
        filename = "daily_activity.png"

        total = sum(wins) + sum(losses) + sum(draws)
        active_days = sum(1 for d in days if daily.get(d, {}).get("wins", 0) + daily.get(d, {}).get("losses", 0) + daily.get(d, {}).get("draws", 0) > 0)
        emb = discord.Embed(title=f"\U0001f4c5 Monthly Activity \u2014 {name}")
        emb.description = f"**{total}** games across **{active_days}** active days \u2014 {ml}"

        return buf, filename, emb

    async def _chart_points_rank(self, rows, row, uid, name, mk, ml):
        history = []
        if uid:
            history = await get_player_history(uid, TOPDECK_BRACKET_ID)

        # Append current month from live data
        rank = _rank_of_row(rows, row)
        current = {
            "month": mk,
            "pts": float(getattr(row, "pts", 0.0) or 0.0),
            "rank": rank or 0,
        }

        # Deduplicate: remove current month from history if present
        history = [h for h in history if h.get("month") != mk]
        history.append(current)

        months = [h["month"] for h in history]
        points = [h["pts"] for h in history]
        ranks = [h.get("rank") or 0 for h in history]

        # Replace 0 ranks with None-safe values for display
        ranks = [r if r > 0 else 1 for r in ranks]

        buf = await asyncio.to_thread(render_points_rank, months, points, ranks, name)
        filename = "points_rank.png"

        emb = discord.Embed(title=f"\U0001f4c8 Points & Rank Progression \u2014 {name}")
        if len(history) <= 1:
            emb.description = f"Only current month data available ({ml}). Historical data will appear after month dumps are saved."
        else:
            emb.description = f"Showing **{len(history)}** months of data"

        return buf, filename, emb

    async def _chart_winrate(self, rows, row, uid, name, mk, ml):
        history = []
        if uid:
            history = await get_player_history(uid, TOPDECK_BRACKET_ID)

        # Append current month
        current = {
            "month": mk,
            "win_pct": float(getattr(row, "win_pct", 0.0) or 0.0),
        }
        history = [h for h in history if h.get("month") != mk]
        history.append(current)

        months = [h["month"] for h in history]
        win_pcts = [h.get("win_pct", 0.0) for h in history]

        buf = await asyncio.to_thread(render_win_rate_trend, months, win_pcts, name)
        filename = "win_rate.png"

        emb = discord.Embed(title=f"\U0001f3af Win Rate Trend \u2014 {name}")
        current_pct = win_pcts[-1] * 100 if win_pcts else 0
        if len(history) <= 1:
            emb.description = f"Current win rate: **{current_pct:.1f}%** ({ml}). Historical data will appear after month dumps are saved."
        else:
            emb.description = f"Showing **{len(history)}** months \u2014 Current: **{current_pct:.1f}%**"

        return buf, filename, emb


def setup(bot: commands.Bot):
    bot.add_cog(GraphsCog(bot))
