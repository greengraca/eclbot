"""/leaguegraphs — League-wide charts for the current month.

Chart types:
  - League Activity     — stacked bar (W/L/D) of total games per day
  - Standings Top 16    — horizontal bar of top 16 players by points
  - Points Distribution — histogram of all player points
  - Games Distribution  — histogram of games played per player
"""

from __future__ import annotations

import asyncio
from typing import List, Optional, Tuple

import discord
from discord.ext import commands
from discord import Option

from topdeck_fetch import PlayerRow, get_league_rows_cached
from utils.interactions import safe_ctx_defer, safe_ctx_followup
from utils.settings import GUILD_ID, SUBS, TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN
from utils.dates import current_month_key, month_label as fmt_month_label
from utils.logger import log_sync, log_warn
from utils.month_dump_reader import (
    get_live_matches,
    get_league_daily_activity,
    _get_current_month_matches,
)
from utils.graph_renderer import (
    render_league_activity,
    render_league_standings,
    render_league_points_distribution,
    render_league_games_distribution,
)


LEAGUE_CHART_CHOICES = [
    discord.OptionChoice("League Activity", "league_activity"),
    discord.OptionChoice("Standings Top 16", "standings"),
    discord.OptionChoice("Points Distribution", "points_dist"),
    discord.OptionChoice("Games Distribution", "games_dist"),
]


class LeagueGraphsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.slash_command(
        name="leaguegraphs",
        description="Generate league-wide charts for the current month.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def leaguegraphs(
        self,
        ctx: discord.ApplicationContext,
        chart: str = Option(
            str,
            "Chart type",
            choices=LEAGUE_CHART_CHOICES,
            required=True,
        ),
    ):
        if ctx.guild is None:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        await safe_ctx_defer(ctx, ephemeral=False, label="leaguegraphs")

        if not TOPDECK_BRACKET_ID:
            await safe_ctx_followup(ctx, "TOPDECK_BRACKET_ID is not configured.", ephemeral=True)
            return

        mk = current_month_key()
        ml = fmt_month_label(mk)

        log_sync(f"[leaguegraphs] chart={chart}")

        try:
            if chart == "league_activity":
                buf, filename, emb = await self._chart_league_activity(mk, ml)
            elif chart == "standings":
                buf, filename, emb = await self._chart_standings(ml)
            elif chart == "points_dist":
                buf, filename, emb = await self._chart_points_distribution(ml)
            elif chart == "games_dist":
                buf, filename, emb = await self._chart_games_distribution(ml)
            else:
                await safe_ctx_followup(ctx, "Unknown chart type.", ephemeral=True)
                return
        except Exception as e:
            log_warn(f"[leaguegraphs] Error generating chart={chart}: "
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

        emb.set_footer(text=f"ECL \u2022 {mk} \u2022 /leaguegraphs")

        await safe_ctx_followup(ctx, embed=emb, file=discord.File(buf, filename=filename))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _fetch_rows(self) -> List[PlayerRow]:
        """Fetch league rows, raise on failure."""
        rows, _ = await get_league_rows_cached(TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN)
        if not rows:
            raise ValueError("No league data available.")
        return rows

    # ------------------------------------------------------------------
    # Chart: League Activity (stacked bar)
    # ------------------------------------------------------------------

    async def _chart_league_activity(self, mk, ml):
        matches, entrant_to_uid = await get_live_matches(TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN)
        month_matches, _, _ = _get_current_month_matches(matches, entrant_to_uid)

        daily = get_league_daily_activity(month_matches)

        if not daily:
            raise ValueError("No completed games found for this month yet.")

        max_day = max(daily.keys())
        days = list(range(1, max_day + 1))
        wins = [daily.get(d, {}).get("wins", 0) for d in days]
        losses = [daily.get(d, {}).get("losses", 0) for d in days]
        draws = [daily.get(d, {}).get("draws", 0) for d in days]

        buf = await asyncio.to_thread(render_league_activity, days, wins, losses, draws, ml)

        total = sum(wins) + sum(losses) + sum(draws)
        active_days = sum(1 for d in days if (daily.get(d, {}).get("wins", 0)
                                               + daily.get(d, {}).get("losses", 0)
                                               + daily.get(d, {}).get("draws", 0)) > 0)
        emb = discord.Embed(title=f"\U0001f4c5 League Activity \u2014 {ml}")
        emb.description = f"**{total}** total results across **{active_days}** active days"
        return buf, "league_activity.png", emb

    # ------------------------------------------------------------------
    # Chart: Standings Top 16 (horizontal bar)
    # ------------------------------------------------------------------

    async def _chart_standings(self, ml):
        rows = await self._fetch_rows()

        # Sort by points descending, take top 16 with games > 0
        ranked = sorted(
            [r for r in rows if int(getattr(r, "games", 0) or 0) > 0],
            key=lambda r: (
                -float(getattr(r, "pts", 0.0) or 0.0),
                -float(getattr(r, "ow_pct", 0.0) or 0.0),
                -float(getattr(r, "win_pct", 0.0) or 0.0),
            ),
        )
        top = ranked[:16]

        if not top:
            raise ValueError("No players with games found.")

        names = [getattr(r, "name", "?") for r in top]
        points = [float(getattr(r, "pts", 0.0) or 0.0) for r in top]

        buf = await asyncio.to_thread(render_league_standings, names, points, ml)

        emb = discord.Embed(title=f"\U0001f3c6 Standings \u2014 Top {len(top)}")
        emb.description = (
            f"Top {len(top)} players by points \u2014 {ml}\n"
            f"Leader: **{names[0]}** ({points[0]:.0f} pts)"
        )
        return buf, "league_standings.png", emb

    # ------------------------------------------------------------------
    # Chart: Points Distribution (histogram)
    # ------------------------------------------------------------------

    async def _chart_points_distribution(self, ml):
        rows = await self._fetch_rows()

        # Include all players with games
        pts_list = [
            float(getattr(r, "pts", 0.0) or 0.0)
            for r in rows
            if int(getattr(r, "games", 0) or 0) > 0
        ]

        if not pts_list:
            raise ValueError("No players with games found.")

        buf = await asyncio.to_thread(render_league_points_distribution, pts_list, ml)

        emb = discord.Embed(title=f"\U0001f4ca Points Distribution \u2014 {ml}")
        avg_pts = sum(pts_list) / len(pts_list)
        emb.description = (
            f"**{len(pts_list)}** players \u2022 "
            f"Avg: **{avg_pts:.0f}** pts \u2022 "
            f"Range: **{min(pts_list):.0f}** \u2013 **{max(pts_list):.0f}**"
        )
        return buf, "points_distribution.png", emb

    # ------------------------------------------------------------------
    # Chart: Games Distribution (histogram)
    # ------------------------------------------------------------------

    async def _chart_games_distribution(self, ml):
        rows = await self._fetch_rows()

        games_list = [
            int(getattr(r, "games", 0) or 0)
            for r in rows
            if int(getattr(r, "games", 0) or 0) > 0
        ]

        if not games_list:
            raise ValueError("No players with games found.")

        buf = await asyncio.to_thread(render_league_games_distribution, games_list, ml)

        emb = discord.Embed(title=f"\U0001f3ae Games Distribution \u2014 {ml}")
        avg_games = sum(games_list) / len(games_list)
        emb.description = (
            f"**{len(games_list)}** players \u2022 "
            f"Avg: **{avg_games:.1f}** games \u2022 "
            f"Range: **{min(games_list)}** \u2013 **{max(games_list)}**"
        )
        return buf, "games_distribution.png", emb


def setup(bot: commands.Bot):
    bot.add_cog(LeagueGraphsCog(bot))
