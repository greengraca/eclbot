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
from utils.settings import GUILD_ID, SUBS, FIREBASE_ID_TOKEN
from utils.monthly_config import get_bracket_id
from utils.mod_check import is_mod
from utils.dates import current_month_key, month_label as fmt_month_label
from utils.logger import log_sync, log_warn
from utils.month_dump_reader import (
    get_live_matches,
    get_league_daily_activity,
    get_league_monthly_aggregates,
    get_league_avg_daily_activity,
    compute_turn_order_stats,
    _get_current_month_matches,
)
from utils.graph_renderer import (
    render_league_activity,
    render_league_standings,
    render_league_points_distribution,
    render_league_games_distribution,
    render_league_activity_alltime,
    render_league_activity_daily_avg,
    render_league_participation_alltime,
    render_league_points_alltime,
    render_turn_order_winrates,
)


LEAGUE_CHART_CHOICES = [
    discord.OptionChoice("League Activity", "league_activity"),
    discord.OptionChoice("Standings Top 16", "standings"),
    discord.OptionChoice("Points Distribution", "points_dist"),
    discord.OptionChoice("Games Distribution", "games_dist"),
    discord.OptionChoice("Turn Order Win Rates", "turn_order"),
    discord.OptionChoice("All-Time Activity", "activity_alltime"),
    discord.OptionChoice("All-Time Participation", "participation_alltime"),
    discord.OptionChoice("All-Time Points", "points_alltime"),
    discord.OptionChoice("All-Time Turn Order", "turn_order_alltime"),
    discord.OptionChoice("All-Time Avg Games by Day", "daily_avg_alltime"),
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

        # All-time charts are mod-only
        if chart.endswith("_alltime"):
            member = ctx.author
            if not isinstance(member, discord.Member) or not is_mod(member):
                await ctx.respond("These charts are not yet available.", ephemeral=True)
                return

        await safe_ctx_defer(ctx, ephemeral=False, label="leaguegraphs")

        bracket_id = await get_bracket_id()
        if not bracket_id:
            await safe_ctx_followup(ctx, "TOPDECK_BRACKET_ID is not configured.", ephemeral=True)
            return

        mk = current_month_key()
        ml = fmt_month_label(mk)

        log_sync(f"[leaguegraphs] chart={chart}")

        try:
            if chart == "league_activity":
                buf, filename, emb = await self._chart_league_activity(mk, ml, bracket_id)
            elif chart == "standings":
                buf, filename, emb = await self._chart_standings(ml, bracket_id)
            elif chart == "points_dist":
                buf, filename, emb = await self._chart_points_distribution(ml, bracket_id)
            elif chart == "games_dist":
                buf, filename, emb = await self._chart_games_distribution(ml, bracket_id)
            elif chart == "turn_order":
                buf, filename, emb = await self._chart_turn_order(mk, ml, bracket_id)
            elif chart == "activity_alltime":
                buf, filename, emb = await self._chart_activity_alltime()
            elif chart == "participation_alltime":
                buf, filename, emb = await self._chart_participation_alltime()
            elif chart == "points_alltime":
                buf, filename, emb = await self._chart_points_alltime()
            elif chart == "turn_order_alltime":
                buf, filename, emb = await self._chart_turn_order_alltime(bracket_id)
            elif chart == "daily_avg_alltime":
                buf, filename, emb = await self._chart_activity_daily_avg()
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

        footer_label = "All Time" if chart.endswith("_alltime") else mk
        emb.set_footer(text=f"ECL \u2022 {footer_label} \u2022 /leaguegraphs")

        await safe_ctx_followup(ctx, embed=emb, file=discord.File(buf, filename=filename))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _fetch_rows(self, bracket_id: str) -> List[PlayerRow]:
        """Fetch league rows, raise on failure."""
        rows, _ = await get_league_rows_cached(bracket_id, FIREBASE_ID_TOKEN)
        if not rows:
            raise ValueError("No league data available.")
        return rows

    # ------------------------------------------------------------------
    # Chart: League Activity (stacked bar)
    # ------------------------------------------------------------------

    async def _chart_league_activity(self, mk, ml, bracket_id):
        matches, entrant_to_uid = await get_live_matches(bracket_id, FIREBASE_ID_TOKEN)
        log_sync(f"[leaguegraphs] league_activity: got {len(matches)} total matches from live data")
        month_matches, _, _ = _get_current_month_matches(matches, entrant_to_uid)
        log_sync(f"[leaguegraphs] league_activity: filtered to {len(month_matches)} for current month ({mk})")

        daily = get_league_daily_activity(month_matches)

        if not daily:
            raise ValueError("No completed games found for this month yet.")

        max_day = max(daily.keys())
        days = list(range(1, max_day + 1))
        wins = [daily.get(d, {}).get("wins", 0) for d in days]
        losses = [daily.get(d, {}).get("losses", 0) for d in days]
        draws = [daily.get(d, {}).get("draws", 0) for d in days]

        buf = await asyncio.to_thread(render_league_activity, days, wins, losses, draws, ml)

        total = sum(wins) + sum(draws)
        active_days = sum(1 for d in days if (daily.get(d, {}).get("wins", 0)
                                               + daily.get(d, {}).get("draws", 0)) > 0)
        emb = discord.Embed(title=f"\U0001f4c5 League Activity \u2014 {ml}")
        emb.description = f"**{total}** games across **{active_days}** active days"
        return buf, "league_activity.png", emb

    # ------------------------------------------------------------------
    # Chart: Standings Top 16 (horizontal bar)
    # ------------------------------------------------------------------

    async def _chart_standings(self, ml, bracket_id):
        rows = await self._fetch_rows(bracket_id)

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

    async def _chart_points_distribution(self, ml, bracket_id):
        rows = await self._fetch_rows(bracket_id)

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

    async def _chart_games_distribution(self, ml, bracket_id):
        rows = await self._fetch_rows(bracket_id)

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


    # ------------------------------------------------------------------
    # All-Time helpers
    # ------------------------------------------------------------------

    async def _fetch_aggregates(self):
        """Fetch monthly aggregates, raise on empty."""
        aggs = await get_league_monthly_aggregates(
            bracket_id=None,
            firebase_id_token=FIREBASE_ID_TOKEN,
        )
        if not aggs:
            raise ValueError("No historical data available.")
        return aggs

    # ------------------------------------------------------------------
    # Chart: All-Time Activity (bar)
    # ------------------------------------------------------------------

    async def _chart_activity_alltime(self):
        aggs = await self._fetch_aggregates()

        months = [a["month"] for a in aggs]
        month_labels = [fmt_month_label(m) for m in months]
        games = [a["total_games"] for a in aggs]

        buf = await asyncio.to_thread(render_league_activity_alltime, month_labels, games)

        total = sum(games)
        emb = discord.Embed(title="\U0001f4c8 League Activity \u2014 All Time")
        emb.description = (
            f"**{total}** total games across **{len(aggs)}** months\n"
            f"Peak: **{max(games)}** games ({month_labels[games.index(max(games))]})"
        )
        return buf, "league_activity_alltime.png", emb

    # ------------------------------------------------------------------
    # Chart: All-Time Participation (bar)
    # ------------------------------------------------------------------

    async def _chart_participation_alltime(self):
        aggs = await self._fetch_aggregates()

        months = [a["month"] for a in aggs]
        month_labels = [fmt_month_label(m) for m in months]
        players = [a["active_players"] for a in aggs]

        buf = await asyncio.to_thread(render_league_participation_alltime, month_labels, players)

        emb = discord.Embed(title="\U0001f465 Participation \u2014 All Time")
        emb.description = (
            f"Active players per month across **{len(aggs)}** months\n"
            f"Peak: **{max(players)}** players ({month_labels[players.index(max(players))]})"
        )
        return buf, "league_participation_alltime.png", emb

    # ------------------------------------------------------------------
    # Chart: All-Time Points (line with shaded range)
    # ------------------------------------------------------------------

    async def _chart_points_alltime(self):
        aggs = await self._fetch_aggregates()

        months = [a["month"] for a in aggs]
        month_labels = [fmt_month_label(m) for m in months]
        avg_pts = [a["avg_pts"] for a in aggs]
        min_pts = [a["min_pts"] for a in aggs]
        max_pts = [a["max_pts"] for a in aggs]

        buf = await asyncio.to_thread(
            render_league_points_alltime, month_labels, avg_pts, min_pts, max_pts
        )

        emb = discord.Embed(title="\U0001f4ca Points Spread \u2014 All Time")
        emb.description = (
            f"Average, min & max points across **{len(aggs)}** months\n"
            f"Latest avg: **{avg_pts[-1]:.0f}** pts "
            f"(range: {min_pts[-1]:.0f}\u2013{max_pts[-1]:.0f})"
        )
        return buf, "league_points_alltime.png", emb

    # ------------------------------------------------------------------
    # Chart: Turn Order Win Rates (current month)
    # ------------------------------------------------------------------

    async def _chart_turn_order(self, mk, ml, bracket_id):
        matches, entrant_to_uid = await get_live_matches(bracket_id, FIREBASE_ID_TOKEN)
        month_matches, _, _ = _get_current_month_matches(matches, entrant_to_uid)

        stats = compute_turn_order_stats(month_matches)
        if stats["total_pods"] == 0:
            raise ValueError("No completed 4-player pods found for this month yet.")

        buf = await asyncio.to_thread(
            render_turn_order_winrates,
            stats["turn_rates"],
            stats["draw_rate"],
            stats["turn_wins"],
            stats["draws"],
            stats["total_pods"],
            f"ECL Turn Order Win Rates \u2014 {ml}",
        )

        emb = discord.Embed(title=f"\U0001f3b2 Turn Order Win Rates \u2014 {ml}")
        rates_str = " / ".join(f"{r*100:.1f}%" for r in stats["turn_rates"])
        emb.description = (
            f"**{stats['total_pods']}** completed pods "
            f"({stats['completed_pods']} decisive, {stats['draws']} draws)\n"
            f"Seat 1\u20134: {rates_str} · Draw: {stats['draw_rate']*100:.1f}%"
        )
        return buf, "turn_order.png", emb

    # ------------------------------------------------------------------
    # Chart: Turn Order Win Rates (all-time)
    # ------------------------------------------------------------------

    async def _chart_turn_order_alltime(self, bracket_id):
        from utils.month_dump_reader import (
            get_historical_months,
            reassemble_month_dump,
            _rebuild_matches_from_dump,
            _fetch_entrant_to_uid_for_bracket,
        )

        months_info = await get_historical_months(bracket_id=None)

        all_turn_wins = [0, 0, 0, 0]
        all_draws = 0
        all_completed = 0

        sem = asyncio.Semaphore(3)

        async def _process(month_info):
            async with sem:
                dump = await reassemble_month_dump(month_info)
                if dump is None:
                    return None
                e2u = dump.get("entrant_to_uid")
                if not e2u:
                    bid = dump.get("bracket_id") or month_info.get("bracket_id", "")
                    if bid:
                        fetched = await _fetch_entrant_to_uid_for_bracket(bid, FIREBASE_ID_TOKEN)
                        if fetched:
                            dump["entrant_to_uid"] = {str(k): v for k, v in fetched.items()}
                matches = _rebuild_matches_from_dump(dump)
                return compute_turn_order_stats(matches)

        tasks = [_process(m) for m in months_info]
        results = await asyncio.gather(*tasks)

        for r in results:
            if r is None:
                continue
            for i in range(4):
                all_turn_wins[i] += r["turn_wins"][i]
            all_draws += r["draws"]
            all_completed += r["completed_pods"]

        # Also add current month
        try:
            matches, e2u = await get_live_matches(bracket_id, FIREBASE_ID_TOKEN)
            month_matches, _, _ = _get_current_month_matches(matches, e2u)
            current = compute_turn_order_stats(month_matches)
            for i in range(4):
                all_turn_wins[i] += current["turn_wins"][i]
            all_draws += current["draws"]
            all_completed += current["completed_pods"]
        except Exception:
            pass

        total = all_completed + all_draws
        if total == 0:
            raise ValueError("No historical turn order data found.")

        all_rates = [(w / total) for w in all_turn_wins]
        all_draw_rate = all_draws / total

        buf = await asyncio.to_thread(
            render_turn_order_winrates,
            all_rates, all_draw_rate, all_turn_wins, all_draws, total,
            "ECL Turn Order Win Rates \u2014 All Time",
        )

        emb = discord.Embed(title="\U0001f3b2 Turn Order Win Rates \u2014 All Time")
        rates_str = " / ".join(f"{r*100:.1f}%" for r in all_rates)
        emb.description = (
            f"**{total}** pods across **{len(months_info) + 1}** months\n"
            f"Seat 1\u20134: {rates_str} · Draw: {all_draw_rate*100:.1f}%"
        )
        return buf, "turn_order_alltime.png", emb


    # ------------------------------------------------------------------
    # Chart: All-Time Avg Games by Day of Month
    # ------------------------------------------------------------------

    async def _chart_activity_daily_avg(self):
        avg = await get_league_avg_daily_activity(
            bracket_id=None,
            firebase_id_token=FIREBASE_ID_TOKEN,
        )

        if not avg:
            raise ValueError("No historical data available.")

        days = sorted(avg.keys())
        avg_games = [avg[d] for d in days]

        buf = await asyncio.to_thread(render_league_activity_daily_avg, days, avg_games)

        overall_avg = sum(avg_games) / len(avg_games)
        peak_day = days[avg_games.index(max(avg_games))]
        emb = discord.Embed(title="\U0001f4c5 Avg Games by Day of Month \u2014 All Time")
        emb.description = (
            f"Average across all historical months\n"
            f"Overall avg: **{overall_avg:.1f}** games/day \u2022 "
            f"Peak: day **{peak_day}** ({max(avg_games):.1f} avg)"
        )
        return buf, "league_daily_avg_alltime.png", emb


def setup(bot: commands.Bot):
    bot.add_cog(LeagueGraphsCog(bot))
