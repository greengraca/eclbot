# cogs/topdeck_league.py
import os
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict

import discord
from discord.ext import commands

from topdeck_fetch import get_league_rows_cached, PlayerRow
from online_games_store import has_recent_game_by_topdeck_uid, is_recency_active
from utils.top16_eligibility import is_top16_eligible, needs_recency_check
from utils.topdeck_identity import find_row_for_member, build_member_index, resolve_row_discord_id
from utils.logger import log_sync, log_warn
from utils.interactions import safe_ctx_defer, safe_ctx_followup
from utils.mod_check import is_mod
from utils.settings import GUILD_ID, load_subs_config


from utils.monthly_config import get_bracket_id, get_mostgames_image

FIREBASE_ID_TOKEN = os.getenv("FIREBASE_ID_TOKEN", None)


def _ts(dt: datetime) -> int:
    return int(dt.timestamp())


def _month_start_utc() -> datetime:
    now = datetime.now(timezone.utc)
    return datetime(now.year, now.month, 1, tzinfo=timezone.utc)


async def _get_member_index(guild: discord.Guild):
    # Prefer cache (fast). If empty, fetch from API.
    members = list(getattr(guild, "members", []) or [])
    if not members:
        try:
            members = [m async for m in guild.fetch_members(limit=None)]
            log_sync(f"[topdeck] fetched {len(members)} members for identity index (cache was empty).")
        except Exception as e:
            log_warn(f"[topdeck] fetch_members failed; using empty index: {type(e).__name__}: {e}")
            members = []
    return build_member_index(members)


class TopdeckLeagueCog(commands.Cog):
    """Slash commands that expose TopDeck league stats inside Discord."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # All caching now lives in topdeck_fetch.get_league_rows_cached

    # ------------- helpers -------------

    async def _load_rows(
        self,
        force_refresh: bool = False,
    ) -> Tuple[List[PlayerRow], datetime]:
        """
        Returns (rows, fetched_at), using the shared cache in topdeck_fetch.
        """
        bracket_id = await get_bracket_id()
        if not bracket_id:
            raise RuntimeError(
                "TOPDECK_BRACKET_ID is not configured (no DB config or environment variable)."
            )

        rows, fetched_at = await get_league_rows_cached(
            bracket_id,
            FIREBASE_ID_TOKEN,
            force_refresh=force_refresh,
        )
        return rows, fetched_at

    @staticmethod
    def _find_author_row(
        member: discord.Member,
        rows: List[PlayerRow],
    ) -> Optional[PlayerRow]:
        """Match the Discord member to a TopDeck PlayerRow.

        Prefer Discord ID stored in row.discord (mention/raw digits). Then fall back
        to unique handle match, then unique name match.
        """

        m = find_row_for_member(rows, member)
        if not m:
            return None

        # Lightweight debug signal for confidence (helps spot bad TopDeck discord fields)
        try:
            log_sync(
                f"[topdeck] author row match: "
                f"member_id={member.id} conf={m.confidence} key={m.matched_key!r} detail={m.detail}"
            )
        except Exception:
            pass

        return m.row

    # ------------- /mostgames -------------

    @commands.slash_command(
        name="mostgames",
        description="Show Top 5 players with the most games in the current bracket.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def mostgames(self, ctx: discord.ApplicationContext):
        # Server only
        if ctx.guild is None:
            await ctx.respond(
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        # Defer as NON-ephemeral so we can send a public embed first
        await safe_ctx_defer(ctx, ephemeral=False)

        try:
            rows, fetched_at = await self._load_rows()
        except Exception as e:
            log_warn(f"[topdeck] /mostgames fetch error: {type(e).__name__}: {e}")
            await safe_ctx_followup(ctx,
                "I couldn't fetch TopDeck data right now. "
                "Please try again in a bit.",
                ephemeral=True,
            )
            return

        if not rows:
            await safe_ctx_followup(ctx,
                "I couldn't find any players in this bracket.",
                ephemeral=True,
            )
            return

        top5 = sorted(rows, key=lambda r: (-r.games, -r.pts))[:5]

        ts_int = _ts(fetched_at)

        # ---- Public embed ----
        embed = discord.Embed(
            title="Most Games Played — Prize Chance Leaderboard",
            description=(
                "Top 5 by total games played.\n"
                "-# If a player advances to the finals, they forfeit their access to the raffle."
            ),
        )

        mostgames_img = await get_mostgames_image()
        if mostgames_img:
            embed.set_thumbnail(url=mostgames_img)

        if not top5:
            embed.add_field(
                name="Leaderboard",
                value="No data yet.",
                inline=False,
            )
        else:
            index = await _get_member_index(ctx.guild)

            lines = []
            for i, r in enumerate(top5, start=1):
                res = resolve_row_discord_id(r, index)
                tag = f"<@{res.discord_id}>" if res.discord_id else "`(unmapped)`"
                dropped_suffix = " *(dropped)*" if r.dropped else ""
                lines.append(f"`#{i}` {tag} — {r.name} — **{r.games}** games{dropped_suffix}")
            
            embed.add_field(name="Leaderboard", value="\n".join(lines), inline=False)

        await safe_ctx_followup(ctx,embed=embed)

        # ---- Ephemeral personal message ----
        member = ctx.author
        row_for_author = (
            self._find_author_row(member, rows)
            if isinstance(member, discord.Member)
            else None
        )

        if row_for_author:
            if top5:
                threshold_games = top5[-1].games
                games_diff = threshold_games - row_for_author.games + 1
            else:
                threshold_games = 0
                games_diff = 0

            if not top5:
                gap_line = "There isn't an eligible Top 5 yet."
            elif games_diff <= 0:
                gap_line = "You're already in, or tied with, the eligible Top 5 by games. Congrats!"
            else:
                gap_line = f"You're **{games_diff}** game(s) away from entering the eligible Top 5 by games."

            personal_msg = (
                f"You have **{row_for_author.games}** games in this bracket "
                f"({row_for_author.wins}W / {row_for_author.draws}D / {row_for_author.losses}L).\n"
                f"{gap_line}\n\n"
                f"TopDeck data: last updated <t:{ts_int}:R>."
            )
        else:
            personal_msg = (
                "I couldn't find you in this bracket's data. "
                "Please check that your Discord on TopDeck is up to date.\n\n"
                f"TopDeck data: last updated <t:{ts_int}:R>."
            )

        await safe_ctx_followup(ctx,personal_msg, ephemeral=True)

    # ------------- /top16 -------------

    @commands.slash_command(
        name="top16",
        description="Show Top 16 qualified players (>= 10 games).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def top16(self, ctx: discord.ApplicationContext):
        if ctx.guild is None:
            try:
                await ctx.respond("This command can only be used in a server.", ephemeral=True)
            except Exception:
                pass
            return

        # --- SAFELY ACK THE INTERACTION ASAP ---
        try:
            await safe_ctx_defer(ctx, ephemeral=False)
        except discord.errors.NotFound:
            # Interaction already expired (bot lagged). Nothing we can do.
            log_warn("[topdeck] /top16 interaction expired before defer (Unknown interaction).")
            return
        except discord.errors.HTTPException as e:
            log_warn(f"[topdeck] /top16 failed to defer: {type(e).__name__}: {e}")
            return

        async def _safe_followup_send(
            content: Optional[str] = None,
            *,
            embed: Optional[discord.Embed] = None,
            ephemeral: bool,
        ):
            await safe_ctx_followup(ctx, content=content, embed=embed, ephemeral=ephemeral)

        # Load league rows
        try:
            rows, fetched_at = await self._load_rows()
        except Exception as e:
            log_warn(f"[topdeck] /top16 fetch error: {type(e).__name__}: {e}")
            await _safe_followup_send(
                "I couldn't fetch TopDeck data right now. Please try again in a bit.",
                ephemeral=True,
            )
            return

        if not rows:
            await _safe_followup_send("I couldn't find any players in this bracket.", ephemeral=True)
            return

        # ---- Step 1: active players with >= min total games ----
        cfg = load_subs_config()
        active_by_games = [r for r in rows if (not r.dropped) and (r.games >= cfg.top16_min_total_games)]
        active_by_games = sorted(active_by_games, key=lambda r: (-r.pts, -r.games))

        log_sync(f"[topdeck] Active players with >={cfg.top16_min_total_games} games: {len(active_by_games)}.")

        # ---- Step 2: qualified = total games + recency (any game) ----
        min_total = cfg.top16_min_total_games
        no_recency_threshold = cfg.top16_min_online_games_no_recency  # 20, total-games threshold
        recency_after_day = cfg.top16_recency_after_day

        ms = _month_start_utc()
        bracket_id = await get_bracket_id()
        recency_active = is_recency_active(ms.year, ms.month, recency_after_day)

        uids_need_recency: List[str] = []
        if recency_active:
            for r in active_by_games:
                uid = (r.uid or "").strip()
                if uid and needs_recency_check(r.games, min_total, no_recency_threshold):
                    uids_need_recency.append(uid)

        recency_check: Dict[str, bool] = {}
        if uids_need_recency:
            try:
                recency_check = await has_recent_game_by_topdeck_uid(
                    bracket_id, ms.year, ms.month, uids_need_recency,
                    after_day=recency_after_day, online_only=False,
                )
            except Exception as e:
                log_warn(f"[topdeck] /top16 recency check error: {type(e).__name__}: {e}")

        qualified_candidates: List[PlayerRow] = []
        failed_recency: List[PlayerRow] = []
        for r in active_by_games:
            uid = (r.uid or "").strip()
            if not uid:
                continue
            if is_top16_eligible(
                dropped=False, total_games=r.games, has_recent=recency_check.get(uid, False),
                recency_active=recency_active, min_total=min_total, no_recency_games=no_recency_threshold,
            ):
                qualified_candidates.append(r)
            elif recency_active and needs_recency_check(r.games, min_total, no_recency_threshold):
                failed_recency.append(r)

        log_sync(f"[topdeck] Players qualified: {len(qualified_candidates)}.")
        log_sync(f"[topdeck] Players failed recency: {len(failed_recency)}.")

        qualified_top16: List[PlayerRow] = qualified_candidates[:16]

        if len(qualified_top16) < 16:
            log_warn("[topdeck] WARNING: fewer than 16 players meet the games + recency requirement.")

        # ---- Step 3: personal info ----
        member = ctx.author
        row_for_author = (
            self._find_author_row(member, rows)
            if isinstance(member, discord.Member)
            else None
        )

        ts_int = _ts(fetched_at)

        if row_for_author:
            author_uid = (row_for_author.uid or "").strip()
            total_games = row_for_author.games

            if total_games >= no_recency_threshold:
                missing_msg = (
                    f"You're already eligible: you have **{total_games}** games.\n"
                    f"(TopDeck record: {row_for_author.wins}W / {row_for_author.draws}D / {row_for_author.losses}L.)\n\n"
                    f"TopDeck data: last updated <t:{ts_int}:R>."
                )
            elif total_games >= min_total:
                if not recency_active:
                    missing_msg = (
                        f"You're already eligible: you have **{total_games}** games. "
                        f"After day {recency_after_day}, you'll also need at least 1 game played that day or later "
                        f"(or reach {no_recency_threshold}+ games to skip the recency requirement).\n"
                        f"(TopDeck record: {row_for_author.wins}W / {row_for_author.draws}D / {row_for_author.losses}L.)\n\n"
                        f"TopDeck data: last updated <t:{ts_int}:R>."
                    )
                elif recency_check.get(author_uid, False):
                    missing_msg = (
                        f"You're already eligible: you have **{total_games}** games "
                        f"and a game after day {recency_after_day}.\n"
                        f"(TopDeck record: {row_for_author.wins}W / {row_for_author.draws}D / {row_for_author.losses}L.)\n\n"
                        f"TopDeck data: last updated <t:{ts_int}:R>."
                    )
                else:
                    missing_msg = (
                        f"You have **{total_games}** games but **no game after day {recency_after_day}**.\n"
                        f"Play at least 1 game after day {recency_after_day} to be eligible, "
                        f"or reach {no_recency_threshold}+ games to skip the recency requirement.\n"
                        f"(TopDeck record: {row_for_author.wins}W / {row_for_author.draws}D / {row_for_author.losses}L.)\n\n"
                        f"TopDeck data: last updated <t:{ts_int}:R>."
                    )
            else:
                missing = min_total - total_games
                missing_msg = (
                    f"You have **{total_games}** games. "
                    f"You need **{missing}** more game(s) to be eligible.\n"
                    f"(TopDeck record: {row_for_author.wins}W / {row_for_author.draws}D / {row_for_author.losses}L.)\n\n"
                    f"TopDeck data: last updated <t:{ts_int}:R>."
                )
        else:
            missing_msg = (
                "I couldn't find you in this bracket's data. "
                "Please check that your Discord on TopDeck is up to date.\n\n"
                f"TopDeck data: last updated <t:{ts_int}:R>."
            )

        # ---- Warning: players bumped from top16 by recency ----
        bumped_from_top16: List[PlayerRow] = []
        if failed_recency:
            all_in_band = sorted(
                qualified_candidates + failed_recency,
                key=lambda r: (-r.pts, -r.games),
            )
            for r in all_in_band[:16]:
                if r in failed_recency:
                    bumped_from_top16.append(r)

        caller_is_mod = is_mod(member if isinstance(member, discord.Member) else None)

        if bumped_from_top16 and caller_is_mod:
            bump_lines = []
            for r in bumped_from_top16:
                bump_lines.append(f"\u2022 {r.name} \u2014 {int(round(r.pts))} pts, {r.games} games")
            missing_msg += (
                f"\n\n\u26a0\ufe0f **Players excluded from Top 16 due to recency** "
                f"(no game after day {recency_after_day}):\n"
                + "\n".join(bump_lines)
            )
        elif bumped_from_top16 and row_for_author and row_for_author in bumped_from_top16:
            missing_msg += (
                f"\n\n\u26a0\ufe0f **You would be in the Top 16 by points, but you have no game "
                f"after day {recency_after_day}.** Play at least 1 game after day "
                f"{recency_after_day} to qualify, or reach {no_recency_threshold}+ games."
            )

        # ---- Step 4: public Top 16 ----
        if not qualified_top16:
            await _safe_followup_send(
                "There are no players with at least 10 games in this bracket yet.",
                ephemeral=False,
            )
        else:
            embed = discord.Embed(
                title="Top 16 \u2014 Qualified Players",
                description="Players with **>= 10 games** in the current bracket.",
            )
            index = await _get_member_index(ctx.guild)

            lines = []
            for i, r in enumerate(qualified_top16, start=1):
                res = resolve_row_discord_id(r, index)
                tag = f"<@{res.discord_id}>" if res.discord_id else "`(unmapped)`"
                lines.append(f"`#{i:02}` {tag} \u2014 {r.name} - {int(round(r.pts))} pts")


            embed.add_field(name="Leaderboard", value="\n".join(lines), inline=False)
            await _safe_followup_send(embed=embed, ephemeral=False)

        # ---- Step 5: ephemeral message just for the caller ----
        await _safe_followup_send(missing_msg, ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(TopdeckLeagueCog(bot))
