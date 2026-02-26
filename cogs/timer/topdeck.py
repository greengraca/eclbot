# cogs/timer/topdeck.py
"""TopDeck online game tagging for the timer cog.

Matches voice channels to in-progress TopDeck pods and marks games as online.
Also handles Bring a Friend Treasure Pod detection.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, List, Optional

import discord

from topdeck_fetch import get_in_progress_pods, InProgressPod
from online_games_store import OnlineGameRecord, get_record, upsert_record
from db import treasure_pod_schedule, treasure_pods as treasure_pods_col

from utils.logger import log_sync, log_ok, log_warn, log_debug
from utils.treasure_pods import TreasurePodManager
from utils.dates import month_key

from .helpers import norm_member_handles, month_start_utc

if TYPE_CHECKING:
    from ..timer_cog import ECLTimerCog


class TopDeckTagger:
    """Handles matching voice channels to TopDeck pods and tagging games as online."""

    def __init__(
        self,
        cog: "ECLTimerCog",
        bracket_id: str,
        firebase_token: Optional[str] = None,
    ):
        self.cog = cog
        self.bracket_id = bracket_id
        self.firebase_token = firebase_token
        self._lock = asyncio.Lock()
        self._treasure_manager = TreasurePodManager(treasure_pod_schedule, treasure_pods_col)

    async def match_vc_to_pod(
        self,
        voice_channel: discord.VoiceChannel,
        members: List[discord.Member],
    ) -> Optional[InProgressPod]:
        """
        Compare VC member handles with TopDeck in-progress pods.

        - Normalize Discord names (a-z0-9 only)
        - Allow extra people in VC (judge/spectator)
        - Require good overlap: at least 3 shared names AND >=75% of the pod
        """
        handles: set[str] = set()
        for m in members:
            if m.bot:
                continue
            handles |= norm_member_handles(m)

        handles_sorted = sorted(handles)
        log_sync(
            f"[timer/topdeck] VC {voice_channel.name} -> "
            f"vc_handles={handles_sorted}"
        )

        if not handles:
            log_sync(
                f"[timer/topdeck] VC {voice_channel.name} has no usable handles; "
                "skipping TopDeck match."
            )
            return None

        if not self.bracket_id:
            log_sync("[timer/topdeck] TOPDECK_BRACKET_ID not set; skipping lookup.")
            return None

        pods = await get_in_progress_pods(self.bracket_id, self.firebase_token)
        log_sync(
            f"[timer/topdeck] get_in_progress_pods returned {len(pods)} pods "
            f"for bracket={self.bracket_id!r}."
        )

        if not pods:
            return None

        # Debug log every pod we got from TopDeck
        for pod in pods:
            pod_norm = list(getattr(pod, "entrant_discords_norm", []) or [])
            pod_raw = list(getattr(pod, "entrant_discords_raw", []) or [])
            pod_uids = list(getattr(pod, "entrant_uids", []) or [])
            pod_eids = list(getattr(pod, "entrant_ids", []) or [])

            pod_handles_set = {h for h in pod_norm if h}
            log_debug(
                "[timer/topdeck] Pod "
                f"S{pod.season}:T{pod.table} | "
                f"norm_handles={sorted(pod_handles_set)} | "
                f"raw_discords={pod_raw} | "
                f"uids={pod_uids} | "
                f"eids={pod_eids} | "
                f"start={pod.start}"
            )

        best: Optional[InProgressPod] = None
        best_score: Optional[tuple] = None  # (coverage, inter_count, start_ts)

        for pod in pods:
            pod_norm = list(getattr(pod, "entrant_discords_norm", []) or [])
            pod_handles = {h for h in pod_norm if h}
            if not pod_handles:
                continue

            intersection = handles & pod_handles
            inter_count = len(intersection)
            if inter_count == 0:
                continue

            pod_size = len(pod_handles)
            coverage = inter_count / pod_size if pod_size else 0.0
            start_ts = float(getattr(pod, "start", 0.0) or 0.0)

            log_debug(
                "[timer/topdeck]   candidate "
                f"S{pod.season}:T{pod.table} | "
                f"pod_handles={sorted(pod_handles)} | "
                f"intersection={sorted(intersection)} | "
                f"inter_count={inter_count} | coverage={coverage:.2f}"
            )

            # Require:
            # - at least 3 shared players (for 4-player pods)
            # - and at least 75% of that pod present in VC
            if inter_count < 3:
                continue
            if coverage < 0.75:
                continue

            score = (coverage, inter_count, start_ts)
            if best_score is None or score > best_score:
                best_score = score
                best = pod

        if best:
            pod_norm = list(getattr(best, "entrant_discords_norm", []) or [])
            pod_handles = {h for h in pod_norm if h}
            log_ok(
                f"[timer/topdeck] VC {voice_channel.name} matched TopDeck pod "
                f"S{best.season}:T{best.table} with pod_handles="
                f"{sorted(pod_handles)}."
            )
        else:
            log_sync(
                f"[timer/topdeck] No in-progress TopDeck pod matched VC "
                f"{voice_channel.name}; vc_handles={handles_sorted}, pods={len(pods)}."
            )

        return best

    async def mark_match_online(
        self,
        guild: discord.Guild,
        match: InProgressPod,
    ) -> None:
        """Persist a TopDeck match as online (Mongo)."""
        if not self.bracket_id:
            return

        ms = month_start_utc()
        year, month = ms.year, ms.month

        season = int(getattr(match, "season", 0) or 0)
        tid = int(getattr(match, "table", 0) or 0)
        start_ts = float(getattr(match, "start", 0.0) or 0.0)

        entrant_ids: list[int] = []
        for x in (getattr(match, "entrant_ids", None) or []):
            try:
                entrant_ids.append(int(x))
            except Exception:
                continue

        # entrant_uids are TopDeck UIDs (strings)
        topdeck_uids: list[str] = []
        for u in (getattr(match, "entrant_uids", None) or []):
            if u is None:
                continue
            s = str(u).strip()
            if s:
                topdeck_uids.append(s)

        # de-dupe, stable order
        seen = set()
        topdeck_uids = [x for x in topdeck_uids if not (x in seen or seen.add(x))]

        async with self._lock:
            existing = await get_record(
                self.bracket_id, year, month, season=season, tid=tid
            )
            already_online = bool(existing and existing.online)

            rec = OnlineGameRecord(
                season=season,
                tid=tid,
                start_ts=start_ts or None,
                entrant_ids=entrant_ids,
                topdeck_uids=topdeck_uids,
                online=True,
            )
            await upsert_record(self.bracket_id, year, month, rec)

        log_ok(
            f"[timer/topdeck] Marked TopDeck match S{season}:T{tid} as online "
            f"(already_online={already_online}). Players in match: {topdeck_uids}."
        )

    async def check_treasure_pod(
        self,
        guild: discord.Guild,
        match: InProgressPod,
        channel: discord.TextChannel,
        vc_members: Optional[List[discord.Member]] = None,
    ) -> Optional[dict]:
        """
        Check if this match is a Treasure Pod.

        Returns the treasure pod doc if it is, None otherwise.
        Sends announcement to channel if triggered.
        """
        ms = month_start_utc()
        mk = month_key(ms)

        table = int(getattr(match, "table", 0) or 0)

        # Get player discord IDs (entrant_ids from TopDeck)
        player_discord_ids: list[int] = []
        for x in (getattr(match, "entrant_ids", None) or []):
            try:
                player_discord_ids.append(int(x))
            except Exception:
                continue

        # Get player TopDeck UIDs
        player_topdeck_uids: list[str] = []
        for u in (getattr(match, "entrant_uids", None) or []):
            if u is None:
                continue
            s = str(u).strip()
            if s:
                player_topdeck_uids.append(s)

        # Check for treasure pod
        try:
            treasure = await self._treasure_manager.check_if_treasure_pod(
                guild_id=guild.id,
                month=mk,
                table=table,
                player_discord_ids=player_discord_ids,
                player_topdeck_uids=player_topdeck_uids,
            )
        except Exception as e:
            log_warn(f"[timer/treasure] Error checking treasure pod: {type(e).__name__}: {e}")
            return None

        if treasure:
            # Build player mentions from VC members that matched the pod (exclude spectators)
            mentions = ""
            if vc_members:
                pod_handles = {h for h in (getattr(match, "entrant_discords_norm", []) or []) if h}
                matched_members = []
                for m in vc_members:
                    if m.bot:
                        continue
                    member_handles = norm_member_handles(m)
                    if member_handles & pod_handles:
                        matched_members.append(m)
                mentions = " ".join(f"<@{m.id}>" for m in matched_members)

            # Use dynamic title/description/image from treasure doc
            pod_title = treasure.get("pod_title", "Treasure Pod!")
            pod_description = treasure.get("pod_description", "")
            pod_image_url = treasure.get("pod_image_url", "")

            try:
                embed = discord.Embed(
                    title=f"🎁 {pod_title}",
                    description=pod_description,
                    color=0xFFD700,  # Gold
                )
                embed.add_field(name="Pod Number", value=str(table), inline=True)
                if pod_image_url:
                    embed.set_thumbnail(url=pod_image_url)
                pod_type_label = treasure.get("pod_type", "treasure")
                embed.set_footer(text=f"ECL • {pod_type_label.replace('_', ' ').title()} Treasure Pod")

                content = mentions if mentions else None
                await channel.send(content=content, embed=embed)
            except Exception as e:
                log_warn(f"[timer/treasure] Failed to send treasure pod announcement: {e}")

        return treasure

    async def tag_online_game_for_timer(
        self,
        ctx: discord.ApplicationContext,
        voice_channel: discord.VoiceChannel,
        non_bot_members: List[discord.Member],
    ) -> None:
        """Match VC to a TopDeck pod and mark it as online. Warn in chat if no match."""
        guild = ctx.guild
        if guild is None:
            return
        if not self.bracket_id:
            return

        try:
            match = await self.match_vc_to_pod(voice_channel, non_bot_members)
        except Exception as e:
            log_warn(
                "[timer/topdeck] Error while matching VC to TopDeck pods: "
                f"{type(e).__name__}: {e}"
            )
            return

        if match is None:
            # No match → warn chat (public)
            try:
                await ctx.channel.send(
                    "⚠️ I couldn't find a matching **TopDeck game in progress** "
                    "for this table. Make sure your game is started in TopDeck "
                    "and that your Discord name on TopDeck matches your name here."
                )
            except Exception as e:
                log_warn(
                    "[timer/topdeck] Failed to send 'no TopDeck match' warning: "
                    f"{type(e).__name__}: {e}"
                )
            return

        await self.mark_match_online(guild, match)
        
        # Check for Treasure Pod
        await self.check_treasure_pod(guild, match, ctx.channel, vc_members=non_bot_members)
