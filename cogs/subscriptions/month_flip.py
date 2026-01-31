# cogs/subscriptions/month_flip.py
"""Month-end flip logic for subscriptions.

Handles:
- Month close at 00:00 Lisbon on last day (midnight between penultimate and last day)
- Top16 cut application for next month
- Monthly midnight role revocation
- Flip reminder DMs (mods + free-role users)
- Free-entry role DB persistence
- Bring a Friend Treasure Pod schedule generation
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

import discord

from topdeck_fetch import get_in_progress_pods, get_league_rows_cached
from db import subs_jobs, subs_free_entries, treasure_pod_schedule, treasure_pods as treasure_pods_col

from utils.settings import LISBON_TZ, TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN
from utils.dates import add_months, month_bounds, month_label
from utils.logger import log_sync
from utils.treasure_pods import TreasurePodManager

from .embeds import build_flip_mods_embed

if TYPE_CHECKING:
    from ..subscriptions_cog import SubscriptionsCog


class MonthFlipHandler:
    """Handles all month-end flip logic for the SubscriptionsCog."""

    def __init__(self, cog: "SubscriptionsCog"):
        self.cog = cog
        self._month_close_lock = asyncio.Lock()
        self._warned_revoke_delayed_for: set[str] = set()
        self._treasure_manager = TreasurePodManager(treasure_pod_schedule, treasure_pods_col)

    @property
    def cfg(self):
        return self.cog.cfg

    @property
    def log(self):
        return self.cog.log

    # -------------------- Job ID helpers --------------------

    def month_close_pending_job_id(self, guild_id: int, cut_month: str) -> str:
        return f"month-close-pending:{guild_id}:{cut_month}"

    def month_close_done_job_id(self, guild_id: int, cut_month: str) -> str:
        return f"month-close:{guild_id}:{cut_month}"

    # -------------------- In-progress games check --------------------

    async def in_progress_games_count(self) -> Optional[int]:
        """
        Returns:
        - int >= 0 when we can determine the in-progress count
        - None when unknown (fail-safe => do NOT run 7pm close logic)
        """
        if not TOPDECK_BRACKET_ID:
            await self.log.warn("[subs] TOPDECK_BRACKET_ID not set; month-close will NOT run (fail-safe).")
            return None

        try:
            pods = await get_in_progress_pods(
                TOPDECK_BRACKET_ID,
                firebase_id_token=FIREBASE_ID_TOKEN,
            )
            return len(pods)
        except Exception as e:
            await self.log.info(
                f"[subs] ⚠️ in-progress check failed: {type(e).__name__}: {e} "
                "(fail-safe, month-close paused)"
            )
            return None

    # -------------------- Month close logic --------------------

    async def ensure_month_close_pending(self, guild: discord.Guild, *, cut_month: str) -> None:
        """Create the pending marker once the close window begins (idempotent)."""
        pid = self.month_close_pending_job_id(guild.id, cut_month)
        if await subs_jobs.find_one({"_id": pid}):
            return
        await subs_jobs.insert_one({"_id": pid, "ran_at": datetime.now(timezone.utc)})

    async def run_month_close_logic(self, guild: discord.Guild, *, cut_month: str) -> None:
        """
        The close logic (may run later if games are still in progress).
        """
        target_month = add_months(cut_month, 1)

        # 0) Sync online games FIRST (required for accurate Top16 eligibility)
        await self._run_synconline(guild, month_str=cut_month)

        # 1) Apply Top16 cut => Top16 role
        await self.apply_top16_cut_for_next_month(
            guild,
            cut_month=cut_month,
            target_month=target_month,
        )

        # 2) Mods checklist for the NEXT month
        await self.run_flip_mods_reminder_job(guild, mk=target_month)

        # 3) Free-role info DMs for the NEXT month
        await self.run_free_role_flip_info_job(guild, mk=target_month)

        # 4) Dump the CLOSED month (cut_month) to Mongo
        await self.cog._run_topdeck_month_dump_flip_job(guild, month_str=cut_month)

        # 5) Generate Bring a Friend Treasure Pod schedule for NEXT month
        await self.generate_treasure_pod_schedule(guild, target_month=target_month)

    async def maybe_run_month_close_job(self, guild: discord.Guild, *, cut_month: str) -> None:
        """
        If pending exists and not done, run when in-progress games == 0.
        """
        done_id = self.month_close_done_job_id(guild.id, cut_month)
        if await subs_jobs.find_one({"_id": done_id}):
            return

        pending_id = self.month_close_pending_job_id(guild.id, cut_month)
        pending = await subs_jobs.find_one({"_id": pending_id})
        if not pending:
            return  # not scheduled / not started

        # gate on in-progress games
        n = await self.in_progress_games_count()
        if n is None:
            return  # fail-safe: unknown => don't run
        if n > 0:
            await self.log.info(f"[subs] month-close pending ({cut_month}) blocked: {n} game(s) still in progress")
            return

        async with self._month_close_lock:
            # re-check inside lock
            if await subs_jobs.find_one({"_id": done_id}):
                return

            try:
                await self.log.ok(f"[subs] month-close starting for {cut_month} (in-progress=0)")
                await self.run_month_close_logic(guild, cut_month=cut_month)
            except Exception as e:
                await self.log.error(f"[subs] month-close FAILED for {cut_month}: {type(e).__name__}: {e}")
                return

            await subs_jobs.insert_one({"_id": done_id, "ran_at": datetime.now(timezone.utc)})
            await self.log.ok(f"[subs] month-close done for {cut_month}")

    # -------------------- Monthly midnight revoke --------------------

    async def run_monthly_midnight_revoke_job(self, guild: discord.Guild, *, target_month: str) -> None:
        """
        Remove ECL for members not eligible for target_month (new month).
        Idempotent per month.
        """
        cfg = self.cfg
        job_id = f"monthly-revoke:{guild.id}:{target_month}"
        if await subs_jobs.find_one({"_id": job_id}):
            return

        # If month-close for previous month is pending but not done, delay revoke to avoid
        # wrongly removing Top16 winners before their free-entry is written.
        prev_month = add_months(target_month, -1)
        prev_pending = await subs_jobs.find_one({"_id": self.month_close_pending_job_id(guild.id, prev_month)})
        prev_done = await subs_jobs.find_one({"_id": self.month_close_done_job_id(guild.id, prev_month)})
        if prev_pending and not prev_done:
            if target_month not in self._warned_revoke_delayed_for:
                self._warned_revoke_delayed_for.add(target_month)
                await self.log.info(f"[subs] monthly revoke delayed for {target_month}: month-close still pending for {prev_month}")
            return

        if not self.cog._enforcement_active(datetime.now(LISBON_TZ)):
            await self.log.info("[subs] monthly revoke skipped: enforcement not active yet")
            return

        if not cfg.ecl_role_id:
            return
        role = guild.get_role(cfg.ecl_role_id)
        if not role:
            return

        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        flip_at = month_bounds(target_month)[0]  # start of target_month @ 00:00 Lisbon

        members = list(role.members)
        if len(members) < 50:
            try:
                members = [m async for m in guild.fetch_members(limit=None)]
                members = [m for m in members if role in m.roles]
            except Exception:
                members = list(role.members)

        removed = 0
        checked = 0

        for m in members:
            if m.bot:
                continue
            checked += 1
            ok, _ = await self.cog._eligibility(m, target_month, at=flip_at)
            if ok:
                continue
            did = await self.cog._revoke_ecl_member(m, reason=f"Monthly reset: not eligible for {target_month}", dm=True)
            if did:
                removed += 1
            if cfg.dm_sleep_seconds:
                await asyncio.sleep(cfg.dm_sleep_seconds)

        await self.log.info(f"[subs] monthly revoke {target_month}: checked={checked} removed={removed}")

    # -------------------- Top16 cut application --------------------

    async def apply_top16_cut_for_next_month(
        self, guild: discord.Guild, *, cut_month: str, target_month: str
    ) -> None:
        """Apply Top16 cut: grant free entry for next month + Top16 role."""
        cfg = self.cfg

        top16_ids, missing = await self.cog._eligible_top16_discord_ids_for_month(guild, cut_month)

        if missing and missing != ["no qualified top16"]:
            await self.log.info(f"[subs] Top16 mapping misses ({cut_month}): " + ", ".join(missing[:20]))

        if not top16_ids:
            await self.log.info(f"[subs] Top16 cut ({cut_month}) produced 0 Discord IDs. (Nothing applied)")
            return

        applied = 0
        for uid in top16_ids:
            #TOP16 free entry DB write + grant role
            # await subs_free_entries.update_one(
            #     {"guild_id": cfg.guild_id, "user_id": int(uid), "month": target_month},
            #     {
            #         "$setOnInsert": {
            #             "guild_id": cfg.guild_id,
            #             "user_id": int(uid),
            #             "month": target_month,
            #             "created_at": datetime.now(timezone.utc),
            #         },
            #         "$set": {"reason": f"Top16 ({cut_month})", "updated_at": datetime.now(timezone.utc)},
            #     },
            #     upsert=True,
            # )

            # await self.cog._grant_ecl(uid, reason=f"Top16 free entry ({target_month})")
            await self.cog._grant_top16(uid, reason=f"Top16 qualifier ({cut_month})")
            applied += 1

        await self.log.ok(f"[subs] Applied Top16 cut: {applied} users -> free entry {target_month} + Top16 role")

    # -------------------- Cleanup job (legacy?) --------------------

    async def run_cleanup_job(self, guild: discord.Guild, target_month: str) -> None:
        """Remove ECL from ineligible users for target_month."""
        job_id = f"cleanup:{guild.id}:{target_month}"
        if await subs_jobs.find_one({"_id": job_id}):
            return
        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        cfg = self.cfg
        role = guild.get_role(cfg.ecl_role_id) if cfg.ecl_role_id else None
        if not role:
            return

        cut_month = add_months(target_month, -1)  # month that just ended
        await self.apply_top16_cut_for_next_month(guild, cut_month=cut_month, target_month=target_month)

        members = list(role.members)
        if len(members) < 50:
            members = [m async for m in guild.fetch_members(limit=None)]
            members = [m for m in members if role in m.roles]

        flip_at = month_bounds(target_month)[0]

        to_remove: list[discord.Member] = []
        for m in members:
            if m.bot:
                continue
            ok, _ = await self.cog._eligibility(m, target_month, at=flip_at)
            if not ok:
                to_remove.append(m)

        await self.log.info(f"[subs] Cleanup for {target_month}: removing ECL from {len(to_remove)} users")

        for m in to_remove:
            with contextlib.suppress(Exception):
                await m.remove_roles(role, reason=f"Not subscribed/free for {target_month}")

    # -------------------- Flip reminder jobs --------------------

    async def run_flip_mods_reminder_job(self, guild: discord.Guild, *, mk: str) -> None:
        """DM mods the month-flip checklist."""
        job_id = f"flip-mods:{guild.id}:{mk}"
        if await subs_jobs.find_one({"_id": job_id}):
            return
        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        emb = self.cog._build_flip_mods_embed(guild, mk)
        await self.cog._dm_mods_embed(guild, embed=emb)

    async def run_free_role_flip_info_job(self, guild: discord.Guild, *, mk: str) -> None:
        """Once-per-month DM to players who have free-entry via specific roles.
        
        Also writes free-entry to database so the source of truth is the DB
        (in case someone loses the role mid-month).
        """
        cfg = self.cfg
        role_ids = set(int(x) for x in (cfg.free_entry_role_ids or set()) if int(x))
        if not role_ids:
            return

        job_id = f"flip-free-role-info:{guild.id}:{mk}"
        if await subs_jobs.find_one({"_id": job_id}):
            return
        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        # Build per-user list of role names that grant free entry
        user_roles: dict[int, list[str]] = {}
        for rid in role_ids:
            role = guild.get_role(rid)
            if not role:
                continue
            for m in getattr(role, "members", []) or []:
                if m.bot:
                    continue
                user_roles.setdefault(int(m.id), []).append(role.name)

        if not user_roles:
            return

        nice_month = month_label(mk)
        sem = asyncio.Semaphore(cfg.dm_concurrency)
        sent = 0
        db_written = 0

        async def _send_one(uid: int, role_names: list[str]):
            nonlocal sent, db_written
            async with sem:
                try:
                    member = guild.get_member(uid) or await guild.fetch_member(uid)
                except Exception:
                    return
                if not member or member.bot:
                    return

                # Write free entry to database (source of truth)
                roles_txt = ", ".join(sorted(set(role_names)))
                reason = f"free role: {roles_txt}"
                await subs_free_entries.update_one(
                    {"guild_id": cfg.guild_id, "user_id": int(uid), "month": mk},
                    {
                        "$setOnInsert": {
                            "guild_id": cfg.guild_id,
                            "user_id": int(uid),
                            "month": mk,
                            "created_at": datetime.now(timezone.utc),
                        },
                        "$set": {"reason": reason, "updated_at": datetime.now(timezone.utc)},
                    },
                    upsert=True,
                )
                db_written += 1

                # ensure access role is present
                await self.cog._grant_ecl(uid, reason=f"Free entry role(s) ({nice_month})")

                emb = discord.Embed(
                    title=f"✅ Free entry — {nice_month}",
                    description=(
                        f"You have **free entry** for **{nice_month}** because you have: **{roles_txt}**.\n\n"
                        "Your access is secured for this month."
                    ),
                    color=cfg.embed_color if isinstance(cfg.embed_color, int) else 0x2ECC71,
                )
                emb.set_footer(text="ECL • Free entry notice")

                try:
                    await member.send(embed=emb)
                    sent += 1
                except Exception:
                    return

                if cfg.dm_sleep_seconds:
                    await asyncio.sleep(cfg.dm_sleep_seconds)

        await asyncio.gather(*[_send_one(uid, rnames) for uid, rnames in user_roles.items()])
        await self.log.info(f"[subs] flip free-role info {mk}: sent {sent}/{len(user_roles)} DMs, wrote {db_written} DB entries")

    # -------------------- Sync Online Games --------------------

    async def _run_synconline(self, guild: discord.Guild, *, month_str: str) -> None:
        """
        Run online games sync before month close.
        
        This ensures accurate online game counts for Top16 eligibility.
        """
        job_id = f"synconline-monthclose:{guild.id}:{month_str}"
        if await subs_jobs.find_one({"_id": job_id}):
            await self.log.info(f"[subs] synconline already ran for {month_str}")
            return
        
        try:
            # Get the TopdeckOnlineSyncCog
            sync_cog = self.bot.get_cog("TopdeckOnlineSyncCog")
            if not sync_cog:
                await self.log.warn("[subs] TopdeckOnlineSyncCog not found, skipping synconline")
                return
            
            await self.log.info(f"[subs] Running synconline for {month_str} before month close...")
            result = await sync_cog.run_sync(guild, month_str=month_str)
            
            if result.get("success"):
                await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})
                await self.log.ok(
                    f"[subs] synconline completed for {month_str}: "
                    f"spellbot={result.get('spellbot_games', 0)}, "
                    f"topdeck={result.get('topdeck_matches', 0)}, "
                    f"online={result.get('online_count', 0)}, "
                    f"players={result.get('players_with_online', 0)}"
                )
            else:
                await self.log.error(f"[subs] synconline failed for {month_str}: {result.get('error', 'unknown')}")
        except Exception as e:
            await self.log.error(f"[subs] synconline error for {month_str}: {type(e).__name__}: {e}")

    # -------------------- Treasure Pod Schedule --------------------

    async def generate_treasure_pod_schedule(self, guild: discord.Guild, *, target_month: str) -> None:
        """
        Generate the Bring a Friend Treasure Pod schedule for a month.
        
        Called at month flip to set up treasure pods for the upcoming month.
        Uses the current player count from TopDeck to estimate total games.
        """
        job_id = f"treasure-schedule:{guild.id}:{target_month}"
        if await subs_jobs.find_one({"_id": job_id}):
            return  # Already created
        
        # Get current player count from TopDeck
        player_count = 100  # default fallback
        if TOPDECK_BRACKET_ID:
            try:
                rows, _ = await get_league_rows_cached(
                    TOPDECK_BRACKET_ID,
                    FIREBASE_ID_TOKEN,
                    force_refresh=True,
                )
                if rows:
                    # Count non-dropped players
                    player_count = len([r for r in rows if not r.dropped])
            except Exception as e:
                await self.log.warn(f"[treasure] Failed to get player count: {type(e).__name__}: {e}")
        
        if player_count < 50:
            player_count = 100  # Minimum reasonable estimate
        
        try:
            await self._treasure_manager.create_schedule(
                guild_id=guild.id,
                month=target_month,
                player_count=player_count,
            )
            await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})
            await self.log.ok(f"[treasure] Created treasure pod schedule for {target_month} (players={player_count})")
        except Exception as e:
            await self.log.error(f"[treasure] Failed to create schedule for {target_month}: {type(e).__name__}: {e}")
