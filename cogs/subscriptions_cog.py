# cogs/subscriptions_cog.py
"""ECL monthly subscriptions + free entry controller.

What it does:
  - Grants ECL role when a user pays on Ko-fi (via an inbox channel webhook post)
  - Treats Patreon integration as "subscribed" if the member has any of PATREON_ROLE_IDS
  - Treats certain roles as monthly free-entry (e.g., Judge, Arena Vanguard)
  - Supports per-user free-entry for a given month (e.g., Top16 cut) stored in Mongo
  - Sends DM reminders (3 days before month end and on the last day)
  - Removes ECL role on the last day of the month for members not eligible for next month
"""

import asyncio
import contextlib
import os
import traceback
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set, Tuple

import discord
from discord.ext import commands, tasks
from pymongo.errors import DuplicateKeyError

from topdeck_fetch import get_league_rows_cached, get_in_progress_pods, get_cached_matches, PlayerRow
from online_games_store import count_online_games_by_topdeck_uid_str, has_recent_game_by_topdeck_uid
from db import ensure_indexes, ping, subs_access, subs_free_entries, subs_jobs, subs_kofi_events, treasure_pod_schedule, treasure_pods as treasure_pods_col, job_once
from .topdeck_month_dump import dump_topdeck_month_to_mongo
from utils.interactions import resolve_member
from utils.logger import get_logger, log_sync, log_ok, log_warn, log_error
from utils.treasure_pods import TreasurePodManager

from cogs.lfg_cog import LFG_ELO_MIN_DAY
from utils.topdeck_identity import MemberIndex, build_member_index, resolve_row_discord_id

from utils.settings import (
    GUILD_ID,
    SUBS,
    LISBON_TZ,
    TOPDECK_BRACKET_ID,
    NEXT_MONTH_TOPDECK_BRACKET_ID,
    FIREBASE_ID_TOKEN,
)

# Import consolidated date utilities
from utils.dates import (
    month_key,
    add_months,
    month_bounds,
    league_close_at,
    month_label,
    month_end_inclusive,
    parse_month_from_text,
    now_lisbon,
)

# Import from subscriptions submodule
from .subscriptions import (
    compute_one_time_window as compute_kofi_one_time_window,
    extract_discord_user_id,
    extract_json_from_message_content,
    SubsLinksView,
    build_reminder_embed,
    build_flip_mods_embed,
    build_top16_online_reminder_embed,
    build_topcut_prize_reminder_embed,
    MonthFlipHandler,
)
from .subscriptions.embeds import _get_color, _apply_thumbnail


TOURNAMENT_UPDATES_CHANNEL_ID = int(os.getenv("TOURNAMENT_UPDATES_CHANNEL_ID", "1439720684170248293"))



# -------------------- cog --------------------

class SubscriptionsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.cfg = SUBS
        self.log = get_logger(bot, self.cfg)
        self._bootstrapped = False
        self._tick.start()
        self._access_audit.start()

        # small locks to avoid overlap
        self._top16_reminder_lock = asyncio.Lock()
        self._access_audit_lock = asyncio.Lock()
        self._topdeck_dump_lock = asyncio.Lock()

        # log throttles (avoid spamming every 5 mins)
        self._warned_missing_inprogress_checker = False

        # Month flip handler (manages month-end logic)
        self.flip_handler = MonthFlipHandler(self)

        # Treasure pod manager (Bring a Friend)
        self._treasure_manager = TreasurePodManager(treasure_pod_schedule, treasure_pods_col)


    def cog_unload(self):
        self._tick.cancel()
        self._access_audit.cancel()

    def _enforcement_active(self, at: Optional[datetime] = None) -> bool:
        """True when it's expected/safe to revoke roles in production."""
        start = getattr(self.cfg, "enforcement_start", None)
        if not isinstance(start, datetime):
            return False
        now = at or now_lisbon()
        if now.tzinfo is None:
            now = now.replace(tzinfo=LISBON_TZ)
        now = now.astimezone(LISBON_TZ)
        return now >= start
    
    def _dm_opted_in(self, member: discord.Member) -> bool:
        """If DM_OPTIN_ROLE_ID is set, only DM members who have that role."""
        rid = int(getattr(self.cfg, "dm_optin_role_id", 0) or 0)
        if not rid:
            return True  # backwards compatible: no opt-in role configured => DM everyone
        return any(r.id == rid for r in (member.roles or []))


    @commands.Cog.listener()
    async def on_ready(self):
        if self._bootstrapped:
            return
        self._bootstrapped = True
        try:
            await ping()
            await ensure_indexes()
            log_ok("[subs] MongoDB OK + indexes ensured")
        except Exception as e:
            log_error(f"[subs] MongoDB error: {e}")

    # -------------------- Marketing embed helpers --------------------

    def _build_links_view(self) -> Optional[discord.ui.View]:
        cfg = self.cfg
        v = SubsLinksView(cfg.kofi_url, cfg.patreon_url)
        if not v._ok(v.kofi_url) and not v._ok(v.patreon_url):
            return None
        return v
    
    async def _dm_mods_summary(self, guild: discord.Guild, *, summary: str) -> None:
        """
        DM everyone with ECL_MOD_ROLE_ID a short summary.
        Fail-safe: never raises.
        """
        cfg = self.cfg
        rid = int(getattr(cfg, "ecl_mod_role_id", 0) or 0)
        if not rid:
            return

        role = guild.get_role(rid)
        mods: list[discord.Member] = []

        # Best case: cached role.members
        if role:
            mods = [m for m in (role.members or []) if isinstance(m, discord.Member) and not m.bot]

        # Fallback: fetch all members and filter by role
        if not mods:
            try:
                members = [m async for m in guild.fetch_members(limit=None)]
                mods = [m for m in members if (not m.bot) and any(rr.id == rid for rr in m.roles)]
            except Exception:
                return

        if not mods:
            await self.log.info(f"[subs] mod summary skipped: no members found for ECL_MOD_ROLE_ID={rid}")
            return

        sent = 0
        for m in mods:
            try:
                await m.send(summary)
                sent += 1
            except Exception:
                pass

            if cfg.dm_sleep_seconds:
                await asyncio.sleep(cfg.dm_sleep_seconds)

        await self.log.info(f"[subs] mod summary DM sent {sent}/{len(mods)} — {summary}")

    async def _count_registered_for_month(self, guild: discord.Guild, mk: str) -> int:
        cfg = self.cfg

        start, end = month_bounds(mk)
        start_utc = start.astimezone(timezone.utc)
        end_utc = end.astimezone(timezone.utc)

        # ---- DB-based entitlements breakdown ----
        month_ent_ids: list[int] = []
        pass_ids: list[int] = []

        try:
            month_ent_ids = await subs_access.distinct("user_id", {
                "guild_id": cfg.guild_id,
                "month": mk,
                "kind": {"$ne": "kofi-one-time"},
            })
        except Exception as e:
            log_error(f"[subs] DB error fetching month entitlements for {mk}: {type(e).__name__}: {e}")
            month_ent_ids = []

        try:
            pass_ids = await subs_access.distinct("user_id", {
                "guild_id": cfg.guild_id,
                "kind": "kofi-one-time",
                "starts_at": {"$lt": end_utc},
                "expires_at": {"$gt": start_utc},
            })
        except Exception as e:
            log_error(f"[subs] DB error fetching one-time passes for {mk}: {type(e).__name__}: {e}")
            pass_ids = []

        def _to_int_set(xs) -> set[int]:
            out: set[int] = set()
            for x in xs or []:
                try:
                    out.add(int(x))
                except Exception:
                    pass
            return out

        month_ent_set = _to_int_set(month_ent_ids)
        pass_set = _to_int_set(pass_ids)
        kofi_set = month_ent_set | pass_set  # DB-based access union

        # ---- Free-entry DB list ----
        try:
            free_ids = await subs_free_entries.distinct("user_id", {"guild_id": cfg.guild_id, "month": mk})
        except Exception as e:
            log_error(f"[subs] DB error fetching free entries for {mk}: {type(e).__name__}: {e}")
            free_ids = []
        free_set = _to_int_set(free_ids)

        eligible: set[int] = set()
        eligible |= kofi_set
        eligible |= free_set

        # ---- Role-based breakdown per role ----
        role_ids = cfg.patreon_role_ids | cfg.kofi_role_ids | cfg.free_entry_role_ids

        role_to_member_ids: dict[int, set[int]] = {}
        role_member_ids: set[int] = set()
        used_fetch_fallback = False

        if role_ids:
            # fast path: role.members (works well if member cache is populated)
            for rid in role_ids:
                role = guild.get_role(int(rid))
                if not role:
                    role_to_member_ids[int(rid)] = set()
                    continue
                s = {int(m.id) for m in (getattr(role, "members", []) or []) if not m.bot}
                role_to_member_ids[int(rid)] = s
                role_member_ids |= s

            # fallback: fetch all members if cache seems too small
            if guild.member_count and len(role_member_ids) < min(10, guild.member_count // 50):
                used_fetch_fallback = True
                try:
                    members = [m async for m in guild.fetch_members(limit=None)]
                    role_to_member_ids = {int(rid): set() for rid in role_ids}
                    role_member_ids = set()

                    for m in members:
                        if m.bot:
                            continue
                        mids = {rr.id for rr in m.roles}
                        hit = mids.intersection(role_ids)
                        if not hit:
                            continue
                        uid = int(m.id)
                        for rid in hit:
                            role_to_member_ids[int(rid)].add(uid)
                        role_member_ids.add(uid)
                except Exception:
                    pass

            eligible |= role_member_ids

        # pretty breakdown list
        role_breakdown = []
        for rid in sorted(role_ids):
            role = guild.get_role(int(rid))
            role_breakdown.append({
                "name": role.name if role else "(missing role)",
                "count": len(role_to_member_ids.get(int(rid), set())),
            })

        # ---- (3) projected Top16 for NEXT month (no IDs yet) ----
        now_mk = month_key(now_lisbon())
        next_mk = add_months(now_mk, 1)
        projected_top16 = 16 if mk == next_mk else 0

        known_total = len(eligible)
        reported_total = known_total + projected_top16

        # ---- (4) clearer debug print (no ids) ----
        role_lines = [f"{rb['name']}: {rb['count']}" for rb in role_breakdown]

        log_sync(
            "[subs] count\n"
            f"  mk: {mk}\n"
            f"  db: month_ent={len(month_ent_set)} | kofi_pass={len(pass_set)} | db_union={len(kofi_set)}\n"
            f"  free_db: {len(free_set)}\n"
            f"  roles: union={len(role_member_ids)} | fetch_fallback={used_fetch_fallback}\n"
            f"  roles breakdown: " + " | ".join(role_lines) + "\n"
            f"  total known: {known_total}\n"
            f"  projected top16: {projected_top16}\n"
            f"  total reported: {reported_total}"
        )

        return reported_total



    async def _build_reminder_embed(self, kind: str, target_month: str, registered_count: int) -> discord.Embed:
        """Build subscription reminder embed."""
        cfg = self.cfg
        return build_reminder_embed(
            kind=kind,
            target_month=target_month,
            registered_count=registered_count,
            embed_color=cfg.embed_color,
            embed_thumbnail_url=cfg.embed_thumbnail_url,
        )

    # -------------------- MOD reminder helpers --------------------
    
    def _build_flip_mods_embed(self, guild: discord.Guild, mk: str) -> discord.Embed:
        """Build month-flip checklist embed for mods."""
        cfg = self.cfg
        return build_flip_mods_embed(
            guild=guild,
            mk=mk,
            current_bracket=TOPDECK_BRACKET_ID or "",
            next_bracket=NEXT_MONTH_TOPDECK_BRACKET_ID or "",
            free_entry_role_ids=cfg.free_entry_role_ids,
            embed_color=cfg.embed_color,
            embed_thumbnail_url=cfg.embed_thumbnail_url,
        )

    async def _dm_mods_embed(self, guild: discord.Guild, *, embed: discord.Embed) -> None:
        cfg = self.cfg
        rid = int(getattr(cfg, "ecl_mod_role_id", 0) or 0)
        if not rid:
            return

        role = guild.get_role(rid)
        mods: list[discord.Member] = []
        if role:
            mods = [m for m in (role.members or []) if not m.bot]

        if not mods:
            try:
                members = [m async for m in guild.fetch_members(limit=None)]
                mods = [m for m in members if (not m.bot) and any(rr.id == rid for rr in m.roles)]
            except Exception:
                return

        sent = 0
        for m in mods:
            try:
                await m.send(embed=embed)
                sent += 1
            except Exception:
                pass
            if cfg.dm_sleep_seconds:
                await asyncio.sleep(cfg.dm_sleep_seconds)

        await self.log.info(f"[subs] mod embed sent {sent}/{len(mods)} — {embed.title}")


    # -------------------- Ko-fi inbox reactions --------------------
            
    async def _react_kofi(
        self,
        message: discord.Message,
        ok: bool,
        *,
        note: str = "",
    ) -> None:
        """
        React on the Ko-fi inbox webhook message so mods can see what happened.

        ok=True  -> ✅
        ok=False -> ❌
        note is optional; logs only (keeps inbox clean).
        """
        emoji = "✅" if ok else "❌"

        with contextlib.suppress(Exception):
            await message.add_reaction(emoji)

        if note:
            await self.log.info(f"[subs] kofi inbox {emoji} — {note}")


    # -------------------- Top16 online-games reminder helpers --------------------

    async def _top16_unqualified_for_month(
        self,
        guild: discord.Guild,
        *,
        mk: str,
    ) -> tuple[list[dict], list[str]]:
        """
        Returns:
          - entries: [{rank, row, online_games, missing, discord_id}]
          - debug_missing: list[str] of mapping misses
        Only includes players who are currently in TopDeck Top16 AND have < required online games.
        """
        cfg = self.cfg

        bracket_id = TOPDECK_BRACKET_ID
        firebase_token = FIREBASE_ID_TOKEN

        if not bracket_id:
            return ([], ["TOPDECK_BRACKET_ID not set"])

        # Fetch rows
        try:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=True)
        except Exception:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=False)

        if not rows:
            return ([], ["no TopDeck rows"])

        # Online counts for mk
        try:
            y, m = mk.split("-")
            year, month = int(y), int(m)
        except Exception:
            return ([], [f"bad mk: {mk!r}"])

        try:
            online_counts = await count_online_games_by_topdeck_uid_str(bracket_id, year, month, online_only=True)
        except Exception as e:
            return ([], [f"online_counts error: {type(e).__name__}: {e}"])

        # Active, sorted like TopDeck leaderboard (points first)
        active = [r for r in rows if not r.dropped]
        active = sorted(active, key=lambda r: (-r.pts, -r.games))
        top16 = active[:16]

        # Shared identity index (Discord ID first; unique handle/name fallback)
        index = await self._build_member_index(guild)

        entries: list[dict] = []
        misses: list[str] = []

        conf_counts: dict[str, int] = {}

        for idx, r in enumerate(top16, start=1):
            uid = (r.uid or "").strip()
            online = online_counts.get(uid, 0) if uid else 0
            need = max(0, cfg.top16_min_online_games - online)
            if need <= 0:
                continue  # already qualified

            # Map to Discord ID (ID -> unique handle -> unique name)
            res = resolve_row_discord_id(r, index)
            conf_counts[res.confidence] = conf_counts.get(res.confidence, 0) + 1

            if not res.discord_id:
                misses.append(
                    f"{r.discord or r.name or 'unknown'} ({res.confidence}{(' ' + res.matched_key) if res.matched_key else ''})"
                )
                continue

            discord_id = int(res.discord_id)

            # Log when we had to fall back (or when ambiguous/no-match happens)
            if res.confidence != "discord_id":
                await self.log.info(
                    "[subs/identity] top16-unqualified "
                    f"mk={mk} row={getattr(r, 'name', '')!r} discord={getattr(r, 'discord', '')!r} "
                    f"-> discord_id={discord_id} conf={res.confidence} key={res.matched_key!r}"
                )

            entries.append({
                "rank": idx,
                "row": r,
                "online_games": int(online),
                "missing": int(need),
                "discord_id": int(discord_id),
            })

        if conf_counts:
            parts = ", ".join(f"{k}={v}" for k, v in sorted(conf_counts.items()))
            await self.log.info(f"[subs/identity] top16-unqualified mk={mk} mapping_counts: {parts}")

        return entries, misses

    async def _build_top16_online_reminder_embed(
        self,
        *,
        kind: str,          # "5d" | "3d" | "last"
        mk: str,            # YYYY-MM
        rank: int,
        name: str,
        online_games: int,
        need_total: int,
        mention: str,       # e.g. member.mention
    ) -> discord.Embed:
        """Build Top16 online games reminder embed."""
        cfg = self.cfg
        return build_top16_online_reminder_embed(
            kind=kind,
            mk=mk,
            rank=rank,
            name=name,
            online_games=online_games,
            need_total=need_total,
            mention=mention,
            embed_color=cfg.embed_color,
            embed_thumbnail_url=cfg.embed_thumbnail_url,
        )
    
    async def _run_topdeck_month_dump_flip_job(self, guild: discord.Guild, *, month_str: str) -> None:
        job_id = f"topdeckdumpmonth:auto:{guild.id}:{month_str}"
        if not await job_once(job_id):
            return

        async with self._topdeck_dump_lock:
            try:
                meta = await dump_topdeck_month_to_mongo(
                    guild_id=guild.id,
                    month_str=month_str,
                    bracket_id=TOPDECK_BRACKET_ID,
                    firebase_id_token=FIREBASE_ID_TOKEN,
                )
                await self._log(
                    f"[subs] ✅ TopDeck month dump stored: month={month_str} "
                    f"run_id={meta['run_id']} chunks={meta['chunks']} sha256={meta['sha256'][:12]}…"
                )
                await self._dm_mods_summary(
                    guild,
                    summary=(
                        f"[ECL] TopDeck month dump stored: {month_str} "
                        f"(run_id={meta['run_id']}, chunks={meta['chunks']})"
                    ),
                )
            except Exception as e:
                await self.log.error(f"[subs] ❌ TopDeck month dump FAILED for {month_str}: {type(e).__name__}: {e}")
                await self._dm_mods_summary(
                    guild,
                    summary=f"[ECL] TopDeck month dump FAILED for {month_str}: {type(e).__name__}: {e}",
                )


    async def _run_top16_online_reminder_job(self, guild: discord.Guild, *, mk: str, kind: str) -> None:
        """
        DM players who are currently TopDeck Top16 but not qualified (< min online games).
        Runs at most once per (mk, kind).
        Also logs/prints who would receive it.
        """
        job_id = f"top16-online-remind:{guild.id}:{mk}:{kind}"
        if not await job_once(job_id):
            return

        cfg = self.cfg

        async with self._top16_reminder_lock:
            entries, misses = await self._top16_unqualified_for_month(guild, mk=mk)

            if misses:
                await self.log.info(f"[subs] Top16-online mapping misses ({mk} {kind}): " + ", ".join(misses[:20]))

            if not entries:
                await self.log.info(f"[subs] Top16-online reminder ({mk} {kind}): 0 targets")
                log_sync(f"[subs] Top16-online reminder ({mk} {kind}): 0 targets")
                await self._dm_mods_summary(
                    guild,
                    summary=f"[ECL] Top16-online reminder ({mk} {kind}) — sent 0 DMs (0 targets).",
                )
                return

            # ---- Log/print who will be targeted (sample up to 20) ----
            sample_lines: list[str] = []
            for e in entries[:20]:
                try:
                    did = int(e["discord_id"])
                except Exception:
                    did = 0
                try:
                    rank = int(e["rank"])
                except Exception:
                    rank = 0
                try:
                    og = int(e["online_games"])
                except Exception:
                    og = 0
                row = e.get("row")
                nm = str(getattr(row, "name", "") or "Player")
                sample_lines.append(f"#{rank:02d} {nm} | discord_id={did} | online={og}/{int(cfg.top16_min_online_games)}")

            msg = (
                f"[subs] Top16-online reminder ({mk} {kind}) targets={len(entries)}. "
                f"Sample (up to 20):\n" + "\n".join(sample_lines)
            )
            log_sync(msg)
            await self._log(msg)

            sem = asyncio.Semaphore(cfg.dm_concurrency)
            sent = 0

            async def _send_one(entry: dict):
                nonlocal sent
                async with sem:
                    uid = int(entry["discord_id"])
                    member = await resolve_member(guild, uid)
                    if not member or member.bot:
                        return

                    try:
                        emb = await self._build_top16_online_reminder_embed(
                            kind=kind,
                            mk=mk,
                            rank=int(entry["rank"]),
                            name=str(getattr(entry["row"], "name", "") or ""),
                            online_games=int(entry["online_games"]),
                            need_total=int(cfg.top16_min_online_games),
                            mention=member.mention,  
                        )
                        await member.send(embed=emb)
                        sent += 1
                    except Exception:
                        return

                    if cfg.dm_sleep_seconds:
                        await asyncio.sleep(cfg.dm_sleep_seconds)

            await asyncio.gather(*[_send_one(e) for e in entries])
            await self.log.ok(f"[subs] Top16-online reminder ({mk} {kind}) sent {sent}/{len(entries)}")
            log_ok(f"[subs] Top16-online reminder ({mk} {kind}) sent {sent}/{len(entries)}")
            await self._dm_mods_summary(
                guild,
                summary=f"[ECL] Top16-online reminder ({mk} {kind}) — sent {sent}/{len(entries)} DMs.",
            )



    # -------------------- Ko-fi ingestion --------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        cfg = self.cfg

        if not cfg.kofi_inbox_channel_id:
            return
        if message.channel.id != cfg.kofi_inbox_channel_id:
            return
        if not message.webhook_id:
            return

        payload = extract_json_from_message_content(message.content or "")
        if not payload:
            await self._react_kofi(message, False, note="no JSON payload / parse failed")
            return


        # Verify token (optional)
        if cfg.kofi_verify_token:
            token = str(payload.get("verification_token") or "").strip()
            if not token or token != cfg.kofi_verify_token:
                await self._react_kofi(message, False, note="bad verification_token")
                return


        txn_id = str(payload.get("kofi_transaction_id") or payload.get("transaction_id") or "").strip()
        if not txn_id:
            await self._react_kofi(message, False, note="missing transaction id")
            return


        # If Ko-fi Discord bot already grants/removes roles for MEMBERSHIPS,
        # ignore subscription payments here to avoid fighting over roles / double entitlement.
        if bool(payload.get("is_subscription_payment")):
            await self._react_kofi(message, True, note=f"ignored subscription payment txn={txn_id}")
            return



        # We only handle one-time tips (single-month passes)
        user_id = extract_discord_user_id(payload)
        if not user_id:
            await self._react_kofi(message, False, note=f"txn={txn_id} could not extract discord user id")
            return


        # Optional: enforce minimum one-time amount (EUR 7)
        currency = str(payload.get("currency") or "").upper().strip()
        try:
            amount = float(payload.get("amount") or 0)
        except Exception as e:
            log_warn(f"[subs] Ko-fi amount parse failed (txn={txn_id}): {type(e).__name__}: {e}")
            amount = 0.0

        if currency == "EUR" and amount < 7.0:
            await self._react_kofi(message, False, note=f"txn={txn_id} amount too low: {amount} {currency}")
            return



        # Timestamp parsing
        when = datetime.now(timezone.utc)
        for key in ("timestamp", "time", "created_at"):
            if payload.get(key):
                try:
                    when = datetime.fromisoformat(str(payload[key]).replace("Z", "+00:00")).astimezone(timezone.utc)
                    break
                except Exception:
                    pass

        when_lisbon = when.astimezone(LISBON_TZ)
        purchase_mk = month_key(when_lisbon)

        # Late-month prereg: treat one-time payments after the cutoff day as "next month"
        # so they don't expire mid-next-league.
        effective_mk = purchase_mk
        try:
            if int(when_lisbon.day) >= int(cfg.entitlement_cutoff_day or 99):
                effective_mk = add_months(purchase_mk, 1)
        except Exception:
            effective_mk = purchase_mk

        pass_mk = f"pass:{effective_mk}"

        # One-time pass window:
        # - normal: rolling from now
        # - prereg (effective_mk != purchase_mk): expire as-if paid on day 1 of effective_mk
        starts_at_utc, expires_at_utc = compute_kofi_one_time_window(when_lisbon, cfg.kofi_one_time_days)
        if effective_mk != purchase_mk:
            eff_start_lisbon, _ = month_bounds(effective_mk)
            eff_start_utc = eff_start_lisbon.astimezone(timezone.utc)
            expires_at_utc = eff_start_utc + timedelta(days=max(1, int(cfg.kofi_one_time_days or 30)))
            # keep starts_at_utc as "now" so prereg users can access immediately

        # If the user already bought a one-time pass THIS purchase month, extend it
        existing = None
        try:
            existing = await subs_access.find_one({
                "guild_id": cfg.guild_id,
                "user_id": int(user_id),
                "month": pass_mk,
                "kind": "kofi-one-time",
            })
        except Exception as e:
            log_warn(f"[subs] DB error checking existing pass (txn={txn_id}): {type(e).__name__}: {e}")
            existing = None

        if existing and isinstance(existing.get("expires_at"), datetime):
            prev_exp = existing["expires_at"]
            if prev_exp.tzinfo is None:
                prev_exp = prev_exp.replace(tzinfo=timezone.utc)
            base = prev_exp if prev_exp > starts_at_utc else starts_at_utc
            expires_at_utc = base + timedelta(days=max(1, int(cfg.kofi_one_time_days or 30)))

        source = "kofi-one-time"

        # Dedup by txn_id (store raw event for auditing)
        try:
            await subs_kofi_events.insert_one(
                {
                    "_id": f"{cfg.guild_id}:{txn_id}",
                    "txn_id": txn_id,
                    "guild_id": cfg.guild_id,
                    "user_id": int(user_id),
                    "source": source,
                    "purchase_month": purchase_mk,
                    "effective_month": effective_mk,
                    "starts_at": starts_at_utc,
                    "expires_at": expires_at_utc,
                    "amount": payload.get("amount"),
                    "currency": payload.get("currency"),
                    "created_at": datetime.now(timezone.utc),
                }
            )
        except DuplicateKeyError:
            await self._react_kofi(message, True, note=f"duplicate txn already processed txn={txn_id}")
            return



        # Store / extend rolling access pass
        await subs_access.update_one(
            {"guild_id": cfg.guild_id, "user_id": int(user_id), "month": pass_mk, "kind": "kofi-one-time"},
            {
                "$setOnInsert": {
                    "guild_id": cfg.guild_id,
                    "user_id": int(user_id),
                    "month": pass_mk,  # key: pass:YYYY-MM
                    "purchase_month": purchase_mk,
                    "effective_month": effective_mk,
                    "kind": "kofi-one-time",
                    "created_at": datetime.now(timezone.utc),
                    "starts_at": starts_at_utc,
                },
                "$set": {
                    "updated_at": datetime.now(timezone.utc),
                    "expires_at": expires_at_utc,
                    "last_txn_id": txn_id,
                    "last_source": source,
                    "last_amount": payload.get("amount"),
                    "last_currency": payload.get("currency"),
                },
                "$addToSet": {"sources": source, "txn_ids": txn_id},
            },
            upsert=True,
        )

        # Give ECL now (access is time-based)
        await self._grant_ecl(user_id, reason=f"Ko-fi one-time pass (expires {expires_at_utc.astimezone(LISBON_TZ).strftime('%Y-%m-%d')})")
        await self.log.ok(f"[subs] Ko-fi one-time processed: user_id={user_id} purchase_month={purchase_mk} expires={expires_at_utc.astimezone(LISBON_TZ).strftime('%Y-%m-%d %H:%M')} txn={txn_id}")
        await self._react_kofi(
            message,
            True,
            note=f"processed one-time pass txn={txn_id} user_id={user_id} effective_month={effective_mk}",
        )



    # -------------------- Patreon + role-based free entry --------------------

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        cfg = self.cfg
        if cfg.guild_id and after.guild.id != cfg.guild_id:
            return
        if after.bot:
            return

        before_ids = {r.id for r in (before.roles or [])}
        after_ids = {r.id for r in (after.roles or [])}

        watched = set(cfg.patreon_role_ids or set()) | set(cfg.kofi_role_ids or set()) | set(cfg.free_entry_role_ids or set())
        if not watched:
            return

        # Only react if watched roles actually changed
        before_watch = before_ids.intersection(watched)
        after_watch = after_ids.intersection(watched)
        if before_watch == after_watch:
            return

        added = sorted(after_watch - before_watch)
        removed = sorted(before_watch - after_watch)

        def _label_role(rid: int) -> str:
            r = after.guild.get_role(int(rid))
            if r:
                return f"{r.name}({rid})"
            return f"(missing:{rid})"

        added_lbl = ", ".join(_label_role(r) for r in added) if added else "-"
        removed_lbl = ", ".join(_label_role(r) for r in removed) if removed else "-"

        now_dt = now_lisbon()
        mk = month_key(now_dt)

        # ---- eligibility snapshot (so logs are unambiguous) ----
        has_patreon = self._has_any_role_id(after, cfg.patreon_role_ids)
        has_kofi_role = self._has_any_role_id(after, cfg.kofi_role_ids)
        has_free_role = self._has_any_role_id(after, cfg.free_entry_role_ids)
        has_free_list = await self._has_free_entry(after.id, mk)
        has_db_access = await self._has_db_access(after.id, mk, at=now_dt)

        ok = any([has_patreon, has_kofi_role, has_free_role, has_free_list, has_db_access])

        sources = (
            f"patreon={int(has_patreon)} "
            f"kofi_role={int(has_kofi_role)} "
            f"free_role={int(has_free_role)} "
            f"free_list={int(has_free_list)} "
            f"db_access={int(has_db_access)}"
        )

        # Useful context: do they currently have ECL?
        ecl_role = after.guild.get_role(cfg.ecl_role_id) if cfg.ecl_role_id else None
        has_ecl = bool(ecl_role and ecl_role in (after.roles or []))

        await self._log(
            "[subs] watched roles changed "
            f"user_id={after.id} mk={mk} "
            f"added=[{added_lbl}] removed=[{removed_lbl}] "
            f"has_ecl={int(has_ecl)} sources=({sources})"
        )

        if ok:
            await self._grant_ecl(after.id, reason="Eligibility re-check after role change")
            return

        # Not eligible after the change: only revoke if enforcement is active
        if self._enforcement_active(now_dt):
            await self._log(
                "[subs] revoking ECL (eligibility false) "
                f"user_id={after.id} mk={mk} "
                f"added=[{added_lbl}] removed=[{removed_lbl}] sources=({sources})"
            )
            await self._revoke_ecl_member(after, reason="Eligibility lost", dm=True)
        else:
            await self._log(
                "[subs] (pre-enforcement) would revoke ECL (eligibility false) "
                f"user_id={after.id} mk={mk} "
                f"added=[{added_lbl}] removed=[{removed_lbl}] sources=({sources})"
            )

    # -------------------- Admin commands --------------------

    @commands.slash_command(
        name="subfreeadd",
        description="Add free entry for a user for a given month (YYYY-MM).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def subfreeadd(
        self,
        ctx: discord.ApplicationContext,
        member: discord.Member,
        month: str= discord.Option(str, "Month in format YYYY-MM (e.g., 2026-01)"), 
        reason: str = "free-entry",
    ):
        if not ctx.user.guild_permissions.manage_roles:
            await ctx.respond("You need **Manage Roles**.", ephemeral=True)
            return
        if not re.match(r"^20\d{2}-(0[1-9]|1[0-2])$", month):
            await ctx.respond("Month must be **YYYY-MM**.", ephemeral=True)
            return

        discord_name = member.display_name
        discord_username = member.name

        await subs_free_entries.update_one(
            {"guild_id": ctx.guild.id, "user_id": member.id, "month": month},
            {
                "$setOnInsert": {
                    "guild_id": ctx.guild.id,
                    "user_id": member.id,
                    "month": month,
                    "created_at": datetime.now(timezone.utc),
                },
                "$set": {
                    "reason": reason,
                    "discord_name": discord_name,
                    "discord_username": discord_username,
                    "updated_at": datetime.now(timezone.utc),
                },
            },
            upsert=True,
        )

        await self._grant_ecl(member.id, reason=f"Free entry ({month})")
        await ctx.respond(f"✅ Added free entry for {member.mention} for **{month}**.", ephemeral=True)

    @commands.slash_command(
        name="substatus",
        description="Check if a user is eligible for a given month (YYYY-MM).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def substatus(
        self,
        ctx: discord.ApplicationContext,
        member: discord.Member,
        month: Optional[str] = None,
    ):
        mk = month or month_key(now_lisbon())
        ok, why = await self._eligibility(member, mk, at=now_lisbon())
        await ctx.respond(
            f"**{member.display_name}** for **{mk}**: {'✅ eligible' if ok else '❌ not eligible'}\n{why}",
            ephemeral=True,
        )


    async def subtestdm(self, ctx: discord.ApplicationContext):
        if not ctx.user.guild_permissions.manage_roles:
            await ctx.respond("You need **Manage Roles**.", ephemeral=True)
            return
        if ctx.guild is None:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        # ACK immediately so the interaction doesn't expire
        await ctx.defer(ephemeral=True)

        cfg = self.cfg
        now = now_lisbon()

        # "first option" choices:
        kind_sub = "3d"     # subscription reminder
        kind_top16 = "5d"   # top16-online reminder
        kind_prize = "5d"   # prize reminder

        target_month = add_months(month_key(now), 1)
        current_mk = month_key(now)

        view = self._build_links_view()

        embeds_to_send: list[tuple[str, discord.Embed, Optional[discord.ui.View]]] = []

        # 1) Subscription reminder (3d)
        try:
            registered_count = await self._count_registered_for_month(ctx.guild, target_month)
        except Exception as e:
            log_warn(f"[subs] Error counting registered for {target_month}: {type(e).__name__}: {e}")
            registered_count = 0

        emb_sub = await self._build_reminder_embed(
            kind=kind_sub,
            target_month=target_month,
            registered_count=registered_count,
        )
        embeds_to_send.append(("1/5 • Subscription reminder (3d)", emb_sub, view))

        # 2) Top16 online-games reminder (5d) (placeholder)
        emb_top16 = await self._build_top16_online_reminder_embed(
            kind=kind_top16,
            mk=current_mk,
            rank=1,
            name=str(getattr(ctx.user, "display_name", "") or "Player"),
            online_games=max(0, int(cfg.top16_min_online_games) - 2),
            need_total=int(cfg.top16_min_online_games),
            mention=ctx.user.mention,
        )
        embeds_to_send.append(("2/5 • Top16 online-games reminder (5d)", emb_top16, None))

        # 3) Prize eligibility reminder (5d) (placeholder)
        emb_prize = await self._build_topcut_prize_reminder_embed(
            kind=kind_prize,
            mk=current_mk,
            rank=12,
            pts=1350,
            cutoff_pts=1500,
            mention=ctx.user.mention,
        )
        embeds_to_send.append(("3/5 • Prize eligibility reminder (5d)", emb_prize, view))

        # 4) Access removed notice preview
        emb_removed = discord.Embed(
            title="⚠️ ECL access removed",
            description=(
                "Looks like your subscription/eligibility role is no longer active, so your **ECL** access was removed.\n\n"
                "The league is still running — you can rejoin anytime by subscribing again."
            ),
            color=_get_color(cfg.embed_color),
        )
        emb_removed.add_field(
            name="Need help?",
            value="If you believe this is a mistake, please open a ticket and an admin will help you.",
            inline=False,
        )
        _apply_thumbnail(emb_removed, cfg.embed_thumbnail_url)
        embeds_to_send.append(("4/5 • Access removed notice", emb_removed, view))

        # 5) Free-entry role notice preview (placeholder)
        nice_month = month_label(current_mk)
        roles_txt = "Judge, Arena Vanguard"
        emb_free = discord.Embed(
            title=f"✅ Free entry — {nice_month}",
            description=(
                f"You have **free entry** for **{nice_month}** because you have: **{roles_txt}**.\n\n"
                "If you lose that role, your free entry goes away."
            ),
            color=_get_color(cfg.embed_color),
        )
        emb_free.set_footer(text="ECL • Free entry notice")
        _apply_thumbnail(emb_free, cfg.embed_thumbnail_url)
        embeds_to_send.append(("5/5 • Free-entry role notice", emb_free, None))
        
        emb_flip = self._build_flip_mods_embed(ctx.guild, current_mk)
        embeds_to_send.append(("6/6 • Month flip mods summary", emb_flip, None))

        # Send 1 DM per embed
        try:
            for header, emb, vw in embeds_to_send:
                await ctx.user.send(content=header, embed=emb, view=vw)
                await asyncio.sleep(0.25)
        except Exception:
            await ctx.followup.send("❌ Couldn’t DM you (privacy settings).", ephemeral=True)
            return

        await ctx.followup.send("✅ Sent you all embed previews (one per DM).", ephemeral=True)

    # -------------------- Scheduler (month flip delegated to handler) --------------------

    def _month_close_pending_job_id(self, guild_id: int, cut_month: str) -> str:
        return self.flip_handler.month_close_pending_job_id(guild_id, cut_month)

    def _month_close_done_job_id(self, guild_id: int, cut_month: str) -> str:
        return self.flip_handler.month_close_done_job_id(guild_id, cut_month)

    async def _in_progress_games_count(self) -> Optional[int]:
        return await self.flip_handler.in_progress_games_count()

    async def _ensure_month_close_pending(self, guild: discord.Guild, *, cut_month: str) -> None:
        await self.flip_handler.ensure_month_close_pending(guild, cut_month=cut_month)

    async def _run_month_close_logic(self, guild: discord.Guild, *, cut_month: str) -> None:
        await self.flip_handler.run_month_close_logic(guild, cut_month=cut_month)

    async def _maybe_run_month_close_job(self, guild: discord.Guild, *, cut_month: str) -> None:
        await self.flip_handler.maybe_run_month_close_job(guild, cut_month=cut_month)

    async def _run_monthly_midnight_revoke_job(self, guild: discord.Guild, *, target_month: str) -> None:
        await self.flip_handler.run_monthly_midnight_revoke_job(guild, target_month=target_month)

    @tasks.loop(minutes=5)
    async def _tick(self):
        await self.bot.wait_until_ready()

        try:
            cfg = self.cfg
            if not cfg.guild_id:
                return
            guild = self.bot.get_guild(cfg.guild_id)
            if not guild:
                return

            now = now_lisbon()
            now_mk = month_key(now)

            # Calculate close time and reminder days relative to close_at
            close_at = league_close_at(now_mk)
            d5_before_close = (close_at - timedelta(days=5)).date()
            d3_before_close = (close_at - timedelta(days=3)).date()
            d1_before_close = (close_at - timedelta(days=1)).date()

            # regular "register for next month" reminders
            target_month = add_months(now_mk, 1)  # next month

            if cfg.dm_enabled and now.hour == 10 and now.minute < 5:
                if now.date() == d3_before_close:
                    await self._run_reminder_job(guild, target_month, kind="3d")
                elif now.date() == d1_before_close:
                    await self._run_reminder_job(guild, target_month, kind="last")

            # -------------------- 00:00 month close logic (last day) --------------------
            # When we reach/past close time, mark pending and try to run.
            if now >= close_at:
                await self._ensure_month_close_pending(guild, cut_month=now_mk)
                await self._maybe_run_month_close_job(guild, cut_month=now_mk)
                # Revoke ECL for users not eligible for next month (runs after close completes)
                await self._run_monthly_midnight_revoke_job(guild, target_month=target_month)

            # Also: if we crossed into the new month but last month close is still pending,
            # keep trying to finish it (e.g., games ran long).
            prev_mk = add_months(now_mk, -1)
            if await subs_jobs.find_one({"_id": self._month_close_pending_job_id(guild.id, prev_mk)}):
                await self._maybe_run_month_close_job(guild, cut_month=prev_mk)
                # Also retry revoke for current month if previous close was delayed
                await self._run_monthly_midnight_revoke_job(guild, target_month=now_mk)

            # --- Top16 online-games reminders for CURRENT month ---
            if cfg.dm_enabled and now.hour == 10 and now.minute < 5:
                mk_current = now_mk

                if now.date() == d5_before_close:
                    await self._run_top16_online_reminder_job(guild, mk=mk_current, kind="5d")
                    await self._run_topcut_prize_reminder_job(guild, mk=mk_current, kind="5d")
                elif now.date() == d3_before_close:
                    await self._run_top16_online_reminder_job(guild, mk=mk_current, kind="3d")
                elif now.date() == d1_before_close:
                    await self._run_topcut_prize_reminder_job(guild, mk=mk_current, kind="1d")
                    await self._run_top16_online_reminder_job(guild, mk=mk_current, kind="last")

            # --- /lfgelo unlock announcement ---
            if LFG_ELO_MIN_DAY and now.day >= LFG_ELO_MIN_DAY:
                elo_job_id = f"lfgelo-unlock-announce:{guild.id}:{now_mk}"
                if not await subs_jobs.find_one({"_id": elo_job_id}):
                    elo_ch = guild.get_channel(TOURNAMENT_UPDATES_CHANNEL_ID)
                    if elo_ch and isinstance(elo_ch, discord.TextChannel):
                        embed = discord.Embed(
                            title="⚔️ /lfgelo is now available!",
                            description=(
                                f"Elo-matched lobbies are unlocked for the rest of **{month_label(now_mk)}**.\n"
                                "Use `/lfgelo` to queue into an elo-limited pod!"
                            ),
                            color=0x5865F2,
                        )
                        await elo_ch.send(embed=embed)
                    await subs_jobs.insert_one({"_id": elo_job_id, "ran_at": datetime.now(timezone.utc)})

            # --- Treasure pod checks (using cached TopDeck data) ---
            if TOPDECK_BRACKET_ID:
                try:
                    cached = get_cached_matches(TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN)
                    if cached:
                        matches, entrant_to_uid, player_map = cached
                        
                        # Get current_max_table from matches
                        current_max_table = max((m.id for m in matches), default=0)
                        
                        # Get player count for estimation
                        player_count = None
                        try:
                            rows, _ = await get_league_rows_cached(
                                TOPDECK_BRACKET_ID,
                                FIREBASE_ID_TOKEN,
                                force_refresh=False,
                            )
                            if rows:
                                player_count = len([r for r in rows if not r.dropped])
                        except Exception:
                            pass
                        
                        # Check pending treasure pod results (win/draw)
                        await self._treasure_manager.check_pending_results(
                            guild_id=guild.id,
                            month=now_mk,
                            matches=matches,
                            entrant_to_uid=entrant_to_uid,
                            player_map=player_map,
                            current_max_table=current_max_table,
                            new_player_count=player_count,
                        )

                        # Redistribute any treasure pods that were skipped
                        # (table number already passed without the bot timer firing)
                        await self._treasure_manager.redistribute_skipped_pods(
                            guild_id=guild.id,
                            month=now_mk,
                            current_max_table=current_max_table,
                        )

                        # Check if recalculation needed (if nearing month end)
                        days_until_close = (close_at - now).total_seconds() / 86400.0
                        if days_until_close <= 11 and days_until_close > 0:
                            await self._treasure_manager.check_and_recalculate_if_needed(
                                guild_id=guild.id,
                                month=now_mk,
                                days_until_close=days_until_close,
                                current_max_table=current_max_table,
                                new_player_count=player_count,
                            )
                except Exception as e:
                    await self.log.warn(f"[treasure] Check failed: {type(e).__name__}: {e}")

        except Exception as e:
            tb = traceback.format_exc()
            await self.log.error(f"[subs] ❌ _tick crashed: {type(e).__name__}: {e}\n{tb}")
            return

    @_tick.before_loop
    async def _before_tick(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)

    @tasks.loop(hours=8)
    async def _access_audit(self):
        """Periodic audit: if someone has ECL but no longer has eligibility sources, remove ECL."""
        await self.bot.wait_until_ready()
        cfg = self.cfg
        if not cfg.guild_id:
            return
        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return

        now_dt = now_lisbon()
        if not self._enforcement_active(now_dt):
            return

        async with self._access_audit_lock:
            await self._run_access_audit(guild)

    @_access_audit.before_loop
    async def _before_access_audit(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(10)

    async def _run_access_audit(self, guild: discord.Guild) -> None:
        cfg = self.cfg
        if not cfg.ecl_role_id:
            return

        role = guild.get_role(cfg.ecl_role_id)
        if not role:
            return

        now_dt = now_lisbon()
        if not self._enforcement_active(now_dt):
            return

        mk = month_key(now_dt)

        members = list(role.members)
        if len(members) < 50:
            try:
                members = [m async for m in guild.fetch_members(limit=None)]
                members = [m for m in members if (role in m.roles)]
            except Exception:
                members = list(role.members)

        removed = 0
        checked = 0

        for m in members:
            if m.bot:
                continue
            checked += 1
            ok, _ = await self._eligibility(m, mk, at=now_lisbon())
            if ok:
                continue
            did = await self._revoke_ecl_member(m, reason=f"Audit: not eligible for {mk}", dm=True)
            if did:
                removed += 1

            if cfg.dm_sleep_seconds:
                await asyncio.sleep(cfg.dm_sleep_seconds)

        await self.log.info(f"[subs] access-audit {mk}: checked={checked} removed={removed}")
        # print(f"[subs] access-audit {mk}: checked={checked} removed={removed}")


    # -------------------- Core operations --------------------

    def _has_any_role_id(self, member: discord.Member, role_ids: Set[int]) -> bool:
        if not role_ids:
            return False
        have = {r.id for r in member.roles}
        return bool(have.intersection(role_ids))

    async def _has_db_access(self, user_id: int, month: str, *, at: Optional[datetime] = None) -> bool:
        cfg = self.cfg
        # Legacy calendar-month entitlement (docs without kind=kofi-one-time)
        doc_month = await subs_access.find_one({
            "guild_id": cfg.guild_id,
            "user_id": int(user_id),
            "month": month,
            "kind": {"$ne": "kofi-one-time"},
        })
        if doc_month:
            return True

        # Rolling one-time pass (active at a specific moment)
        if at is None:
            at_utc = datetime.now(timezone.utc)
        else:
            at_utc = at
            if at_utc.tzinfo is None:
                at_utc = at_utc.replace(tzinfo=LISBON_TZ)
            at_utc = at_utc.astimezone(timezone.utc)

        doc_pass = await subs_access.find_one({
            "guild_id": cfg.guild_id,
            "user_id": int(user_id),
            "kind": "kofi-one-time",
            "starts_at": {"$lte": at_utc},
            "expires_at": {"$gt": at_utc},
        })
        return bool(doc_pass)

    async def _has_free_entry(self, user_id: int, month: str) -> bool:
        doc = await subs_free_entries.find_one({"guild_id": self.cfg.guild_id, "user_id": int(user_id), "month": month})
        return bool(doc)

    async def _build_member_index(self, guild: discord.Guild) -> MemberIndex:
        """Fetch members once and build a shared identity index.

        Used for TopDeck row -> Discord ID resolution.
        """
        members: list[discord.Member] = []
        try:
            members = [m async for m in guild.fetch_members(limit=None)]
        except Exception:
            members = []
        return build_member_index(members)


    async def _qualified_top16_discord_ids_for_month(
        self,
        guild: discord.Guild,
        cut_month: str
    ) -> tuple[list[int], list[str]]:
        cfg = self.cfg

        bracket_id = TOPDECK_BRACKET_ID
        firebase_token = FIREBASE_ID_TOKEN
        if not bracket_id:
            return ([], ["TOPDECK_BRACKET_ID not set"])

        try:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=True)
        except Exception:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=False)

        if not rows:
            return ([], ["no TopDeck rows"])

        try:
            y, m = cut_month.split("-")
            year, month = int(y), int(m)
        except Exception:
            return ([], [f"bad cut_month: {cut_month!r}"])

        try:
            online_counts = await count_online_games_by_topdeck_uid_str(
                bracket_id, year, month, online_only=True
            )
        except Exception as e:
            return ([], [f"online_counts error: {type(e).__name__}: {e}"])

        active_by_games = [r for r in rows if (not r.dropped) and (r.games >= cfg.top16_min_total_games)]
        active_by_games = sorted(active_by_games, key=lambda r: (-r.pts, -r.games))

        qualified_candidates: list[PlayerRow] = []
        for r in active_by_games:
            uid = (r.uid or "").strip()
            if not uid:
                continue
            if online_counts.get(uid, 0) >= cfg.top16_min_online_games:
                qualified_candidates.append(r)

        qualified_top16 = qualified_candidates[:16]
        if not qualified_top16:
            return ([], ["no qualified top16"])

        # Shared identity index (Discord ID first; unique handle/name fallback)
        index = await self._build_member_index(guild)

        discord_ids: list[int] = []
        missing: list[str] = []
        conf_counts: dict[str, int] = {}

        for r in qualified_top16:
            res = resolve_row_discord_id(r, index)
            conf_counts[res.confidence] = conf_counts.get(res.confidence, 0) + 1

            if res.discord_id:
                discord_ids.append(int(res.discord_id))
                if res.confidence != "discord_id":
                    await self.log.info(
                        "[subs/identity] qualified-top16 "
                        f"mk={cut_month} row={getattr(r, 'name', '')!r} discord={getattr(r, 'discord', '')!r} "
                        f"-> discord_id={int(res.discord_id)} conf={res.confidence} key={res.matched_key!r}"
                    )
                continue

            missing.append(
                f"{r.discord or r.name or 'unknown'} ({res.confidence}{(' ' + res.matched_key) if res.matched_key else ''})"
            )

        if conf_counts:
            parts = ", ".join(f"{k}={v}" for k, v in sorted(conf_counts.items()))
            await self.log.info(f"[subs/identity] qualified-top16 mk={cut_month} mapping_counts: {parts}")

        seen = set()
        discord_ids = [x for x in discord_ids if not (x in seen or seen.add(x))]

        return (discord_ids, missing)

    async def _eligible_top16_entries_for_month(
        self,
        guild: discord.Guild,
        cut_month: str,
    ) -> tuple[list[dict], list[str]]:
        """Top16 cut (prize eligible): skip ineligible players and promote next eligible.

        Eligibility for online games requirement:
          - Option A: >= top16_min_online_games_no_recency (default 20) games - no recency check
          - Option B: >= top16_min_online_games (default 10) games AND at least 1 game
                      after day top16_recency_after_day (default 20) of the month

        Returns:
          - entries: [{discord_id, row, pts, games}] in award order (after de-dupe)
          - missing: mapping misses (TopDeck row -> Discord)

        NOTE: To keep behavior stable, we still stop after 16 *raw* eligible picks,
        then de-dupe at the end (same as _eligible_top16_discord_ids_for_month did).
        """
        cfg = self.cfg

        bracket_id = TOPDECK_BRACKET_ID
        firebase_token = FIREBASE_ID_TOKEN
        if not bracket_id:
            return ([], ["TOPDECK_BRACKET_ID not set"])

        try:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=True)
        except Exception:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=False)

        if not rows:
            return ([], ["no TopDeck rows"])

        try:
            y, m = cut_month.split("-")
            year, month = int(y), int(m)
        except Exception:
            return ([], [f"bad cut_month: {cut_month!r}"])

        try:
            online_counts = await count_online_games_by_topdeck_uid_str(
                bracket_id, year, month, online_only=True
            )
        except Exception as e:
            return ([], [f"online_counts error: {type(e).__name__}: {e}"])

        active_by_games = [r for r in rows if (not r.dropped) and (r.games >= cfg.top16_min_total_games)]
        active_by_games = sorted(active_by_games, key=lambda r: (-r.pts, -r.games))

        # Eligibility thresholds
        min_games = cfg.top16_min_online_games  # default 10
        min_games_no_recency = cfg.top16_min_online_games_no_recency  # default 20
        recency_after_day = cfg.top16_recency_after_day  # default 20

        # Collect UIDs that need recency check (between min_games and min_games_no_recency)
        uids_need_recency = []
        for r in active_by_games:
            uid = (r.uid or "").strip()
            if not uid:
                continue
            online = online_counts.get(uid, 0)
            if min_games <= online < min_games_no_recency:
                uids_need_recency.append(uid)

        # Check recency for those UIDs
        recency_check: dict[str, bool] = {}
        if uids_need_recency:
            try:
                recency_check = await has_recent_game_by_topdeck_uid(
                    bracket_id, year, month, uids_need_recency,
                    after_day=recency_after_day, online_only=True
                )
            except Exception as e:
                return ([], [f"recency_check error: {type(e).__name__}: {e}"])

        # Qualification:
        # - Option A: >= min_games_no_recency (20) games - no recency needed
        # - Option B: >= min_games (10) games AND has game after day 20
        qualified: list[PlayerRow] = []
        for r in active_by_games:
            uid = (r.uid or "").strip()
            if not uid:
                continue
            online = online_counts.get(uid, 0)
            # Option A: >= 20 games (no recency needed)
            if online >= min_games_no_recency:
                qualified.append(r)
            # Option B: >= 10 games AND has recent game
            elif online >= min_games and recency_check.get(uid, False):
                qualified.append(r)

        if not qualified:
            return ([], ["no qualified candidates"])

        index = await self._build_member_index(guild)

        entries_raw: list[dict] = []
        missing: list[str] = []

        conf_counts: dict[str, int] = {}

        for r in qualified:
            res = resolve_row_discord_id(r, index)
            conf_counts[res.confidence] = conf_counts.get(res.confidence, 0) + 1

            if not res.discord_id:
                missing.append(
                    f"{r.discord or r.name or 'unknown'} ({res.confidence}{(' ' + res.matched_key) if res.matched_key else ''})"
                )
                continue

            did = int(res.discord_id)

            if res.confidence != "discord_id":
                await self.log.info(
                    "[subs/identity] eligible-top16 "
                    f"mk={cut_month} row={getattr(r, 'name', '')!r} discord={getattr(r, 'discord', '')!r} "
                    f"-> discord_id={did} conf={res.confidence} key={res.matched_key!r}"
                )

            member = index.id_to_member.get(int(did)) or await resolve_member(guild, did)
            if not member or member.bot:
                continue

            ok, _ = await self._eligibility(member, cut_month, at=league_close_at(cut_month))

            if not ok:
                continue

            # Keep this aligned with the original behavior:
            # stop after 16 raw picks, then de-dupe afterwards.
            pts_i = int(round(float(getattr(r, "pts", 0) or 0)))
            games_i = int(getattr(r, "games", 0) or 0)
            entries_raw.append({
                "discord_id": int(did),
                "row": r,
                "pts": pts_i,
                "games": games_i,
            })

            if len(entries_raw) >= 16:
                break

        if conf_counts:
            parts = ", ".join(f"{k}={v}" for k, v in sorted(conf_counts.items()))
            await self.log.info(f"[subs/identity] eligible-top16 mk={cut_month} mapping_counts: {parts}")

        # De-dupe, preserving first occurrence (matches previous return behavior)
        seen: set[int] = set()
        entries: list[dict] = []
        for e in entries_raw:
            did = int(e.get("discord_id") or 0)
            if not did or did in seen:
                continue
            seen.add(did)
            entries.append(e)

        return (entries, missing)

    async def _eligible_top16_discord_ids_for_month(
        self,
        guild: discord.Guild,
        cut_month: str,
    ) -> tuple[list[int], list[str]]:
        """Top16 cut (prize eligible): skip ineligible players and promote next eligible."""
        entries, missing = await self._eligible_top16_entries_for_month(guild, cut_month)
        return ([int(e["discord_id"]) for e in (entries or [])], missing)

    async def _topcut_prize_reminder_targets(
        self,
        guild: discord.Guild,
        *,
        mk: str,
    ) -> tuple[list[dict], list[str], int]:
        """Targets for 'prize eligibility' reminder."""
        cfg = self.cfg

        bracket_id = TOPDECK_BRACKET_ID
        firebase_token = FIREBASE_ID_TOKEN

        if not bracket_id:
            return ([], ["TOPDECK_BRACKET_ID not set"], 0)

        try:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=True)
        except Exception:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=False)

        if not rows:
            return ([], ["no TopDeck rows"], 0)

        try:
            y, m = mk.split("-")
            year, month = int(y), int(m)
        except Exception:
            return ([], [f"bad mk: {mk!r}"], 0)

        try:
            online_counts = await count_online_games_by_topdeck_uid_str(
                bracket_id, year, month, online_only=True
            )
        except Exception as e:
            return ([], [f"online_counts error: {type(e).__name__}: {e}"], 0)

        active_by_games = [r for r in rows if (not r.dropped) and (r.games >= cfg.top16_min_total_games)]
        active_by_games = sorted(active_by_games, key=lambda r: (-r.pts, -r.games))

        qualified: list[tuple[int, PlayerRow]] = []
        for idx, r in enumerate(active_by_games, start=1):
            uid = (r.uid or "").strip()
            if not uid:
                continue
            if online_counts.get(uid, 0) >= cfg.top16_min_online_games:
                qualified.append((idx, r))

        if not qualified:
            return ([], ["no qualified candidates"], 0)

        index = await self._build_member_index(guild)

        checked: list[dict] = []
        misses: list[str] = []

        conf_counts: dict[str, int] = {}

        eligible_count = 0
        cutoff_pts: Optional[int] = None
        margin = int(getattr(cfg, "topcut_close_pts", 250) or 250)

        for rank, r in qualified:
            res = resolve_row_discord_id(r, index)
            conf_counts[res.confidence] = conf_counts.get(res.confidence, 0) + 1

            if not res.discord_id:
                misses.append(
                    f"{r.discord or r.name or 'unknown'} ({res.confidence}{(' ' + res.matched_key) if res.matched_key else ''})"
                )
                continue

            did = int(res.discord_id)

            # Only log the non-ID cases to avoid noise (this can scan more than 16 rows)
            if res.confidence != "discord_id":
                await self.log.info(
                    "[subs/identity] topcut-prize "
                    f"mk={mk} row={getattr(r, 'name', '')!r} discord={getattr(r, 'discord', '')!r} "
                    f"-> discord_id={did} conf={res.confidence} key={res.matched_key!r}"
                )

            member = index.id_to_member.get(int(did)) or await resolve_member(guild, did)
            if not member or member.bot:
                continue

            ok, _ = await self._eligibility(member, mk, at=league_close_at(mk))

            pts_int = int(round(float(getattr(r, "pts", 0) or 0)))
            checked.append({
                "rank": int(rank),
                "discord_id": int(did),
                "name": str(getattr(r, "name", "") or ""),
                "pts": pts_int,
                "eligible": bool(ok),
            })

            if ok:
                eligible_count += 1
                if eligible_count == 16:
                    cutoff_pts = pts_int

            if cutoff_pts is not None and pts_int < (cutoff_pts - margin):
                break

        if conf_counts:
            parts = ", ".join(f"{k}={v}" for k, v in sorted(conf_counts.items()))
            await self.log.info(f"[subs/identity] topcut-prize mk={mk} mapping_counts: {parts}")

        if cutoff_pts is None:
            cutoff_pts = int(checked[min(15, len(checked)-1)]["pts"]) if checked else 0

        min_pts = cutoff_pts - margin
        targets = [e for e in checked if (not e["eligible"]) and (e["pts"] >= min_pts)]

        seen: set[int] = set()
        out: list[dict] = []
        for e in targets:
            did = int(e["discord_id"])
            if did in seen:
                continue
            seen.add(did)
            out.append(e)

        return (out, misses, int(cutoff_pts))

    async def _build_topcut_prize_reminder_embed(
        self,
        *,
        kind: str,          # "5d" | "1d"
        mk: str,
        rank: int,
        pts: int,
        cutoff_pts: int,
        mention: str,
    ) -> discord.Embed:
        """Build Top16 prize eligibility reminder embed."""
        cfg = self.cfg
        margin = int(getattr(cfg, "topcut_close_pts", 250) or 250)
        return build_topcut_prize_reminder_embed(
            kind=kind,
            mk=mk,
            rank=rank,
            pts=pts,
            cutoff_pts=cutoff_pts,
            mention=mention,
            margin=margin,
            embed_color=cfg.embed_color,
            embed_thumbnail_url=cfg.embed_thumbnail_url,
        )

    async def _run_topcut_prize_reminder_job(self, guild: discord.Guild, *, mk: str, kind: str) -> None:
        job_id = f"topcut-prize-remind:{guild.id}:{mk}:{kind}"
        if not await job_once(job_id):
            return

        cfg = self.cfg
        targets, misses, cutoff_pts = await self._topcut_prize_reminder_targets(guild, mk=mk)

        if misses:
            await self.log.info(f"[subs] Topcut-prize mapping misses ({mk} {kind}): " + ", ".join(misses[:20]))

        if not targets:
            await self.log.info(f"[subs] Topcut-prize reminder ({mk} {kind}): 0 targets")
            log_sync(f"[subs] Topcut-prize reminder ({mk} {kind}): 0 targets")
            await self._dm_mods_summary(
                guild,
                summary=f"[ECL] Topcut-prize reminder ({mk} {kind}) — sent 0 DMs (0 targets).",
            )
            return

        sem = asyncio.Semaphore(cfg.dm_concurrency)
        sent = 0

        async def _send_one(entry: dict):
            nonlocal sent
            async with sem:
                uid = int(entry["discord_id"])
                member = await resolve_member(guild, uid)
                if not member or member.bot:
                    return

                try:
                    emb = await self._build_topcut_prize_reminder_embed(
                        kind=kind,
                        mk=mk,
                        rank=int(entry["rank"]),
                        pts=int(entry["pts"]),
                        cutoff_pts=int(cutoff_pts),
                        mention=member.mention,
                    )
                    await member.send(embed=emb, view=self._build_links_view())
                    sent += 1
                except Exception:
                    return

                if cfg.dm_sleep_seconds:
                    await asyncio.sleep(cfg.dm_sleep_seconds)

        await asyncio.gather(*[_send_one(e) for e in targets])
        await self.log.ok(f"[subs] Topcut-prize reminder ({mk} {kind}) sent {sent}/{len(targets)}")
        await self._dm_mods_summary(
            guild,
            summary=f"[ECL] Topcut-prize reminder ({mk} {kind}) — sent {sent}/{len(targets)} DMs.",
        )


    async def _eligibility(self, member: discord.Member, month: str, *, at: Optional[datetime] = None) -> Tuple[bool, str]:
        cfg = self.cfg
        if self._has_any_role_id(member, cfg.patreon_role_ids):
            return True, "Reason: Patreon role"
        if self._has_any_role_id(member, cfg.kofi_role_ids):
            return True, "Reason: Ko-fi role"
        if self._has_any_role_id(member, cfg.free_entry_role_ids):
            return True, "Reason: Free-entry role"
        if await self._has_free_entry(member.id, month):
            return True, "Reason: Free-entry list"
        if await self._has_db_access(member.id, month, at=at):
            return True, "Reason: Ko-fi entitlement"
        return False, "Reason: none"

    async def _grant_ecl(self, user_id: int, reason: str):
        cfg = self.cfg
        if not cfg.guild_id or not cfg.ecl_role_id:
            return
        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return
        role = guild.get_role(cfg.ecl_role_id)
        if not role:
            return
        member = await resolve_member(guild, user_id)
        if not member or member.bot:
            return
        if role in member.roles:
            return
        with contextlib.suppress(Exception):
            await member.add_roles(role, reason=reason)

    async def _revoke_ecl_member(self, member: discord.Member, reason: str, *, dm: bool = False) -> bool:
        """Remove ECL role from a member if present. Returns True if removed."""
        cfg = self.cfg
        if not cfg.guild_id or not cfg.ecl_role_id:
            return False
        if member.guild.id != cfg.guild_id:
            return False

        role = member.guild.get_role(cfg.ecl_role_id)
        if not role:
            return False

        if role not in member.roles:
            return False

        removed = False
        with contextlib.suppress(Exception):
            await member.remove_roles(role, reason=reason)
            removed = role not in member.roles

        if removed and dm:
            await self._dm_access_removed(member)

        return removed

    async def _revoke_ecl(self, user_id: int, reason: str, *, dm: bool = False) -> bool:
        cfg = self.cfg
        if not cfg.guild_id or not cfg.ecl_role_id:
            return False
        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return False

        member = await resolve_member(guild, user_id)
        if not member or member.bot:
            return False

        return await self._revoke_ecl_member(member, reason=reason, dm=dm)

    async def _dm_access_removed(self, member: discord.Member) -> None:
        """One-time DM when we remove ECL due to lost eligibility."""
        cfg = self.cfg
        
        if not self._dm_opted_in(member):
            return

        now_dt = now_lisbon()
        if not self._enforcement_active(now_dt):
            return

        mk = month_key(now_dt)
        job_id = f"ecl-revoked-dm:{cfg.guild_id}:{int(member.id)}:{mk}"
        with contextlib.suppress(Exception):
            if await subs_jobs.find_one({"_id": job_id}):
                return
            await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        emb = discord.Embed(
            title="⚠️ ECL access removed",
            description=(
                "Looks like your subscription/eligibility role is no longer active, so your **ECL** access was removed.\n\n"
                "The league is still running — you can rejoin anytime by subscribing again."
            ),
            color=_get_color(cfg.embed_color),
        )

        emb.add_field(
            name="Need help?",
            value="If you believe this is a mistake, please open a ticket and an admin will help you.",
            inline=False,
        )

        _apply_thumbnail(emb, cfg.embed_thumbnail_url)

        view = self._build_links_view()
        with contextlib.suppress(Exception):
            await member.send(embed=emb, view=view)


    async def _grant_top16(self, user_id: int, reason: str):
        cfg = self.cfg
        if not cfg.guild_id or not cfg.top16_role_id:
            return

        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return

        role = guild.get_role(cfg.top16_role_id)
        if not role:
            return

        member = await resolve_member(guild, user_id)
        if not member or member.bot or role in member.roles:
            return

        with contextlib.suppress(Exception):
            await member.add_roles(role, reason=reason)

    async def _run_reminder_job(self, guild: discord.Guild, target_month: str, kind: str):
        job_id = f"remind:{guild.id}:{target_month}:{kind}"
        if not await job_once(job_id):
            return

        cfg = self.cfg
        role = guild.get_role(cfg.ecl_role_id) if cfg.ecl_role_id else None
        if not role:
            return

        # Evaluate eligibility at the exact flip moment (start of target_month).
        flip_at = month_bounds(target_month)[0]

        members = list(role.members)
        if len(members) < 50:
            members = [m async for m in guild.fetch_members(limit=None)]
            members = [m for m in members if role in m.roles]

        # Build the raw target list (ineligible for target_month)
        to_dm_all: list[discord.Member] = []
        for m in members:
            if m.bot:
                continue
            ok, _ = await self._eligibility(m, target_month, at=flip_at)
            if not ok:
                to_dm_all.append(m)

        # Opt-in gate: only DM members who opted in (if DM opt-in role is configured)
        to_dm = [m for m in to_dm_all if self._dm_opted_in(m)]
        skipped_optout = len(to_dm_all) - len(to_dm)

        count = await self._count_registered_for_month(guild, target_month)
        emb = await self._build_reminder_embed(
            kind=kind,
            target_month=target_month,
            registered_count=count,
        )

        await self._log(
            f"[subs] Reminder '{kind}' for {target_month}: "
            f"ineligible={len(to_dm_all)} opted_in={len(to_dm)} skipped_optout={skipped_optout} "
            f"(registered={count})"
        )

        sem = asyncio.Semaphore(cfg.dm_concurrency)
        sent = 0

        async def _send(member: discord.Member):
            nonlocal sent
            async with sem:
                try:
                    await member.send(embed=emb, view=self._build_links_view())
                    sent += 1
                except Exception:
                    pass
                if cfg.dm_sleep_seconds:
                    await asyncio.sleep(cfg.dm_sleep_seconds)

        await asyncio.gather(*[_send(m) for m in to_dm])

        await self.log.ok(f"[subs] Reminder '{kind}' for {target_month}: sent {sent}/{len(to_dm)}")

        await self._dm_mods_summary(
            guild,
            summary=(
                f"[ECL] Subscription reminder ({target_month} {kind}) — "
                f"sent {sent}/{len(to_dm)} DMs (skipped_optout={skipped_optout})."
            ),
        )



    async def _apply_top16_cut_for_next_month(self, guild: discord.Guild, *, cut_month: str, target_month: str):
        """Apply Top16 cut - delegated to flip handler."""
        await self.flip_handler.apply_top16_cut_for_next_month(guild, cut_month=cut_month, target_month=target_month)

    async def _run_cleanup_job(self, guild: discord.Guild, target_month: str):
        """Cleanup job - delegated to flip handler."""
        await self.flip_handler.run_cleanup_job(guild, target_month)

    # -------------------- Flip reminders (delegated to handler) --------------------

    async def _run_flip_mods_reminder_job(self, guild: discord.Guild, *, mk: str) -> None:
        """DM mods the month-flip checklist - delegated to flip handler."""
        await self.flip_handler.run_flip_mods_reminder_job(guild, mk=mk)

    async def _run_free_role_flip_info_job(self, guild: discord.Guild, *, mk: str) -> None:
        """DM free-role users - delegated to flip handler."""
        await self.flip_handler.run_free_role_flip_info_job(guild, mk=mk)

    async def _log(self, text: str):
        await self.log.info(text)


def setup(bot: commands.Bot):
    bot.add_cog(SubscriptionsCog(bot))