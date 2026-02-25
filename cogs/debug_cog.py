# cogs/debug_cog.py
"""
Moderator-only debug utilities.

Goal: provide SAFE "dry-run" tools that emulate logic without applying side-effects
(no role changes, no DB writes), so mods can verify behavior before the scheduled jobs run.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, List

import discord
from discord.ext import commands
from discord import Option

from db import subs_free_entries, treasure_pod_schedule, treasure_pods as treasure_pods_col
from utils.logger import get_logger
from utils.settings import GUILD_ID, SUBS, LISBON_TZ
from utils.mod_check import is_mod
from utils.treasure_pods import TreasurePodManager
from utils.persistence import (
    get_guild_timers as db_get_guild_timers,
    get_guild_lobbies as db_get_guild_lobbies,
)
from utils.dates import (
    month_key,
    add_months,
    month_bounds,
    league_close_at,
    month_label,
    looks_like_month,
    now_lisbon,
)





def _chunk_lines_for_embed(lines: list[str], limit: int = 1024) -> list[str]:
    """Split lines into chunks that fit Discord embed field value limits.

    Prevents slicing mid-line (which looks like truncated entries).
    """
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0

    for raw in lines:
        line = (raw or "").rstrip()
        if not line:
            continue

        # If a single line is too long, hard-truncate it (rare).
        if len(line) > limit:
            line = line[: max(0, limit - 1)] + "…"

        add_len = len(line) + (1 if cur else 0)
        if cur and (cur_len + add_len) > limit:
            chunks.append("\n".join(cur))
            cur = [line]
            cur_len = len(line)
        else:
            cur.append(line)
            cur_len += add_len

    if cur:
        chunks.append("\n".join(cur))

    return chunks


class DebugCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.cfg = SUBS
        self.log = get_logger(bot, self.cfg)

    def _is_mod(self, member: discord.Member) -> bool:
        """Check if member is a mod. Delegates to utils.mod_check.is_mod."""
        return is_mod(member, check_manage_roles=True)

    def _subs_cog(self):
        return self.bot.get_cog("SubscriptionsCog")

    @commands.slash_command(
        name="debug",
        description="Moderator-only debug tools (dry-runs; no role/DB changes).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def debug(
        self,
        ctx: discord.ApplicationContext,
        action: str = Option(
            str,
            "Which debug action to run.",
            required=True,
            choices=["month_flip", "top16onflip", "subs_dms_preview", "timers", "lobbies", "backfill_free_roles", "treasure_stats"],
        ),
        month: Optional[str] = Option(
            str,
            "Month YYYY-MM for month_flip/top16onflip/backfill/treasure_stats (default: current).",
            required=False,
        ),
    ):
        if ctx.guild is None:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        if not isinstance(ctx.user, discord.Member) or not self._is_mod(ctx.user):
            await ctx.respond("You need **Manage Roles** (or the configured mod role).", ephemeral=True)
            return

        action = (action or "").strip().lower()

        # --- Full month flip dry-run preview ---
        if action == "month_flip":
            await self._debug_month_flip(ctx, month)
            return

        # Delegate to existing command (keeps behavior identical)
        if action == "subs_dms_preview":
            subs = self._subs_cog()
            if subs is None:
                await ctx.respond("SubscriptionsCog is not loaded.", ephemeral=True)
                return
            await subs.subtestdm(ctx)
            return

        # --- Persisted timers debug ---
        if action == "timers":
            await self._debug_timers(ctx)
            return

        # --- Persisted lobbies debug ---
        if action == "lobbies":
            await self._debug_lobbies(ctx)
            return

        # --- Backfill free-role DB entries (one-time migration tool) ---
        if action == "backfill_free_roles":
            await self._backfill_free_roles(ctx, month)
            return

        # --- Treasure pod stats ---
        if action == "treasure_stats":
            await self._debug_treasure_stats(ctx, month)
            return

        # --- top16onflip dry-run preview ---
        mk = (month or "").strip()
        now = now_lisbon()
        if not mk:
            mk = month_key(now)
        if not looks_like_month(mk):
            await ctx.respond("Month must be **YYYY-MM** (e.g., 2026-01).", ephemeral=True)
            return

        subs = self._subs_cog()
        if subs is None:
            await ctx.respond("SubscriptionsCog is not loaded.", ephemeral=True)
            return

        if not ctx.response.is_done():
            try:
                await ctx.defer(ephemeral=True)
            except discord.HTTPException:
                pass

        target_mk = add_months(mk, 1)
        close_at = league_close_at(mk)

        # IMPORTANT: use the SAME logic used by the real month-close job.
        # This is a pure read path (no roles, no DB writes).
        try:
            if hasattr(subs, "_eligible_top16_entries_for_month"):
                entries, missing = await subs._eligible_top16_entries_for_month(ctx.guild, mk)
            else:
                eligible_ids, missing = await subs._eligible_top16_discord_ids_for_month(ctx.guild, mk)
                entries = [{"discord_id": int(d), "pts": 0, "games": 0, "row": None} for d in (eligible_ids or [])]
        except Exception as e:
            await ctx.followup.send(f"❌ Failed to compute Top16 preview: {type(e).__name__}: {e}", ephemeral=True)
            return

        # Resolve members for display (keep aligned with entries order)
        display_entries: List[dict] = []
        for e in (entries or [])[:16]:
            did = int(e.get("discord_id") or 0)
            if not did:
                continue
            mbr = ctx.guild.get_member(did)
            if mbr is None:
                try:
                    mbr = await ctx.guild.fetch_member(did)
                except Exception:
                    mbr = None
            if mbr is None:
                continue
            e2 = dict(e)
            e2["member"] = mbr
            display_entries.append(e2)

        cfg = subs.cfg if hasattr(subs, "cfg") else self.cfg
        top16_role_id = int(getattr(cfg, "top16_role_id", 0) or 0)
        top16_role = ctx.guild.get_role(top16_role_id) if top16_role_id else None

        emb = discord.Embed(
            title=f"🧪 Top16 flip preview — {month_label(mk)} → {month_label(target_mk)}",
            description=(
                "**Dry-run only** (no role/DB changes).\n\n"
                f"At **{close_at.strftime('%Y-%m-%d %H:%M')} Lisbon**, the month-close job would award:\n"
                f"• **Top16 role** ({top16_role.mention if top16_role else 'not configured'})\n\n"
                f"**Projected winners ({len(display_entries)}/16):**"
            ),
            color=int(getattr(cfg, "embed_color", 0x2ECC71) or 0x2ECC71),
        )

        if display_entries:
            lines = []
            for i, e in enumerate(display_entries[:16], start=1):
                m = e.get("member")
                if not isinstance(m, discord.Member):
                    continue
                pts = int(e.get("pts") or 0)
                games = int(e.get("games") or 0)
                row = e.get("row")
                td_name = str(getattr(row, "name", "") or "").strip() if row else ""
                if not td_name:
                    td_name = m.display_name
                lines.append(f"**{i:02d}.** {m.mention} — {td_name} - {pts} - {games} games")
            
            # Now chunk and add fields AFTER building all lines
            chunks = _chunk_lines_for_embed(lines, limit=1024)
            for idx, chunk in enumerate(chunks):
                emb.add_field(
                    name="Would receive Top16 role" if idx == 0 else "​",
                    value=chunk,
                    inline=False,
                )
        else:
            emb.add_field(
                name="Would receive Top16 role",
                value="(none resolved — likely mapping misses)",
                inline=False,
            )

        # Show config requirements (same as subscriptions logic)
        try:
            req_total = int(getattr(cfg, "top16_min_total_games", 0) or 0)
            req_online = int(getattr(cfg, "top16_min_online_games", 0) or 0)
            emb.add_field(
                name="Requirements used",
                value=f"min_total_games={req_total} • min_online_games={req_online} • eligibility checked at close time",
                inline=False,
            )
        except Exception:
            pass

        if missing:
            preview = "\n".join(f"• {x}" for x in missing[:10])
            if len(missing) > 10:
                preview += f"\n… (+{len(missing) - 10} more)"
            emb.add_field(
                name=f"Mapping misses ({len(missing)})",
                value=preview[:1024],
                inline=False,
            )

        emb.set_footer(text="ECL Debug • top16onflip")

        await ctx.followup.send(embed=emb, ephemeral=True)

    async def _debug_month_flip(self, ctx: discord.ApplicationContext, month: Optional[str]) -> None:
        """Comprehensive dry-run preview of everything that happens on month flip."""
        subs = self._subs_cog()
        if subs is None:
            await ctx.respond("SubscriptionsCog is not loaded.", ephemeral=True)
            return

        mk = (month or "").strip()
        now = now_lisbon()
        if not mk:
            mk = month_key(now)
        if not looks_like_month(mk):
            await ctx.respond("Month must be **YYYY-MM** (e.g., 2026-01).", ephemeral=True)
            return

        # Safely defer - handle case where interaction might already be acknowledged
        if not ctx.response.is_done():
            try:
                await ctx.defer(ephemeral=True)
            except discord.HTTPException:
                pass  # Already acknowledged, continue anyway

        guild = ctx.guild
        cfg = subs.cfg if hasattr(subs, "cfg") else self.cfg
        target_mk = add_months(mk, 1)
        close_at = league_close_at(mk)
        flip_at = month_bounds(target_mk)[0]

        embeds: List[discord.Embed] = []

        # ==================== EMBED 1: Overview & Status ====================
        emb1 = discord.Embed(
            title=f"🔄 Month Flip Preview — {month_label(mk)}",
            description=(
                "**Dry-run only** — no roles changed, no DB writes.\n\n"
                f"**Cut month:** {month_label(mk)}\n"
                f"**Target month:** {month_label(target_mk)}\n"
                f"**Close time:** {close_at.strftime('%Y-%m-%d %H:%M')} Lisbon\n"
                f"At close: Top16 role granted, free-role DB entries, ECL revoked for ineligible"
            ),
            color=0x3498DB,
        )

        # Check in-progress games
        try:
            if hasattr(subs, 'flip_handler'):
                in_progress = await subs.flip_handler.in_progress_games_count()
            else:
                in_progress = await subs._in_progress_games_count()
            
            if in_progress is None:
                games_status = "❓ Unknown (TopDeck check failed)"
            elif in_progress > 0:
                games_status = f"⏳ **{in_progress}** game(s) in progress — close would be BLOCKED"
            else:
                games_status = "✅ No games in progress — close would proceed"
        except Exception as e:
            games_status = f"❌ Error: {type(e).__name__}"

        emb1.add_field(name="In-Progress Games", value=games_status, inline=False)

        # Check job status
        from db import subs_jobs
        pending_id = subs._month_close_pending_job_id(guild.id, mk)
        done_id = subs._month_close_done_job_id(guild.id, mk)
        pending_doc = await subs_jobs.find_one({"_id": pending_id})
        done_doc = await subs_jobs.find_one({"_id": done_id})

        if done_doc:
            job_status = f"✅ Already completed at {done_doc.get('ran_at', '?')}"
        elif pending_doc:
            job_status = f"⏳ Pending since {pending_doc.get('ran_at', '?')}"
        else:
            job_status = "🔜 Not started yet"

        emb1.add_field(name="Job Status", value=job_status, inline=False)
        emb1.set_footer(text="ECL Debug • month_flip (1/4)")
        embeds.append(emb1)

        # ==================== EMBED 2: Top16 Recipients ====================
        emb2 = discord.Embed(
            title="🏆 Step 1: Top16 Cut",
            description="Who would receive **Top16 role**:",
            color=0xF1C40F,
        )

        try:
            if hasattr(subs, "_eligible_top16_entries_for_month"):
                entries, missing = await subs._eligible_top16_entries_for_month(guild, mk)
            else:
                eligible_ids, missing = await subs._eligible_top16_discord_ids_for_month(guild, mk)
                entries = [{"discord_id": int(d)} for d in (eligible_ids or [])]

            if entries:
                lines = []
                for i, e in enumerate(entries[:16], start=1):
                    did = int(e.get("discord_id") or 0)
                    mbr = guild.get_member(did)
                    if mbr:
                        pts = int(e.get("pts") or 0)
                        lines.append(f"**{i:02d}.** {mbr.mention} ({pts} pts)")
                    else:
                        lines.append(f"**{i:02d}.** User {did} (not in server)")
                
                chunks = _chunk_lines_for_embed(lines, limit=1024)
                for idx, chunk in enumerate(chunks):
                    emb2.add_field(
                        name="Recipients" if idx == 0 else "​",
                        value=chunk,
                        inline=False,
                    )
            else:
                emb2.add_field(name="Recipients", value="(none qualified)", inline=False)

            if missing and missing != ["no qualified top16"]:
                preview = ", ".join(missing[:5])
                if len(missing) > 5:
                    preview += f" (+{len(missing) - 5} more)"
                emb2.add_field(name=f"Mapping misses ({len(missing)})", value=preview[:1024], inline=False)

        except Exception as e:
            emb2.add_field(name="Error", value=f"{type(e).__name__}: {e}", inline=False)

        emb2.set_footer(text="ECL Debug • month_flip (2/4)")
        embeds.append(emb2)

        # ==================== EMBED 3: Flip Notifications ====================
        emb3 = discord.Embed(
            title="📬 Step 2-3: Flip Notifications",
            description="Who would receive DMs after the flip:",
            color=0x9B59B6,
        )

        # Mods who would get checklist
        mod_role_id = int(getattr(cfg, "ecl_mod_role_id", 0) or 0)
        mod_role = guild.get_role(mod_role_id) if mod_role_id else None
        if mod_role:
            mod_members = [m for m in (mod_role.members or []) if not m.bot]
            mod_names = ", ".join(m.display_name for m in mod_members[:10])
            if len(mod_members) > 10:
                mod_names += f" (+{len(mod_members) - 10} more)"
            emb3.add_field(
                name=f"Mods Checklist DM ({len(mod_members)})",
                value=mod_names or "(none)",
                inline=False,
            )
        else:
            emb3.add_field(name="Mods Checklist DM", value="(mod role not configured)", inline=False)

        # Free-role users who would get DMs
        free_role_ids = set(int(x) for x in (getattr(cfg, "free_entry_role_ids", set()) or set()) if int(x))
        free_role_users: dict[int, list[str]] = {}
        for rid in free_role_ids:
            role = guild.get_role(rid)
            if not role:
                continue
            for m in getattr(role, "members", []) or []:
                if m.bot:
                    continue
                free_role_users.setdefault(m.id, []).append(role.name)

        if free_role_users:
            sample = []
            for uid, roles in list(free_role_users.items())[:5]:
                mbr = guild.get_member(uid)
                name = mbr.display_name if mbr else f"User {uid}"
                sample.append(f"{name} ({', '.join(roles)})")
            preview = "\n".join(f"• {s}" for s in sample)
            if len(free_role_users) > 5:
                preview += f"\n… (+{len(free_role_users) - 5} more)"
            emb3.add_field(
                name=f"Free-Role Info DM ({len(free_role_users)})",
                value=preview,
                inline=False,
            )
        else:
            emb3.add_field(name="Free-Role Info DM", value="(none — no free-entry roles configured or no members)", inline=False)

        emb3.set_footer(text="ECL Debug • month_flip (3/4)")
        embeds.append(emb3)

        # ==================== EMBED 4: ECL Revoke ====================
        emb4 = discord.Embed(
            title="🔒 ECL Revoke Preview",
            description=f"Who would **lose ECL role** at close time (not eligible for {month_label(target_mk)}):",
            color=0xE74C3C,
        )

        ecl_role_id = int(getattr(cfg, "ecl_role_id", 0) or 0)
        ecl_role = guild.get_role(ecl_role_id) if ecl_role_id else None

        if ecl_role:
            members_with_ecl = list(ecl_role.members)
            would_lose: List[discord.Member] = []
            would_keep: int = 0

            # Check eligibility for each (sample up to 100 to avoid timeout)
            check_limit = min(len(members_with_ecl), 100)
            for m in members_with_ecl[:check_limit]:
                if m.bot:
                    continue
                try:
                    ok, reason = await subs._eligibility(m, target_mk, at=flip_at)
                    if not ok:
                        would_lose.append(m)
                    else:
                        would_keep += 1
                except Exception:
                    pass

            if would_lose:
                lines = [f"• {m.mention}" for m in would_lose[:15]]
                if len(would_lose) > 15:
                    lines.append(f"… (+{len(would_lose) - 15} more)")
                emb4.add_field(
                    name=f"Would LOSE ECL ({len(would_lose)})",
                    value="\n".join(lines),
                    inline=False,
                )
            else:
                emb4.add_field(name="Would LOSE ECL", value="(none in sample)", inline=False)

            emb4.add_field(
                name="Summary",
                value=f"Checked: {check_limit}/{len(members_with_ecl)} • Keep: {would_keep} • Lose: {len(would_lose)}",
                inline=False,
            )

            if len(members_with_ecl) > check_limit:
                emb4.add_field(
                    name="⚠️ Note",
                    value=f"Only checked first {check_limit} members. Full check happens at close time.",
                    inline=False,
                )
        else:
            emb4.add_field(name="ECL Role", value="(not configured)", inline=False)

        emb4.set_footer(text="ECL Debug • month_flip (4/4)")
        embeds.append(emb4)

        # Send all embeds
        await ctx.followup.send(embeds=embeds, ephemeral=True)

    async def _debug_timers(self, ctx: discord.ApplicationContext) -> None:
        """Show persisted timers for this guild (from DB + in-memory)."""
        if not ctx.response.is_done():
            try:
                await ctx.defer(ephemeral=True)
            except discord.HTTPException:
                pass

        guild = ctx.guild
        if not guild:
            await ctx.followup.send("No guild.", ephemeral=True)
            return

        # Get from DB
        try:
            db_timers = await db_get_guild_timers(guild.id)
        except Exception as e:
            await ctx.followup.send(f"Error fetching timers from DB: {e}", ephemeral=True)
            return

        # Get from in-memory (timer cog)
        timer_cog = self.bot.get_cog("ECLTimerCog")
        in_memory_active = {}
        in_memory_paused = {}
        if timer_cog:
            in_memory_active = dict(getattr(timer_cog, "active_timers", {}))
            in_memory_paused = dict(getattr(timer_cog, "paused_timers", {}))

        emb = discord.Embed(
            title="🕐 Persisted Timers",
            description=f"Guild: {guild.name}",
            color=0x3498DB,
        )

        if db_timers:
            lines = []
            for t in db_timers[:10]:
                tid = t.get("timer_id", "?")
                status = t.get("status", "?")
                vc_id = t.get("voice_channel_id", 0)
                vc = guild.get_channel(vc_id)
                vc_name = vc.name if vc else f"(deleted: {vc_id})"
                in_mem = "✅" if (tid in in_memory_active or tid in in_memory_paused) else "❌"
                lines.append(f"• `{tid}` — {status} — {vc_name} — in-mem: {in_mem}")
            emb.add_field(
                name=f"DB Timers ({len(db_timers)})",
                value="\n".join(lines) or "(none)",
                inline=False,
            )
        else:
            emb.add_field(name="DB Timers", value="(none)", inline=False)

        # In-memory only (not in DB - shouldn't happen normally)
        db_ids = {t.get("timer_id") for t in db_timers}
        mem_only = []
        for tid in list(in_memory_active.keys()) + list(in_memory_paused.keys()):
            if tid not in db_ids:
                status = "active" if tid in in_memory_active else "paused"
                mem_only.append(f"• `{tid}` — {status}")
        if mem_only:
            emb.add_field(
                name="In-memory only (not in DB)",
                value="\n".join(mem_only[:10]),
                inline=False,
            )

        emb.set_footer(text="ECL Debug • timers")
        await ctx.followup.send(embed=emb, ephemeral=True)

    async def _debug_lobbies(self, ctx: discord.ApplicationContext) -> None:
        """Show persisted lobbies for this guild (from DB + in-memory)."""
        if not ctx.response.is_done():
            try:
                await ctx.defer(ephemeral=True)
            except discord.HTTPException:
                pass

        guild = ctx.guild
        if not guild:
            await ctx.followup.send("No guild.", ephemeral=True)
            return

        # Get from DB
        try:
            db_lobbies = await db_get_guild_lobbies(guild.id)
        except Exception as e:
            await ctx.followup.send(f"Error fetching lobbies from DB: {e}", ephemeral=True)
            return

        # Get from in-memory (lfg cog)
        lfg_cog = self.bot.get_cog("LFGCog")
        in_memory_lobbies = {}
        if lfg_cog and hasattr(lfg_cog, "state"):
            in_memory_lobbies = lfg_cog.state.peek_guild_lobbies(guild.id)

        emb = discord.Embed(
            title="🎮 Persisted LFG Lobbies",
            description=f"Guild: {guild.name}",
            color=0x2ECC71,
        )

        if db_lobbies:
            lines = []
            for lobby in db_lobbies[:10]:
                lid = lobby.get("lobby_id", "?")
                host_id = lobby.get("host_id", 0)
                host = guild.get_member(host_id)
                host_name = host.display_name if host else f"(user {host_id})"
                players = len(lobby.get("player_ids") or [])
                max_seats = lobby.get("max_seats", 4)
                elo = "Elo" if lobby.get("elo_mode") else "Normal"
                in_mem = "✅" if lid in in_memory_lobbies else "❌"
                lines.append(f"• `{lid}` — {host_name} — {players}/{max_seats} — {elo} — in-mem: {in_mem}")
            emb.add_field(
                name=f"DB Lobbies ({len(db_lobbies)})",
                value="\n".join(lines) or "(none)",
                inline=False,
            )
        else:
            emb.add_field(name="DB Lobbies", value="(none)", inline=False)

        # In-memory only
        db_ids = {lobby.get("lobby_id") for lobby in db_lobbies}
        mem_only = []
        for lid, lobby in in_memory_lobbies.items():
            if lid not in db_ids:
                host = guild.get_member(lobby.host_id)
                host_name = host.display_name if host else f"(user {lobby.host_id})"
                mem_only.append(f"• `{lid}` — {host_name} — {len(lobby.player_ids)}/{lobby.max_seats}")
        if mem_only:
            emb.add_field(
                name="In-memory only (not in DB)",
                value="\n".join(mem_only[:10]),
                inline=False,
            )

        emb.set_footer(text="ECL Debug • lobbies")
        await ctx.followup.send(embed=emb, ephemeral=True)

    async def _backfill_free_roles(self, ctx: discord.ApplicationContext, month: Optional[str]) -> None:
        """One-time backfill: write DB free entries for all members with free-entry roles.
        
        This allows removing roles from FREE_ENTRY_ROLE_IDS env var while preserving
        the free entry for users who already had those roles.
        """
        subs = self._subs_cog()
        if subs is None:
            await ctx.respond("SubscriptionsCog is not loaded.", ephemeral=True)
            return

        mk = (month or "").strip()
        now = now_lisbon()
        if not mk:
            mk = month_key(now)
        if not looks_like_month(mk):
            await ctx.respond("Month must be **YYYY-MM** (e.g., 2026-01).", ephemeral=True)
            return

        if not ctx.response.is_done():
            try:
                await ctx.defer(ephemeral=True)
            except discord.HTTPException:
                pass

        guild = ctx.guild
        cfg = subs.cfg if hasattr(subs, "cfg") else self.cfg

        role_ids = getattr(cfg, "free_entry_role_ids", set()) or set()
        if not role_ids:
            await ctx.followup.send("❌ No free-entry role IDs configured (FREE_ENTRY_ROLE_IDS).", ephemeral=True)
            return

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
            await ctx.followup.send(f"❌ No members found with free-entry roles for {month_label(mk)}.", ephemeral=True)
            return

        # Build role summary for preview
        role_counts: dict[str, int] = {}
        for rnames in user_roles.values():
            for rn in rnames:
                role_counts[rn] = role_counts.get(rn, 0) + 1
        
        role_summary = "\n".join(f"• **{rn}**: {cnt} members" for rn, cnt in sorted(role_counts.items(), key=lambda x: -x[1]))

        # Preview embed
        emb = discord.Embed(
            title=f"⚠️ Backfill free-entry — {month_label(mk)}",
            description=(
                f"This will write **{len(user_roles)}** free-entry records to the database.\n\n"
                f"After this, these users will have DB-backed free entry for **{month_label(mk)}** "
                f"and will keep it even if the role is removed from `FREE_ENTRY_ROLE_IDS`.\n\n"
                f"**This action writes to the database.**"
            ),
            color=0xF39C12,
        )
        emb.add_field(name="Roles to process", value=role_summary[:1024] or "(none)", inline=False)
        emb.set_footer(text="ECL Debug • backfill_free_roles")

        # Create confirmation view
        view = BackfillConfirmView(
            cog=self,
            ctx=ctx,
            mk=mk,
            user_roles=user_roles,
            cfg=cfg,
        )

        await ctx.followup.send(embed=emb, view=view, ephemeral=True)

    async def _debug_treasure_stats(self, ctx: discord.ApplicationContext, month: Optional[str]) -> None:
        """Show Bring a Friend Treasure Pod stats for a month (safe - doesn't reveal pod numbers)."""
        mk = (month or "").strip()
        now = now_lisbon()
        if not mk:
            mk = month_key(now)
        if not looks_like_month(mk):
            await ctx.respond("Month must be **YYYY-MM** (e.g., 2026-01).", ephemeral=True)
            return

        if not ctx.response.is_done():
            try:
                await ctx.defer(ephemeral=True)
            except discord.HTTPException:
                pass

        guild = ctx.guild
        manager = TreasurePodManager(treasure_pod_schedule, treasure_pods_col)

        stats = await manager.get_stats(guild.id, mk)
        won_pods = await manager.get_won_pods(guild.id, mk)

        emb = discord.Embed(
            title=f"🎁 Treasure Pod Stats — {month_label(mk)}",
            color=0xFFD700,  # Gold
        )

        if not stats.get("scheduled"):
            emb.description = "No treasure pod schedule found for this month."
        else:
            desc_lines = [
                f"**Estimated total tables:** {stats['estimated_total']}",
                "",
                f"**Treasures fired:** {stats['treasures_fired']}",
                f"**Treasures remaining:** {stats['treasures_remaining']}",
            ]

            # Per-type breakdown
            type_stats = stats.get("type_stats", {})
            if len(type_stats) > 1 or (type_stats and next(iter(type_stats)) != "bring_a_friend"):
                desc_lines.append("")
                desc_lines.append("**Per-type breakdown:**")
                for type_id, ts in type_stats.items():
                    label = ts.get("title", type_id.replace("_", " ").title())
                    desc_lines.append(
                        f"  • {label}: {ts['fired']}/{ts['total']} fired, "
                        f"{ts['remaining']} remaining"
                    )

            emb.description = "\n".join(desc_lines)

        if won_pods:
            lines = []
            for pod in won_pods[:10]:
                # Try to get winner display name from stored data
                winner_handle = pod.get("winner_discord_handle")
                winner_uid = pod.get("winner_topdeck_uid")

                if winner_handle:
                    winner_name = winner_handle
                elif winner_uid:
                    winner_name = winner_uid
                else:
                    winner_name = "Unknown"

                pod_title = pod.get("pod_title", "Treasure Pod")
                lines.append(f"• Table #{pod['table']} ({pod_title}) → **{winner_name}**")

            emb.add_field(
                name=f"🏆 Winners ({len(won_pods)})",
                value="\n".join(lines) or "(none)",
                inline=False,
            )

        emb.set_footer(text="ECL Debug • treasure_stats")
        await ctx.followup.send(embed=emb, ephemeral=True)


class BackfillConfirmView(discord.ui.View):
    """Confirmation view for backfill_free_roles command."""

    def __init__(self, cog: "DebugCog", ctx: discord.ApplicationContext, mk: str, user_roles: dict, cfg):
        super().__init__(timeout=60)
        self.cog = cog
        self.ctx = ctx
        self.mk = mk
        self.user_roles = user_roles
        self.cfg = cfg
        self.completed = False

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        if interaction.user.id != self.ctx.user.id:
            await interaction.response.send_message("Only the command user can confirm.", ephemeral=True)
            return

        if self.completed:
            await interaction.response.send_message("Already processed.", ephemeral=True)
            return

        self.completed = True
        await interaction.response.defer()

        # Disable buttons
        for item in self.children:
            item.disabled = True
        await interaction.edit_original_response(view=self)

        # Execute backfill
        written = 0
        for uid, role_names in self.user_roles.items():
            roles_txt = ", ".join(sorted(set(role_names)))
            reason = f"free role: {roles_txt}"
            await subs_free_entries.update_one(
                {"guild_id": self.cfg.guild_id, "user_id": int(uid), "month": self.mk},
                {
                    "$setOnInsert": {
                        "guild_id": self.cfg.guild_id,
                        "user_id": int(uid),
                        "month": self.mk,
                        "created_at": datetime.now(timezone.utc),
                    },
                    "$set": {"reason": reason, "updated_at": datetime.now(timezone.utc)},
                },
                upsert=True,
            )
            written += 1

        # Build role summary
        role_counts: dict[str, int] = {}
        for rnames in self.user_roles.values():
            for rn in rnames:
                role_counts[rn] = role_counts.get(rn, 0) + 1
        role_summary = "\n".join(f"• **{rn}**: {cnt} members" for rn, cnt in sorted(role_counts.items(), key=lambda x: -x[1]))

        # Success embed
        emb = discord.Embed(
            title=f"✅ Backfill complete — {month_label(self.mk)}",
            description=(
                f"Wrote **{written}** free-entry records to database.\n\n"
                f"These users now have DB-backed free entry for **{month_label(self.mk)}** "
                f"and will keep it even if the role is removed from `FREE_ENTRY_ROLE_IDS`."
            ),
            color=0x2ECC71,
        )
        emb.add_field(name="Roles processed", value=role_summary[:1024] or "(none)", inline=False)
        emb.set_footer(text="ECL Debug • backfill_free_roles")

        await interaction.followup.send(embed=emb, ephemeral=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        if interaction.user.id != self.ctx.user.id:
            await interaction.response.send_message("Only the command user can cancel.", ephemeral=True)
            return

        self.completed = True
        
        # Disable buttons
        for item in self.children:
            item.disabled = True

        await interaction.response.edit_message(
            content="❌ Backfill cancelled.",
            view=self,
        )

    async def on_timeout(self):
        if not self.completed:
            for item in self.children:
                item.disabled = True
            try:
                await self.ctx.edit(view=self)
            except Exception:
                pass


def setup(bot: commands.Bot):
    bot.add_cog(DebugCog(bot))
