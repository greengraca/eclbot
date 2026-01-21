# cogs/debug_cog.py
"""
Moderator-only debug utilities.

Goal: provide SAFE "dry-run" tools that emulate logic without applying side-effects
(no role changes, no DB writes), so mods can verify behavior before the scheduled jobs run.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List

import discord
from discord.ext import commands
from discord import Option

from utils.logger import get_logger
from utils.settings import SUBS, LISBON_TZ
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
)


GUILD_ID = int(getattr(SUBS, "guild_id", 0) or 0)



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
        """Manage Roles OR has configured mod role."""
        if getattr(member, "guild_permissions", None) and member.guild_permissions.manage_roles:
            return True
        rid = int(getattr(self.cfg, "ecl_mod_role_id", 0) or 0)
        if rid and any(r.id == rid for r in (member.roles or [])):
            return True
        return False

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
            choices=["top16onflip", "subs_dms_preview", "timers", "lobbies"],
        ),
        month: Optional[str] = Option(
            str,
            "For top16onflip: cut month YYYY-MM (defaults to current month).",
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

        # --- top16onflip dry-run preview ---
        mk = (month or "").strip()
        now = datetime.now(LISBON_TZ)
        if not mk:
            mk = month_key(now)
        if not looks_like_month(mk):
            await ctx.respond("Month must be **YYYY-MM** (e.g., 2026-01).", ephemeral=True)
            return

        subs = self._subs_cog()
        if subs is None:
            await ctx.respond("SubscriptionsCog is not loaded.", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)

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
                f"• **Top16 role** ({top16_role.mention if top16_role else 'not configured'})\n"
                f"• **Free entry** for **{month_label(target_mk)}**\n\n"
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
                    name="Would receive Top16 + free entry" if idx == 0 else "​",
                    value=chunk,
                    inline=False,
                )
        else:
            emb.add_field(
                name="Would receive Top16 + free entry",
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

    async def _debug_timers(self, ctx: discord.ApplicationContext) -> None:
        """Show persisted timers for this guild (from DB + in-memory)."""
        await ctx.defer(ephemeral=True)

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
        await ctx.defer(ephemeral=True)

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


def setup(bot: commands.Bot):
    bot.add_cog(DebugCog(bot))
