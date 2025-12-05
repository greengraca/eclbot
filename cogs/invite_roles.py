# cogs/invite_roles.py
import os
import re
from typing import Dict, Optional

import discord
from discord.ext import commands

# ---- Role / Guild config from environment -----------------------------------

PT_ROLE_ID = int(os.getenv("PT_ROLE", "0"))
ECL_ROLE_ID = int(os.getenv("ECL_ROLE", "0"))
GUILD_ID = int(os.getenv("GUILD_ID", "0"))

# ---- Reaction-role config (leave 0/empty to disable) ------------------------

ECL_RR_CHANNEL_ID = int(os.getenv("ECL_RR_CHANNEL_ID", "0"))
ECL_RR_MESSAGE_ID = int(os.getenv("ECL_RR_MESSAGE_ID", "0"))
ECL_RR_EMOJI = (os.getenv("ECL_RR_EMOJI", "") or "").strip()

LFG_ROLE_ID = int(os.getenv("LFG_ROLE", "0"))
LFG_RR_CHANNEL_ID = int(os.getenv("LFG_RR_CHANNEL_ID", "0"))
LFG_RR_MESSAGE_ID = int(os.getenv("LFG_RR_MESSAGE_ID", "0"))
LFG_RR_EMOJI = (os.getenv("LFG_RR_EMOJI", "") or "").strip()


def _emoji_matches_config(payload_emoji: discord.PartialEmoji) -> bool:
    """Return True if the event's emoji matches ECL_RR_EMOJI."""
    if not ECL_RR_EMOJI:
        return False

    # Custom emoji: match by ID if present in config, else try name
    if payload_emoji.id:
        m = re.search(r"(\d{15,25})", ECL_RR_EMOJI)
        if m:
            return int(m.group(1)) == payload_emoji.id
        if payload_emoji.name and ECL_RR_EMOJI.startswith("<:"):
            m2 = re.match(r"<:([^:>]+):\d+>", ECL_RR_EMOJI)
            if m2:
                return payload_emoji.name == m2.group(1)
        return False

    # Unicode emoji path
    return payload_emoji.name == ECL_RR_EMOJI


def _emoji_matches_generic(
    config_emoji: str, payload_emoji: discord.PartialEmoji
) -> bool:
    """Match unicode or custom emoji using a provided config string."""
    if not config_emoji:
        return False
    if payload_emoji.id:
        m = re.search(r"(\d{15,25})", config_emoji)
        if m:
            return int(m.group(1)) == payload_emoji.id
        m2 = re.match(r"<:([^:>]+):\d+>", config_emoji)
        return bool(m2 and payload_emoji.name and payload_emoji.name == m2.group(1))
    return payload_emoji.name == config_emoji


class InviteRoles(commands.Cog):
    """1-use invite → PT role, reaction-role for ECL + LFG, and /eclgive command."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # guild_id -> { invite_code: {"uses": int, "max_uses": int} }
        self.invite_cache: Dict[int, Dict[str, Dict[str, int]]] = {}

    # ---------------------- Invite cache utilities ---------------------------

    async def build_invite_cache(self, guild: discord.Guild):
        """Fetch current invites and store them in memory."""
        invites = await guild.invites()
        self.invite_cache[guild.id] = {
            inv.code: {
                "uses": inv.uses or 0,
                "max_uses": inv.max_uses or 0,  # 0 = infinite
            }
            for inv in invites
        }
        print(f"[invite_roles] cached {len(invites)} invites for {guild.name}")

    @commands.Cog.listener()
    async def on_invite_create(self, invite: discord.Invite):
        """Whenever a new invite is created, refresh the cache for that guild."""
        guild = invite.guild
        if guild is None:
            return
        await self.build_invite_cache(guild)

    # ---------------------- On member join: detect invite --------------------

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        guild = member.guild
        if GUILD_ID and guild.id != GUILD_ID:
            return

        cached = self.invite_cache.get(guild.id, {})
        current_invites = await guild.invites()

        current = {
            inv.code: {
                "uses": inv.uses or 0,
                "max_uses": inv.max_uses or 0,  # 0 means unlimited
            }
            for inv in current_invites
        }

        used_code: Optional[str] = None
        before: Optional[Dict[str, int]] = None
        after: Optional[Dict[str, int]] = None

        # 1) Find invite whose uses increased
        for code, data in current.items():
            prev = cached.get(code)
            if prev and data["uses"] > prev["uses"]:
                used_code, before, after = code, prev, data
                break

        # 2) If none increased, check if an invite disappeared (likely 1-use consumed)
        if used_code is None:
            for old_code, old_data in cached.items():
                if old_code not in current:
                    used_code, before, after = old_code, old_data, None
                    break

        # Update cache right away
        self.invite_cache[guild.id] = current

        print(
            f"[invite_roles] {member} joined. used={used_code} before={before} after={after}"
        )

        # Rule: any invite with max_uses == 1 counts as PT
        is_pt = False
        if used_code:
            if after is None and before and before.get("max_uses") == 1:
                is_pt = True
            elif (after and after.get("max_uses") == 1) or (
                before and before.get("max_uses") == 1
            ):
                is_pt = True
            elif after and after.get("max_uses", 0) > 0:
                # Handle odd edge: we hit max uses exactly on this join
                if (
                    before
                    and after["uses"] >= after["max_uses"]
                    and after["uses"] > before["uses"]
                ):
                    is_pt = (after["max_uses"] == 1) or (before.get("max_uses") == 1)

        if is_pt:
            pt_role = guild.get_role(PT_ROLE_ID)
            if pt_role:
                try:
                    await member.add_roles(
                        pt_role, reason="1-use invite → PT Community"
                    )
                    print(f"[invite_roles] ✅ Gave PT to {member} (1-use invite).")
                except discord.Forbidden:
                    print("[invite_roles] ⚠️ Missing permission to add PT role.")
                except Exception as e:
                    print(f"[invite_roles] ⚠️ Error adding PT: {e}")
            else:
                print("[invite_roles] ⚠️ PT role not found.")
        else:
            print(f"[invite_roles] no auto-role to {member}")

    # ---------------------- Reaction roles: ECL + LFG ------------------------

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        """Grant role when reacting on configured messages (ECL + LFG), and log to terminal."""
        if payload.guild_id != GUILD_ID:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return

        # ----- ECL block -----
        if ECL_RR_CHANNEL_ID and ECL_RR_MESSAGE_ID and ECL_RR_EMOJI:
            if (
                payload.channel_id == ECL_RR_CHANNEL_ID
                and payload.message_id == ECL_RR_MESSAGE_ID
            ):
                if _emoji_matches_config(payload.emoji):
                    role = guild.get_role(ECL_ROLE_ID)
                    if not role:
                        print(
                            "[invite_roles] ⚠️ ECL role not found for reaction-role."
                        )
                        return
                    member = guild.get_member(payload.user_id) or await guild.fetch_member(
                        payload.user_id
                    )
                    if member and not member.bot:
                        try:
                            await member.add_roles(
                                role, reason="Reaction role (ECL)"
                            )
                            msg_link = f"https://discord.com/channels/{guild.id}/{payload.channel_id}/{payload.message_id}"
                            print(
                                f"[invite_roles] ✅ ECL ADDED: {member} via reaction {payload.emoji} • {msg_link}"
                            )
                        except discord.Forbidden:
                            print(
                                "[invite_roles] ⚠️ Missing permission to add ECL role."
                            )
                        except Exception as e:
                            print(
                                f"[invite_roles] ⚠️ Error adding ECL (reaction): {e}"
                            )
                    return  # stop after handling ECL

        # ----- LFG block -----
        if LFG_RR_CHANNEL_ID and LFG_RR_MESSAGE_ID and LFG_RR_EMOJI:
            if (
                payload.channel_id == LFG_RR_CHANNEL_ID
                and payload.message_id == LFG_RR_MESSAGE_ID
            ):
                if _emoji_matches_generic(LFG_RR_EMOJI, payload.emoji):
                    role = guild.get_role(LFG_ROLE_ID)
                    if not role:
                        print(
                            "[invite_roles] ⚠️ LFG role not found for reaction-role."
                        )
                        return
                    member = guild.get_member(payload.user_id) or await guild.fetch_member(
                        payload.user_id
                    )
                    if member and not member.bot:
                        try:
                            await member.add_roles(
                                role, reason="Reaction role (LFGLEAGUE)"
                            )
                            msg_link = f"https://discord.com/channels/{guild.id}/{payload.channel_id}/{payload.message_id}"
                            print(
                                f"[invite_roles] ✅ LFGLEAGUE ADDED: {member} via reaction {payload.emoji} • {msg_link}"
                            )
                        except discord.Forbidden:
                            print(
                                "[invite_roles] ⚠️ Missing permission to add LFGLEAGUE role."
                            )
                        except Exception as e:
                            print(
                                f"[invite_roles] ⚠️ Error adding LFGLEAGUE (reaction): {e}"
                            )
                    return

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        """Remove role when the reaction is removed (ECL + LFG), and log to terminal."""
        if payload.guild_id != GUILD_ID:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return

        # ----- ECL block -----
        if ECL_RR_CHANNEL_ID and ECL_RR_MESSAGE_ID and ECL_RR_EMOJI:
            if (
                payload.channel_id == ECL_RR_CHANNEL_ID
                and payload.message_id == ECL_RR_MESSAGE_ID
            ):
                if _emoji_matches_config(payload.emoji):
                    role = guild.get_role(ECL_ROLE_ID)
                    if not role:
                        print(
                            "[invite_roles] ⚠️ ECL role not found for reaction-role."
                        )
                        return
                    member = guild.get_member(payload.user_id) or await guild.fetch_member(
                        payload.user_id
                    )
                    if member and not member.bot:
                        try:
                            await member.remove_roles(
                                role, reason="Reaction role (ECL) removed"
                            )
                            msg_link = f"https://discord.com/channels/{guild.id}/{payload.channel_id}/{payload.message_id}"
                            print(
                                f"[invite_roles] 🗑️ ECL REMOVED: {member} (reaction removed {payload.emoji}) • {msg_link}"
                            )
                        except discord.Forbidden:
                            print(
                                "[invite_roles] ⚠️ Missing permission to remove ECL role."
                            )
                        except Exception as e:
                            print(
                                f"[invite_roles] ⚠️ Error removing ECL (reaction): {e}"
                            )
                    return

        # ----- LFG block -----
        if LFG_RR_CHANNEL_ID and LFG_RR_MESSAGE_ID and LFG_RR_EMOJI:
            if (
                payload.channel_id == LFG_RR_CHANNEL_ID
                and payload.message_id == LFG_RR_MESSAGE_ID
            ):
                if _emoji_matches_generic(LFG_RR_EMOJI, payload.emoji):
                    role = guild.get_role(LFG_ROLE_ID)
                    if not role:
                        print(
                            "[invite_roles] ⚠️ LFG role not found for reaction-role."
                        )
                        return
                    member = guild.get_member(payload.user_id) or await guild.fetch_member(
                        payload.user_id
                    )
                    if member and not member.bot:
                        try:
                            await member.remove_roles(
                                role,
                                reason="Reaction role (LFGLEAGUE) removed",
                            )
                            msg_link = f"https://discord.com/channels/{guild.id}/{payload.channel_id}/{payload.message_id}"
                            print(
                                f"[invite_roles] 🗑️ LFGLEAGUE REMOVED: {member} (reaction {payload.emoji}) • {msg_link}"
                            )
                        except discord.Forbidden:
                            print(
                                "[invite_roles] ⚠️ Missing permission to remove LFGLEAGUE role."
                            )
                        except Exception as e:
                            print(
                                f"[invite_roles] ⚠️ Error removing LFGLEAGUE (reaction): {e}"
                            )
                    return

    # ---------------------- Slash command: /eclgive --------------------------

    @commands.slash_command(
        name="eclgive",
        description="Give the ECL role to a member.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def give_ecl(
        self,
        ctx: discord.ApplicationContext,
        member: discord.Member,
    ):
        """Give ECL role to a member (Manage Roles required)."""
        if not ctx.user.guild_permissions.manage_roles:
            await ctx.respond(
                "You need **Manage Roles** to use this command.", ephemeral=True
            )
            return

        role = ctx.guild.get_role(ECL_ROLE_ID)
        if role is None:
            await ctx.respond("ECL role not found.", ephemeral=True)
            return

        try:
            await member.add_roles(role, reason=f"Added by {ctx.user}")
        except discord.Forbidden:
            await ctx.respond(
                "I don't have permission to add that role (check role position).",
                ephemeral=True,
            )
            return

        await ctx.respond(
            f"Added **{role.name}** to {member.mention}.", ephemeral=True
        )
        print(
            f"[invite_roles] 🛠️ ECL added to {member} by {ctx.user} via /eclgive"
        )


def setup(bot: commands.Bot):
    bot.add_cog(InviteRoles(bot))
