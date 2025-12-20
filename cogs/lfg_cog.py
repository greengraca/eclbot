# cogs/lfg_cog.py
import os
import asyncio
import contextlib
from typing import Dict, Optional, List

import discord
from discord.ext import commands
from discord import Option

from spelltable_client import create_spelltable_game  # <- your real SpellTable helper

GUILD_ID = int(os.getenv("GUILD_ID", "0"))


class LFGLobby:
    def __init__(
        self,
        guild_id: int,
        channel_id: int,
        host_id: int,
        link: str,
        max_seats: int = 4,
        invited_ids: Optional[List[int]] = None,
    ) -> None:
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.host_id = host_id
        self.link = link
        self.max_seats = max_seats
        self.player_ids: List[int] = [host_id]  # host always first
        self.invited_ids: List[int] = invited_ids or []
        self.message_id: Optional[int] = None   # set after we send the embed

    def is_full(self) -> bool:
        return len(self.player_ids) >= self.max_seats


class LFGJoinView(discord.ui.View):
    def __init__(self, cog: "LFGCog", lobby: LFGLobby):
        # 1 hour timeout; after that the lobby expires
        super().__init__(timeout=60 * 60)
        self.cog = cog
        self.lobby = lobby

    @discord.ui.button(
        label="Join Game",
        style=discord.ButtonStyle.green,
        custom_id="lfg_join_button",
    )
    async def join_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ):
        await self.cog._handle_join(interaction, self, button)

    async def on_timeout(self) -> None:
        # Disable the button + mark lobby as expired
        guild = self.cog.bot.get_guild(self.lobby.guild_id)
        if not guild:
            return

        channel = guild.get_channel(self.lobby.channel_id)
        if not isinstance(channel, discord.TextChannel):
            return

        if not self.lobby.message_id:
            return

        try:
            msg = await channel.fetch_message(self.lobby.message_id)
        except Exception:
            return

        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

        if msg.embeds:
            embed = msg.embeds[0]
        else:
            embed = discord.Embed()

        embed.title = "⌛ LFG lobby expired"
        embed.color = discord.Color.dark_grey()

        with contextlib.suppress(Exception):
            await msg.edit(embed=embed, view=self)

        # Cleanup internal state
        self.cog._clear_lobby(self.lobby.guild_id)


class LFGCog(commands.Cog):
    """
    Simple LFG system for SpellTable Commander pods.

    - /lfg → opens a lobby for up to 4 players, always SpellTable + Commander.
    - One active lobby per guild at a time.
    - Lobby embed updates as people join; closes when full.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # guild_id -> LFGLobby
        self._guild_lobbies: Dict[int, LFGLobby] = {}
        self._lock = asyncio.Lock()

    # ---------- internal helpers ----------

    def _clear_lobby(self, guild_id: int) -> None:
        self._guild_lobbies.pop(guild_id, None)

    def _build_lobby_embed(
        self,
        guild: discord.Guild,
        lobby: LFGLobby,
    ) -> discord.Embed:
        host = guild.get_member(lobby.host_id)
        host_mention = host.mention if host else f"<@{lobby.host_id}>"

        embed = discord.Embed(
            title="🕹️ LFG – Commander",
            description=(
                f"{host_mention} is looking for a SpellTable Commander game!\n"
                f"[Click here to join on SpellTable]({lobby.link})"
            ),
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Service", value="SpellTable", inline=True)
        embed.add_field(name="Format", value="Commander", inline=True)

        # Players field
        lines = []
        for i in range(lobby.max_seats):
            if i < len(lobby.player_ids):
                uid = lobby.player_ids[i]
                member = guild.get_member(uid)
                if member:
                    name = member.display_name
                    mention = member.mention
                else:
                    name = f"User {uid}"
                    mention = f"<@{uid}>"
                suffix = " *(host)*" if uid == lobby.host_id else ""
                lines.append(f"**Slot {i+1}:** {mention} ({name}){suffix}")
            else:
                lines.append(f"**Slot {i+1}:** *(open)*")

        embed.add_field(
            name=f"Players ({len(lobby.player_ids)}/{lobby.max_seats})",
            value="\n".join(lines),
            inline=False,
        )

        # Invited friends (if any) — they’re not auto-joined, just shown
        invited_mentions = []
        for uid in lobby.invited_ids:
            member = guild.get_member(uid)
            if member:
                invited_mentions.append(member.mention)
            else:
                invited_mentions.append(f"<@{uid}>")
        if invited_mentions:
            embed.add_field(
                name="Invited friends",
                value=" ".join(invited_mentions),
                inline=False,
            )

        # SpellBot credit
        embed.add_field(
            name="Support SpellBot",
            value=(
                "Support SpellBot – Become a monthly patron or give a one-off tip! ❤️\n"
                "[Patreon](https://patreon.com/lexicalunit) • "
                "[Ko-fi](https://ko-fi.com/lexicalunit)"
            ),
            inline=False,
        )

        if lobby.is_full():
            embed.title = "✅ Lobby is full – ready to play!"
            embed.color = discord.Color.green()

        return embed

    async def _handle_join(
        self,
        interaction: discord.Interaction,
        view: LFGJoinView,
        button: discord.ui.Button,
    ):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "This lobby can only be joined from within a server.",
                ephemeral=True,
            )
            return

        async with self._lock:
            lobby = self._guild_lobbies.get(guild.id)
            if lobby is None or lobby is not view.lobby:
                # Lobby no longer active
                button.disabled = True
                with contextlib.suppress(Exception):
                    await interaction.response.edit_message(
                        content="This lobby is no longer active.",
                        view=view,
                    )
                return

            user = interaction.user
            if not isinstance(user, discord.Member):
                await interaction.response.send_message(
                    "Only server members can join this lobby.",
                    ephemeral=True,
                )
                return

            if user.id in lobby.player_ids:
                await interaction.response.send_message(
                    "You're already in this lobby.",
                    ephemeral=True,
                )
                return

            if lobby.is_full():
                button.disabled = True
                with contextlib.suppress(Exception):
                    await interaction.response.edit_message(view=view)
                await interaction.followup.send(
                    "This lobby is already full.",
                    ephemeral=True,
                )
                return

            lobby.player_ids.append(user.id)

        # Rebuild embed outside of the lock
        embed = self._build_lobby_embed(guild, lobby)

        # Disable button + clear lobby if it just became full
        if lobby.is_full():
            for child in view.children:
                if isinstance(child, discord.ui.Button):
                    child.disabled = True
            self._clear_lobby(guild.id)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            # Fallback if already responded
            with contextlib.suppress(Exception):
                await interaction.edit_original_response(embed=embed, view=view)

        # DM joiner with link
        try:
            await interaction.user.send(
                f"You've joined a SpellTable game! Here is the link: {lobby.link}"
            )
        except discord.Forbidden:
            with contextlib.suppress(Exception):
                await interaction.followup.send(
                    f"{interaction.user.mention}, I couldn't DM you the game link. "
                    "Please enable DMs from this server.",
                    ephemeral=True,
                )

    # ---------- slash command ----------

    @commands.slash_command(
        name="lfg",
        description="Open a SpellTable Commander lobby (4 players max).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def lfg(
        self,
        ctx: discord.ApplicationContext,
        friends: Optional[str] = Option(
            str,
            "Mention one or more friends to invite (optional).",
            required=False,
        ),
    ):
        # Only in guilds
        if ctx.guild is None:
            await ctx.respond(
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        async with self._lock:
            if ctx.guild.id in self._guild_lobbies:
                await ctx.respond(
                    "There is already an active LFG lobby in this server. "
                    "Please wait for it to fill or expire before creating another.",
                    ephemeral=True,
                )
                return

            # Public response (not ephemeral) – we’re posting the lobby embed
            await ctx.defer()

            # Parse invited friend mentions from the string
            invited_ids: List[int] = []
            if friends:
                for token in friends.split():
                    if token.startswith("<@") and token.endswith(">"):
                        cleaned = token.strip("<@!>")
                        if cleaned.isdigit():
                            invited_ids.append(int(cleaned))

            # Create SpellTable game via real helper
            try:
                link = await create_spelltable_game(
                    game_name=f"{ctx.guild.name} – Commander LFG",
                    format_name="Commander",
                    is_public=False,
                )
            except Exception as e:
                print(f"[lfg] Failed to create SpellTable game: {e}")
                await ctx.followup.send(
                    "I couldn't create a SpellTable game right now. "
                    "Please try again in a bit.",
                    ephemeral=True,
                )
                return

            lobby = LFGLobby(
                guild_id=ctx.guild.id,
                channel_id=ctx.channel.id,
                host_id=ctx.author.id,
                link=link,
                max_seats=4,
                invited_ids=invited_ids,
            )
            self._guild_lobbies[ctx.guild.id] = lobby

        # Build embed + view and send
        embed = self._build_lobby_embed(ctx.guild, lobby)
        view = LFGJoinView(self, lobby)

        try:
            msg = await ctx.followup.send(embed=embed, view=view)
        except Exception as e:
            print(f"[lfg] Failed to send lobby message: {e}")
            # Cleanup on failure
            self._clear_lobby(ctx.guild.id)
            await ctx.followup.send(
                "Something went wrong creating the lobby message.",
                ephemeral=True,
            )
            return

        lobby.message_id = msg.id

        # DM host with link
        try:
            await ctx.author.send(
                f"Your SpellTable Commander lobby is ready: {link}"
            )
        except discord.Forbidden:
            with contextlib.suppress(Exception):
                await ctx.followup.send(
                    "I couldn't DM you the SpellTable link. "
                    "Please enable DMs from this server.",
                    ephemeral=True,
                )


def setup(bot: commands.Bot):
    bot.add_cog(LFGCog(bot))
