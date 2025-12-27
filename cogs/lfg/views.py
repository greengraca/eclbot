from __future__ import annotations

import contextlib
from typing import Any

import discord

from .models import LFGLobby


class LFGJoinView(discord.ui.View):
    """Buttons for an LFG lobby message.

    This view is intentionally thin: it delegates actions back to the owning cog.
    """

    def __init__(self, cog: Any, lobby: LFGLobby, *, timeout_seconds: int):
        # timeout resets on every interaction; this gives us
        # "X minutes of no joins/leaves" behaviour.
        super().__init__(timeout=int(timeout_seconds))
        self.cog = cog
        self.lobby = lobby

        # Hide the last-seat button for non-Elo lobbies
        if not lobby.elo_mode:
            for child in list(self.children):
                if isinstance(child, discord.ui.Button) and child.custom_id == "lfg_open_last_seat_button":
                    with contextlib.suppress(Exception):
                        self.remove_item(child)

        self._sync_open_last_seat_button()

    def _sync_open_last_seat_button(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.custom_id == "lfg_open_last_seat_button":
                child.disabled = not (
                    self.lobby.elo_mode
                    and self.lobby.host_elo is not None
                    and self.lobby.remaining_slots() == 1
                    and not self.cog._is_last_seat_open(self.lobby)
                    and not self.lobby.has_link()
                )

    @discord.ui.button(
        label="Join this game!",
        style=discord.ButtonStyle.primary,
        emoji="🎮",
        custom_id="lfg_join_button",
    )
    async def join_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        await self.cog._handle_join(interaction, self, button)

    @discord.ui.button(
        label="Leave",
        style=discord.ButtonStyle.secondary,
        emoji="🚫",
        custom_id="lfg_leave_button",
    )
    async def leave_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        await self.cog._handle_leave(interaction, self, button)

    @discord.ui.button(
        label="Open last seat now",
        style=discord.ButtonStyle.danger,
        emoji="🔓",
        custom_id="lfg_open_last_seat_button",
    )
    async def open_last_seat_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        await self.cog._handle_open_last_seat(interaction, self, button)

    async def on_timeout(self) -> None:
        guild = self.cog.bot.get_guild(self.lobby.guild_id)
        if not guild:
            return

        channel = guild.get_channel(self.lobby.channel_id)
        if not isinstance(channel, discord.TextChannel):
            return

        if not self.lobby.message_id:
            return

        # Clear the in-memory lobby first (fast, under lock), then do Discord I/O.
        async with self.cog.state.lock:
            current = self.cog.state.get_lobby(self.lobby.guild_id, self.lobby.lobby_id)
            if current is None or current is not self.lobby:
                return
            self.cog._clear_lobby(self.lobby.guild_id, self.lobby.lobby_id)

        try:
            msg = await channel.fetch_message(self.lobby.message_id)
        except Exception:
            return

        # Replace the embed+buttons with a plain message
        try:
            await msg.edit(
                content="This lobby has expired due to inactivity.",
                embeds=[],
                view=None,
            )
        except TypeError:
            # older libs: no 'embeds' kwarg
            with contextlib.suppress(Exception):
                await msg.edit(
                    content="This lobby has expired due to inactivity.",
                    embed=None,
                    view=None,
                )
        except Exception:
            pass
