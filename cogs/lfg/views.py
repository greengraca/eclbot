from __future__ import annotations

import contextlib
from typing import Any, Optional

import discord

from .models import LFGLobby


def _make_custom_id(base: str, guild_id: int, lobby_id: int) -> str:
    """Generate a persistent custom_id that encodes guild and lobby."""
    return f"{base}:{guild_id}:{lobby_id}"


def _parse_custom_id(custom_id: str) -> Optional[tuple[str, int, int]]:
    """Parse a persistent custom_id. Returns (base, guild_id, lobby_id) or None."""
    parts = custom_id.split(":")
    if len(parts) != 3:
        return None
    try:
        return (parts[0], int(parts[1]), int(parts[2]))
    except (ValueError, TypeError):
        return None


class LFGJoinView(discord.ui.View):
    """Buttons for an LFG lobby message.

    This view is intentionally thin: it delegates actions back to the owning cog.
    
    For persistence across restarts:
    - custom_ids encode guild_id:lobby_id
    - The cog registers a PersistentLFGView that handles all button clicks
    - This class is used for creating NEW lobbies (with proper timeout)
    """

    def __init__(self, cog: Any, lobby: LFGLobby, *, timeout_seconds: int):
        # timeout resets on every interaction; this gives us
        # "X minutes of no joins/leaves" behaviour.
        super().__init__(timeout=int(timeout_seconds))
        self.cog = cog
        self.lobby = lobby

        # Update custom_ids to include guild/lobby for persistence
        self._update_custom_ids()

        # Hide the last-seat button for non-Elo lobbies
        if not lobby.elo_mode:
            for child in list(self.children):
                if isinstance(child, discord.ui.Button) and "lfg_open_last_seat" in (child.custom_id or ""):
                    with contextlib.suppress(Exception):
                        self.remove_item(child)

        self._sync_open_last_seat_button()

    def _update_custom_ids(self) -> None:
        """Update button custom_ids to encode guild and lobby."""
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.custom_id:
                base = child.custom_id.split(":")[0]  # strip any existing suffix
                child.custom_id = _make_custom_id(base, self.lobby.guild_id, self.lobby.lobby_id)

    def _sync_open_last_seat_button(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.custom_id and "lfg_open_last_seat" in child.custom_id:
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


class PersistentLFGView(discord.ui.View):
    """
    A persistent view that handles LFG button interactions after bot restarts.
    
    This view is registered ONCE on startup and handles ALL LFG lobbies by:
    1. Parsing the custom_id to get guild_id and lobby_id
    2. Looking up the lobby in the cog's state
    3. Delegating to the cog's handlers
    
    Uses timeout=None for true persistence.
    """

    def __init__(self, cog: Any):
        super().__init__(timeout=None)
        self.cog = cog

    async def _get_lobby_and_view(
        self, interaction: discord.Interaction, custom_id: str
    ) -> Optional[tuple[LFGLobby, "LFGJoinView"]]:
        """Look up the lobby from the custom_id."""
        parsed = _parse_custom_id(custom_id)
        if not parsed:
            await interaction.response.send_message(
                "This lobby is no longer active (invalid button).",
                ephemeral=True,
            )
            return None

        base, guild_id, lobby_id = parsed

        async with self.cog.state.lock:
            lobby = self.cog.state.get_lobby(guild_id, lobby_id)

        if lobby is None:
            await interaction.response.send_message(
                "This lobby is no longer active.",
                ephemeral=True,
            )
            return None

        if lobby.view is None:
            await interaction.response.send_message(
                "This lobby is in an invalid state. Please try again.",
                ephemeral=True,
            )
            return None

        return lobby, lobby.view

    @discord.ui.button(
        label="Join this game!",
        style=discord.ButtonStyle.primary,
        emoji="🎮",
        custom_id="lfg_join_button",
    )
    async def join_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        result = await self._get_lobby_and_view(interaction, button.custom_id or "")
        if not result:
            return
        lobby, view = result
        await self.cog._handle_join(interaction, view, button)

    @discord.ui.button(
        label="Leave",
        style=discord.ButtonStyle.secondary,
        emoji="🚫",
        custom_id="lfg_leave_button",
    )
    async def leave_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        result = await self._get_lobby_and_view(interaction, button.custom_id or "")
        if not result:
            return
        lobby, view = result
        await self.cog._handle_leave(interaction, view, button)

    @discord.ui.button(
        label="Open last seat now",
        style=discord.ButtonStyle.danger,
        emoji="🔓",
        custom_id="lfg_open_last_seat_button",
    )
    async def open_last_seat_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        result = await self._get_lobby_and_view(interaction, button.custom_id or "")
        if not result:
            return
        lobby, view = result
        await self.cog._handle_open_last_seat(interaction, view, button)
