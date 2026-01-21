# cogs/timer/views.py
"""Discord UI views for the timer cog."""

from __future__ import annotations

from typing import TYPE_CHECKING

import discord

if TYPE_CHECKING:
    from ..timer_cog import ECLTimerCog


class ReplaceTimerView(discord.ui.View):
    """Ask whether to replace an existing timer for a given game room."""

    def __init__(
        self,
        cog: "ECLTimerCog",
        ctx: discord.ApplicationContext,
        voice_channel: discord.VoiceChannel,
        game_number: int,
        existing_timer_id: str,
        *,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(timeout=timeout)
        self.cog = cog
        self.ctx = ctx
        self.voice_channel = voice_channel
        self.game_number = game_number
        self.existing_timer_id = existing_timer_id

    async def _check_user(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "Only the user who called `/timer` can use these buttons.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(
        label="Start new timer (replace)",
        style=discord.ButtonStyle.danger,
    )
    async def confirm_replace(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ):
        if not await self._check_user(interaction):
            return

        await interaction.response.edit_message(
            content=f"Stopping existing timer and starting a new one for {self.voice_channel.name}…",
            view=None,
        )

        # stop old, start new
        await self.cog.set_timer_stopped(self.existing_timer_id, reason="replace")

        ignore_autostop = self.cog._ignore_autostop_for_start(
            interaction.user if isinstance(interaction.user, discord.Member) else None,
            self.voice_channel,
        )

        await self.cog._start_timer(
            self.ctx,
            self.voice_channel,
            game_number=self.game_number,
            ignore_autostop=ignore_autostop,
        )
        self.stop()

    @discord.ui.button(
        label="Keep current timer",
        style=discord.ButtonStyle.secondary,
    )
    async def keep_current(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ):
        if not await self._check_user(interaction):
            return

        await interaction.response.edit_message(
            content=f"Keeping the existing timer for {self.voice_channel.name}.",
            view=None,
        )
        self.stop()
