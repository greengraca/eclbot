# cogs/subscriptions/views.py
"""Discord UI views for subscriptions."""

import discord


class SubsLinksView(discord.ui.View):
    """View with Ko-fi and Patreon subscription buttons."""

    def __init__(self, kofi_url: str, patreon_url: str):
        super().__init__(timeout=None)
        self.kofi_url = (kofi_url or "").strip()
        self.patreon_url = (patreon_url or "").strip()

    @staticmethod
    def _ok(url: str) -> bool:
        return url.startswith("http://") or url.startswith("https://")

    @discord.ui.button(label="💚 Subscribe on Ko-fi", style=discord.ButtonStyle.primary)
    async def kofi_primary(self, button: discord.ui.Button, interaction: discord.Interaction):
        if not self._ok(self.kofi_url):
            await interaction.response.send_message("Ko-fi link not configured.", ephemeral=True)
            return
        await interaction.response.send_message(self.kofi_url, ephemeral=True)

    @discord.ui.button(label="🔥 Join Patreon (ECL Grinder+)", style=discord.ButtonStyle.secondary)
    async def patreon_secondary(self, button: discord.ui.Button, interaction: discord.Interaction):
        if not self._ok(self.patreon_url):
            await interaction.response.send_message("Patreon link not configured.", ephemeral=True)
            return
        await interaction.response.send_message(self.patreon_url, ephemeral=True)
