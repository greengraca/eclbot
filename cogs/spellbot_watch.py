# cogs/spellbot_watch.py
import os
from typing import Optional, Set

import discord
from discord.ext import commands

GUILD_ID = int(os.getenv("GUILD_ID", "0"))

# SpellBot's user ID (recommended) – set this in your .env
# e.g. SPELLBOT_USER_ID=892092052023427062
SPELLBOT_USER_ID = int(os.getenv("SPELLBOT_USER_ID", "0"))

# Optional: restrict to a specific LFG channel (SpellBot's queue channel)
# e.g. SPELLBOT_LFG_CHANNEL_ID=123456789012345678
SPELLBOT_LFG_CHANNEL_ID = int(os.getenv("SPELLBOT_LFG_CHANNEL_ID", "0"))

# The title SpellBot uses when the game is ready
SPELLBOT_READY_TITLE_PREFIX = os.getenv("SPELLBOT_READY_TITLE_PREFIX", "Your game is ready!")


class SpellBotWatchCog(commands.Cog):
    """
    Watches SpellBot embeds and detects when a lobby becomes a ready SpellTable game.

    When SpellBot edits its lobby message and the embed title starts with
    'Your game is ready!', we log it to the console.

    Later you can hook this into other logic (timers, tracking, etc.).
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # To avoid double-logging the same message
        self._seen_ready_messages: Set[int] = set()

    # --------- helpers ---------

    @staticmethod
    def _extract_spelltable_link(embed: discord.Embed) -> Optional[str]:
        """
        Best-effort extraction of the SpellTable link from the embed description.
        SpellBot usually puts a markdown link like:
          [Join your SpellTable game now!](https://spelltable.wizards.com/game/XXXX)
        """
        desc = embed.description or ""
        if not desc:
            return None

        for token in desc.split():
            if "spelltable.wizards.com/game" in token:
                # strip markdown / angle brackets
                return token.strip("()<>")
        return None

    def _is_spellbot_message(self, msg: discord.Message) -> bool:
        # Prefer ID match if provided
        if SPELLBOT_USER_ID:
            return msg.author.id == SPELLBOT_USER_ID
        # Fallback to name check (less reliable, but OK)
        return msg.author.bot and msg.author.name.lower() == "spellbot"

    def _is_in_lfg_channel(self, msg: discord.Message) -> bool:
        if SPELLBOT_LFG_CHANNEL_ID:
            return msg.channel.id == SPELLBOT_LFG_CHANNEL_ID
        # If no channel is configured, listen everywhere in the guild
        return True

    # --------- event listener ---------

    @commands.Cog.listener()
    async def on_message_edit(
        self,
        before: discord.Message,
        after: discord.Message,
    ):
        # Guild filter (optional but cheap)
        if GUILD_ID and (after.guild is None or after.guild.id != GUILD_ID):
            return

        # Only care about SpellBot messages
        if after.author is None or not self._is_spellbot_message(after):
            return

        # Optional channel restriction
        if not self._is_in_lfg_channel(after):
            return

        # Need at least one embed
        if not after.embeds:
            return

        embed = after.embeds[0]
        title = (embed.title or "").strip()

        if not title:
            return

        # Check if title looks like the "ready" state
        if not title.lower().startswith(SPELLBOT_READY_TITLE_PREFIX.lower()):
            return

        # Avoid duplicate logs for the same message
        if after.id in self._seen_ready_messages:
            return
        self._seen_ready_messages.add(after.id)

        # Try to grab the SpellTable link (if present)
        link = self._extract_spelltable_link(embed)
        channel_name = f"#{after.channel.name}" if hasattr(after.channel, "name") else str(after.channel.id)

        print(
            "[spellbot-watch] Detected ready game: "
            f"message_id={after.id}, channel={channel_name}, "
            f"link={link or 'unknown'}"
        )


def setup(bot: commands.Bot):
    bot.add_cog(SpellBotWatchCog(bot))
