# cogs/timestamp_cog.py
"""/timestamp — generate Discord timestamp strings for cross-timezone coordination.

Converts a user-specified date/time + timezone into Discord's <t:UNIX:format> syntax,
which renders in each viewer's local timezone automatically.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands
from discord import Option

from utils.dates import now_lisbon
from utils.settings import GUILD_ID
from utils.interactions import safe_ctx_defer, safe_ctx_followup

import db


# ── Timezone mapping ──────────────────────────────────────────────────────────

TIMEZONE_CHOICES = {
    "Portugal / UK (WET)": "Europe/Lisbon",
    "Spain / Germany / France (CET)": "Europe/Berlin",
    "Greece / Finland / Romania (EET)": "Europe/Athens",
    "US East Coast (ET)": "America/New_York",
    "US Central (CT)": "America/Chicago",
    "US West Coast (PT)": "America/Los_Angeles",
    "Brazil (BRT)": "America/Sao_Paulo",
}

# Reverse lookup: zone id → label
_ZONE_TO_LABEL = {v: k for k, v in TIMEZONE_CHOICES.items()}

# Half-hour time slots for autocomplete
_TIME_SLOTS = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]


# ── Views ─────────────────────────────────────────────────────────────────────

class TimezoneConfirmView(discord.ui.View):
    """Ephemeral Yes/No confirmation for first-time timezone selection."""

    def __init__(self, user_id: int, zone_id: str, unix_ts: int):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.zone_id = zone_id
        self.unix_ts = unix_ts
        self.confirmed: bool | None = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    @discord.ui.button(label="Yes, that's correct", style=discord.ButtonStyle.green)
    async def confirm(self, button: discord.ui.Button, interaction: discord.Interaction):
        self.confirmed = True
        self.stop()
        # Save timezone preference
        await db.user_preferences.update_one(
            {"user_id": self.user_id},
            {"$set": {"user_id": self.user_id, "timezone": self.zone_id}},
            upsert=True,
        )
        # Send the public timestamp message
        ts_msg = _format_timestamp_message(self.unix_ts)
        await interaction.response.send_message(ts_msg)

    @discord.ui.button(label="No, wrong timezone", style=discord.ButtonStyle.red)
    async def deny(self, button: discord.ui.Button, interaction: discord.Interaction):
        self.confirmed = False
        self.stop()
        # Show timezone select menu
        view = TimezoneSelectView(self.user_id, self.unix_ts)
        await interaction.response.send_message(
            "Please select the correct timezone:",
            view=view,
            ephemeral=True,
        )


class TimezoneSelectView(discord.ui.View):
    """Select menu to pick a different timezone after denying confirmation."""

    def __init__(self, user_id: int, original_unix_ts: int):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.original_unix_ts = original_unix_ts

        options = [
            discord.SelectOption(label=label, value=zone_id)
            for label, zone_id in TIMEZONE_CHOICES.items()
        ]
        self.select = discord.ui.Select(
            placeholder="Choose your timezone...",
            options=options,
        )
        self.select.callback = self.on_select
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    async def on_select(self, interaction: discord.Interaction):
        new_zone_id = self.select.values[0]
        now_in_tz = datetime.now(ZoneInfo(new_zone_id))
        label = _ZONE_TO_LABEL.get(new_zone_id, new_zone_id)

        view = TimezoneConfirmView(self.user_id, new_zone_id, self.original_unix_ts)
        await interaction.response.send_message(
            f"Your current time in **{label}** is **{now_in_tz.strftime('%H:%M, %A %d %B %Y')}**.\n"
            "Is this correct?",
            view=view,
            ephemeral=True,
        )
        self.stop()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_timestamp_message(unix_ts: int) -> str:
    return (
        f"<t:{unix_ts}:F>\n"
        f"<t:{unix_ts}:R>"
    )


def _build_unix_ts(day: int, time_str: str, month: int, year: int, zone_id: str) -> int:
    hour, minute = map(int, time_str.split(":"))
    tz = ZoneInfo(zone_id)
    dt = datetime(year, month, day, hour, minute, tzinfo=tz)
    return int(dt.timestamp())


# ── Cog ───────────────────────────────────────────────────────────────────────

class TimestampCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _time_autocomplete(self, ctx: discord.AutocompleteContext) -> list[str]:
        current = (ctx.value or "").strip()
        if not current:
            return _TIME_SLOTS[:25]
        return [s for s in _TIME_SLOTS if s.startswith(current)][:25]

    @commands.slash_command(
        name="timestamp",
        description="Generate Discord timestamps that show in everyone's timezone.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def timestamp(
        self,
        ctx: discord.ApplicationContext,
        day: int = Option(int, "Day of the month (1-31)", required=True, min_value=1, max_value=31),
        time: str = Option(str, "Time (e.g. 21:00, 14:30)", required=True, autocomplete=_time_autocomplete),
        month: int = Option(int, "Month (1-12, defaults to current)", required=False, min_value=1, max_value=12, default=None),
        year: int = Option(int, "Year (defaults to current)", required=False, min_value=2024, max_value=2030, default=None),
        timezone: str = Option(
            str,
            "Your timezone (saved for future use)",
            required=False,
            default=None,
            choices=list(TIMEZONE_CHOICES.keys()),
        ),
    ):
        now = now_lisbon()
        month = month or now.month
        year = year or now.year

        # Validate time format
        if time not in _TIME_SLOTS:
            await ctx.respond(
                f"Invalid time `{time}`. Use HH:MM format (e.g. `21:00`, `14:30`).",
                ephemeral=True,
            )
            return

        # Validate day for the given month/year
        try:
            _build_unix_ts(day, time, month, year, "UTC")
        except ValueError:
            await ctx.respond(
                f"Invalid date: {year}-{month:02d}-{day:02d} does not exist.",
                ephemeral=True,
            )
            return

        user_id = ctx.author.id

        # Resolve timezone
        if timezone:
            # Explicit timezone provided → map label to zone id
            zone_id = TIMEZONE_CHOICES.get(timezone)
            if not zone_id:
                await ctx.respond("Unknown timezone selection.", ephemeral=True)
                return
        else:
            # Try loading saved preference
            pref = await db.user_preferences.find_one({"user_id": user_id})
            zone_id = pref["timezone"] if pref else None

        if not zone_id:
            # First-time user, no timezone provided
            await ctx.respond(
                "You haven't set a timezone yet. Please use the `timezone` parameter to set one.\n"
                "Example: `/timestamp day:15 time:21:00 timezone:Portugal / UK (WET)`",
                ephemeral=True,
            )
            return

        unix_ts = _build_unix_ts(day, time, month, year, zone_id)

        # Check if this is a first-time timezone setup (explicit tz + no saved pref)
        saved_pref = await db.user_preferences.find_one({"user_id": user_id})

        if timezone and not saved_pref:
            # First-time: confirm timezone with the user
            now_in_tz = datetime.now(ZoneInfo(zone_id))
            label = _ZONE_TO_LABEL.get(zone_id, zone_id)

            view = TimezoneConfirmView(user_id, zone_id, unix_ts)
            await ctx.respond(
                f"Your current time in **{label}** is **{now_in_tz.strftime('%H:%M, %A %d %B %Y')}**.\n"
                "Is this correct?",
                view=view,
                ephemeral=True,
            )
            return

        # Returning user with explicit timezone override → update preference
        if timezone and saved_pref and saved_pref.get("timezone") != zone_id:
            await db.user_preferences.update_one(
                {"user_id": user_id},
                {"$set": {"timezone": zone_id}},
            )

        # Generate and send publicly
        ts_msg = _format_timestamp_message(unix_ts)
        await ctx.respond(ts_msg)


def setup(bot: commands.Bot):
    bot.add_cog(TimestampCog(bot))
