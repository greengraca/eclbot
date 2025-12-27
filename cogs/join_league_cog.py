# cogs/join_league_cog.py
"""
Join-league channel helper.

Goal:
- Post a "Join <target month>" embed in #join-<month>-league with Ko-fi + Patreon buttons.
- Post a second small embed with an "Enter" button.
- When a user clicks "Enter", the bot checks eligibility (roles + DB entitlements) for target_month.
  If eligible, it grants the ECL role and DMs them to read #rules and #get-started.

This is intentionally separate from subscriptions_cog.py to keep that file smaller.
"""

from __future__ import annotations

import contextlib
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Set, Tuple

import discord
from discord.ext import commands
from zoneinfo import ZoneInfo

from db import subs_access, subs_free_entries, subs_jobs


LISBON_TZ = ZoneInfo("Europe/Lisbon")


# -------------------- tiny env helpers --------------------

def _env_int(name: str, default: int = 0) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw)
    except Exception:
        return default


def _parse_int_set(csv: str) -> Set[int]:
    out: Set[int] = set()
    for part in re.split(r"[\s,]+", (csv or "").strip()):
        if not part:
            continue
        if part.isdigit():
            out.add(int(part))
    return out


def month_key(dt: datetime) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def add_months(mk: str, n: int) -> str:
    y, m = mk.split("-")
    y_i, m_i = int(y), int(m)
    m_i += n
    while m_i > 12:
        y_i += 1
        m_i -= 12
    while m_i < 1:
        y_i -= 1
        m_i += 12
    return f"{y_i:04d}-{m_i:02d}"


def month_bounds(mk: str) -> tuple[datetime, datetime]:
    y, m = mk.split("-")
    start = datetime(int(y), int(m), 1, 0, 0, 0, tzinfo=LISBON_TZ)
    end_mk = add_months(mk, 1)
    y2, m2 = end_mk.split("-")
    end = datetime(int(y2), int(m2), 1, 0, 0, 0, tzinfo=LISBON_TZ)
    return start, end


def month_label(mk: str) -> str:
    try:
        y, m = mk.split("-")
        dt = datetime(int(y), int(m), 1, tzinfo=LISBON_TZ)
        return dt.strftime("%B %Y")
    except Exception:
        return mk


# -------------------- config --------------------

@dataclass(frozen=True)
class JoinLeagueConfig:
    guild_id: int
    ecl_role_id: int
    join_channel_id: int

    patreon_role_ids: Set[int]
    kofi_role_ids: Set[int]
    free_entry_role_ids: Set[int]

    kofi_url: str
    patreon_url: str

    target_month: str  # YYYY-MM

    rules_channel_id: int
    get_started_channel_id: int


def load_config() -> JoinLeagueConfig:
    guild_id = _env_int("GUILD_ID", 0)
    ecl_role_id = _env_int("ECL_ROLE", 0)

    join_channel_id = _env_int("JOIN_LEAGUE_CHANNEL_ID", 0)

    patreon_role_ids = _parse_int_set(os.getenv("PATREON_ROLE_IDS", ""))
    kofi_role_ids = _parse_int_set(os.getenv("KOFI_ROLE_IDS", ""))
    free_entry_role_ids = _parse_int_set(os.getenv("FREE_ENTRY_ROLE_IDS", ""))

    # Reuse the same links used by the subscriptions cog
    kofi_url = (os.getenv("SUBS_KOFI_URL") or "").strip()
    patreon_url = (os.getenv("SUBS_PATREON_URL") or "").strip()

    # Target month (defaults to CURRENT month in Lisbon)
    raw_mk = (os.getenv("JOIN_TARGET_MONTH") or "").strip()
    if raw_mk and re.match(r"^20\d{2}-(0[1-9]|1[0-2])$", raw_mk):
        target_month = raw_mk
    else:
        now = datetime.now(LISBON_TZ)
        target_month = month_key(now)


    rules_channel_id = _env_int("RULES_CHANNEL_ID", 0)
    get_started_channel_id = _env_int("GET_STARTED_CHANNEL_ID", 0)

    return JoinLeagueConfig(
        guild_id=guild_id,
        ecl_role_id=ecl_role_id,
        join_channel_id=join_channel_id,
        patreon_role_ids=patreon_role_ids,
        kofi_role_ids=kofi_role_ids,
        free_entry_role_ids=free_entry_role_ids,
        kofi_url=kofi_url,
        patreon_url=patreon_url,
        target_month=target_month,
        rules_channel_id=rules_channel_id,
        get_started_channel_id=get_started_channel_id,
    )


# -------------------- views --------------------

class JoinLinksView(discord.ui.View):
    def __init__(self, *, kofi_url: str, patreon_url: str):
        super().__init__(timeout=None)

        kofi_url = (kofi_url or "").strip()
        patreon_url = (patreon_url or "").strip()

        if kofi_url.startswith(("http://", "https://")):
            self.add_item(discord.ui.Button(label="💚 Subscribe on Ko-fi", style=discord.ButtonStyle.link, url=kofi_url))
        if patreon_url.startswith(("http://", "https://")):
            self.add_item(discord.ui.Button(label="🔥 Join Patreon (ECL Grinder+)", style=discord.ButtonStyle.link, url=patreon_url))


class EnterLeagueView(discord.ui.View):
    def __init__(self, cog: "JoinLeagueCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="✅ Enter League",
        style=discord.ButtonStyle.success,
        custom_id="ecl:joinleague:enter",
    )
    async def enter_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        await self.cog.handle_enter_click(interaction)


# -------------------- cog --------------------

class JoinLeagueCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.cfg = load_config()
        self._views_registered = False

    @commands.Cog.listener()
    async def on_ready(self):
        if self._views_registered:
            return
        self.bot.add_view(EnterLeagueView(self))
        self._views_registered = True
        print("[join] persistent views registered")

    # -------------------- helpers --------------------
    def _has_any_role_id(self, member: discord.Member, role_ids: Set[int]) -> bool:
        if not role_ids:
            return False
        have = {r.id for r in member.roles}
        return bool(have.intersection(role_ids))

    async def _has_free_entry(self, user_id: int, month: str) -> bool:
        doc = await subs_free_entries.find_one({"guild_id": self.cfg.guild_id, "user_id": int(user_id), "month": month})
        return bool(doc)

    async def _has_db_access(self, user_id: int, month: str, *, at: datetime) -> bool:
        cfg = self.cfg
        doc_month = await subs_access.find_one({
            "guild_id": cfg.guild_id,
            "user_id": int(user_id),
            "month": month,
            "kind": {"$ne": "kofi-one-time"},
        })
        if doc_month:
            return True

        at_utc = at.astimezone(timezone.utc)
        doc_pass = await subs_access.find_one({
            "guild_id": cfg.guild_id,
            "user_id": int(user_id),
            "kind": "kofi-one-time",
            "starts_at": {"$lte": at_utc},
            "expires_at": {"$gt": at_utc},
        })
        return bool(doc_pass)

    async def _eligibility(self, member: discord.Member, month: str, *, at: datetime) -> Tuple[bool, str]:
        cfg = self.cfg

        if self._has_any_role_id(member, cfg.patreon_role_ids):
            return True, "Patreon role"
        if self._has_any_role_id(member, cfg.kofi_role_ids):
            return True, "Ko-fi membership role"
        if self._has_any_role_id(member, cfg.free_entry_role_ids):
            return True, "Free-entry role"
        if await self._has_free_entry(member.id, month):
            return True, "Free entry (DB)"
        if await self._has_db_access(member.id, month, at=at):
            return True, "Ko-fi one-time pass (DB)"
        return False, "No eligible roles / entitlements found"

    async def _grant_ecl(self, member: discord.Member, *, reason: str) -> bool:
        cfg = self.cfg
        if not cfg.ecl_role_id:
            return False
        role = member.guild.get_role(cfg.ecl_role_id)
        if not role:
            return False
        if role in member.roles:
            return False
        with contextlib.suppress(Exception):
            await member.add_roles(role, reason=reason)
        return role in member.roles

    def _rules_mention(self, guild: discord.Guild) -> str:
        cfg = self.cfg
        if cfg.rules_channel_id:
            return f"<#{int(cfg.rules_channel_id)}>"
        ch = discord.utils.get(guild.text_channels, name="rules")
        return ch.mention if ch else "#rules"

    def _get_started_mention(self, guild: discord.Guild) -> str:
        cfg = self.cfg
        if cfg.get_started_channel_id:
            return f"<#{int(cfg.get_started_channel_id)}>"
        ch = discord.utils.get(guild.text_channels, name="get-started")
        return ch.mention if ch else "#get-started"

    # -------------------- interactions --------------------

    async def handle_enter_click(self, interaction: discord.Interaction) -> None:
        cfg = self.cfg

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            with contextlib.suppress(Exception):
                await interaction.response.send_message("This button only works inside the server.", ephemeral=True)
            return

        member: discord.Member = interaction.user
        guild = interaction.guild

        if cfg.guild_id and guild.id != cfg.guild_id:
            with contextlib.suppress(Exception):
                await interaction.response.send_message("Wrong server.", ephemeral=True)
            return

        # Evaluate eligibility at the month flip moment (start of target_month).
        flip_at = month_bounds(cfg.target_month)[0]

        ok, why = await self._eligibility(member, cfg.target_month, at=flip_at)

        if not ok:
            view = JoinLinksView(kofi_url=cfg.kofi_url, patreon_url=cfg.patreon_url)
            msg = (
                f"❌ You're **not registered** for **{month_label(cfg.target_month)}**.\n\n"
                f"**Why:** {why}\n\n"
                "If you just subscribed, make sure your Discord account is linked/synced on **Ko-fi/Patreon**, "
                "wait a moment for roles to appear, then click **Enter League** again."
            )
            with contextlib.suppress(Exception):
                await interaction.response.send_message(msg, ephemeral=True, view=view)
            return

        added = await self._grant_ecl(member, reason=f"Join button — eligible for {cfg.target_month}")

        # Welcome DM (once per month per user)
        job_id = f"join-welcome-dm:{guild.id}:{int(member.id)}:{cfg.target_month}"
        sent_dm = False
        try:
            if not await subs_jobs.find_one({"_id": job_id}):
                await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})
                rules = self._rules_mention(guild)
                get_started = self._get_started_mention(guild)
                await member.send(
                    f"✅ You're in for **{month_label(cfg.target_month)}**!\n\n"
                    f"Please read {rules} and {get_started} before playing. 🐸"
                )
                sent_dm = True
        except Exception:
            sent_dm = False

        # Ephemeral confirmation
        if added:
            text = f"✅ Access granted (ECL role added).{' Check your DMs.' if sent_dm else ''}"
        else:
            text = f"✅ You're already in.{' Check your DMs.' if sent_dm else ''}"

        with contextlib.suppress(Exception):
            await interaction.response.send_message(text, ephemeral=True)

    # -------------------- admin command --------------------

    @commands.slash_command(
        name="joinpost",
        description="Post the #join-... embeds (links + Enter button) in this channel.",
        guild_ids=[_env_int("GUILD_ID", 0)] if _env_int("GUILD_ID", 0) else None,
    )
    async def joinpost(self, ctx: discord.ApplicationContext):
        if not ctx.user.guild_permissions.manage_roles:
            await ctx.respond("You need **Manage Roles**.", ephemeral=True)
            return
        if ctx.guild is None:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        cfg = self.cfg
        nice_month = month_label(cfg.target_month)

        YELLOW = 0xF1C40F  # gold/yellow

        emb1 = discord.Embed(
            title=f"🐉 Welcome to the DragonShield ECL — {nice_month}",
            description=(
                "The **European cEDH League** is a competitive monthly league where you fight for:\n"
                "• **One-of-a-kind ECL Champion Rings**\n"
                "• A **spot at the next European cEDH Championship**\n"
                "• **DragonShield** merch and cEDH staples\n\n"
                "### ✅ How to join\n"
                "**1)** Subscribe using **Ko-fi** or **Patreon** (**€6.5/month**)\n"
                "**2)** Make sure your **Discord account is linked/synced** on the platform\n"
                "**3)** Come back here and press **✅ Enter League** to unlock access\n\n"
                "🕒 If you just subscribed, roles can take a minute to appear — try again shortly."
            ),
            color=YELLOW,
        )
        emb1.add_field(
            name="Need help?",
            value="If something looks wrong, open a ticket and an admin will help you.",
            inline=False,
        )
        if ctx.guild and ctx.guild.icon:
            emb1.set_thumbnail(url=ctx.guild.icon.url)
        emb1.set_footer(text="DragonShield ECL — Join the League")

        view_links = JoinLinksView(kofi_url=cfg.kofi_url, patreon_url=cfg.patreon_url)

        emb2 = discord.Embed(
            title="✅ Enter the League",
            description=(
                "Press the button below to **verify your roles / registration**.\n\n"
                "If you’re eligible, you’ll instantly unlock **ECL access** and get a DM with next steps."
            ),
            color=YELLOW,
        )
        emb2.add_field(
            name="Tip",
            value="*Subscribed but missing roles? Double-check Discord sync on Ko-fi/Patreon and retry.*",
            inline=False,
        )


        view_enter = EnterLeagueView(self)


        try:
            m1 = await ctx.channel.send(embed=emb1, view=view_links)
            m2 = await ctx.channel.send(embed=emb2, view=view_enter)
        except Exception as e:
            await ctx.respond(f"❌ Failed to post embeds: {type(e).__name__}: {e}", ephemeral=True)
            return

        await ctx.respond(
            "✅ Posted the join messages.\n"
            f"- Links message id: `{m1.id}`\n"
            f"- Enter message id: `{m2.id}`\n\n"
            "Tip: you can pin them in the channel.",
            ephemeral=True,
        )


def setup(bot: commands.Bot):
    bot.add_cog(JoinLeagueCog(bot))
