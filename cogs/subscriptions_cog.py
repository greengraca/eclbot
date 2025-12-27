# cogs/subscriptions_cog.py
"""ECL monthly subscriptions + free entry controller.

What it does:
  - Grants ECL role when a user pays on Ko-fi (via an inbox channel webhook post)
  - Treats Patreon integration as "subscribed" if the member has any of PATREON_ROLE_IDS
  - Treats certain roles as monthly free-entry (e.g., Judge, Arena Vanguard)
  - Supports per-user free-entry for a given month (e.g., Top16 cut) stored in Mongo
  - Sends DM reminders (3 days before month end and on the last day)
  - Removes ECL role on the last day of the month for members not eligible for next month

Ko-fi note:
Ko-fi webhooks only fire on payments (not on cancellation). So we treat each
payment as a 30-day pass (duration-based).

Deployment (Heroku worker dyno):
We follow your existing pattern: a Cloudflare/Zapier/etc. forwards the Ko-fi
payload into a Discord channel using a Discord webhook. The bot listens to that
channel and processes JSON payloads.
"""

import asyncio
import contextlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set, Tuple, List

import discord
from discord.ext import commands, tasks
from pymongo.errors import DuplicateKeyError
from zoneinfo import ZoneInfo

from topdeck_fetch import get_league_rows_cached, PlayerRow
from online_games_store import count_online_games_by_topdeck_uid_str
from db import ensure_indexes, ping, subs_access, subs_free_entries, subs_jobs, subs_kofi_events


# -------------------- safe env helpers --------------------

def _env_int(name: str, default: int = 0) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw)
    except Exception:
        return default


def _env_float(name: str, default: float = 0.0) -> float:
    raw = (os.getenv(name) or "").strip()
    try:
        return float(raw)
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


# -------------------- constants --------------------

GUILD_ID = _env_int("GUILD_ID", 0)
LISBON_TZ = ZoneInfo("Europe/Lisbon")
# Safety gate: prevents accidental mass removals in production.
# Override with SUBS_ENFORCEMENT_START (ISO datetime or YYYY-MM-DD).
DEFAULT_SUBS_ENFORCEMENT_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=LISBON_TZ)


# -------------------- date helpers --------------------

def month_key(dt: datetime) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def month_label(mk: str) -> str:
    """Pretty label like 'January 2026'."""
    try:
        y, m = mk.split("-")
        dt = datetime(int(y), int(m), 1, tzinfo=LISBON_TZ)
        return dt.strftime("%B %Y")
    except Exception:
        return mk


def parse_month_from_text(text: str) -> Optional[str]:
    """Find YYYY-MM in text."""
    m = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])\b", text or "")
    return m.group(0) if m else None


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


def month_bounds(mk: str) -> Tuple[datetime, datetime]:
    """Return (start, end_exclusive) of mk in Lisbon TZ."""
    y, m = mk.split("-")
    start = datetime(int(y), int(m), 1, 0, 0, 0, tzinfo=LISBON_TZ)
    end_mk = add_months(mk, 1)
    y2, m2 = end_mk.split("-")
    end = datetime(int(y2), int(m2), 1, 0, 0, 0, tzinfo=LISBON_TZ)
    return start, end


def last_day_of_month(dt: datetime) -> datetime:
    mk = month_key(dt)
    _, end = month_bounds(mk)
    return (end - timedelta(days=1)).astimezone(LISBON_TZ)


def month_end_inclusive(mk: str) -> datetime:
    """Return the last second of mk in Lisbon TZ."""
    _, end = month_bounds(mk)
    return (end - timedelta(seconds=1)).astimezone(LISBON_TZ)


def compute_kofi_one_time_window(when_lisbon: datetime, days: int) -> tuple[datetime, datetime]:
    """Return (starts_at_utc, expires_at_utc) for a Ko-fi one-time pass."""
    if when_lisbon.tzinfo is None:
        when_lisbon = when_lisbon.replace(tzinfo=LISBON_TZ)
    starts = when_lisbon.astimezone(timezone.utc)
    expires = starts + timedelta(days=max(1, int(days or 30)))
    return starts, expires


# -------------------- Ko-fi parsing helpers --------------------

def extract_discord_user_id(payload: Dict[str, Any]) -> Optional[int]:
    """Best-effort mapping from Ko-fi payload -> Discord user id."""
    duid = str(payload.get("discord_userid") or payload.get("discord_user_id") or "").strip()
    if duid.isdigit():
        return int(duid)

    msg = str(payload.get("message") or "")
    m = re.search(r"<@!?(\d{15,25})>", msg)
    if m:
        return int(m.group(1))
    m2 = re.search(r"\b(\d{15,25})\b", msg)
    if m2:
        return int(m2.group(1))
    return None


def extract_json_from_message_content(content: str) -> Optional[Dict[str, Any]]:
    """Supports ```json ...``` or raw JSON."""
    if not content:
        return None
    m = re.search(r"```json\s*([\s\S]+?)\s*```", content)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            return None
    content = content.strip()
    if content.startswith("{") and content.endswith("}"):
        try:
            return json.loads(content)
        except Exception:
            return None
    return None


# -------------------- config --------------------

@dataclass(frozen=True)
class SubsConfig:
    guild_id: int
    ecl_role_id: int
    patreon_role_ids: Set[int]
    kofi_role_ids: Set[int]  # Ko-fi membership roles granted by Ko-fi Discord bot
    free_entry_role_ids: Set[int]
    kofi_inbox_channel_id: int
    kofi_verify_token: str
    entitlement_cutoff_day: int
    kofi_one_time_days: int  # one-time Ko-fi pass duration in days
    enforcement_start: datetime  # when role removals/audits are allowed to start (Lisbon TZ)
    dm_enabled: bool
    dm_concurrency: int
    dm_sleep_seconds: float
    log_channel_id: int
    ecl_mod_role_id: int

    top16_role_id: int
    top16_min_online_games: int
    top16_min_total_games: int

    topcut_close_pts: int  # points margin for 'very close' Top16 prize reminder

    # Marketing / links
    kofi_url: str
    patreon_url: str

    # Cosmetics
    embed_thumbnail_url: str
    embed_color: int

def load_config() -> SubsConfig:
    guild_id = _env_int("GUILD_ID", 0)
    ecl_role_id = _env_int("ECL_ROLE", 0)
    ecl_mod_role_id = _env_int("ECL_MOD_ROLE_ID", 0)

    patreon_role_ids = _parse_int_set(os.getenv("PATREON_ROLE_IDS", ""))
    kofi_role_ids = _parse_int_set(os.getenv("KOFI_ROLE_IDS", ""))
    free_entry_role_ids = _parse_int_set(os.getenv("FREE_ENTRY_ROLE_IDS", ""))

    kofi_inbox_channel_id = _env_int("KOFI_INBOX_CHANNEL_ID", 0)
    kofi_verify_token = (os.getenv("KOFI_VERIFY_TOKEN") or "").strip()

    cutoff_day = _env_int("SUBS_CUTOFF_DAY", 23)
    kofi_one_time_days = max(1, _env_int("KOFI_ONE_TIME_DAYS", 30))

    # Safety gate: do not revoke ECL / run audits before this datetime.
    enforcement_raw = (os.getenv("SUBS_ENFORCEMENT_START") or "").strip()
    enforcement_start = DEFAULT_SUBS_ENFORCEMENT_START
    if enforcement_raw:
        try:
            enforcement_start = datetime.fromisoformat(enforcement_raw)
        except Exception:
            try:
                y, m, d = enforcement_raw.split("-")
                enforcement_start = datetime(int(y), int(m), int(d), 0, 0, 0)
            except Exception:
                enforcement_start = DEFAULT_SUBS_ENFORCEMENT_START
    if enforcement_start.tzinfo is None:
        enforcement_start = enforcement_start.replace(tzinfo=LISBON_TZ)
    enforcement_start = enforcement_start.astimezone(LISBON_TZ)
    dm_enabled = (os.getenv("SUBS_DM_ENABLED") or "1").strip() == "1"
    dm_concurrency = max(1, _env_int("SUBS_DM_CONCURRENCY", 5))
    dm_sleep_seconds = max(0.0, _env_float("SUBS_DM_SLEEP_SECONDS", 0.8))
    log_channel_id = _env_int("SUBS_LOG_CHANNEL_ID", 0)

    # Links for the DM buttons (must be real https URLs)
    kofi_url = (os.getenv("SUBS_KOFI_URL") or "").strip()
    patreon_url = (os.getenv("SUBS_PATREON_URL") or "").strip()

    # Cosmetics
    embed_thumbnail_url = (os.getenv("LFG_EMBED_ICON_URL") or "").strip()
    embed_color = _env_int("SUBS_EMBED_COLOR", 0x2ECC71)

    top16_role_id = _env_int("TOP16_ROLE_ID", 0)
    top16_min_online_games = _env_int("TOP16_MIN_ONLINE_GAMES", 10)
    top16_min_total_games = _env_int("TOP16_MIN_TOTAL_GAMES", 10)
    topcut_close_pts = _env_int("TOPCUT_CLOSE_PTS", 250)

    return SubsConfig(
        guild_id=guild_id,
        ecl_role_id=ecl_role_id,
        patreon_role_ids=patreon_role_ids,
        kofi_role_ids=kofi_role_ids,
        free_entry_role_ids=free_entry_role_ids,
        kofi_inbox_channel_id=kofi_inbox_channel_id,
        kofi_verify_token=kofi_verify_token,
        entitlement_cutoff_day=cutoff_day,
        kofi_one_time_days=kofi_one_time_days,
        enforcement_start=enforcement_start,
        dm_enabled=dm_enabled,
        dm_concurrency=dm_concurrency,
        dm_sleep_seconds=dm_sleep_seconds,
        log_channel_id=log_channel_id,
        top16_role_id=top16_role_id,
        top16_min_online_games=top16_min_online_games,
        top16_min_total_games=top16_min_total_games,
        topcut_close_pts=topcut_close_pts,
        kofi_url=kofi_url,
        patreon_url=patreon_url,
        embed_thumbnail_url=embed_thumbnail_url,
        embed_color=embed_color,
        ecl_mod_role_id=ecl_mod_role_id,
    )


# -------------------- views --------------------

class SubsLinksView(discord.ui.View):
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


# -------------------- cog --------------------

class SubscriptionsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.cfg = load_config()
        self._bootstrapped = False
        self._tick.start()
        self._access_audit.start()

        # small locks to avoid overlap
        self._top16_reminder_lock = asyncio.Lock()
        self._access_audit_lock = asyncio.Lock()

    def cog_unload(self):
        self._tick.cancel()
        self._access_audit.cancel()

    def _enforcement_active(self, at: Optional[datetime] = None) -> bool:
        """True when it's expected/safe to revoke roles in production."""
        start = getattr(self.cfg, "enforcement_start", None)
        if not isinstance(start, datetime):
            return False
        now = at or datetime.now(LISBON_TZ)
        if now.tzinfo is None:
            now = now.replace(tzinfo=LISBON_TZ)
        now = now.astimezone(LISBON_TZ)
        return now >= start

    @commands.Cog.listener()
    async def on_ready(self):
        if self._bootstrapped:
            return
        self._bootstrapped = True
        try:
            await ping()
            await ensure_indexes()
            print("[subs] MongoDB OK + indexes ensured")
        except Exception as e:
            print(f"[subs] MongoDB error: {e}")

    # -------------------- Marketing embed helpers --------------------

    def _build_links_view(self) -> Optional[discord.ui.View]:
        cfg = self.cfg
        v = SubsLinksView(cfg.kofi_url, cfg.patreon_url)
        if not v._ok(v.kofi_url) and not v._ok(v.patreon_url):
            return None
        return v
    
    async def _dm_mods_summary(self, guild: discord.Guild, *, summary: str) -> None:
        """
        DM everyone with ECL_MOD_ROLE_ID a short summary.
        Fail-safe: never raises.
        """
        cfg = self.cfg
        rid = int(getattr(cfg, "ecl_mod_role_id", 0) or 0)
        if not rid:
            return

        role = guild.get_role(rid)
        mods: list[discord.Member] = []

        # Best case: cached role.members
        if role:
            mods = [m for m in (role.members or []) if isinstance(m, discord.Member) and not m.bot]

        # Fallback: fetch all members and filter by role
        if not mods:
            try:
                members = [m async for m in guild.fetch_members(limit=None)]
                mods = [m for m in members if (not m.bot) and any(rr.id == rid for rr in m.roles)]
            except Exception:
                return

        if not mods:
            await self._log(f"[subs] mod summary skipped: no members found for ECL_MOD_ROLE_ID={rid}")
            return

        sent = 0
        for m in mods:
            try:
                await m.send(summary)
                sent += 1
            except Exception:
                pass

            if cfg.dm_sleep_seconds:
                await asyncio.sleep(cfg.dm_sleep_seconds)

        await self._log(f"[subs] mod summary DM sent {sent}/{len(mods)} — {summary}")
        print(f"[subs] mod summary DM sent {sent}/{len(mods)} — {summary}")


    async def _count_registered_for_month(self, guild: discord.Guild, mk: str) -> int:
        cfg = self.cfg

        start, end = month_bounds(mk)
        start_utc = start.astimezone(timezone.utc)
        end_utc = end.astimezone(timezone.utc)

        # ---- DB-based entitlements breakdown ----
        month_ent_ids: list[int] = []
        pass_ids: list[int] = []

        try:
            month_ent_ids = await subs_access.distinct("user_id", {
                "guild_id": cfg.guild_id,
                "month": mk,
                "kind": {"$ne": "kofi-one-time"},
            })
        except Exception:
            month_ent_ids = []

        try:
            pass_ids = await subs_access.distinct("user_id", {
                "guild_id": cfg.guild_id,
                "kind": "kofi-one-time",
                "starts_at": {"$lt": end_utc},
                "expires_at": {"$gt": start_utc},
            })
        except Exception:
            pass_ids = []

        def _to_int_set(xs) -> set[int]:
            out: set[int] = set()
            for x in xs or []:
                try:
                    out.add(int(x))
                except Exception:
                    pass
            return out

        month_ent_set = _to_int_set(month_ent_ids)
        pass_set = _to_int_set(pass_ids)
        kofi_set = month_ent_set | pass_set  # DB-based access union

        # ---- Free-entry DB list ----
        try:
            free_ids = await subs_free_entries.distinct("user_id", {"guild_id": cfg.guild_id, "month": mk})
        except Exception:
            free_ids = []
        free_set = _to_int_set(free_ids)

        eligible: set[int] = set()
        eligible |= kofi_set
        eligible |= free_set

        # ---- Role-based breakdown per role ----
        role_ids = cfg.patreon_role_ids | cfg.kofi_role_ids | cfg.free_entry_role_ids

        role_to_member_ids: dict[int, set[int]] = {}
        role_member_ids: set[int] = set()
        used_fetch_fallback = False

        if role_ids:
            # fast path: role.members (works well if member cache is populated)
            for rid in role_ids:
                role = guild.get_role(int(rid))
                if not role:
                    role_to_member_ids[int(rid)] = set()
                    continue
                s = {int(m.id) for m in (getattr(role, "members", []) or []) if not m.bot}
                role_to_member_ids[int(rid)] = s
                role_member_ids |= s

            # fallback: fetch all members if cache seems too small
            if guild.member_count and len(role_member_ids) < min(10, guild.member_count // 50):
                used_fetch_fallback = True
                try:
                    members = [m async for m in guild.fetch_members(limit=None)]
                    role_to_member_ids = {int(rid): set() for rid in role_ids}
                    role_member_ids = set()

                    for m in members:
                        if m.bot:
                            continue
                        mids = {rr.id for rr in m.roles}
                        hit = mids.intersection(role_ids)
                        if not hit:
                            continue
                        uid = int(m.id)
                        for rid in hit:
                            role_to_member_ids[int(rid)].add(uid)
                        role_member_ids.add(uid)
                except Exception:
                    pass

            eligible |= role_member_ids

        # pretty breakdown list
        role_breakdown = []
        for rid in sorted(role_ids):
            role = guild.get_role(int(rid))
            role_breakdown.append({
                "name": role.name if role else "(missing role)",
                "count": len(role_to_member_ids.get(int(rid), set())),
            })

        # ---- (3) projected Top16 for NEXT month (no IDs yet) ----
        now_mk = month_key(datetime.now(LISBON_TZ))
        next_mk = add_months(now_mk, 1)
        projected_top16 = 16 if mk == next_mk else 0

        known_total = len(eligible)
        reported_total = known_total + projected_top16

        # ---- (4) clearer debug print (no ids) ----
        role_lines = [f"{rb['name']}: {rb['count']}" for rb in role_breakdown]

        print(
            "[subs] count\n"
            f"  mk: {mk}\n"
            f"  db: month_ent={len(month_ent_set)} | kofi_pass={len(pass_set)} | db_union={len(kofi_set)}\n"
            f"  free_db: {len(free_set)}\n"
            f"  roles: union={len(role_member_ids)} | fetch_fallback={used_fetch_fallback}\n"
            f"  roles breakdown: " + " | ".join(role_lines) + "\n"
            f"  total known: {known_total}\n"
            f"  projected top16: {projected_top16}\n"
            f"  total reported: {reported_total}"
        )

        return reported_total



    async def _build_reminder_embed(self, kind: str, target_month: str, registered_count: int) -> discord.Embed:
        cfg = self.cfg
        nice_month = month_label(target_month)

        if kind == "last":
            title = f"🔥 Join {nice_month} — last day before the reset"
            urgency = "Today is the **monthly reset** — lock in access for next league."
        else:
            title = f"🔥 Join {nice_month} — league starts soon"
            urgency = "The **monthly reset** is in **3 days** — lock in access for next league."

        desc = (
            f"{urgency}\n\n"
            "📌 **You can register any day of the league.**\n"
            "This is just a reminder so you **don’t lose access** when the month flips — "
            "and if you do lose it, you can **regain access anytime** by registering.\n\n"
            f"Right now, you’re **not registered** for **{nice_month}** — "
            f"and you’ll lose **ECL** access when the month flips.\n\n"
            f"👥 **Already registered:** **{registered_count}** players\n"
        )

        emb = discord.Embed(
            title=title,
            description=desc,
            color=cfg.embed_color if isinstance(cfg.embed_color, int) else 0x2ECC71,
        )

        emb.add_field(
            name="How to register",
            value=(
                "• **Ko-fi**: **monthly** subscription or pay for a **30-day pass**\n"
                "• **Patreon**: **ECL Grinder** tier (or above)\n"
            ),
            inline=False,
        )

        emb.set_footer(text="ECL • If you're having any issues please open a ticket !")

        if cfg.embed_thumbnail_url and cfg.embed_thumbnail_url.startswith(("http://", "https://")):
            emb.set_thumbnail(url=cfg.embed_thumbnail_url)

        return emb
    
    # -------------------- MOD reminder helpers --------------------
    
    def _build_flip_mods_embed(self, guild: discord.Guild, mk: str) -> discord.Embed:
        cfg = self.cfg

        current_bracket = (os.getenv("TOPDECK_BRACKET_ID") or "").strip() or "(not set)"
        next_bracket = (os.getenv("NEXT_MONTH_TOPDECK_BRACKET_ID") or "").strip() or "(not set)"

        # Build free-entry role names (mentions render as names)
        role_ids = sorted(int(x) for x in (cfg.free_entry_role_ids or set()) if int(x))
        role_lines: list[str] = []
        missing_count = 0
        for rid in role_ids:
            r = guild.get_role(rid)
            if r:
                role_lines.append(f"• {r.name}")
            else:
                missing_count += 1

        roles_value = "\n".join(role_lines) if role_lines else "(none configured)"
        # Keep within embed field limits
        if len(roles_value) > 950:
            roles_value = roles_value[:950] + "\n…"

        if missing_count:
            roles_value += f"\n\n⚠️ Missing roles in guild: {missing_count}"

        emb = discord.Embed(
            title=f"🧰 Month flip checklist — {month_label(mk)}",
            description="Do these steps after the month flips:",
            color=cfg.embed_color if isinstance(cfg.embed_color, int) else 0x2ECC71,
        )

        emb.add_field(
            name="1) TopDeck",
            value=(
                "Set the **new** bracket id, restart the worker, then run:\n"
                "`/unlink` → `/link`"
            ),
            inline=False,
        )
        emb.add_field(name="Current TOPDECK_BRACKET_ID", value=f"`{current_bracket}`", inline=False)
        emb.add_field(name="NEXT_MONTH_TOPDECK_BRACKET_ID", value=f"`{next_bracket}`", inline=False)

        emb.add_field(
            name="2) Free-entry roles",
            value="Review/update free-entry roles for this month, then restart the worker.",
            inline=False,
        )
        emb.add_field(name="Free-entry roles (names)", value=roles_value, inline=False)

        if cfg.embed_thumbnail_url and cfg.embed_thumbnail_url.startswith(("http://", "https://")):
            emb.set_thumbnail(url=cfg.embed_thumbnail_url)

        emb.set_footer(text="ECL • Mods month flip checklist")
        return emb



    async def _dm_mods_embed(self, guild: discord.Guild, *, embed: discord.Embed) -> None:
        cfg = self.cfg
        rid = int(getattr(cfg, "ecl_mod_role_id", 0) or 0)
        if not rid:
            return

        role = guild.get_role(rid)
        mods: list[discord.Member] = []
        if role:
            mods = [m for m in (role.members or []) if not m.bot]

        if not mods:
            try:
                members = [m async for m in guild.fetch_members(limit=None)]
                mods = [m for m in members if (not m.bot) and any(rr.id == rid for rr in m.roles)]
            except Exception:
                return

        sent = 0
        for m in mods:
            try:
                await m.send(embed=embed)
                sent += 1
            except Exception:
                pass
            if cfg.dm_sleep_seconds:
                await asyncio.sleep(cfg.dm_sleep_seconds)

        await self._log(f"[subs] mod embed sent {sent}/{len(mods)} — {embed.title}")


    # -------------------- Top16 online-games reminder helpers --------------------

    async def _top16_unqualified_for_month(
        self,
        guild: discord.Guild,
        *,
        mk: str,
    ) -> tuple[list[dict], list[str]]:
        """
        Returns:
          - entries: [{rank, row, online_games, missing, discord_id}]
          - debug_missing: list[str] of mapping misses
        Only includes players who are currently in TopDeck Top16 AND have < required online games.
        """
        cfg = self.cfg

        bracket_id = (os.getenv("TOPDECK_BRACKET_ID") or "").strip()
        firebase_token = os.getenv("FIREBASE_ID_TOKEN", None)
        if not bracket_id:
            return ([], ["TOPDECK_BRACKET_ID not set"])

        # Fetch rows
        try:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=True)
        except Exception:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=False)

        if not rows:
            return ([], ["no TopDeck rows"])

        # Online counts for mk
        try:
            y, m = mk.split("-")
            year, month = int(y), int(m)
        except Exception:
            return ([], [f"bad mk: {mk!r}"])

        try:
            online_counts = await count_online_games_by_topdeck_uid_str(bracket_id, year, month, online_only=True)
        except Exception as e:
            return ([], [f"online_counts error: {type(e).__name__}: {e}"])

        # Active, sorted like TopDeck leaderboard (points first)
        active = [r for r in rows if not r.dropped]
        active = sorted(active, key=lambda r: (-r.pts, -r.games))
        top16 = active[:16]

        # Pre-build name->discord id mapping for fallbacks
        members = [m async for m in guild.fetch_members(limit=None)]
        name_to_id: dict[str, int] = {}
        for m in members:
            if m.bot:
                continue
            for cand in (m.name, getattr(m, "global_name", None), getattr(m, "display_name", None)):
                if isinstance(cand, str) and cand.strip():
                    name_to_id.setdefault(self._norm_name(cand), int(m.id))

            discrim = getattr(m, "discriminator", None)
            if discrim and discrim != "0":
                name_to_id.setdefault(self._norm_name(f"{m.name}#{discrim}"), int(m.id))

        entries: list[dict] = []
        misses: list[str] = []

        for idx, r in enumerate(top16, start=1):
            uid = (r.uid or "").strip()
            online = online_counts.get(uid, 0) if uid else 0
            need = max(0, cfg.top16_min_online_games - online)
            if need <= 0:
                continue  # already qualified

            # Map to Discord ID (best-effort)
            discord_id: Optional[int] = None

            did = self._extract_discord_id_from_text(r.discord or "")
            if did:
                discord_id = did
            elif r.discord:
                key = self._norm_name(r.discord)
                discord_id = name_to_id.get(key)
            elif r.name:
                key = self._norm_name(r.name)
                discord_id = name_to_id.get(key)

            if not discord_id:
                misses.append(r.discord or r.name or "unknown")
                continue

            entries.append({
                "rank": idx,
                "row": r,
                "online_games": int(online),
                "missing": int(need),
                "discord_id": int(discord_id),
            })

        return entries, misses

    async def _build_top16_online_reminder_embed(
        self,
        *,
        kind: str,          # "5d" | "3d" | "last"
        mk: str,            # YYYY-MM
        rank: int,
        name: str,
        online_games: int,
        need_total: int,
        mention: str,       # e.g. member.mention
    ) -> discord.Embed:
        cfg = self.cfg

        kind = (kind or "").strip().lower()
        if kind not in ("5d", "3d", "last"):
            kind = "5d"

        nice_month = month_label(mk)
        missing = max(0, int(need_total) - int(online_games))

        if kind == "last":
            title = f"⏳ Last day — finish your online games for Top16"
            urgency = "Today is the **last day** of the league."
        elif kind == "3d":
            title = f"⚠️ 3 days left — finish your online games for Top16"
            urgency = "Only **3 days** left in the league."
        else:
            title = f"👀 5 days left — finish your online games for Top16"
            urgency = "Only **5 days** left in the league."

        desc = (
            f"Hey {mention} 👋\n\n"
            f"{urgency}\n\n"
            f"You're currently **#{rank:02d}** on **TopDeck** for **{nice_month}**, "
            f"but you’re **not qualified** for the Top16 cut yet because of the **online games requirement**.\n\n"
            f"✅ Online games: **{online_games} / {need_total}**\n"
            f"❗ You need **{missing}** more online game(s) to qualify.\n\n"
            "If you want to keep your spot, try to finish the remaining online games before the league ends."
        )

        emb = discord.Embed(
            title=title,
            description=desc,
            color=cfg.embed_color if isinstance(cfg.embed_color, int) else 0x2ECC71,
        )

        emb.set_footer(text="ECL • Top16 qualification reminder")

        if cfg.embed_thumbnail_url and cfg.embed_thumbnail_url.startswith(("http://", "https://")):
            emb.set_thumbnail(url=cfg.embed_thumbnail_url)

        return emb

    async def _run_top16_online_reminder_job(self, guild: discord.Guild, *, mk: str, kind: str) -> None:
        """
        DM players who are currently TopDeck Top16 but not qualified (< min online games).
        Runs at most once per (mk, kind).
        Also logs/prints who would receive it.
        """
        job_id = f"top16-online-remind:{guild.id}:{mk}:{kind}"
        if await subs_jobs.find_one({"_id": job_id}):
            return
        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        cfg = self.cfg

        async with self._top16_reminder_lock:
            entries, misses = await self._top16_unqualified_for_month(guild, mk=mk)

            if misses:
                await self._log(f"[subs] Top16-online mapping misses ({mk} {kind}): " + ", ".join(misses[:20]))

            if not entries:
                await self._log(f"[subs] Top16-online reminder ({mk} {kind}): 0 targets")
                print(f"[subs] Top16-online reminder ({mk} {kind}): 0 targets")
                await self._dm_mods_summary(
                    guild,
                    summary=f"[ECL] Top16-online reminder ({mk} {kind}) — sent 0 DMs (0 targets).",
                )
                return

            # ---- Log/print who will be targeted (sample up to 20) ----
            sample_lines: list[str] = []
            for e in entries[:20]:
                try:
                    did = int(e["discord_id"])
                except Exception:
                    did = 0
                try:
                    rank = int(e["rank"])
                except Exception:
                    rank = 0
                try:
                    og = int(e["online_games"])
                except Exception:
                    og = 0
                row = e.get("row")
                nm = str(getattr(row, "name", "") or "Player")
                sample_lines.append(f"#{rank:02d} {nm} | discord_id={did} | online={og}/{int(cfg.top16_min_online_games)}")

            msg = (
                f"[subs] Top16-online reminder ({mk} {kind}) targets={len(entries)}. "
                f"Sample (up to 20):\n" + "\n".join(sample_lines)
            )
            print(msg)
            await self._log(msg)

            sem = asyncio.Semaphore(cfg.dm_concurrency)
            sent = 0

            async def _send_one(entry: dict):
                nonlocal sent
                async with sem:
                    uid = int(entry["discord_id"])
                    try:
                        member = guild.get_member(uid) or await guild.fetch_member(uid)
                    except Exception:
                        return

                    if not member or member.bot:
                        return

                    try:
                        emb = await self._build_top16_online_reminder_embed(
                            kind=kind,
                            mk=mk,
                            rank=int(entry["rank"]),
                            name=str(getattr(entry["row"], "name", "") or ""),
                            online_games=int(entry["online_games"]),
                            need_total=int(cfg.top16_min_online_games),
                            mention=member.mention,  # ✅ "Hey @discordname" in real DMs
                        )
                        await member.send(embed=emb)
                        sent += 1
                    except Exception:
                        return

                    if cfg.dm_sleep_seconds:
                        await asyncio.sleep(cfg.dm_sleep_seconds)

            await asyncio.gather(*[_send_one(e) for e in entries])
            await self._log(f"[subs] ✅ Top16-online reminder ({mk} {kind}) sent {sent}/{len(entries)}")
            print(f"[subs] ✅ Top16-online reminder ({mk} {kind}) sent {sent}/{len(entries)}")
            await self._dm_mods_summary(
                guild,
                summary=f"[ECL] Top16-online reminder ({mk} {kind}) — sent {sent}/{len(entries)} DMs.",
            )



    # -------------------- Ko-fi ingestion --------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        cfg = self.cfg

        if not cfg.kofi_inbox_channel_id:
            return
        if message.channel.id != cfg.kofi_inbox_channel_id:
            return
        if not message.webhook_id:
            return

        payload = extract_json_from_message_content(message.content or "")
        if not payload:
            return

        # Verify token (optional)
        if cfg.kofi_verify_token:
            token = str(payload.get("verification_token") or "").strip()
            if not token or token != cfg.kofi_verify_token:
                await self._log("[subs] Ignored Ko-fi payload: bad verification_token")
                return

        txn_id = str(payload.get("kofi_transaction_id") or payload.get("transaction_id") or "").strip()
        if not txn_id:
            await self._log("[subs] Ignored Ko-fi payload: missing transaction id")
            return

        # If Ko-fi Discord bot already grants/removes roles for MEMBERSHIPS,
        # ignore subscription payments here to avoid fighting over roles / double entitlement.
        if bool(payload.get("is_subscription_payment")):
            await self._log(f"[subs] Ignored Ko-fi subscription payment txn={txn_id} (handled by Ko-fi bot roles).")
            try:
                await message.delete()
            except Exception:
                pass
            return

        # We only handle one-time tips (single-month passes)
        user_id = extract_discord_user_id(payload)
        if not user_id:
            await self._log(f"[subs] Ko-fi txn {txn_id}: could not find Discord user id in payload")
            return

        # Optional: enforce minimum one-time amount (EUR 7)
        currency = str(payload.get("currency") or "").upper().strip()
        try:
            amount = float(payload.get("amount") or 0)
        except Exception:
            amount = 0.0

        if currency == "EUR" and amount < 7.0:
            await self._log(f"[subs] Ignored Ko-fi one-time payment txn={txn_id}: amount={amount} {currency} (< 7 EUR).")
            try:
                await message.delete()
            except Exception:
                pass
            return

        # Timestamp parsing
        when = datetime.now(timezone.utc)
        for key in ("timestamp", "time", "created_at"):
            if payload.get(key):
                try:
                    when = datetime.fromisoformat(str(payload[key]).replace("Z", "+00:00")).astimezone(timezone.utc)
                    break
                except Exception:
                    pass

        when_lisbon = when.astimezone(LISBON_TZ)
        purchase_mk = month_key(when_lisbon)

        # Late-month prereg: treat one-time payments after the cutoff day as "next month"
        # so they don't expire mid-next-league.
        effective_mk = purchase_mk
        try:
            if int(when_lisbon.day) >= int(cfg.entitlement_cutoff_day or 99):
                effective_mk = add_months(purchase_mk, 1)
        except Exception:
            effective_mk = purchase_mk

        pass_mk = f"pass:{effective_mk}"

        # One-time pass window:
        # - normal: rolling from now
        # - prereg (effective_mk != purchase_mk): expire as-if paid on day 1 of effective_mk
        starts_at_utc, expires_at_utc = compute_kofi_one_time_window(when_lisbon, cfg.kofi_one_time_days)
        if effective_mk != purchase_mk:
            eff_start_lisbon, _ = month_bounds(effective_mk)
            eff_start_utc = eff_start_lisbon.astimezone(timezone.utc)
            expires_at_utc = eff_start_utc + timedelta(days=max(1, int(cfg.kofi_one_time_days or 30)))
            # keep starts_at_utc as "now" so prereg users can access immediately

        # If the user already bought a one-time pass THIS purchase month, extend it
        existing = None
        try:
            existing = await subs_access.find_one({
                "guild_id": cfg.guild_id,
                "user_id": int(user_id),
                "month": pass_mk,
                "kind": "kofi-one-time",
            })
        except Exception:
            existing = None

        if existing and isinstance(existing.get("expires_at"), datetime):
            prev_exp = existing["expires_at"]
            if prev_exp.tzinfo is None:
                prev_exp = prev_exp.replace(tzinfo=timezone.utc)
            base = prev_exp if prev_exp > starts_at_utc else starts_at_utc
            expires_at_utc = base + timedelta(days=max(1, int(cfg.kofi_one_time_days or 30)))

        source = "kofi-one-time"

        # Dedup by txn_id (store raw event for auditing)
        try:
            await subs_kofi_events.insert_one(
                {
                    "_id": f"{cfg.guild_id}:{txn_id}",
                    "txn_id": txn_id,
                    "guild_id": cfg.guild_id,
                    "user_id": int(user_id),
                    "source": source,
                    "purchase_month": purchase_mk,
                    "effective_month": effective_mk,
                    "starts_at": starts_at_utc,
                    "expires_at": expires_at_utc,
                    "amount": payload.get("amount"),
                    "currency": payload.get("currency"),
                    "created_at": datetime.now(timezone.utc),
                }
            )
        except DuplicateKeyError:
            try:
                await message.delete()
            except Exception:
                pass
            return

        # Store / extend rolling access pass
        await subs_access.update_one(
            {"guild_id": cfg.guild_id, "user_id": int(user_id), "month": pass_mk, "kind": "kofi-one-time"},
            {
                "$setOnInsert": {
                    "guild_id": cfg.guild_id,
                    "user_id": int(user_id),
                    "month": pass_mk,  # key: pass:YYYY-MM
                    "purchase_month": purchase_mk,
                    "effective_month": effective_mk,
                    "kind": "kofi-one-time",
                    "created_at": datetime.now(timezone.utc),
                    "starts_at": starts_at_utc,
                },
                "$set": {
                    "updated_at": datetime.now(timezone.utc),
                    "expires_at": expires_at_utc,
                    "last_txn_id": txn_id,
                    "last_source": source,
                    "last_amount": payload.get("amount"),
                    "last_currency": payload.get("currency"),
                },
                "$addToSet": {"sources": source, "txn_ids": txn_id},
            },
            upsert=True,
        )

        # Give ECL now (access is time-based)
        await self._grant_ecl(user_id, reason=f"Ko-fi one-time pass (expires {expires_at_utc.astimezone(LISBON_TZ).strftime('%Y-%m-%d')})")
        await self._log(f"[subs] ✅ Ko-fi one-time processed: user_id={user_id} purchase_month={purchase_mk} expires={expires_at_utc.astimezone(LISBON_TZ).strftime('%Y-%m-%d %H:%M')} txn={txn_id}")

        try:
            await message.delete()
        except Exception:
            pass

    # -------------------- Patreon + role-based free entry --------------------

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        cfg = self.cfg
        if cfg.guild_id and after.guild.id != cfg.guild_id:
            return
        if after.bot:
            return

        before_ids = {r.id for r in before.roles}
        after_ids = {r.id for r in after.roles}

        watched = cfg.patreon_role_ids | cfg.kofi_role_ids | cfg.free_entry_role_ids
        if not watched:
            return

        if before_ids.intersection(watched) == after_ids.intersection(watched):
            return

        now_lisbon = datetime.now(LISBON_TZ)
        mk = month_key(now_lisbon)
        ok, _ = await self._eligibility(after, mk, at=now_lisbon)

        if ok:
            await self._grant_ecl(after.id, reason="Eligibility role gained")
        else:
            # Safety: don't revoke before enforcement start (pre-season).
            if self._enforcement_active(now_lisbon):
                await self._revoke_ecl_member(after, reason="Eligibility role lost", dm=True)
            else:
                await self._log(f"[subs] (pre-enforcement) would revoke ECL for user_id={after.id} (role lost)")

    # -------------------- Admin commands --------------------
    
    @commands.slash_command(
        name="subtesttop16reminder",
        description="DM yourself a preview of ALL Top16-online reminder embeds that would be sent (choose 5/3/1 days).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def subtesttop16reminder(
        self,
        ctx: discord.ApplicationContext,
        days: int = 5,               # 5 | 3 | 1  (1 = last day)
        mk: Optional[str] = None,    # YYYY-MM (defaults to current month)
    ):
        if not ctx.user.guild_permissions.manage_roles:
            await ctx.respond("You need **Manage Roles**.", ephemeral=True)
            return
        if ctx.guild is None:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        try:
            days = int(days)
        except Exception:
            days = 5
        if days not in (5, 3, 1):
            days = 5

        kind = "5d" if days == 5 else ("3d" if days == 3 else "last")

        mk = (mk or month_key(datetime.now(LISBON_TZ))).strip()
        if not re.match(r"^20\d{2}-(0[1-9]|1[0-2])$", mk):
            await ctx.respond("mk must be **YYYY-MM**.", ephemeral=True)
            return

        # compute targets
        try:
            entries, misses = await self._top16_unqualified_for_month(ctx.guild, mk=mk)
        except Exception as e:
            await ctx.respond(f"Failed to compute targets: {type(e).__name__}: {e}", ephemeral=True)
            return

        if misses:
            print(f"[subs] Top16-online TEST mapping misses ({mk} {kind}): {misses[:20]}")
            await self._log(f"[subs] Top16-online TEST mapping misses ({mk} {kind}): " + ", ".join(misses[:20]))

        if not entries:
            print(f"[subs] Top16-online TEST ({mk} {kind}): 0 targets")
            await self._log(f"[subs] Top16-online TEST ({mk} {kind}): 0 targets")
            await ctx.respond(f"✅ No targets for mk={mk}, days={days}.", ephemeral=True)
            return

        # build embeds for ALL targets (so preview matches real sends)
        embeds: list[discord.Embed] = []
        recipient_lines: list[str] = []

        need_total = int(self.cfg.top16_min_online_games)

        for e in entries:
            try:
                did = int(e["discord_id"])
                rank = int(e["rank"])
                og = int(e["online_games"])
            except Exception:
                continue

            row = e.get("row")
            name = str(getattr(row, "name", "") or "Player")

            # Try to resolve member for nicer logging + mention
            member = None
            try:
                member = ctx.guild.get_member(did) or await ctx.guild.fetch_member(did)
            except Exception:
                member = None

            mention = member.mention if member else f"<@{did}>"
            recip_label = f"{mention} ({member.display_name})" if member else mention
            recipient_lines.append(f"#{rank:02d} {name} -> {recip_label} | online={og}/{need_total}")

            emb = await self._build_top16_online_reminder_embed(
                kind=kind,
                mk=mk,
                rank=rank,
                name=name,
                online_games=og,
                need_total=need_total,
                mention=mention,  # ✅ "Hey @discordname" in the embed (preview + real)
            )
            embeds.append(emb)

        # log/print recipients
        print(f"[subs] Top16-online TEST ({mk} {kind}) targets={len(embeds)}:\n  " + "\n  ".join(recipient_lines))
        await self._log(f"[subs] Top16-online TEST ({mk} {kind}) targets={len(embeds)}:\n" + "\n".join(recipient_lines))

        # DM yourself previews (batch up to 10 embeds per message due to Discord limits)
        try:
            chunk_size = 10
            for i in range(0, len(embeds), chunk_size):
                await ctx.user.send(embeds=embeds[i : i + chunk_size])
        except Exception:
            await ctx.respond("❌ Couldn’t DM you (privacy settings).", ephemeral=True)
            return

        # ephemeral summary
        preview_text = "\n".join(recipient_lines[:30])
        if len(recipient_lines) > 30:
            preview_text += f"\n... (+{len(recipient_lines) - 30} more)"

        await ctx.respond(
            f"✅ Sent you **{len(embeds)}** preview embed(s).\n"
            f"- mk: `{mk}`\n"
            f"- days: `{days}` (kind=`{kind}`)\n\n"
            f"```{preview_text}```",
            ephemeral=True,
        )



    @commands.slash_command(
        name="subfreeadd",
        description="Add free entry for a user for a given month (YYYY-MM).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def subfreeadd(
        self,
        ctx: discord.ApplicationContext,
        member: discord.Member,
        month: str,
        reason: str = "free-entry",
    ):
        if not ctx.user.guild_permissions.manage_roles:
            await ctx.respond("You need **Manage Roles**.", ephemeral=True)
            return
        if not re.match(r"^20\d{2}-(0[1-9]|1[0-2])$", month):
            await ctx.respond("Month must be **YYYY-MM**.", ephemeral=True)
            return

        await subs_free_entries.update_one(
            {"guild_id": ctx.guild.id, "user_id": member.id, "month": month},
            {
                "$setOnInsert": {
                    "guild_id": ctx.guild.id,
                    "user_id": member.id,
                    "month": month,
                    "created_at": datetime.now(timezone.utc),
                },
                "$set": {"reason": reason, "updated_at": datetime.now(timezone.utc)},
            },
            upsert=True,
        )

        await self._grant_ecl(member.id, reason=f"Free entry ({month})")
        await ctx.respond(f"✅ Added free entry for {member.mention} for **{month}**.", ephemeral=True)

    @commands.slash_command(
        name="substatus",
        description="Check if a user is eligible for a given month (YYYY-MM).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def substatus(
        self,
        ctx: discord.ApplicationContext,
        member: discord.Member,
        month: Optional[str] = None,
    ):
        mk = month or month_key(datetime.now(LISBON_TZ))
        ok, why = await self._eligibility(member, mk, at=datetime.now(LISBON_TZ))
        await ctx.respond(
            f"**{member.display_name}** for **{mk}**: {'✅ eligible' if ok else '❌ not eligible'}\n{why}",
            ephemeral=True,
        )

    @commands.slash_command(
        name="subtestdm",
        description="DM yourself a preview of ALL subscription-related embeds (one per message).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def subtestdm(self, ctx: discord.ApplicationContext):
        if not ctx.user.guild_permissions.manage_roles:
            await ctx.respond("You need **Manage Roles**.", ephemeral=True)
            return
        if ctx.guild is None:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        # ✅ ACK immediately so the interaction doesn't expire
        await ctx.defer(ephemeral=True)

        cfg = self.cfg
        now = datetime.now(LISBON_TZ)

        # "first option" choices:
        kind_sub = "3d"     # subscription reminder
        kind_top16 = "5d"   # top16-online reminder
        kind_prize = "5d"   # prize reminder

        target_month = add_months(month_key(now), 1)
        current_mk = month_key(now)

        view = self._build_links_view()

        embeds_to_send: list[tuple[str, discord.Embed, Optional[discord.ui.View]]] = []

        # 1) Subscription reminder (3d)
        try:
            registered_count = await self._count_registered_for_month(ctx.guild, target_month)
        except Exception:
            registered_count = 0

        emb_sub = await self._build_reminder_embed(
            kind=kind_sub,
            target_month=target_month,
            registered_count=registered_count,
        )
        embeds_to_send.append(("1/5 • Subscription reminder (3d)", emb_sub, view))

        # 2) Top16 online-games reminder (5d) (placeholder)
        emb_top16 = await self._build_top16_online_reminder_embed(
            kind=kind_top16,
            mk=current_mk,
            rank=1,
            name=str(getattr(ctx.user, "display_name", "") or "Player"),
            online_games=max(0, int(cfg.top16_min_online_games) - 2),
            need_total=int(cfg.top16_min_online_games),
            mention=ctx.user.mention,
        )
        embeds_to_send.append(("2/5 • Top16 online-games reminder (5d)", emb_top16, None))

        # 3) Prize eligibility reminder (5d) (placeholder)
        emb_prize = await self._build_topcut_prize_reminder_embed(
            kind=kind_prize,
            mk=current_mk,
            rank=12,
            pts=1350,
            cutoff_pts=1500,
            mention=ctx.user.mention,
        )
        embeds_to_send.append(("3/5 • Prize eligibility reminder (5d)", emb_prize, view))

        # 4) Access removed notice preview
        emb_removed = discord.Embed(
            title="⚠️ ECL access removed",
            description=(
                "Looks like your subscription/eligibility role is no longer active, so your **ECL** access was removed.\n\n"
                "The league is still running — you can rejoin anytime by subscribing again."
            ),
            color=cfg.embed_color if isinstance(cfg.embed_color, int) else 0x2ECC71,
        )
        emb_removed.add_field(
            name="Need help?",
            value="If you believe this is a mistake, please open a ticket and an admin will help you.",
            inline=False,
        )
        if cfg.embed_thumbnail_url and cfg.embed_thumbnail_url.startswith(("http://", "https://")):
            emb_removed.set_thumbnail(url=cfg.embed_thumbnail_url)
        embeds_to_send.append(("4/5 • Access removed notice", emb_removed, view))

        # 5) Free-entry role notice preview (placeholder)
        nice_month = month_label(current_mk)
        roles_txt = "Judge, Arena Vanguard"
        emb_free = discord.Embed(
            title=f"✅ Free entry — {nice_month}",
            description=(
                f"You have **free entry** for **{nice_month}** because you have: **{roles_txt}**.\n\n"
                "If you lose that role, your free entry goes away."
            ),
            color=cfg.embed_color if isinstance(cfg.embed_color, int) else 0x2ECC71,
        )
        emb_free.set_footer(text="ECL • Free entry notice")
        if cfg.embed_thumbnail_url and cfg.embed_thumbnail_url.startswith(("http://", "https://")):
            emb_free.set_thumbnail(url=cfg.embed_thumbnail_url)
        embeds_to_send.append(("5/5 • Free-entry role notice", emb_free, None))
        
        emb_flip = self._build_flip_mods_embed(ctx.guild, current_mk)
        embeds_to_send.append(("6/6 • Month flip mods summary", emb_flip, None))

        # Send 1 DM per embed
        try:
            for header, emb, vw in embeds_to_send:
                await ctx.user.send(content=header, embed=emb, view=vw)
                await asyncio.sleep(0.25)
        except Exception:
            await ctx.followup.send("❌ Couldn’t DM you (privacy settings).", ephemeral=True)
            return

        await ctx.followup.send("✅ Sent you all embed previews (one per DM).", ephemeral=True)


    @commands.slash_command(
        name="subremindnow",
        description="Run reminder logic now (dry-run by default).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def subremindnow(
        self,
        ctx: discord.ApplicationContext,
        kind: str = "3d",  # "3d" or "last"
        target_month: Optional[str] = None,
        dry_run: bool = True,
        limit: int = 5,
    ):
        if not ctx.user.guild_permissions.manage_roles:
            await ctx.respond("You need **Manage Roles**.", ephemeral=True)
            return

        limit = max(1, min(50, int(limit or 5)))

        if target_month is None:
            now = datetime.now(LISBON_TZ)
            target_month = add_months(month_key(now), 1)
        else:
            if not re.match(r"^20\d{2}-(0[1-9]|1[0-2])$", target_month):
                await ctx.respond("target_month must be **YYYY-MM**.", ephemeral=True)
                return

        flip_at = month_bounds(target_month)[0]

        guild = ctx.guild
        cfg = self.cfg
        role = guild.get_role(cfg.ecl_role_id) if cfg.ecl_role_id else None
        if not role:
            await ctx.respond("❌ ECL role not found / not configured.", ephemeral=True)
            return

        members = list(role.members)
        if len(members) < 50:
            members = [m async for m in guild.fetch_members(limit=None)]
            members = [m for m in members if role in m.roles]

        flip_at = month_bounds(target_month)[0]

        to_dm: list[discord.Member] = []
        for m in members:
            if m.bot:
                continue
            ok, _ = await self._eligibility(m, target_month, at=flip_at)
            if not ok:
                to_dm.append(m)

        preview = ", ".join([m.mention for m in to_dm[:10]]) or "(none)"
        await ctx.respond(
            f"**Reminder `{kind}` for `{target_month}`**\n"
            f"- not eligible (would DM): **{len(to_dm)}**\n"
            f"- sample: {preview}\n"
            f"- dry_run: `{dry_run}`\n"
            f"- limit: `{limit}`",
            ephemeral=True,
        )

        count = await self._count_registered_for_month(guild, target_month)
        emb = await self._build_reminder_embed(
            kind=("last" if kind == "last" else "3d"),
            target_month=target_month,
            registered_count=count,
        )

        if dry_run:
            try:
                await ctx.user.send(embed=emb, view=self._build_links_view())
            except Exception:
                pass
            return

        sent = 0
        sem = asyncio.Semaphore(cfg.dm_concurrency)

        async def _send(member: discord.Member):
            nonlocal sent
            async with sem:
                try:
                    await member.send(embed=emb, view=self._build_links_view())
                    sent += 1
                except Exception:
                    pass
                if cfg.dm_sleep_seconds:
                    await asyncio.sleep(cfg.dm_sleep_seconds)

        await asyncio.gather(*[_send(m) for m in to_dm[:limit]])
        await self._log(f"[subs] subremindnow sent {sent}/{min(limit, len(to_dm))} for {target_month} kind={kind}")

    # -------------------- Scheduler --------------------

    @tasks.loop(minutes=5)
    async def _tick(self):
        await self.bot.wait_until_ready()
        cfg = self.cfg
        if not cfg.guild_id:
            return
        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return

        now = datetime.now(LISBON_TZ)
        last_day = last_day_of_month(now)
        last_day_date = last_day.date()

        # regular "register for next month" reminders
        target_month = add_months(month_key(now), 1)  # next month
        d3 = (last_day - timedelta(days=3)).date()

        if cfg.dm_enabled and now.hour == 10 and now.minute < 5:
            if now.date() == d3:
                await self._run_reminder_job(guild, target_month, kind="3d")
            elif now.date() == last_day_date:
                await self._run_reminder_job(guild, target_month, kind="last")

        # Run the month flip cleanup shortly AFTER midnight on the 1st (avoid early removals).
        if now.day == 1 and now.hour < 6 and now.minute < 5:
            await self._run_flip_mods_reminder_job(guild, mk=month_key(now))
            await self._run_free_role_flip_info_job(guild, mk=month_key(now))
            if self._enforcement_active(now):
                await self._run_cleanup_job(guild, month_key(now))
            else:
                await self._log("[subs] cleanup skipped: enforcement not active yet")

        # --- Top16 online-games reminders for CURRENT month ---
        # We DM players who are currently in TopDeck Top16 but don't meet online-games requirement.
        if cfg.dm_enabled and now.hour == 10 and now.minute < 5:
            mk = month_key(now)  # current month
            d5 = (last_day - timedelta(days=5)).date()
            d1 = (last_day - timedelta(days=1)).date()

            if now.date() == d5:
                await self._run_top16_online_reminder_job(guild, mk=mk, kind="5d")
                await self._run_topcut_prize_reminder_job(guild, mk=mk, kind="5d")
            elif now.date() == d3:
                await self._run_top16_online_reminder_job(guild, mk=mk, kind="3d")
            elif now.date() == d1:
                await self._run_topcut_prize_reminder_job(guild, mk=mk, kind="1d")
            elif now.date() == last_day_date:
                await self._run_top16_online_reminder_job(guild, mk=mk, kind="last")

    @_tick.before_loop
    async def _before_tick(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)

    @tasks.loop(hours=8)
    async def _access_audit(self):
        """Periodic audit: if someone has ECL but no longer has eligibility sources, remove ECL."""
        await self.bot.wait_until_ready()
        cfg = self.cfg
        if not cfg.guild_id:
            return
        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return

        now_lisbon = datetime.now(LISBON_TZ)
        if not self._enforcement_active(now_lisbon):
            return

        async with self._access_audit_lock:
            await self._run_access_audit(guild)

    @_access_audit.before_loop
    async def _before_access_audit(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(10)

    async def _run_access_audit(self, guild: discord.Guild) -> None:
        cfg = self.cfg
        if not cfg.ecl_role_id:
            return

        role = guild.get_role(cfg.ecl_role_id)
        if not role:
            return

        now_lisbon = datetime.now(LISBON_TZ)
        if not self._enforcement_active(now_lisbon):
            return

        mk = month_key(now_lisbon)

        members = list(role.members)
        if len(members) < 50:
            try:
                members = [m async for m in guild.fetch_members(limit=None)]
                members = [m for m in members if (role in m.roles)]
            except Exception:
                members = list(role.members)

        removed = 0
        checked = 0

        for m in members:
            if m.bot:
                continue
            checked += 1
            ok, _ = await self._eligibility(m, mk, at=datetime.now(LISBON_TZ))
            if ok:
                continue
            did = await self._revoke_ecl_member(m, reason=f"Audit: not eligible for {mk}", dm=True)
            if did:
                removed += 1

            if cfg.dm_sleep_seconds:
                await asyncio.sleep(cfg.dm_sleep_seconds)

        await self._log(f"[subs] access-audit {mk}: checked={checked} removed={removed}")
        print(f"[subs] access-audit {mk}: checked={checked} removed={removed}")


    # -------------------- Core operations --------------------

    def _has_any_role_id(self, member: discord.Member, role_ids: Set[int]) -> bool:
        if not role_ids:
            return False
        have = {r.id for r in member.roles}
        return bool(have.intersection(role_ids))

    async def _has_db_access(self, user_id: int, month: str, *, at: Optional[datetime] = None) -> bool:
        cfg = self.cfg
        # Legacy calendar-month entitlement (docs without kind=kofi-one-time)
        doc_month = await subs_access.find_one({
            "guild_id": cfg.guild_id,
            "user_id": int(user_id),
            "month": month,
            "kind": {"$ne": "kofi-one-time"},
        })
        if doc_month:
            return True

        # Rolling one-time pass (active at a specific moment)
        if at is None:
            at_utc = datetime.now(timezone.utc)
        else:
            at_utc = at
            if at_utc.tzinfo is None:
                at_utc = at_utc.replace(tzinfo=LISBON_TZ)
            at_utc = at_utc.astimezone(timezone.utc)

        doc_pass = await subs_access.find_one({
            "guild_id": cfg.guild_id,
            "user_id": int(user_id),
            "kind": "kofi-one-time",
            "starts_at": {"$lte": at_utc},
            "expires_at": {"$gt": at_utc},
        })
        return bool(doc_pass)

    async def _has_free_entry(self, user_id: int, month: str) -> bool:
        doc = await subs_free_entries.find_one({"guild_id": self.cfg.guild_id, "user_id": int(user_id), "month": month})
        return bool(doc)

    def _norm_name(self, s: str) -> str:
        s = (s or "").strip()
        if s.startswith("@"):
            s = s[1:]
        return "".join(ch for ch in s.lower() if ch.isalnum())

    def _extract_discord_id_from_text(self, text: str) -> Optional[int]:
        if not text:
            return None
        t = text.strip()
        m = re.search(r"<@!?(\d{15,25})>", t)
        if m:
            return int(m.group(1))
        m2 = re.search(r"\b(\d{15,25})\b", t)
        if m2:
            return int(m2.group(1))
        return None

    async def _build_member_index(self, guild: discord.Guild) -> tuple[dict[str, int], dict[int, discord.Member]]:
        """Fetch members once and build:
          - norm_name -> discord_id
          - discord_id -> Member
        """
        members = []
        try:
            members = [m async for m in guild.fetch_members(limit=None)]
        except Exception:
            members = []

        name_to_id: dict[str, int] = {}
        id_to_member: dict[int, discord.Member] = {}

        for m in members:
            if not isinstance(m, discord.Member):
                continue
            id_to_member[int(m.id)] = m
            if m.bot:
                continue

            for cand in (m.name, getattr(m, "global_name", None), getattr(m, "display_name", None)):
                if isinstance(cand, str) and cand.strip():
                    name_to_id.setdefault(self._norm_name(cand), int(m.id))

            discrim = getattr(m, "discriminator", None)
            if discrim and discrim != "0":
                name_to_id.setdefault(self._norm_name(f"{m.name}#{discrim}"), int(m.id))

        return name_to_id, id_to_member

    def _discord_id_for_row(self, row: PlayerRow, name_to_id: dict[str, int]) -> Optional[int]:
        """Resolve a TopDeck row to a Discord user id using row.discord then row.name."""
        did = self._extract_discord_id_from_text(getattr(row, "discord", "") or "")
        if did:
            return did

        disc = getattr(row, "discord", None)
        if isinstance(disc, str) and disc.strip():
            key = self._norm_name(disc)
            if key in name_to_id:
                return int(name_to_id[key])

        nm = getattr(row, "name", None)
        if isinstance(nm, str) and nm.strip():
            key = self._norm_name(nm)
            if key in name_to_id:
                return int(name_to_id[key])

        return None


    async def _qualified_top16_discord_ids_for_month(
        self,
        guild: discord.Guild,
        cut_month: str
    ) -> tuple[list[int], list[str]]:
        cfg = self.cfg

        bracket_id = (os.getenv("TOPDECK_BRACKET_ID") or "").strip()
        firebase_token = os.getenv("FIREBASE_ID_TOKEN", None)
        if not bracket_id:
            return ([], ["TOPDECK_BRACKET_ID not set"])

        try:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=True)
        except Exception:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=False)

        if not rows:
            return ([], ["no TopDeck rows"])

        try:
            y, m = cut_month.split("-")
            year, month = int(y), int(m)
        except Exception:
            return ([], [f"bad cut_month: {cut_month!r}"])

        try:
            online_counts = await count_online_games_by_topdeck_uid_str(
                bracket_id, year, month, online_only=True
            )
        except Exception as e:
            return ([], [f"online_counts error: {type(e).__name__}: {e}"])

        active_by_games = [r for r in rows if (not r.dropped) and (r.games >= cfg.top16_min_total_games)]
        active_by_games = sorted(active_by_games, key=lambda r: (-r.pts, -r.games))

        qualified_candidates: list[PlayerRow] = []
        for r in active_by_games:
            uid = (r.uid or "").strip()
            if not uid:
                continue
            if online_counts.get(uid, 0) >= cfg.top16_min_online_games:
                qualified_candidates.append(r)

        qualified_top16 = qualified_candidates[:16]
        if not qualified_top16:
            return ([], ["no qualified top16"])

        members = [m async for m in guild.fetch_members(limit=None)]
        name_to_id: dict[str, int] = {}

        for m in members:
            if m.bot:
                continue
            for cand in (m.name, getattr(m, "global_name", None), getattr(m, "display_name", None)):
                if isinstance(cand, str) and cand.strip():
                    name_to_id.setdefault(self._norm_name(cand), int(m.id))

            discrim = getattr(m, "discriminator", None)
            if discrim and discrim != "0":
                name_to_id.setdefault(self._norm_name(f"{m.name}#{discrim}"), int(m.id))

        discord_ids: list[int] = []
        missing: list[str] = []

        for r in qualified_top16:
            did = self._extract_discord_id_from_text(r.discord or "")
            if did:
                discord_ids.append(did)
                continue

            if r.discord:
                key = self._norm_name(r.discord)
                if key in name_to_id:
                    discord_ids.append(name_to_id[key])
                    continue

            if r.name:
                key = self._norm_name(r.name)
                if key in name_to_id:
                    discord_ids.append(name_to_id[key])
                    continue

            missing.append(r.discord or r.name or "unknown")

        seen = set()
        discord_ids = [x for x in discord_ids if not (x in seen or seen.add(x))]

        return (discord_ids, missing)

    async def _eligible_top16_discord_ids_for_month(
        self,
        guild: discord.Guild,
        cut_month: str,
    ) -> tuple[list[int], list[str]]:
        """Top16 cut (prize eligible): skip ineligible players and promote next eligible."""
        cfg = self.cfg

        bracket_id = (os.getenv("TOPDECK_BRACKET_ID") or "").strip()
        firebase_token = os.getenv("FIREBASE_ID_TOKEN", None)
        if not bracket_id:
            return ([], ["TOPDECK_BRACKET_ID not set"])

        try:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=True)
        except Exception:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=False)

        if not rows:
            return ([], ["no TopDeck rows"])

        try:
            y, m = cut_month.split("-")
            year, month = int(y), int(m)
        except Exception:
            return ([], [f"bad cut_month: {cut_month!r}"])

        try:
            online_counts = await count_online_games_by_topdeck_uid_str(
                bracket_id, year, month, online_only=True
            )
        except Exception as e:
            return ([], [f"online_counts error: {type(e).__name__}: {e}"])

        active_by_games = [r for r in rows if (not r.dropped) and (r.games >= cfg.top16_min_total_games)]
        active_by_games = sorted(active_by_games, key=lambda r: (-r.pts, -r.games))

        qualified: list[PlayerRow] = []
        for r in active_by_games:
            uid = (r.uid or "").strip()
            if not uid:
                continue
            if online_counts.get(uid, 0) >= cfg.top16_min_online_games:
                qualified.append(r)

        if not qualified:
            return ([], ["no qualified candidates"])

        name_to_id, id_to_member = await self._build_member_index(guild)

        eligible_ids: list[int] = []
        missing: list[str] = []

        for r in qualified:
            did = self._discord_id_for_row(r, name_to_id)
            if not did:
                missing.append(r.discord or r.name or "unknown")
                continue

            member = id_to_member.get(int(did)) or guild.get_member(int(did))
            if member is None:
                try:
                    member = await guild.fetch_member(int(did))
                except Exception:
                    member = None

            if not member or member.bot:
                continue

            ok, _ = await self._eligibility(member, cut_month, at=month_end_inclusive(cut_month))
            if not ok:
                continue

            eligible_ids.append(int(did))
            if len(eligible_ids) >= 16:
                break

        seen = set()
        eligible_ids = [x for x in eligible_ids if not (x in seen or seen.add(x))]

        return (eligible_ids, missing)

    async def _topcut_prize_reminder_targets(
        self,
        guild: discord.Guild,
        *,
        mk: str,
    ) -> tuple[list[dict], list[str], int]:
        """Targets for 'prize eligibility' reminder."""
        cfg = self.cfg

        bracket_id = (os.getenv("TOPDECK_BRACKET_ID") or "").strip()
        firebase_token = os.getenv("FIREBASE_ID_TOKEN", None)
        if not bracket_id:
            return ([], ["TOPDECK_BRACKET_ID not set"], 0)

        try:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=True)
        except Exception:
            rows, _ = await get_league_rows_cached(bracket_id, firebase_token, force_refresh=False)

        if not rows:
            return ([], ["no TopDeck rows"], 0)

        try:
            y, m = mk.split("-")
            year, month = int(y), int(m)
        except Exception:
            return ([], [f"bad mk: {mk!r}"], 0)

        try:
            online_counts = await count_online_games_by_topdeck_uid_str(
                bracket_id, year, month, online_only=True
            )
        except Exception as e:
            return ([], [f"online_counts error: {type(e).__name__}: {e}"], 0)

        active_by_games = [r for r in rows if (not r.dropped) and (r.games >= cfg.top16_min_total_games)]
        active_by_games = sorted(active_by_games, key=lambda r: (-r.pts, -r.games))

        qualified: list[tuple[int, PlayerRow]] = []
        for idx, r in enumerate(active_by_games, start=1):
            uid = (r.uid or "").strip()
            if not uid:
                continue
            if online_counts.get(uid, 0) >= cfg.top16_min_online_games:
                qualified.append((idx, r))

        if not qualified:
            return ([], ["no qualified candidates"], 0)

        name_to_id, id_to_member = await self._build_member_index(guild)

        checked: list[dict] = []
        misses: list[str] = []

        eligible_count = 0
        cutoff_pts: Optional[int] = None
        margin = int(getattr(cfg, "topcut_close_pts", 250) or 250)

        for rank, r in qualified:
            did = self._discord_id_for_row(r, name_to_id)
            if not did:
                misses.append(r.discord or r.name or "unknown")
                continue

            member = id_to_member.get(int(did)) or guild.get_member(int(did))
            if member is None:
                try:
                    member = await guild.fetch_member(int(did))
                except Exception:
                    member = None

            if not member or member.bot:
                continue

            ok, _ = await self._eligibility(member, mk, at=month_end_inclusive(mk))

            pts_int = int(round(float(getattr(r, "pts", 0) or 0)))
            checked.append({
                "rank": int(rank),
                "discord_id": int(did),
                "name": str(getattr(r, "name", "") or ""),
                "pts": pts_int,
                "eligible": bool(ok),
            })

            if ok:
                eligible_count += 1
                if eligible_count == 16:
                    cutoff_pts = pts_int

            if cutoff_pts is not None and pts_int < (cutoff_pts - margin):
                break

        if cutoff_pts is None:
            cutoff_pts = int(checked[min(15, len(checked)-1)]["pts"]) if checked else 0

        min_pts = cutoff_pts - margin
        targets = [e for e in checked if (not e["eligible"]) and (e["pts"] >= min_pts)]

        seen: set[int] = set()
        out: list[dict] = []
        for e in targets:
            did = int(e["discord_id"])
            if did in seen:
                continue
            seen.add(did)
            out.append(e)

        return (out, misses, int(cutoff_pts))

    async def _build_topcut_prize_reminder_embed(
        self,
        *,
        kind: str,          # "5d" | "1d"
        mk: str,
        rank: int,
        pts: int,
        cutoff_pts: int,
        mention: str,
    ) -> discord.Embed:
        cfg = self.cfg
        kind = (kind or "").strip().lower()
        if kind not in ("5d", "1d"):
            kind = "5d"

        nice_month = month_label(mk)
        margin = int(getattr(cfg, "topcut_close_pts", 250) or 250)

        if kind == "1d":
            title = "⏳ 1 day left — prize eligibility reminder"
            urgency = "Only **1 day** left in the league."
        else:
            title = "👀 5 days left — prize eligibility reminder"
            urgency = "Only **5 days** left in the league."

        desc = (
            f"Hey {mention} 👋\n\n"
            f"{urgency}\n\n"
            f"You're currently **#{rank:02d}** on TopDeck for **{nice_month}** with **{pts}** points.\n"
            f"You're within **{margin}** points of the current eligible Top16 cutoff (~**{cutoff_pts}** pts).\n\n"
            "**Important:** only players who are **registered / subscribed at the end of the month** are eligible "
            "for **Top16 / prizes**.\n\n"
            "If you want to stay eligible, make sure your subscription / registration is active before the league ends."
        )

        emb = discord.Embed(
            title=title,
            description=desc,
            color=cfg.embed_color if isinstance(cfg.embed_color, int) else 0x2ECC71,
        )
        emb.set_footer(text="ECL • Prize eligibility reminder")

        if cfg.embed_thumbnail_url and cfg.embed_thumbnail_url.startswith(("http://", "https://")):
            emb.set_thumbnail(url=cfg.embed_thumbnail_url)

        return emb

    async def _run_topcut_prize_reminder_job(self, guild: discord.Guild, *, mk: str, kind: str) -> None:
        job_id = f"topcut-prize-remind:{guild.id}:{mk}:{kind}"
        if await subs_jobs.find_one({"_id": job_id}):
            return
        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        cfg = self.cfg
        targets, misses, cutoff_pts = await self._topcut_prize_reminder_targets(guild, mk=mk)

        if misses:
            await self._log(f"[subs] Topcut-prize mapping misses ({mk} {kind}): " + ", ".join(misses[:20]))

        if not targets:
            await self._log(f"[subs] Topcut-prize reminder ({mk} {kind}): 0 targets")
            print(f"[subs] Topcut-prize reminder ({mk} {kind}): 0 targets")
            await self._dm_mods_summary(
                guild,
                summary=f"[ECL] Topcut-prize reminder ({mk} {kind}) — sent 0 DMs (0 targets).",
            )
            return

        sem = asyncio.Semaphore(cfg.dm_concurrency)
        sent = 0

        async def _send_one(entry: dict):
            nonlocal sent
            async with sem:
                uid = int(entry["discord_id"])
                try:
                    member = guild.get_member(uid) or await guild.fetch_member(uid)
                except Exception:
                    return
                if not member or member.bot:
                    return

                try:
                    emb = await self._build_topcut_prize_reminder_embed(
                        kind=kind,
                        mk=mk,
                        rank=int(entry["rank"]),
                        pts=int(entry["pts"]),
                        cutoff_pts=int(cutoff_pts),
                        mention=member.mention,
                    )
                    await member.send(embed=emb, view=self._build_links_view())
                    sent += 1
                except Exception:
                    return

                if cfg.dm_sleep_seconds:
                    await asyncio.sleep(cfg.dm_sleep_seconds)

        await asyncio.gather(*[_send_one(e) for e in targets])
        await self._log(f"[subs] ✅ Topcut-prize reminder ({mk} {kind}) sent {sent}/{len(targets)}")
        print(f"[subs] ✅ Topcut-prize reminder ({mk} {kind}) sent {sent}/{len(targets)}")
        await self._dm_mods_summary(
            guild,
            summary=f"[ECL] Topcut-prize reminder ({mk} {kind}) — sent {sent}/{len(targets)} DMs.",
        )


    async def _eligibility(self, member: discord.Member, month: str, *, at: Optional[datetime] = None) -> Tuple[bool, str]:
        cfg = self.cfg
        if self._has_any_role_id(member, cfg.patreon_role_ids):
            return True, "Reason: Patreon role"
        if self._has_any_role_id(member, cfg.kofi_role_ids):
            return True, "Reason: Ko-fi role"
        if self._has_any_role_id(member, cfg.free_entry_role_ids):
            return True, "Reason: Free-entry role"
        if await self._has_free_entry(member.id, month):
            return True, "Reason: Free-entry list"
        if await self._has_db_access(member.id, month, at=at):
            return True, "Reason: Ko-fi entitlement"
        return False, "Reason: none"

    async def _grant_ecl(self, user_id: int, reason: str):
        cfg = self.cfg
        if not cfg.guild_id or not cfg.ecl_role_id:
            return
        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return
        role = guild.get_role(cfg.ecl_role_id)
        if not role:
            return
        try:
            member = guild.get_member(int(user_id)) or await guild.fetch_member(int(user_id))
        except Exception:
            return
        if member.bot:
            return
        if role in member.roles:
            return
        with contextlib.suppress(Exception):
            await member.add_roles(role, reason=reason)

    async def _revoke_ecl_member(self, member: discord.Member, reason: str, *, dm: bool = False) -> bool:
        """Remove ECL role from a member if present. Returns True if removed."""
        cfg = self.cfg
        if not cfg.guild_id or not cfg.ecl_role_id:
            return False
        if member.guild.id != cfg.guild_id:
            return False

        role = member.guild.get_role(cfg.ecl_role_id)
        if not role:
            return False

        if role not in member.roles:
            return False

        removed = False
        with contextlib.suppress(Exception):
            await member.remove_roles(role, reason=reason)
            removed = role not in member.roles

        if removed and dm:
            await self._dm_access_removed(member)

        return removed

    async def _revoke_ecl(self, user_id: int, reason: str, *, dm: bool = False) -> bool:
        cfg = self.cfg
        if not cfg.guild_id or not cfg.ecl_role_id:
            return False
        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return False

        try:
            member = guild.get_member(int(user_id)) or await guild.fetch_member(int(user_id))
        except Exception:
            return False

        if not member or member.bot:
            return False

        return await self._revoke_ecl_member(member, reason=reason, dm=dm)

    async def _dm_access_removed(self, member: discord.Member) -> None:
        """One-time DM when we remove ECL due to lost eligibility."""
        cfg = self.cfg

        now_lisbon = datetime.now(LISBON_TZ)
        if not self._enforcement_active(now_lisbon):
            return

        mk = month_key(now_lisbon)
        job_id = f"ecl-revoked-dm:{cfg.guild_id}:{int(member.id)}:{mk}"
        with contextlib.suppress(Exception):
            if await subs_jobs.find_one({"_id": job_id}):
                return
            await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        emb = discord.Embed(
            title="⚠️ ECL access removed",
            description=(
                "Looks like your subscription/eligibility role is no longer active, so your **ECL** access was removed.\n\n"
                "The league is still running — you can rejoin anytime by subscribing again."
            ),
            color=cfg.embed_color if isinstance(cfg.embed_color, int) else 0x2ECC71,
        )

        emb.add_field(
            name="Need help?",
            value="If you believe this is a mistake, please open a ticket and an admin will help you.",
            inline=False,
        )

        if cfg.embed_thumbnail_url and cfg.embed_thumbnail_url.startswith(("http://", "https://")):
            emb.set_thumbnail(url=cfg.embed_thumbnail_url)

        view = self._build_links_view()
        with contextlib.suppress(Exception):
            await member.send(embed=emb, view=view)


    async def _grant_top16(self, user_id: int, reason: str):
        cfg = self.cfg
        if not cfg.guild_id or not cfg.top16_role_id:
            return

        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return

        role = guild.get_role(cfg.top16_role_id)
        if not role:
            return

        try:
            member = guild.get_member(int(user_id)) or await guild.fetch_member(int(user_id))
        except Exception:
            return

        if member.bot or role in member.roles:
            return

        with contextlib.suppress(Exception):
            await member.add_roles(role, reason=reason)

    async def _run_reminder_job(self, guild: discord.Guild, target_month: str, kind: str):
        job_id = f"remind:{guild.id}:{target_month}:{kind}"
        if await subs_jobs.find_one({"_id": job_id}):
            return
        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        cfg = self.cfg
        role = guild.get_role(cfg.ecl_role_id) if cfg.ecl_role_id else None
        if not role:
            return

        # Evaluate eligibility at the exact flip moment (start of target_month).
        flip_at = month_bounds(target_month)[0]

        members = list(role.members)
        if len(members) < 50:
            members = [m async for m in guild.fetch_members(limit=None)]
            members = [m for m in members if role in m.roles]

        to_dm: list[discord.Member] = []
        for m in members:
            if m.bot:
                continue
            ok, _ = await self._eligibility(m, target_month, at=flip_at)
            if not ok:
                to_dm.append(m)

        count = await self._count_registered_for_month(guild, target_month)
        emb = await self._build_reminder_embed(kind=kind, target_month=target_month, registered_count=count)

        await self._log(f"[subs] Reminder '{kind}' for {target_month}: {len(to_dm)} users (registered={count})")

        sem = asyncio.Semaphore(cfg.dm_concurrency)
        sent = 0

        async def _send(member: discord.Member):
            nonlocal sent
            async with sem:
                try:
                    await member.send(embed=emb, view=self._build_links_view())
                    sent += 1
                except Exception:
                    pass
                if cfg.dm_sleep_seconds:
                    await asyncio.sleep(cfg.dm_sleep_seconds)

        await asyncio.gather(*[_send(m) for m in to_dm])

        await self._log(f"[subs] ✅ Reminder '{kind}' for {target_month}: sent {sent}/{len(to_dm)}")
        print(f"[subs] ✅ Reminder '{kind}' for {target_month}: sent {sent}/{len(to_dm)}")

        await self._dm_mods_summary(
            guild,
            summary=f"[ECL] Subscription reminder ({target_month} {kind}) — sent {sent}/{len(to_dm)} DMs.",
        )


    async def _apply_top16_cut_for_next_month(self, guild: discord.Guild, *, cut_month: str, target_month: str):
        cfg = self.cfg

        top16_ids, missing = await self._eligible_top16_discord_ids_for_month(guild, cut_month)

        if missing and missing != ["no qualified top16"]:
            await self._log(f"[subs] Top16 mapping misses ({cut_month}): " + ", ".join(missing[:20]))

        if not top16_ids:
            await self._log(f"[subs] Top16 cut ({cut_month}) produced 0 Discord IDs. (Nothing applied)")
            return

        applied = 0
        for uid in top16_ids:
            await subs_free_entries.update_one(
                {"guild_id": cfg.guild_id, "user_id": int(uid), "month": target_month},
                {
                    "$setOnInsert": {
                        "guild_id": cfg.guild_id,
                        "user_id": int(uid),
                        "month": target_month,
                        "created_at": datetime.now(timezone.utc),
                    },
                    "$set": {"reason": f"Top16 ({cut_month})", "updated_at": datetime.now(timezone.utc)},
                },
                upsert=True,
            )

            await self._grant_ecl(uid, reason=f"Top16 free entry ({target_month})")
            await self._grant_top16(uid, reason=f"Top16 qualifier ({cut_month})")
            applied += 1

        await self._log(f"[subs] ✅ Applied Top16 cut: {applied} users -> free entry {target_month} + Top16 role")

    async def _run_cleanup_job(self, guild: discord.Guild, target_month: str):
        job_id = f"cleanup:{guild.id}:{target_month}"
        if await subs_jobs.find_one({"_id": job_id}):
            return
        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        cfg = self.cfg
        role = guild.get_role(cfg.ecl_role_id) if cfg.ecl_role_id else None
        if not role:
            return

        cut_month = add_months(target_month, -1)  # month that just ended
        await self._apply_top16_cut_for_next_month(guild, cut_month=cut_month, target_month=target_month)

        members = list(role.members)
        if len(members) < 50:
            members = [m async for m in guild.fetch_members(limit=None)]
            members = [m for m in members if role in m.roles]

        flip_at = month_bounds(target_month)[0]

        to_remove: list[discord.Member] = []
        for m in members:
            if m.bot:
                continue
            ok, _ = await self._eligibility(m, target_month, at=flip_at)
            if not ok:
                to_remove.append(m)

        await self._log(f"[subs] Cleanup for {target_month}: removing ECL from {len(to_remove)} users")

        for m in to_remove:
            with contextlib.suppress(Exception):
                await m.remove_roles(role, reason=f"Not subscribed/free for {target_month}")

    # -------------------- Flip reminders --------------------
     

    async def _run_flip_mods_reminder_job(self, guild: discord.Guild, *, mk: str) -> None:
        job_id = f"flip-mods:{guild.id}:{mk}"
        if await subs_jobs.find_one({"_id": job_id}):
            return
        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        emb = self._build_flip_mods_embed(guild, mk)
        await self._dm_mods_embed(guild, embed=emb)


    async def _run_free_role_flip_info_job(self, guild: discord.Guild, *, mk: str) -> None:
        """Once-per-month DM to players who have free-entry via specific roles."""
        cfg = self.cfg
        role_ids = set(int(x) for x in (cfg.free_entry_role_ids or set()) if int(x))
        if not role_ids:
            return

        job_id = f"flip-free-role-info:{guild.id}:{mk}"
        if await subs_jobs.find_one({"_id": job_id}):
            return
        await subs_jobs.insert_one({"_id": job_id, "ran_at": datetime.now(timezone.utc)})

        # Build per-user list of role names that grant free entry
        user_roles: dict[int, list[str]] = {}
        for rid in role_ids:
            role = guild.get_role(rid)
            if not role:
                continue
            for m in getattr(role, "members", []) or []:
                if m.bot:
                    continue
                user_roles.setdefault(int(m.id), []).append(role.name)

        if not user_roles:
            return

        nice_month = month_label(mk)
        sem = asyncio.Semaphore(cfg.dm_concurrency)
        sent = 0

        async def _send_one(uid: int, role_names: list[str]):
            nonlocal sent
            async with sem:
                try:
                    member = guild.get_member(uid) or await guild.fetch_member(uid)
                except Exception:
                    return
                if not member or member.bot:
                    return

                # ensure access role is present
                await self._grant_ecl(uid, reason=f"Free entry role(s) ({nice_month})")

                roles_txt = ", ".join(sorted(set(role_names)))
                emb = discord.Embed(
                    title=f"✅ Free entry — {nice_month}",
                    description=(
                        f"You have **free entry** for **{nice_month}** because you have: **{roles_txt}**.\n\n"
                        "If you lose that role, your free entry goes away."
                    ),
                    color=cfg.embed_color if isinstance(cfg.embed_color, int) else 0x2ECC71,
                )
                emb.set_footer(text="ECL • Free entry notice")

                try:
                    await member.send(embed=emb)
                    sent += 1
                except Exception:
                    return

                if cfg.dm_sleep_seconds:
                    await asyncio.sleep(cfg.dm_sleep_seconds)

        await asyncio.gather(*[_send_one(uid, rnames) for uid, rnames in user_roles.items()])
        await self._log(f"[subs] flip free-role info {mk}: sent {sent}/{len(user_roles)}")



    async def _log(self, text: str):
        print(text)
        ch_id = self.cfg.log_channel_id
        if not ch_id:
            return
        guild = self.bot.get_guild(self.cfg.guild_id)
        if not guild:
            return
        ch = guild.get_channel(ch_id)
        if not ch:
            try:
                ch = await guild.fetch_channel(ch_id)
            except Exception:
                return
        with contextlib.suppress(Exception):
            await ch.send(text)


def setup(bot: commands.Bot):
    bot.add_cog(SubscriptionsCog(bot))