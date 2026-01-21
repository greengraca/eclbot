# cogs/subscriptions/embeds.py
"""Embed builders for subscription-related messages."""

from typing import Optional, Set
import discord

from utils.dates import month_label


def _apply_thumbnail(embed: discord.Embed, thumbnail_url: Optional[str]) -> None:
    """Apply thumbnail if URL is valid."""
    if thumbnail_url and thumbnail_url.startswith(("http://", "https://")):
        embed.set_thumbnail(url=thumbnail_url)


def _get_color(embed_color: Optional[int]) -> int:
    """Get embed color with fallback."""
    return embed_color if isinstance(embed_color, int) else 0x2ECC71


def build_reminder_embed(
    *,
    kind: str,
    target_month: str,
    registered_count: int,
    embed_color: Optional[int] = None,
    embed_thumbnail_url: Optional[str] = None,
) -> discord.Embed:
    """Build a subscription reminder embed (3d or last day)."""
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
        "This is just a reminder so you **don't lose access** when the month flips — "
        "and if you do lose it, you can **regain access anytime** by registering.\n\n"
        f"Right now, you're **not registered** for **{nice_month}** — "
        f"and you'll lose **ECL** access when the month flips.\n\n"
        f"👥 **Already registered:** **{registered_count}** players\n"
    )

    emb = discord.Embed(
        title=title,
        description=desc,
        color=_get_color(embed_color),
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
    _apply_thumbnail(emb, embed_thumbnail_url)

    return emb


def build_flip_mods_embed(
    *,
    guild: discord.Guild,
    mk: str,
    current_bracket: str,
    next_bracket: str,
    free_entry_role_ids: Optional[Set[int]] = None,
    embed_color: Optional[int] = None,
    embed_thumbnail_url: Optional[str] = None,
) -> discord.Embed:
    """Build the month-flip checklist embed for mods."""
    current_bracket = current_bracket or "(not set)"
    next_bracket = next_bracket or "(not set)"

    # Build free-entry role names
    role_ids = sorted(int(x) for x in (free_entry_role_ids or set()) if int(x))
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
        color=_get_color(embed_color),
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

    _apply_thumbnail(emb, embed_thumbnail_url)
    emb.set_footer(text="ECL • Mods month flip checklist")
    return emb


def build_top16_online_reminder_embed(
    *,
    kind: str,          # "5d" | "3d" | "last"
    mk: str,            # YYYY-MM
    rank: int,
    name: str,
    online_games: int,
    need_total: int,
    mention: str,       # e.g. member.mention
    embed_color: Optional[int] = None,
    embed_thumbnail_url: Optional[str] = None,
) -> discord.Embed:
    """Build the Top16 online games reminder embed."""
    kind = (kind or "").strip().lower()
    if kind not in ("5d", "3d", "last"):
        kind = "5d"

    nice_month = month_label(mk)
    missing = max(0, int(need_total) - int(online_games))

    if kind == "last":
        title = "⏳ Last day — finish your online games for Top16"
        urgency = "Today is the **last day** of the league."
    elif kind == "3d":
        title = "⚠️ 3 days left — finish your online games for Top16"
        urgency = "Only **3 days** left in the league."
    else:
        title = "👀 5 days left — finish your online games for Top16"
        urgency = "Only **5 days** left in the league."

    desc = (
        f"Hey {mention} 👋\n\n"
        f"{urgency}\n\n"
        f"You're currently **#{rank:02d}** on **TopDeck** for **{nice_month}**, "
        f"but you're **not qualified** for the Top16 cut yet because of the **online games requirement**.\n\n"
        f"✅ Online games: **{online_games} / {need_total}**\n"
        f"❗ You need **{missing}** more online game(s) to qualify.\n\n"
        "If you want to keep your spot, try to finish the remaining online games before the league ends."
    )

    emb = discord.Embed(
        title=title,
        description=desc,
        color=_get_color(embed_color),
    )

    emb.set_footer(text="ECL • Top16 qualification reminder")
    _apply_thumbnail(emb, embed_thumbnail_url)

    return emb


def build_topcut_prize_reminder_embed(
    *,
    kind: str,          # "5d" | "1d"
    mk: str,
    rank: int,
    pts: int,
    cutoff_pts: int,
    mention: str,
    margin: int = 250,
    embed_color: Optional[int] = None,
    embed_thumbnail_url: Optional[str] = None,
) -> discord.Embed:
    """Build the Top16 prize eligibility reminder embed."""
    kind = (kind or "").strip().lower()
    if kind not in ("5d", "1d"):
        kind = "5d"

    nice_month = month_label(mk)

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
        color=_get_color(embed_color),
    )
    emb.set_footer(text="ECL • Prize eligibility reminder")
    _apply_thumbnail(emb, embed_thumbnail_url)

    return emb
