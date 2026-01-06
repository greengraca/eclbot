from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Mapping

import discord

from .models import LFGLobby


def ts(dt: datetime) -> int:
    return int(dt.timestamp())


@dataclass(frozen=True)
class LastSeatInfo:
    """Extra info for last-seat behaviour in Elo lobbies."""

    is_open: bool
    min_rating: int
    minutes_left: Optional[int] = None


@dataclass(frozen=True)
class EloLobbyInfo:
    host_elo: int
    min_rating: int
    at_bottom: bool
    last_seat: Optional[LastSeatInfo] = None


def build_lobby_embed(
    guild: discord.Guild,
    lobby: LFGLobby,
    *,
    updated_at: datetime,
    icon_url: str = "",
    elo_info: Optional[EloLobbyInfo] = None,
    expand_interval_min: int = 5,
    last_seat_grace_min: int = 10,
) -> discord.Embed:
    remaining = lobby.remaining_slots()
    if remaining <= 0:
        title = "Waiting for players to join..."
    elif remaining == 1:
        title = "Waiting for 1 more player to join..."
    else:
        title = f"Waiting for {remaining} more players to join..."

    description = "*A SpellTable link will be created when all players have joined.*\n\n"

    if elo_info is not None and lobby.elo_mode:
        description += (
            f"Host rating: **{int(elo_info.host_elo)}**\n"
            f"Minimum rating to join: **≥ {int(elo_info.min_rating)}** points\n"
        )

        if elo_info.at_bottom:
            description += "*(We're already at the bottom.)*\n"
        else:
            description += f"(Floor expands every {int(expand_interval_min)} minutes.)\n"

        if remaining == 1 and elo_info.last_seat is not None:
            ls = elo_info.last_seat
            if ls.is_open:
                description += f"Last seat: **OPEN** *(≥ {int(ls.min_rating)})*\n\n"
            else:
                mins_left = int(ls.minutes_left) if ls.minutes_left is not None else int(last_seat_grace_min)
                description += (
                    f"Last seat: **LOCKED** *(opens to ≥ {int(ls.min_rating)} in ~{mins_left} min "
                    f"or host can open it now.)*\n\n"
                )
        else:
            description += "\n"

    embed = discord.Embed(
        title=title,
        description=description,
        color=discord.Color.yellow() if lobby.elo_mode else discord.Color.dark_grey(),
    )

    lines: List[str] = []
    for uid in lobby.player_ids:
        pts_suffix = ""
        if lobby.elo_mode:
            pts = getattr(lobby, "player_pts", {}).get(uid)
            if pts is not None:
                pts_suffix = f" - {int(pts)}"

        member = guild.get_member(uid)
        if member:
            lines.append(f"• {member.mention} ({member.display_name}){pts_suffix}")
        else:
            lines.append(f"• <@{uid}> (User {uid}){pts_suffix}")

    embed.add_field(name="Players", value="\n".join(lines) if lines else "*No players yet*", inline=False)
    embed.add_field(name="Format", value="Commander", inline=True)
    embed.add_field(name="Updated at", value=f"<t:{ts(updated_at)}:f>", inline=True)

    if icon_url:
        embed.set_thumbnail(url=icon_url)

    return embed


def build_ready_embed(
    guild: discord.Guild,
    lobby: LFGLobby,
    *,
    started_at: datetime,
    icon_url: str = "",
    pts_by_id: Optional[Mapping[int, int]] = None,  # ✅ new
) -> discord.Embed:
    started_at = started_at.astimezone(timezone.utc)
    started_ts = ts(started_at)

    embed = discord.Embed(
        title="Your game is ready!",
        description=f"# [Join your SpellTable game now!]({lobby.link})",
        color=discord.Color.green(),
    )

    lines: List[str] = []
    for uid in lobby.player_ids:
        pts_suffix = ""
        if lobby.elo_mode and pts_by_id is not None and int(uid) in pts_by_id:
            pts_suffix = f" (**{int(pts_by_id[int(uid)])} pts**)"

        member = guild.get_member(uid)
        if member:
            lines.append(f"{member.mention} ({member.display_name}){pts_suffix}")
        else:
            lines.append(f"<@{uid}> (User {uid}){pts_suffix}")

    embed.add_field(name="Players", value="\n".join(lines) if lines else "*Unknown players*", inline=False)
    embed.add_field(name="Format", value="Commander", inline=True)
    embed.add_field(name="Started at", value=f"<t:{started_ts}:f>", inline=True)

    if icon_url:
        embed.set_thumbnail(url=icon_url)

    return embed
