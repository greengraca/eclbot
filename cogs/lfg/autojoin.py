from __future__ import annotations
import os
import contextlib
from typing import Dict, List, Optional

import discord

from spelltable_client import create_spelltable_game
from utils.interactions import safe_ctx_followup, resolve_member
from utils.logger import log_error

from .models import LFGLobby, now_utc

# ✅ Autojoin is ONLY allowed in this channel (0 = disabled / allow anywhere)
SPELLBOT_LFG_CHANNEL_ID = int((os.getenv("SPELLBOT_LFG_CHANNEL_ID") or "0").strip() or "0")


def _autojoin_allowed(ctx: discord.ApplicationContext) -> bool:
    if SPELLBOT_LFG_CHANNEL_ID <= 0:
        return True
    try:
        return int(getattr(ctx.channel, "id", 0)) == SPELLBOT_LFG_CHANNEL_ID
    except Exception:
        return False


def open_lobbies_sorted(
    cog,
    guild_id: int,
    preferred_channel_id: int,
) -> List[LFGLobby]:
    """Open (not full) lobbies, preferring the current channel then oldest-first.

    If SPELLBOT_LFG_CHANNEL_ID is set, only consider lobbies in that channel.
    """
    lobbies = list(cog.state.peek_guild_lobbies(guild_id).values())

    open_lobbies = []
    for lob in lobbies:
        if SPELLBOT_LFG_CHANNEL_ID > 0 and int(lob.channel_id) != int(SPELLBOT_LFG_CHANNEL_ID):
            continue
        if cog._is_lobby_active(lob) and not lob.is_full():
            open_lobbies.append(lob)

    def key(lob: LFGLobby):
        return (0 if lob.channel_id == preferred_channel_id else 1, lob.created_at)

    return sorted(open_lobbies, key=key)


async def _send_ephemeral(ctx: discord.ApplicationContext, content: str) -> None:
    try:
        inter = getattr(ctx, "interaction", None)
        if inter and not inter.response.is_done():
            await ctx.respond(content, ephemeral=True)
        else:
            await ctx.followup.send(content, ephemeral=True)
    except Exception:
        pass



async def can_member_join_elo_lobby(
    cog,
    lobby: LFGLobby,
    member: discord.Member,
    *,
    elo_min_games: int,
) -> bool:
    if not lobby.elo_mode or lobby.host_elo is None:
        return True

    info = await cog._get_player_elo(member)
    if info is None:
        return False
    user_elo, user_games = info
    if int(user_games) < int(elo_min_games):
        return False

    floor = cog._effective_elo_floor(lobby)
    if floor is None:
        return False

    return float(user_elo) >= float(floor)



async def elo_join_reason(
    cog,
    lobby: LFGLobby,
    member: discord.Member,
    *,
    elo_min_games: int,
) -> Optional[str]:
    info = await cog._get_player_elo(member)
    if info is None:
        return "no league rating yet"

    elo, games = info
    if int(games) < int(elo_min_games):
        return f"only {int(games)} games (need {int(elo_min_games)})"

    floor = cog._effective_elo_floor(lobby)
    if floor is None:
        return "lobby misconfigured"

    if float(elo) < float(floor):
        return f"needs ≥ {int(floor)} (has {int(elo)})"

    return None


async def autojoin_specific_lobby_group(
    cog,
    ctx: discord.ApplicationContext,
    lobby: LFGLobby,
    join_ids: List[int],
) -> bool:
    """Attempt to add a whole group (joiner + optionally friends) into one lobby."""

    if ctx.guild is None or not isinstance(ctx.author, discord.Member):
        return False

    guild: discord.Guild = ctx.guild

    seen = set()
    join_ids = [uid for uid in join_ids if isinstance(uid, int) and not (uid in seen or seen.add(uid))]

    lobby_id = lobby.lobby_id
    channel_id = lobby.channel_id
    message_id = lobby.message_id
    view = lobby.view

    pts_by_id: Dict[int, float] = {}
    if lobby.elo_mode:
        for uid in join_ids:
            m = guild.get_member(uid)
            if not isinstance(m, discord.Member):
                continue
            info = await cog._get_player_elo(m)
            if info is not None:
                pts_by_id[int(uid)] = float(info[0])

    became_full = False
    player_ids_snapshot: List[int] = []

    async with cog.state.lock:
        current = cog.state.peek_guild_lobbies(guild.id).get(lobby_id)
        if current is None or current is not lobby:
            return False
        if not cog._is_lobby_active(lobby) or lobby.is_full():
            return False

        if len(join_ids) > lobby.remaining_slots():
            return False

        for uid in join_ids:
            other = cog._find_user_lobby(guild.id, uid, exclude_lobby_id=lobby.lobby_id)
            if other is not None:
                return False
            if uid in lobby.player_ids:
                return False

        lobby.player_ids.extend(join_ids)

        if lobby.elo_mode and pts_by_id:
            for uid, pts in pts_by_id.items():
                if uid in lobby.player_ids:
                    lobby.player_pts[uid] = float(pts)

        if lobby.elo_mode and lobby.remaining_slots() == 1 and lobby.almost_full_at is None:
            lobby.almost_full_at = now_utc()
            if not cog._is_last_seat_open(lobby):
                cog._ensure_elo_embed_updater(lobby)

        became_full = lobby.is_full() and not lobby.has_link() and not getattr(lobby, "link_creating", False)
        if became_full:
            lobby.link_creating = True
        player_ids_snapshot = list(lobby.player_ids)
        
    with contextlib.suppress(Exception):
        await cog._save_lobby_to_db(lobby)


    channel = guild.get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        return True

    msg: Optional[discord.Message] = None
    if message_id:
        with contextlib.suppress(Exception):
            msg = await channel.fetch_message(message_id)

    if became_full:
        started_at = now_utc()
        link_created: Optional[str] = None
        try:
            link_created = await create_spelltable_game(
                game_name="ECL DragonShield",
                format_name="Commander",
                is_public=False,
            )
        except Exception as e:
            log_error(f"[lfg] Failed to create SpellTable game (autojoin group): {e}")

        if link_created:
            async with cog.state.lock:
                current = cog.state.peek_guild_lobbies(guild.id).get(lobby_id)
                if current is not None and current is lobby:
                    lobby.link = link_created
                    lobby.link_creating = False

            ready_embed = await cog._build_ready_embed(guild, lobby, started_at)

            if msg:
                with contextlib.suppress(Exception):
                    await msg.edit(embed=ready_embed, view=None)

            with contextlib.suppress(Exception):
                await cog._maybe_announce_high_stakes(channel, guild, player_ids_snapshot)

            for uid in player_ids_snapshot:
                m = guild.get_member(uid)
                if not m:
                    continue
                with contextlib.suppress(discord.Forbidden):
                    await m.send(embed=ready_embed)

            async with cog.state.lock:
                cog._clear_lobby(guild.id, lobby_id)

            with contextlib.suppress(Exception):
                await safe_ctx_followup(
                    ctx,
                    f"Joined an existing lobby in <#{channel_id}> — pod is now **READY** ✅",
                    ephemeral=True,
                )
        else:
            async with cog.state.lock:
                current = cog.state.peek_guild_lobbies(guild.id).get(lobby_id)
                if current is not None and current is lobby:
                    lobby.link_creating = False
            with contextlib.suppress(Exception):
                await safe_ctx_followup(
                    ctx,
                    "Joined an existing lobby, but I couldn’t create the SpellTable room. Ping a mod.",
                    ephemeral=True,
                )
        return True

    if msg and view:
        embed = cog._build_lobby_embed(guild, lobby)
        view._sync_open_last_seat_button()
        with contextlib.suppress(Exception):
            await msg.edit(embed=embed, view=view)

    with contextlib.suppress(Exception):
        await safe_ctx_followup(ctx, f"Joined an existing lobby in <#{channel_id}> ✅", ephemeral=True)

    return True


async def autojoin_specific_lobby_from_lfg(
    cog,
    ctx: discord.ApplicationContext,
    lobby: LFGLobby,
    invited_ids: List[int],
) -> bool:
    """Attempt to join a specific lobby from /lfg or /lfgelo (joiner + optional invited_ids)."""

    if ctx.guild is None or not isinstance(ctx.author, discord.Member):
        return False

    guild: discord.Guild = ctx.guild
    joiner: discord.Member = ctx.author

    joiner_pts: Optional[float] = None
    if lobby.elo_mode:
        info = await cog._get_player_elo(joiner)
        if info is not None:
            joiner_pts = float(info[0])

    lobby_id = lobby.lobby_id
    channel_id = lobby.channel_id
    message_id = lobby.message_id
    view = lobby.view

    requested_size = 1 + len(invited_ids)

    became_full = False
    player_ids_snapshot: List[int] = []

    async with cog.state.lock:
        current = cog.state.peek_guild_lobbies(guild.id).get(lobby_id)
        if current is None or current is not lobby:
            return False
        if not cog._is_lobby_active(lobby) or lobby.is_full():
            return False

        if cog._find_user_lobby(guild.id, joiner.id) is not None:
            return False

        if joiner.id in lobby.player_ids:
            return False

        if invited_ids and lobby.elo_mode:
            return False

        if requested_size > lobby.remaining_slots():
            return False

        lobby.player_ids.append(joiner.id)

        if lobby.elo_mode and joiner_pts is not None:
            lobby.player_pts[joiner.id] = float(joiner_pts)

        for uid in invited_ids:
            if lobby.is_full():
                break
            if uid == joiner.id:
                continue
            if uid in lobby.player_ids:
                continue
            if cog._find_user_lobby(guild.id, uid, exclude_lobby_id=lobby.lobby_id) is not None:
                continue
            lobby.player_ids.append(uid)

        if lobby.elo_mode and lobby.remaining_slots() == 1 and lobby.almost_full_at is None:
            lobby.almost_full_at = now_utc()
            if not cog._is_last_seat_open(lobby):
                cog._ensure_elo_embed_updater(lobby)

        became_full = lobby.is_full() and not lobby.has_link() and not getattr(lobby, "link_creating", False)
        if became_full:
            lobby.link_creating = True
        player_ids_snapshot = list(lobby.player_ids)
    
    with contextlib.suppress(Exception):
        await cog._save_lobby_to_db(lobby)


    channel = guild.get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        return True

    msg: Optional[discord.Message] = None
    if message_id:
        with contextlib.suppress(Exception):
            msg = await channel.fetch_message(message_id)

    if became_full:
        started_at = now_utc()
        link_created: Optional[str] = None

        try:
            link_created = await create_spelltable_game(
                game_name="ECL DragonShield",
                format_name="Commander",
                is_public=False,
            )
        except Exception as e:
            log_error(f"[lfg] Failed to create SpellTable game (autojoin): {e}")

        if link_created:
            async with cog.state.lock:
                current = cog.state.peek_guild_lobbies(guild.id).get(lobby_id)
                if current is not None and current is lobby:
                    lobby.link = link_created

            # ready_embed = cog._build_ready_embed(guild, lobby, started_at) 
            ready_embed = await cog._build_ready_embed(guild, lobby, started_at)  # ✅


            if msg:
                with contextlib.suppress(Exception):
                    await msg.edit(embed=ready_embed, view=None)

            with contextlib.suppress(Exception):
                await cog._maybe_announce_high_stakes(channel, guild, player_ids_snapshot)

            for uid in player_ids_snapshot:
                m = guild.get_member(uid)
                if not m:
                    continue
                with contextlib.suppress(discord.Forbidden):
                    await m.send(embed=ready_embed)

            async with cog.state.lock:
                cog._clear_lobby(guild.id, lobby_id)

            with contextlib.suppress(Exception):
                await safe_ctx_followup(
                    ctx,
                    f"Joined an existing lobby in <#{channel_id}> — pod is now **READY** ✅",
                    ephemeral=True,
                )
        else:
            async with cog.state.lock:
                current = cog.state.peek_guild_lobbies(guild.id).get(lobby_id)
                if current is not None and current is lobby:
                    lobby.link_creating = False
            with contextlib.suppress(Exception):
                await safe_ctx_followup(
                    ctx,
                    "Joined an existing lobby, but I couldn’t create the SpellTable room. Ping a mod.",
                    ephemeral=True,
                )

        return True

    if msg and view:
        embed = cog._build_lobby_embed(guild, lobby)
        view._sync_open_last_seat_button()
        with contextlib.suppress(Exception):
            await msg.edit(embed=embed, view=view)

    with contextlib.suppress(Exception):
        await safe_ctx_followup(
            ctx,
            f"Joined an existing lobby in <#{channel_id}> ✅",
            ephemeral=True,
        )
    return True


async def try_join_existing_for_lfgelo(
    cog,
    ctx: discord.ApplicationContext,
    *,
    elo_min_games: int,
) -> bool:
    if ctx.guild is None or not isinstance(ctx.author, discord.Member):
        return False

    # ✅ hard gate: no autojoin outside lfg-league
    if not _autojoin_allowed(ctx):
        return False

    guild_id = ctx.guild.id
    preferred_channel_id = ctx.channel.id
    joiner: discord.Member = ctx.author

    async with cog.state.lock:
        if cog._find_user_lobby(guild_id, joiner.id) is not None:
            return False
        open_lobbies = open_lobbies_sorted(cog, guild_id, preferred_channel_id)

    for lob in open_lobbies:
        if not lob.elo_mode:
            continue
        if lob.remaining_slots() <= 0:
            continue
        if not await can_member_join_elo_lobby(cog, lob, joiner, elo_min_games=int(elo_min_games)):
            continue
        if await autojoin_specific_lobby_from_lfg(cog, ctx, lob, []):
            return True

    return False


async def try_join_existing_for_lfg(
    cog,
    ctx: discord.ApplicationContext,
    invited_ids: List[int],
    *,
    elo_min_games: int,
) -> bool:
    if ctx.guild is None or not isinstance(ctx.author, discord.Member):
        return False

    # ✅ hard gate: no autojoin outside lfg-league
    if not _autojoin_allowed(ctx):
        return False

    guild = ctx.guild
    guild_id = guild.id
    preferred_channel_id = ctx.channel.id
    joiner = ctx.author

    seen = set()
    invited_ids = [uid for uid in invited_ids if isinstance(uid, int) and not (uid in seen or seen.add(uid))]

    want_friends = len(invited_ids) > 0
    join_ids = [joiner.id] + invited_ids
    requested_size = len(join_ids)

    async with cog.state.lock:
        if cog._find_user_lobby(guild_id, joiner.id) is not None:
            return False
        open_lobbies = open_lobbies_sorted(cog, guild_id, preferred_channel_id)

    for lob in open_lobbies:
        if lob.elo_mode:
            continue
        if requested_size > lob.remaining_slots():
            continue
        if await autojoin_specific_lobby_group(cog, ctx, lob, join_ids):
            return True

    if not want_friends:
        for lob in open_lobbies:
            if not lob.elo_mode:
                continue
            if lob.remaining_slots() < 1:
                continue
            if not await can_member_join_elo_lobby(cog, lob, joiner, elo_min_games=int(elo_min_games)):
                continue
            if await autojoin_specific_lobby_group(cog, ctx, lob, [joiner.id]):
                return True
        return False

    for lob in open_lobbies:
        if not lob.elo_mode:
            continue
        if requested_size > lob.remaining_slots():
            continue

        failures: List[str] = []
        for uid in join_ids:
            m = await resolve_member(guild, uid)
            if m is None:
                failures.append(f"<@{uid}>: not a server member")
                continue
            reason = await elo_join_reason(cog, lob, m, elo_min_games=int(elo_min_games))
            if reason:
                failures.append(f"{m.mention}: {reason}")

        if failures:
            await _send_ephemeral(
                ctx,
                "Can't join the existing **Elo** lobby with this group:\n"
                + "\n".join(f"• {x}" for x in failures)
                + "\n\nOpening a **normal** /lfg instead.",
            )
            return False


        if await autojoin_specific_lobby_group(cog, ctx, lob, join_ids):
            return True

    return False
