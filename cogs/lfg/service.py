from __future__ import annotations

import contextlib
from datetime import datetime
from typing import List, Optional, Tuple

import discord

from spelltable_client import create_spelltable_game
from utils.interactions import safe_i_send, safe_i_edit
from utils.logger import format_console

from .models import now_utc
from .views import LFGJoinView


def _disable_join_button(view: LFGJoinView) -> None:
    for child in view.children:
        if isinstance(child, discord.ui.Button) and child.custom_id == "lfg_join_button":
            child.disabled = True


def _disable_all_buttons(view: LFGJoinView) -> None:
    for child in view.children:
        if isinstance(child, discord.ui.Button):
            child.disabled = True


async def handle_open_last_seat(
    cog,
    interaction: discord.Interaction,
    view: LFGJoinView,
    button: discord.ui.Button,
):
    """Host-only button to open the last seat early for Elo lobbies."""

    # Ack fast to avoid Unknown interaction (10062) on slow paths
    with contextlib.suppress(Exception):
        if not interaction.response.is_done():
            await interaction.response.defer()

    guild = interaction.guild
    if guild is None:
        await safe_i_send(interaction, "This lobby can only be managed from within a server.", ephemeral=True)
        return

    edit_content: Optional[str] = None
    edit_embed: Optional[discord.Embed] = None
    edit_view: Optional[discord.ui.View] = None
    reply_ephemeral: Optional[str] = None
    success_ephemeral: Optional[str] = None

    async with cog.state.lock:
        lobbies = cog.state.peek_guild_lobbies(guild.id)
        lobby = lobbies.get(view.lobby.lobby_id)

        if lobby is None or lobby is not view.lobby:
            _disable_all_buttons(view)
            edit_content = "This lobby is no longer active."
            edit_view = view
        else:
            user = interaction.user
            if not isinstance(user, discord.Member):
                reply_ephemeral = "Only server members can manage this lobby."
            elif not lobby.elo_mode:
                reply_ephemeral = "This button is only available for /lfgelo lobbies."
            elif user.id != lobby.host_id:
                reply_ephemeral = "Only the host can open the last seat early."
            elif lobby.remaining_slots() != 1:
                reply_ephemeral = "You can only open the last seat when the lobby needs exactly 1 more player."
            elif cog._is_last_seat_open(lobby):
                reply_ephemeral = "The last seat is already open."
            else:
                lobby.last_seat_open = True
                lobby.almost_full_at = lobby.almost_full_at or now_utc()

                embed = cog._build_lobby_embed(guild, lobby)
                view._sync_open_last_seat_button()

                edit_embed = embed
                edit_view = view
                success_ephemeral = "Last seat opened — lower-rated players can now join to close the pod."

    if edit_content is not None or edit_embed is not None or edit_view is not None:
        await safe_i_edit(interaction, content=edit_content, embed=edit_embed, view=edit_view)

    if reply_ephemeral:
        await safe_i_send(interaction, reply_ephemeral, ephemeral=True)
        return

    if success_ephemeral:
        await safe_i_send(interaction, success_ephemeral, ephemeral=True)


async def handle_join(
    cog,
    interaction: discord.Interaction,
    view: LFGJoinView,
    button: discord.ui.Button,
    *,
    elo_min_games: int,
    last_seat_grace_min: int,
):
    """Join button handler (normal + Elo lobbies).

    Important: don't hold the store lock across slow awaits (TopDeck fetches, Discord I/O, SpellTable calls).
    """

    # Ack fast to avoid Unknown interaction (10062) on slow paths
    with contextlib.suppress(Exception):
        if not interaction.response.is_done():
            await interaction.response.defer()

    guild = interaction.guild
    if guild is None:
        await safe_i_send(interaction, "This lobby can only be joined from within a server.", ephemeral=True)
        return

    user = interaction.user
    if not isinstance(user, discord.Member):
        await safe_i_send(interaction, "Only server members can join this lobby.", ephemeral=True)
        return

    # Prefetch TopDeck rating OUTSIDE the lock (can be slow)
    user_info: Optional[Tuple[float, int]] = None
    if view.lobby.elo_mode:
        user_info = await cog._get_player_elo(user)

    # Planned actions after we release the lock
    edit_content: Optional[str] = None
    edit_embed: Optional[discord.Embed] = None
    edit_view: Optional[discord.ui.View] = None
    reply_ephemeral: Optional[str] = None

    create_room = False
    create_room_started_at: Optional[datetime] = None
    create_room_lobby_id: Optional[int] = None

    # After room creation succeeds
    ready_embed_for_dm: Optional[discord.Embed] = None
    ready_lobby_ref = None  # keep a reference to build embed OUTSIDE the lock
    dms_to_send: List[int] = []
    lobby_id_to_clear: Optional[int] = None
    should_check_high_stakes = False
    high_stakes_player_ids: List[int] = []

    async with cog.state.lock:
        lobbies = cog.state.peek_guild_lobbies(guild.id)
        lobby = lobbies.get(view.lobby.lobby_id)

        if lobby is None or lobby is not view.lobby:
            _disable_all_buttons(view)
            edit_content = "This lobby is no longer active."
            edit_view = view
        else:
            other = cog._find_user_lobby(guild.id, user.id, exclude_lobby_id=lobby.lobby_id)
            if other is not None:
                reply_ephemeral = (
                    "You're already in another active lobby in this server. "
                    "Leave it before joining a new one."
                )
            elif user.id in lobby.player_ids:
                reply_ephemeral = "You're already in this lobby."
            elif lobby.is_full():
                _disable_all_buttons(view)
                lobby_id_to_clear = lobby.lobby_id
                edit_view = view
                reply_ephemeral = "This lobby is already full."
            else:
                if lobby.elo_mode:
                    if user_info is None:
                        reply_ephemeral = (
                            "You don\'t have a league rating yet, so you can\'t join this Elo-matched pod.\n"
                            "Use /lfg instead or play some matches first!"
                        )
                    else:
                        user_elo, user_games = user_info
                        if int(user_games) < int(elo_min_games):
                            reply_ephemeral = (
                                f"This Elo-matched pod requires at least **{int(elo_min_games)}** league games.\n"
                                f"You currently have **{int(user_games)}**.\n"
                                "Use /lfg for now and come back once you\'ve got more games logged."
                            )
                        else:
                            floor = cog._effective_elo_floor(lobby)
                            if floor is None:
                                reply_ephemeral = "This Elo-matched lobby is misconfigured. Please ping a mod."
                            elif float(user_elo) < float(floor):
                                msg = (
                                    f"This Elo pod currently requires **≥ {int(floor)}** points.\n"
                                    f"Your rating: **{int(user_elo)}**.\n"
                                )

                                if lobby.remaining_slots() == 1 and not cog._is_last_seat_open(lobby):
                                    relaxed_floor = cog._relaxed_last_seat_floor(lobby)
                                    if relaxed_floor is not None:
                                        mins_left = int(last_seat_grace_min)
                                        if lobby.almost_full_at is not None:
                                            elapsed = (now_utc() - lobby.almost_full_at).total_seconds() / 60.0
                                            mins_left = max(0, int(round(float(last_seat_grace_min) - elapsed)))
                                        msg += (
                                            f"The last seat will open to **≥ {int(relaxed_floor)}** in ~{mins_left} min, "
                                            f"or the host can open it now."
                                        )
                                else:
                                    rng = cog._current_downward_range(lobby) or 0.0
                                    if float(rng) >= float(cog._max_downward_range(lobby)):
                                        msg += "The floor is already at the bottom for this lobby."
                                    else:
                                        msg += "The floor expands over time, so you might be able to join later."

                                reply_ephemeral = msg

                if reply_ephemeral is None:
                    lobby.player_ids.append(user.id)

                    # Store pts snapshot for Elo lobby display
                    if lobby.elo_mode and user_info is not None:
                        lobby.player_pts[user.id] = float(user_info[0])

                    if lobby.elo_mode and lobby.remaining_slots() == 1 and lobby.almost_full_at is None:
                        lobby.almost_full_at = now_utc()
                        if not cog._is_last_seat_open(lobby):
                            cog._ensure_elo_embed_updater(lobby)

                    # If we just became full, start room creation OUTSIDE the lock.
                    if (
                        lobby.is_full()
                        and not lobby.has_link()
                        and not getattr(lobby, "link_creating", False)
                    ):
                        lobby.link_creating = True
                        create_room = True
                        create_room_started_at = now_utc()
                        create_room_lobby_id = lobby.lobby_id
                        _disable_join_button(view)

                    embed = cog._build_lobby_embed(guild, lobby)
                    view._sync_open_last_seat_button()
                    edit_embed = embed
                    edit_view = view

    # Do message edits / replies OUTSIDE the lock
    if edit_content is not None or edit_embed is not None or edit_view is not None:
        await safe_i_edit(interaction, content=edit_content, embed=edit_embed, view=edit_view)

    # Persist lobby state after successful join (refresh expiration too)
    if reply_ephemeral is None and view.lobby is not None:
        try:
            await cog._save_lobby_to_db(view.lobby)
        except Exception as e:
            print(format_console(f"[lfg] Failed to persist lobby after join: {e}", level="error"))

    if reply_ephemeral is not None:
        # Clear a stale/full lobby if we disabled its view
        if lobby_id_to_clear is not None:
            async with cog.state.lock:
                cog._clear_lobby(guild.id, lobby_id_to_clear)

        await safe_i_send(interaction, reply_ephemeral, ephemeral=True)
        return

    # If we didn't trigger SpellTable creation, we're done.
    if not create_room or create_room_lobby_id is None or create_room_started_at is None:
        return

    # Create SpellTable room (slow) OUTSIDE the lock
    link_created: Optional[str] = None
    try:
        link_created = await create_spelltable_game(
            game_name="ECL DragonShield",
            format_name="Commander",
            is_public=False,
        )
    except Exception as e:
        print(format_console(f"[lfg] Failed to create SpellTable game: {e}", level="error"))

    # Apply the created link if the lobby is still active + still full
    async with cog.state.lock:
        lobby = cog.state.peek_guild_lobbies(guild.id).get(create_room_lobby_id)
        if lobby is None:
            return

        # If someone left while we were creating, don't finalize
        if not lobby.is_full() or lobby.has_link() or not getattr(lobby, "link_creating", False):
            lobby.link_creating = False
            return

        if not link_created:
            lobby.link_creating = False
        else:
            lobby.link = link_created
            lobby.link_creating = False

            # ✅ DO NOT build the ready embed here (async) — capture ref and do it outside lock
            ready_lobby_ref = lobby
            dms_to_send = list(lobby.player_ids)
            lobby_id_to_clear = lobby.lobby_id
            should_check_high_stakes = True
            high_stakes_player_ids = list(lobby.player_ids)

            with contextlib.suppress(Exception):
                view.stop()

    if not link_created or ready_lobby_ref is None or lobby_id_to_clear is None:
        await safe_i_send(
            interaction,
            "The lobby filled, but I couldn't create the SpellTable room. Please try /lfg again or ping a mod.",
            ephemeral=True,
        )
        return

    # ✅ Build READY embed (includes pts) OUTSIDE the lock
    ready_embed_for_dm = await cog._build_ready_embed(guild, ready_lobby_ref, create_room_started_at)

    # Edit lobby message to READY + remove buttons
    await safe_i_edit(interaction, embed=ready_embed_for_dm, view=None)

    # Remove from store (fast)
    async with cog.state.lock:
        cog._clear_lobby(guild.id, lobby_id_to_clear)

    # High-stakes announcement (+ logs now in _maybe_announce_high_stakes)
    if should_check_high_stakes and interaction.channel:
        with contextlib.suppress(Exception):
            await cog._maybe_announce_high_stakes(interaction.channel, guild, high_stakes_player_ids)

    # DM players
    for uid in dms_to_send:
        member = guild.get_member(uid)
        if not member:
            continue
        with contextlib.suppress(discord.Forbidden):
            await member.send(embed=ready_embed_for_dm)



async def handle_leave(
    cog,
    interaction: discord.Interaction,
    view: LFGJoinView,
    button: discord.ui.Button,
):
    """Leave button handler."""

    # Ack fast to avoid Unknown interaction (10062) on slow paths
    with contextlib.suppress(Exception):
        if not interaction.response.is_done():
            await interaction.response.defer()

    guild = interaction.guild
    if guild is None:
        await safe_i_send(interaction, "This lobby can only be left from within a server.", ephemeral=True)
        return

    user = interaction.user
    if not isinstance(user, discord.Member):
        await safe_i_send(interaction, "Only server members can leave this lobby.", ephemeral=True)
        return

    edit_content: Optional[str] = None
    edit_embed: Optional[discord.Embed] = None
    edit_view: Optional[discord.ui.View] = None
    reply_ephemeral: Optional[str] = None

    delete_channel_id: Optional[int] = None
    delete_message_id: Optional[int] = None

    async with cog.state.lock:
        lobbies = cog.state.peek_guild_lobbies(guild.id)
        lobby = lobbies.get(view.lobby.lobby_id)

        if lobby is None or lobby is not view.lobby:
            _disable_all_buttons(view)
            edit_content = "This lobby is no longer active."
            edit_view = view
        else:
            if user.id not in lobby.player_ids:
                reply_ephemeral = "You're not in this lobby."
            else:
                lobby.player_ids = [uid for uid in lobby.player_ids if uid != user.id]

                if lobby.elo_mode:
                    lobby.player_pts.pop(user.id, None)

                # If we were creating a room and someone left, allow creation to be triggered again later.
                if getattr(lobby, "link_creating", False) and not lobby.is_full():
                    lobby.link_creating = False

                became_empty = len(lobby.player_ids) == 0

                if lobby.elo_mode:
                    if lobby.remaining_slots() == 1:
                        lobby.almost_full_at = lobby.almost_full_at or now_utc()
                        if not cog._is_last_seat_open(lobby):
                            cog._ensure_elo_embed_updater(lobby)
                    else:
                        lobby.almost_full_at = None
                        lobby.last_seat_open = False

                if became_empty:
                    # Delete from store immediately (fast) then do Discord I/O outside the lock.
                    cog._clear_lobby(guild.id, lobby.lobby_id)

                    delete_channel_id = lobby.channel_id
                    delete_message_id = lobby.message_id

                    with contextlib.suppress(Exception):
                        view.stop()

                    reply_ephemeral = "You left the lobby. It is now empty and has been closed."
                else:
                    embed = cog._build_lobby_embed(guild, lobby)
                    view._sync_open_last_seat_button()
                    edit_embed = embed
                    edit_view = view

    # Outside lock: edits / deletes / responses
    if edit_content is not None or edit_embed is not None or edit_view is not None:
        await safe_i_edit(interaction, content=edit_content, embed=edit_embed, view=edit_view)

    # Persist lobby state after successful leave (if lobby still exists)
    if edit_embed is not None and view.lobby is not None and not delete_message_id:
        try:
            await cog._save_lobby_to_db(view.lobby)
        except Exception as e:
            print(format_console(f"[lfg] Failed to persist lobby after leave: {e}", level="error"))

    if delete_channel_id and delete_message_id:
        channel = guild.get_channel(delete_channel_id)
        if isinstance(channel, discord.TextChannel):
            with contextlib.suppress(Exception):
                msg = await channel.fetch_message(delete_message_id)
                await msg.delete()

    if reply_ephemeral:
        await safe_i_send(interaction, reply_ephemeral, ephemeral=True)
