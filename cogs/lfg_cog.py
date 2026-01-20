# cogs/lfg_cog.py
import os
import asyncio
import contextlib
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Tuple

import discord
from discord.ext import commands
from discord import Option

from spelltable_client import create_spelltable_game  # <- your SpellTable helper
from topdeck_fetch import get_handle_to_best_cached

from utils.interactions import (
    safe_ctx_defer,
    safe_ctx_respond,
    safe_ctx_followup,
    safe_i_send,
    safe_i_edit,
)

from .lfg.models import LFGLobby, now_utc
from .lfg.state import LobbyStore
from .lfg.views import LFGJoinView, PersistentLFGView
from .lfg.embeds import (
    build_lobby_embed,
    build_ready_embed,
    EloLobbyInfo,
    LastSeatInfo,
)
from .lfg.elo import (
    compute_dynamic_window,
    current_downward_range,
    effective_elo_floor,
    is_last_seat_open,
    max_downward_range,
    relaxed_last_seat_floor,
    get_member_points_games,
    resolve_points_games_from_map,
)
from .lfg.service import (
    handle_join,
    handle_leave,
    handle_open_last_seat,
)

from .lfg.autojoin import (
    try_join_existing_for_lfg,
    try_join_existing_for_lfgelo,
)

from utils.persistence import (
    save_lobby as db_save_lobby,
    delete_lobby as db_delete_lobby,
    get_all_active_lobbies as db_get_all_active_lobbies,
    get_max_lobby_id as db_get_max_lobby_id,
    cleanup_expired_lobbies as db_cleanup_expired_lobbies,
    update_lobby_expires_at as db_update_lobby_expires_at,
)
from utils.logger import format_console

GUILD_ID = int(os.getenv("GUILD_ID", "0"))
LFG_EMBED_ICON_URL = os.getenv("LFG_EMBED_ICON_URL", "").strip()

# minutes of inactivity (no button clicks) before a lobby auto-expires
LOBBY_INACTIVITY_MINUTES = int(os.getenv("LOBBY_INACTIVITY_MINUTES", "45"))

# TopDeck league config (for Elo lookup)
TOPDECK_BRACKET_ID = os.getenv("TOPDECK_BRACKET_ID", "").strip()
FIREBASE_ID_TOKEN = os.getenv("FIREBASE_ID_TOKEN", None)

# /lfgelo availability & Elo window behaviour
LFG_ELO_MIN_DAY = int(os.getenv("LFG_ELO_MIN_DAY", "10"))  # earliest day-of-month for /lfgelo

# Defaults / fallback (used when dynamic window isn't available)
LFG_ELO_BASE_RANGE = int(os.getenv("LFG_ELO_BASE_RANGE", "100"))        # start range (downwards)
LFG_ELO_RANGE_STEP = int(os.getenv("LFG_ELO_RANGE_STEP", "100"))        # expand by this each step
LFG_ELO_MAX_STEPS = int(os.getenv("LFG_ELO_MAX_STEPS", "4"))            # 4 => base, +1 step, +2 step, +3 step
LFG_ELO_MAX_RANGE = (
    LFG_ELO_BASE_RANGE + max(0, LFG_ELO_MAX_STEPS - 1) * LFG_ELO_RANGE_STEP
)
LFG_ELO_EXPAND_INTERVAL_MIN = int(os.getenv("LFG_ELO_EXPAND_INTERVAL_MIN", "5"))  # expand every 5min
LFG_ELO_MIN_GAMES = int(os.getenv("LFG_ELO_MIN_GAMES", "5"))

# ---- dynamic window tuning (percentile-based) -------------------------------
# At time=0, we aim to allow ~this fraction of rated players to join (via floor).
LFG_ELO_TARGET_POOL_FRAC = float(os.getenv("LFG_ELO_TARGET_POOL_FRAC", "0.35"))
LFG_ELO_MIN_BASE_RANGE = int(os.getenv("LFG_ELO_MIN_BASE_RANGE", "100"))
# Step grows with base (top players expand faster)
LFG_ELO_STEP_FACTOR = float(os.getenv("LFG_ELO_STEP_FACTOR", "0.35"))
LFG_ELO_MIN_RANGE_STEP = int(os.getenv("LFG_ELO_MIN_RANGE_STEP", "50"))
# Keep ranges “clean”
LFG_ELO_RANGE_ROUND_TO = int(os.getenv("LFG_ELO_RANGE_ROUND_TO", "25"))

# last-seat behaviour (floor-only; no ceiling)
LFG_ELO_LAST_SEAT_GRACE_MIN = int(os.getenv("LFG_ELO_LAST_SEAT_GRACE_MIN", "10"))    # wait this long at 3/4
# last-seat behaviour (absolute floor when unlocked)
LFG_ELO_LAST_SEAT_MIN_RATING = int(os.getenv("LFG_ELO_LAST_SEAT_MIN_RATING", "200"))

# ---- High-stakes pods -------------------------------------------------------
# If either is 0/empty -> feature is effectively disabled
WAGER_RATE = float(os.getenv("WAGER_RATE", "0"))  # e.g. 0.25 means 25% of each player's pts
HIGH_STAKES_THRESHOLD = float(os.getenv("HIGH_STAKES_THRESHOLD", "0"))  # e.g. 400


class LFGCog(commands.Cog):
    """
    Simple LFG system for SpellTable Commander pods.

    - /lfg → opens a lobby for up to 4 players (optionally auto-filling with mentioned friends).
    - /lfgelo → opens an Elo-matched lobby for up to 4 players (no friends option).
    - One active lobby per guild at a time.
    - SpellTable room is created *only when the lobby becomes full*.
    
    Persists lobby state to MongoDB for Heroku restart survival.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = LobbyStore()
        self._rehydrated = False
        self._persistent_view_registered = False

        # NOTE: Do NOT instantiate discord.ui.View objects before the event loop is running.
        # main.py loads extensions before bot.run(), so creating a View in __init__ will crash
        # with: RuntimeError: no running event loop.
        # We create/register this in on_ready() instead.
        self._persistent_view: Optional[PersistentLFGView] = None

    # ---------- Persistence helpers ----------

    async def _save_lobby_to_db(self, lobby: LFGLobby) -> None:
        """Persist the current lobby state to MongoDB."""
        try:
            expires_at = lobby.created_at + timedelta(minutes=LOBBY_INACTIVITY_MINUTES)
            await db_save_lobby(
                guild_id=lobby.guild_id,
                lobby_id=lobby.lobby_id,
                channel_id=lobby.channel_id,
                message_id=lobby.message_id,
                host_id=lobby.host_id,
                player_ids=lobby.player_ids,
                invited_ids=lobby.invited_ids,
                max_seats=lobby.max_seats,
                link=lobby.link,
                link_creating=lobby.link_creating,
                elo_mode=lobby.elo_mode,
                host_elo=lobby.host_elo,
                elo_base_range=lobby.elo_base_range,
                elo_range_step=lobby.elo_range_step,
                elo_max_steps=lobby.elo_max_steps,
                player_pts=lobby.player_pts,
                created_at=lobby.created_at,
                almost_full_at=lobby.almost_full_at,
                last_seat_open=lobby.last_seat_open,
                expires_at=expires_at,
            )
        except Exception as e:
            print(format_console(f"[lfg] Failed to persist lobby {lobby.guild_id}:{lobby.lobby_id}: {type(e).__name__}: {e}", level="error"))

    async def _delete_lobby_from_db(self, guild_id: int, lobby_id: int) -> None:
        """Delete lobby from DB (used by _clear_lobby)."""
        try:
            await db_delete_lobby(guild_id, lobby_id)
        except Exception as e:
            print(format_console(f"[lfg] Failed to delete lobby from DB: {type(e).__name__}: {e}", level="error"))

    async def _refresh_lobby_expiration(self, lobby: LFGLobby) -> None:
        """Called on interactions to reset the inactivity timeout."""
        try:
            expires_at = now_utc() + timedelta(minutes=LOBBY_INACTIVITY_MINUTES)
            await db_update_lobby_expires_at(lobby.guild_id, lobby.lobby_id, expires_at)
        except Exception as e:
            print(format_console(f"[lfg] Failed to refresh lobby expiration: {type(e).__name__}: {e}", level="error"))

    # ---------- Rehydration on startup ----------

    @commands.Cog.listener()
    async def on_ready(self):
        """Rehydrate lobbies from MongoDB after bot restart."""
        # Register persistent view (must happen after event loop starts)
        if not self._persistent_view_registered:
            self._persistent_view_registered = True
            self._persistent_view = PersistentLFGView(self)
            self.bot.add_view(self._persistent_view)
            print(format_console("[lfg] persistent views registered"))

        if self._rehydrated:
            return
        self._rehydrated = True

        try:
            # Clean up any expired lobbies first
            cleaned = await db_cleanup_expired_lobbies()
            if cleaned:
                print(format_console(f"[lfg] Cleaned up {cleaned} expired lobbies from DB"))

            # Load active lobbies
            docs = await db_get_all_active_lobbies()
            if not docs:
                print(format_console("[lfg] No active lobbies to rehydrate"))
                return

            print(format_console(f"[lfg] Rehydrating {len(docs)} lobbies from DB..."))
            rehydrated_count = 0

            for doc in docs:
                try:
                    rehydrated = await self._rehydrate_lobby(doc)
                    if rehydrated:
                        rehydrated_count += 1
                except Exception as e:
                    gid = doc.get('guild_id')
                    lid = doc.get('lobby_id')
                    print(format_console(f"[lfg] Failed to rehydrate lobby {gid}:{lid}: {type(e).__name__}: {e}", level="error"))

            print(format_console(f"[lfg] Successfully rehydrated {rehydrated_count}/{len(docs)} lobbies", level="ok"))

        except Exception as e:
            print(format_console(f"[lfg] Error during lobby rehydration: {type(e).__name__}: {e}", level="error"))

    async def _rehydrate_lobby(self, doc: Dict) -> bool:
        """Reconstruct a single lobby from a DB document. Returns True if successful."""
        guild_id = int(doc.get("guild_id", 0))
        lobby_id = int(doc.get("lobby_id", 0))
        channel_id = int(doc.get("channel_id", 0))
        message_id = doc.get("message_id")

        if not guild_id or not lobby_id or not channel_id:
            return False

        guild = self.bot.get_guild(guild_id)
        if not guild:
            print(format_console(f"[lfg] Rehydrate: guild {guild_id} not found, deleting lobby", level="warn"))
            await db_delete_lobby(guild_id, lobby_id)
            return False

        channel = guild.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            print(format_console(f"[lfg] Rehydrate: channel {channel_id} not found, deleting lobby", level="warn"))
            await db_delete_lobby(guild_id, lobby_id)
            return False

        # Check if message still exists
        if message_id:
            try:
                msg = await channel.fetch_message(int(message_id))
            except discord.NotFound:
                print(format_console(f"[lfg] Rehydrate: message {message_id} not found, deleting lobby", level="warn"))
                await db_delete_lobby(guild_id, lobby_id)
                return False
            except Exception as e:
                print(format_console(f"[lfg] Rehydrate: error fetching message {message_id}: {type(e).__name__}: {e}", level="error"))
                await db_delete_lobby(guild_id, lobby_id)
                return False
        else:
            # No message_id means the lobby was never fully set up
            await db_delete_lobby(guild_id, lobby_id)
            return False

        # Reconstruct the LFGLobby object
        host_elo = doc.get("host_elo")
        lobby = LFGLobby(
            guild_id=guild_id,
            channel_id=channel_id,
            host_id=int(doc.get("host_id", 0)),
            max_seats=int(doc.get("max_seats", 4)),
            invited_ids=[int(x) for x in (doc.get("invited_ids") or [])],
            elo_mode=bool(doc.get("elo_mode", False)),
            host_elo=float(host_elo) if host_elo is not None else None,
            elo_max_steps=int(doc.get("elo_max_steps", LFG_ELO_MAX_STEPS)),
        )

        lobby.lobby_id = lobby_id
        lobby.player_ids = [int(x) for x in (doc.get("player_ids") or [])]
        lobby.message_id = int(message_id)
        lobby.link = str(doc.get("link") or "")
        lobby.link_creating = bool(doc.get("link_creating", False))
        
        # Elo settings
        elo_base = doc.get("elo_base_range")
        elo_step = doc.get("elo_range_step")
        lobby.elo_base_range = int(elo_base) if elo_base is not None else None
        lobby.elo_range_step = int(elo_step) if elo_step is not None else None

        # Player points (convert string keys back to int)
        pts_doc = doc.get("player_pts") or {}
        lobby.player_pts = {int(k): float(v) for k, v in pts_doc.items()}

        # Timing
        created_at = doc.get("created_at")
        if isinstance(created_at, datetime):
            lobby.created_at = created_at
        
        almost_full_at = doc.get("almost_full_at")
        if isinstance(almost_full_at, datetime):
            lobby.almost_full_at = almost_full_at
        
        lobby.last_seat_open = bool(doc.get("last_seat_open", False))

        # Create a new view and attach it
        view = LFGJoinView(self, lobby, timeout_seconds=LOBBY_INACTIVITY_MINUTES * 60)
        lobby.view = view

        # Update max lobby_id counter
        async with self.state.lock:
            current_max = self.state._next_lobby_id
            if lobby_id >= current_max:
                self.state._next_lobby_id = lobby_id + 1

            # Add to in-memory state
            self._get_guild_lobbies(guild_id)[lobby_id] = lobby

        # Re-attach the view to the existing message
        try:
            embed = self._build_lobby_embed(guild, lobby)
            view._sync_open_last_seat_button()
            await msg.edit(embed=embed, view=view)
        except Exception as e:
            print(format_console(f"[lfg] Rehydrate: failed to update message view: {type(e).__name__}: {e}", level="warn"))
            # Still keep the lobby active, the buttons just won't work until next interaction

        # Restart Elo embed updater if needed
        if lobby.elo_mode and not lobby.has_link():
            self._ensure_elo_embed_updater(lobby)

        print(format_console(f"[lfg] Rehydrated lobby {guild_id}:{lobby_id} with {len(lobby.player_ids)} players"))
        return True

    # ---------- internal helpers ----------

    # ------------------------------------------------------------------------

    def _alloc_lobby_id(self) -> int:
        return self.state.alloc_lobby_id()

    def _get_guild_lobbies(self, guild_id: int) -> Dict[int, LFGLobby]:
        return self.state.get_guild_lobbies(guild_id)

    def _find_user_lobby(
        self,
        guild_id: int,
        user_id: int,
        *,
        exclude_lobby_id: Optional[int] = None,
    ) -> Optional[LFGLobby]:
        return self.state.find_user_lobby(
            guild_id,
            user_id,
            exclude_lobby_id=exclude_lobby_id,
        )

    def _clear_lobby(self, guild_id: int, lobby_id: int) -> None:
        lobby = self.state.remove_lobby(guild_id, lobby_id)
        if lobby and lobby.update_task:
            lobby.update_task.cancel()
        # Schedule DB deletion (fire-and-forget)
        asyncio.create_task(self._delete_lobby_from_db(guild_id, lobby_id))
        if lobby and lobby.update_task:
            lobby.update_task.cancel()

    def _is_lobby_active(self, lobby: LFGLobby) -> bool:
        return self.state.is_lobby_active(lobby) and not lobby.has_link() and not getattr(lobby, "link_creating", False)

    def _ensure_elo_embed_updater(self, lobby: LFGLobby) -> None:
        if not lobby.elo_mode:
            return
        if not self._is_lobby_active(lobby):
            return
        if lobby.update_task is None or lobby.update_task.done():
            lobby.update_task = asyncio.create_task(self._run_elo_embed_updater(lobby))

    async def _compute_dynamic_window(self, host_elo: float) -> Tuple[int, int]:
        return await compute_dynamic_window(
            host_elo,
            bracket_id=TOPDECK_BRACKET_ID,
            firebase_id_token=FIREBASE_ID_TOKEN,
            min_games=int(LFG_ELO_MIN_GAMES),
            base_range_default=int(LFG_ELO_BASE_RANGE),
            range_step_default=int(LFG_ELO_RANGE_STEP),
            target_pool_frac=float(LFG_ELO_TARGET_POOL_FRAC),
            min_base_range=int(LFG_ELO_MIN_BASE_RANGE),
            step_factor=float(LFG_ELO_STEP_FACTOR),
            min_range_step=int(LFG_ELO_MIN_RANGE_STEP),
            round_to=int(LFG_ELO_RANGE_ROUND_TO),
        )

    async def _maybe_announce_high_stakes(
        self,
        channel: discord.abc.Messageable,
        guild: discord.Guild,
        player_ids: List[int],
    ) -> None:
        try:
            print(
                "[lfg] high-stakes check: "
                f"bracket_set={bool(TOPDECK_BRACKET_ID)} wager_rate={WAGER_RATE} "
                f"threshold={HIGH_STAKES_THRESHOLD} players={player_ids}"
            )

            if not TOPDECK_BRACKET_ID:
                print("[lfg] high-stakes: skipped (TOPDECK_BRACKET_ID not set)")
                return
            if WAGER_RATE <= 0 or HIGH_STAKES_THRESHOLD <= 0:
                print(format_console("[lfg] high-stakes: skipped (feature disabled via envs)"))
                return
            if len(player_ids) != 4:
                print(format_console(f"[lfg] high-stakes: skipped (expected 4 players, got {len(player_ids)})"))
                return

            members: List[discord.Member] = []
            for uid in player_ids:
                m = guild.get_member(int(uid))
                if m is None:
                    try:
                        m = await guild.fetch_member(int(uid))
                    except Exception:
                        m = None
                if not isinstance(m, discord.Member):
                    print(format_console(f"[lfg] high-stakes: aborted (could not resolve member id={uid})", level="warn"))
                    return
                members.append(m)

            handle_to_best, _ = await get_handle_to_best_cached(
                TOPDECK_BRACKET_ID,
                FIREBASE_ID_TOKEN,
                force_refresh=False,
            )

            resolved: List[Tuple[discord.Member, float, int]] = []
            for m in members:
                found = resolve_points_games_from_map(m, handle_to_best)
                if found is None:
                    print(format_console(f"[lfg] high-stakes: aborted (no TopDeck mapping for {m} / {m.id})", level="warn"))
                    return
                pts, games = found
                resolved.append((m, float(pts), int(games)))

            stakes = [(m, pts * float(WAGER_RATE)) for (m, pts, _games) in resolved]
            pot = float(sum(stake for _m, stake in stakes))
            approx_pot = int(round(pot))

            print(format_console(f"[lfg] high-stakes calc: pot≈{approx_pot} threshold={HIGH_STAKES_THRESHOLD}"))
            for m, pts, _games in resolved:
                stake = pts * float(WAGER_RATE)
                print(format_console(f"[lfg]   player={m.display_name!r} id={m.id} pts={pts:.1f} stake≈{stake:.1f}"))

            if pot < float(HIGH_STAKES_THRESHOLD):
                print(format_console("[lfg] high-stakes: below threshold -> no announcement"))
                return

            print(format_console(f"[lfg] HIGH-STAKES POD DETECTED -> announcing pot≈{approx_pot}", level="ok"))
            await channel.send(
                f"🚨 **HIGH-STAKES POD DETECTED!** 🚨\n"
                f"The winner will take home ~**{approx_pot}** points."
            )

        except Exception as e:
            print(format_console(f"[lfg] Error in _maybe_announce_high_stakes: {type(e).__name__}: {e}", level="error"))


    # ---- Elo updater ----

    async def _run_elo_embed_updater(self, lobby: LFGLobby) -> None:
        try:
            while self._is_lobby_active(lobby) and (lobby.message_id is None or lobby.view is None):
                await asyncio.sleep(0.5)

            interval_sec = max(int(LFG_ELO_EXPAND_INTERVAL_MIN), 1) * 60
            grace_sec = max(int(LFG_ELO_LAST_SEAT_GRACE_MIN), 0) * 60

            while self._is_lobby_active(lobby):
                now = now_utc()

                rng = self._current_downward_range(lobby) or 0.0
                at_bottom = float(rng) >= float(self._max_downward_range(lobby))

                wake_at: Optional[datetime] = None

                if not at_bottom:
                    elapsed_sec = (now - lobby.created_at).total_seconds()
                    step = int(elapsed_sec // interval_sec)
                    wake_at = lobby.created_at + timedelta(seconds=(step + 1) * interval_sec)

                if lobby.remaining_slots() == 1 and not self._is_last_seat_open(lobby) and lobby.almost_full_at:
                    grace_at = lobby.almost_full_at + timedelta(seconds=grace_sec)
                    if wake_at is None or grace_at < wake_at:
                        wake_at = grace_at

                if wake_at is None:
                    break

                sleep_s = max(1.0, (wake_at - now).total_seconds())
                await asyncio.sleep(sleep_s)

                if not self._is_lobby_active(lobby):
                    break

                guild = self.bot.get_guild(lobby.guild_id)
                if not guild:
                    continue

                channel = guild.get_channel(lobby.channel_id)
                if not isinstance(channel, discord.TextChannel):
                    continue

                if not lobby.message_id or not lobby.view:
                    continue

                try:
                    msg = await channel.fetch_message(lobby.message_id)
                except Exception:
                    break

                embed = self._build_lobby_embed(guild, lobby)
                lobby.view._sync_open_last_seat_button()

                with contextlib.suppress(Exception):
                    await msg.edit(embed=embed, view=lobby.view)

        except asyncio.CancelledError:
            return
        except Exception:
            return

    def _max_downward_range(self, lobby: LFGLobby) -> float:
        return float(
            max_downward_range(
                lobby,
                base_range_default=int(LFG_ELO_BASE_RANGE),
                range_step_default=int(LFG_ELO_RANGE_STEP),
                max_steps_default=int(LFG_ELO_MAX_STEPS),
            )
        )

    def _current_downward_range(self, lobby: LFGLobby) -> Optional[float]:
        return current_downward_range(
            lobby,
            base_range_default=int(LFG_ELO_BASE_RANGE),
            range_step_default=int(LFG_ELO_RANGE_STEP),
            expand_interval_min=int(LFG_ELO_EXPAND_INTERVAL_MIN),
            max_steps_default=int(LFG_ELO_MAX_STEPS),
        )

    def _relaxed_last_seat_floor(self, lobby: LFGLobby) -> Optional[float]:
        return relaxed_last_seat_floor(
            lobby,
            base_range_default=int(LFG_ELO_BASE_RANGE),
            range_step_default=int(LFG_ELO_RANGE_STEP),
            expand_interval_min=int(LFG_ELO_EXPAND_INTERVAL_MIN),
            max_steps_default=int(LFG_ELO_MAX_STEPS),
            last_seat_min_rating=int(LFG_ELO_LAST_SEAT_MIN_RATING),
        )

    def _is_last_seat_open(self, lobby: LFGLobby) -> bool:
        return bool(is_last_seat_open(lobby, last_seat_grace_min=int(LFG_ELO_LAST_SEAT_GRACE_MIN)))

    def _effective_elo_floor(self, lobby: LFGLobby) -> Optional[float]:
        return effective_elo_floor(
            lobby,
            base_range_default=int(LFG_ELO_BASE_RANGE),
            range_step_default=int(LFG_ELO_RANGE_STEP),
            expand_interval_min=int(LFG_ELO_EXPAND_INTERVAL_MIN),
            max_steps_default=int(LFG_ELO_MAX_STEPS),
            last_seat_grace_min=int(LFG_ELO_LAST_SEAT_GRACE_MIN),
            last_seat_min_rating=int(LFG_ELO_LAST_SEAT_MIN_RATING),
        )

    async def _get_player_elo(self, member: discord.Member) -> Optional[Tuple[float, int]]:
        """Return (points, games) for the member based on TopDeck rows.

        Matching is done by normalizing the member's username/global_name/display_name
        and comparing to TopDeck's stored discord handle (normalized).
        """
        if not TOPDECK_BRACKET_ID:
            return None
        info = await get_member_points_games(
            member,
            bracket_id=TOPDECK_BRACKET_ID,
            firebase_id_token=FIREBASE_ID_TOKEN,
            force_refresh=False,
        )
        if info is None:
            return None
        pts, games = info
        return float(pts), int(games)

    def _build_lobby_embed(self, guild: discord.Guild, lobby: LFGLobby) -> discord.Embed:
        elo_info: Optional[EloLobbyInfo] = None

        if lobby.elo_mode and lobby.host_elo is not None:
            floor = self._effective_elo_floor(lobby)
            if floor is not None:
                rng = float(self._current_downward_range(lobby) or 0.0)
                at_bottom = rng >= float(self._max_downward_range(lobby))

                last_seat: Optional[LastSeatInfo] = None
                if lobby.remaining_slots() == 1:
                    is_open = self._is_last_seat_open(lobby)

                    relaxed_floor = self._relaxed_last_seat_floor(lobby)
                    last_seat_floor = int(relaxed_floor if relaxed_floor is not None else floor)

                    minutes_left: Optional[int] = None
                    if (not is_open) and lobby.almost_full_at:
                        grace_at = lobby.almost_full_at + timedelta(minutes=int(LFG_ELO_LAST_SEAT_GRACE_MIN))
                        secs_left = (grace_at - now_utc()).total_seconds()
                        minutes_left = max(0, int((secs_left + 59) // 60))  # ceil to minutes

                    last_seat = LastSeatInfo(
                        is_open=is_open,
                        min_rating=last_seat_floor,
                        minutes_left=minutes_left,
                    )

                elo_info = EloLobbyInfo(
                    host_elo=int(lobby.host_elo),
                    min_rating=int(floor),
                    at_bottom=bool(at_bottom),
                    last_seat=last_seat,
                )

        return build_lobby_embed(
            guild,
            lobby,
            updated_at=now_utc(),
            icon_url=LFG_EMBED_ICON_URL,
            elo_info=elo_info,
            expand_interval_min=int(LFG_ELO_EXPAND_INTERVAL_MIN),
            last_seat_grace_min=int(LFG_ELO_LAST_SEAT_GRACE_MIN),
        )




    async def _build_ready_embed(self, guild: discord.Guild, lobby: LFGLobby, started_at: datetime) -> discord.Embed:
        pts_by_id: Dict[int, int] = {}

        # ✅ only fetch/show points for Elo lobbies
        if lobby.elo_mode and TOPDECK_BRACKET_ID:
            try:
                handle_to_best, _ = await get_handle_to_best_cached(
                    TOPDECK_BRACKET_ID,
                    FIREBASE_ID_TOKEN,
                    force_refresh=False,
                )

                for uid in (lobby.player_ids or []):
                    member = guild.get_member(int(uid))
                    if member is None:
                        with contextlib.suppress(Exception):
                            member = await guild.fetch_member(int(uid))

                    if not isinstance(member, discord.Member):
                        continue

                    found = resolve_points_games_from_map(member, handle_to_best)
                    if found is None:
                        continue

                    pts, _games = found
                    pts_by_id[int(uid)] = int(round(float(pts)))

            except Exception as e:
                print(format_console(f"[lfg] Error fetching pts for ready embed: {type(e).__name__}: {e}", level="error"))

        return build_ready_embed(
            guild,
            lobby,
            started_at=started_at,
            icon_url=LFG_EMBED_ICON_URL,
            pts_by_id=pts_by_id or None,  # will be None for normal /lfg
        )


    async def _handle_open_last_seat(
        self,
        interaction: discord.Interaction,
        view: LFGJoinView,
        button: discord.ui.Button,
    ):
        return await handle_open_last_seat(self, interaction, view, button)

    async def _handle_join(
        self,
        interaction: discord.Interaction,
        view: LFGJoinView,
        button: discord.ui.Button,
    ):
        return await handle_join(
            self,
            interaction,
            view,
            button,
            elo_min_games=int(LFG_ELO_MIN_GAMES),
            last_seat_grace_min=int(LFG_ELO_LAST_SEAT_GRACE_MIN),
        )

    async def _handle_leave(
        self,
        interaction: discord.Interaction,
        view: LFGJoinView,
        button: discord.ui.Button,
    ):
        return await handle_leave(self, interaction, view, button)


    # ---------- /lfg (normal) ----------

    @commands.slash_command(
        name="lfg",
        description="Open a SpellTable Commander lobby (4 players max).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def lfg(
        self,
        ctx: discord.ApplicationContext,
        friends: Optional[str] = Option(
            str,
            "Mention one or more friends to auto-fill the pod (optional).",
            required=False,
        ),
    ):
        if ctx.guild is None:
            await safe_ctx_respond(ctx, "This command can only be used in a server.", ephemeral=True)
            return

        already_in_lobby = False
        async with self.state.lock:
            already_in_lobby = self._find_user_lobby(ctx.guild.id, ctx.author.id) is not None

        if already_in_lobby:
            await safe_ctx_respond(
                ctx,
                "You're already in an active lobby in this server. Leave it before creating a new one.",
                ephemeral=True,
            )
            return

        await safe_ctx_defer(ctx, label="lfg")

        raw_invited_ids: List[int] = []
        if friends:
            for token in friends.split():
                if token.startswith("<@") and token.endswith(">"):
                    cleaned = token.strip("<@!>")
                    if cleaned.isdigit():
                        raw_invited_ids.append(int(cleaned))

        invited_ids: List[int] = []
        for uid in raw_invited_ids:
            if uid == ctx.author.id:
                continue
            if uid not in invited_ids:
                invited_ids.append(uid)

        if len(invited_ids) != 3:
            if await try_join_existing_for_lfg(self, ctx, invited_ids, elo_min_games=int(LFG_ELO_MIN_GAMES)):
                return

        full_lobby: Optional[LFGLobby] = None
        lobby: Optional[LFGLobby] = None

        async with self.state.lock:
            lobby = LFGLobby(
                guild_id=ctx.guild.id,
                channel_id=ctx.channel.id,
                host_id=ctx.author.id,
                max_seats=4,
                invited_ids=invited_ids,
                elo_mode=False,
                host_elo=None,
                elo_max_steps=int(LFG_ELO_MAX_STEPS),
            )
            lobby.lobby_id = self._alloc_lobby_id()

            for uid in invited_ids:
                if len(lobby.player_ids) >= lobby.max_seats:
                    break
                if uid in lobby.player_ids:
                    continue
                if self._find_user_lobby(ctx.guild.id, uid) is not None:
                    continue
                lobby.player_ids.append(uid)

            if lobby.is_full():
                full_lobby = lobby
            else:
                self._get_guild_lobbies(ctx.guild.id)[lobby.lobby_id] = lobby

        if full_lobby is not None:
            try:
                link = await create_spelltable_game(
                    game_name="ECL DragonShield",
                    format_name="Commander",
                    is_public=False,
                )
                full_lobby.link = link
            except Exception as e:
                print(format_console(f"[lfg] Failed to create SpellTable game (friends fill): {e}", level="error"))
                await safe_ctx_followup(
                    ctx,
                    "I couldn't create a SpellTable game right now. Please try again in a bit or ping a mod.",
                    ephemeral=True,
                )
                return

            started_at = now_utc()
            ready_embed = await self._build_ready_embed(ctx.guild, full_lobby, started_at)

            msg = await safe_ctx_followup(ctx, embed=ready_embed)

            with contextlib.suppress(Exception):
                await self._maybe_announce_high_stakes(msg.channel, ctx.guild, full_lobby.player_ids)

            for uid in full_lobby.player_ids:
                member = ctx.guild.get_member(uid)
                if not member:
                    continue
                with contextlib.suppress(discord.Forbidden):
                    await member.send(embed=ready_embed)

            return

        if lobby is None:
            await safe_ctx_followup(ctx, "Something went wrong creating the lobby.", ephemeral=True)
            return

        embed = self._build_lobby_embed(ctx.guild, lobby)
        view = LFGJoinView(self, lobby, timeout_seconds=LOBBY_INACTIVITY_MINUTES * 60)
        lobby.view = view

        try:
            msg = await safe_ctx_followup(ctx, embed=embed, view=view)
        except Exception as e:
            print(format_console(f"[lfg] Failed to send lobby message: {e}", level="error"))
            self._clear_lobby(ctx.guild.id, lobby.lobby_id)
            await safe_ctx_followup(ctx, "Something went wrong creating the lobby message.", ephemeral=True)
            return

        lobby.message_id = msg.id

    # ---------- /lfgelo ----------

    @commands.slash_command(
        name="lfgelo",
        description="Open an Elo-matched SpellTable Commander lobby (4 players max).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def lfgelo(self, ctx: discord.ApplicationContext):
        if ctx.guild is None:
            await safe_ctx_respond(ctx, "This command can only be used in a server.", ephemeral=True)
            return

        today_day = datetime.now(timezone.utc).day
        if today_day < LFG_ELO_MIN_DAY:
            await safe_ctx_respond(
                ctx,
                f"/lfgelo will only be available from day {LFG_ELO_MIN_DAY} of the month.",
                ephemeral=True,
            )
            return

        already_in_lobby = False
        async with self.state.lock:
            already_in_lobby = self._find_user_lobby(ctx.guild.id, ctx.author.id) is not None

        if already_in_lobby:
            await safe_ctx_respond(
                ctx,
                "You're already in an active lobby in this server. Leave it before creating a new one.",
                ephemeral=True,
            )
            return

        await safe_ctx_defer(ctx, label="lfgelo")

        if not isinstance(ctx.author, discord.Member):
            await safe_ctx_followup(ctx, "Only server members can use /lfgelo.", ephemeral=True)
            return

        if await try_join_existing_for_lfgelo(self, ctx, elo_min_games=int(LFG_ELO_MIN_GAMES)):
            return

        host_info = await self._get_player_elo(ctx.author)
        if host_info is None:
            await safe_ctx_followup(
                ctx,
                "You don't have a league rating yet, so you can't host /lfgelo pods.\n"
                "Use /lfg instead or play some matches first!",
                ephemeral=True,
            )
            return

        host_elo, host_games = host_info
        if host_games < LFG_ELO_MIN_GAMES:
            await safe_ctx_followup(
                ctx,
                f"You need at least **{LFG_ELO_MIN_GAMES}** league games to host /lfgelo.\n"
                f"You currently have **{host_games}**.\n"
                "Use /lfg for now and come back once you've got more games logged.",
                ephemeral=True,
            )
            return

        base_range, range_step = await self._compute_dynamic_window(host_elo)

        lobby: Optional[LFGLobby] = None
        async with self.state.lock:
            lobby = LFGLobby(
                guild_id=ctx.guild.id,
                channel_id=ctx.channel.id,
                host_id=ctx.author.id,
                max_seats=4,
                invited_ids=[],
                elo_mode=True,
                host_elo=host_elo,
                elo_max_steps=int(LFG_ELO_MAX_STEPS),
            )
            lobby.elo_base_range = int(base_range)
            lobby.elo_range_step = int(range_step)

            lobby.lobby_id = self._alloc_lobby_id()
            self._get_guild_lobbies(ctx.guild.id)[lobby.lobby_id] = lobby

        if lobby is None:
            await safe_ctx_followup(ctx, "Something went wrong creating the Elo lobby.", ephemeral=True)
            return

        embed = self._build_lobby_embed(ctx.guild, lobby)
        view = LFGJoinView(self, lobby, timeout_seconds=LOBBY_INACTIVITY_MINUTES * 60)
        lobby.view = view

        try:
            msg = await safe_ctx_followup(ctx, embed=embed, view=view)
        except Exception as e:
            print(f"[lfgelo] Failed to send Elo lobby message: {e}")
            self._clear_lobby(ctx.guild.id, lobby.lobby_id)
            await safe_ctx_followup(ctx, "Something went wrong creating the Elo lobby.", ephemeral=True)
            return

        lobby.message_id = msg.id
        lobby.update_task = asyncio.create_task(self._run_elo_embed_updater(lobby))


def setup(bot: commands.Bot):
    bot.add_cog(LFGCog(bot))
