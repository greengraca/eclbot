# cogs/timer_cog.py
import os
import re
import asyncio
import imageio_ffmpeg
import contextlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any

import discord
from discord.ext import commands
from discord import Option

from online_games_store import OnlineGameRecord, get_record, upsert_record

from topdeck_fetch import (
    get_in_progress_pods,
    InProgressPod,
)

from utils.interactions import safe_ctx_defer, safe_ctx_respond, safe_ctx_followup
from utils.persistence import (
    save_timer as db_save_timer,
    delete_timer as db_delete_timer,
    get_all_active_timers as db_get_all_active_timers,
    cleanup_expired_timers as db_cleanup_expired_timers,
)
from utils.logger import log_sync, log_ok, log_warn, log_error, log_debug

# Import from timer submodule
from .timer import (
    env_float as _env_float,
    now_utc,
    ts,
    month_start_utc as _month_start_utc,
    make_timer_id,
    norm_handle as _norm_handle,
    norm_member_handles as _norm_member_handles,
    same_channel as _same_channel,
    voice_prereqs_ok as _voice_prereqs_ok,
    ffmpeg_src as _ffmpeg_src,
    non_bot_members as _non_bot_members,
    build_progress_bar as _build_progress_bar,
    VOICE_CONNECT_TIMEOUT,
    ReplaceTimerView,
    TopDeckTagger,
)


# ---------------- env / config ----------------

GUILD_ID = int(os.getenv("GUILD_ID", "0"))

ECL_MOD_ROLE_ID = int(os.getenv("ECL_MOD_ROLE_ID", "0"))
ECL_MOD_ROLE_NAME = os.getenv("ECL_MOD_ROLE_NAME", "ECL MOD")


# Main round duration in minutes
TIMER_MINUTES: float = _env_float("TIMER_MINUTES", 80.0)

# Extra time for turns in minutes
EXTRA_TURNS_MINUTES: float = _env_float("EXTRA_TURNS_MINUTES", 20.0)

# offset: minutes BEFORE main time end when it should play
OFFSET_MINUTES: float = _env_float("OFFSET_MINUTES", 10.0)

# Audio file paths (override via env if needed)
TIMER_START_AUDIO: str = os.getenv("TIMER_START_AUDIO", "./timer/timer80.mp3")
TEN_TO_END_AUDIO: str = os.getenv("TEN_TO_END_AUDIO", "./timer/10minutestoend.mp3")
EXTRA_TIME_AUDIO: str = os.getenv("EXTRA_TIME_AUDIO", "./timer/ap20minutes.mp3")
FINAL_DRAW_AUDIO: str = os.getenv("FINAL_DRAW_AUDIO", "./timer/ggboyz.mp3")

try:
    FFMPEG_EXE = imageio_ffmpeg.get_ffmpeg_exe()
    log_sync(f"[voice] Using ffmpeg from imageio-ffmpeg: {FFMPEG_EXE}")
except Exception as e:
    FFMPEG_EXE = "ffmpeg"
    log_warn(f"[voice] Failed to get imageio-ffmpeg binary, falling back to 'ffmpeg': {e}")

# --- TopDeck / online-games (Mongo) config (shared with other cogs) ---

TOPDECK_BRACKET_ID = os.getenv("TOPDECK_BRACKET_ID", "")
FIREBASE_ID_TOKEN = os.getenv("FIREBASE_ID_TOKEN", None)
SPELLBOT_LFG_CHANNEL_ID = int(os.getenv("SPELLBOT_LFG_CHANNEL_ID", "0"))


# Helpers, views, and progress bar imported from .timer submodule

# ---------------- Cog ----------------


class ECLTimerCog(commands.Cog):
    """
    Multi-room timer system.

    - /timer <game>       → start timer for VC 'ECL Game <game>'
    - /endtimer <game>    → stop that room's timer
    - /pausetimer <game>  → pause
    - /resumetimer <game> → resume

    Constraints:
    - Timer only starts if that VC has ≥ 3 non-bot members.
    - If VC drops below 2 non-bot members, timer auto-stops (with a mod-testing exception).
    - Bot only plays audio in 1 VC at a time (per-guild voice lock).
    - If room already has a timer, user gets buttons to keep/replace.
    
    Persists timer state to MongoDB for Heroku restart survival.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._rehydrated = False

        # timer_id -> metadata
        self.active_timers: Dict[str, Dict] = {}
        self.paused_timers: Dict[str, Dict] = {}

        # voice_channel_id -> latest seq number (for timer_id)
        self.voice_channel_timers: Dict[int, int] = {}

        # timer_id -> (channel_id, message_id)
        self.timer_messages: Dict[str, tuple[int, int]] = {}

        # timer_id -> list[asyncio.Task]
        self.timer_tasks: Dict[str, List[asyncio.Task]] = {}

        # guild_id -> asyncio.Lock (serialize voice ops per guild)
        self._voice_locks: Dict[int, asyncio.Lock] = {}

        # TopDeck online game tagger
        self.topdeck_tagger = TopDeckTagger(
            self,
            bracket_id=TOPDECK_BRACKET_ID,
            firebase_token=FIREBASE_ID_TOKEN,
        )

        log_sync(
            "[timer] init "
            f"TIMER_MINUTES={TIMER_MINUTES}, "
            f"EXTRA_TURNS_MINUTES={EXTRA_TURNS_MINUTES}, "
            f"OFFSET_MINUTES={OFFSET_MINUTES}"
        )

    # ---------- Persistence helpers ----------

    async def _save_timer_to_db(
        self,
        timer_id: str,
        guild_id: int,
        channel_id: int,
        voice_channel_id: int,
        message_id: Optional[int],
        status: str,  # "active" | "paused"
        start_time_utc: Optional[datetime],
        durations: Dict[str, float],
        remaining: Optional[Dict[str, float]],
        ignore_autostop: bool,
        messages: Dict[str, str],
        audio: Dict[str, str],
    ) -> None:
        """Persist timer state to MongoDB."""
        try:
            # Calculate when the timer fully expires
            if status == "active" and start_time_utc:
                total_duration = durations.get("main", 0) + durations.get("extra", 0)
                expires_at = start_time_utc + timedelta(seconds=total_duration + 60)  # +1 min buffer
            elif status == "paused" and remaining:
                total_remaining = remaining.get("extra", 0)  # extra is total remaining
                expires_at = now_utc() + timedelta(seconds=total_remaining + 3600)  # +1 hour for paused
            else:
                expires_at = now_utc() + timedelta(hours=2)

            await db_save_timer(
                timer_id=timer_id,
                guild_id=guild_id,
                channel_id=channel_id,
                voice_channel_id=voice_channel_id,
                message_id=message_id,
                status=status,
                start_time_utc=start_time_utc,
                durations=durations,
                remaining=remaining,
                ignore_autostop=ignore_autostop,
                messages=messages,
                audio=audio,
                expires_at=expires_at,
            )
        except Exception as e:
            log_error(f"[timer] Failed to persist timer {timer_id}: {type(e).__name__}: {e}")

    async def _delete_timer_from_db(self, timer_id: str) -> None:
        """Remove timer from DB."""
        try:
            await db_delete_timer(timer_id)
        except Exception as e:
            log_error(f"[timer] Failed to delete timer from DB: {type(e).__name__}: {e}")

    # ---------- Rehydration on startup ----------

    @commands.Cog.listener()
    async def on_ready(self):
        """Rehydrate timers from MongoDB after bot restart."""
        if self._rehydrated:
            return
        self._rehydrated = True

        try:
            # Clean up expired timers first
            cleaned = await db_cleanup_expired_timers()
            if cleaned:
                log_sync(f"[timer] Cleaned up {cleaned} expired timers from DB")

            # Load active/paused timers
            docs = await db_get_all_active_timers()
            if not docs:
                log_sync("[timer] No active timers to rehydrate")
                return

            log_sync(f"[timer] Rehydrating {len(docs)} timers from DB...")
            rehydrated_count = 0

            for doc in docs:
                try:
                    rehydrated = await self._rehydrate_timer(doc)
                    if rehydrated:
                        rehydrated_count += 1
                except Exception as e:
                    timer_id = doc.get("timer_id", "?")
                    log_error(f"[timer] Failed to rehydrate timer {timer_id}: {type(e).__name__}: {e}")

            log_ok(f"[timer] Successfully rehydrated {rehydrated_count}/{len(docs)} timers")

        except Exception as e:
            log_error(f"[timer] Error during timer rehydration: {type(e).__name__}: {e}")

    async def _rehydrate_timer(self, doc: Dict) -> bool:
        """Reconstruct a single timer from a DB document. Returns True if successful."""
        timer_id = doc.get("timer_id")
        if not timer_id:
            return False

        guild_id = int(doc.get("guild_id", 0))
        channel_id = int(doc.get("channel_id", 0))
        voice_channel_id = int(doc.get("voice_channel_id", 0))
        message_id = doc.get("message_id")
        status = doc.get("status", "active")

        if not guild_id or not channel_id or not voice_channel_id:
            await self._delete_timer_from_db(timer_id)
            return False

        guild = self.bot.get_guild(guild_id)
        if not guild:
            log_sync(f"[timer] Rehydrate: guild {guild_id} not found, deleting timer", level="warn")
            await self._delete_timer_from_db(timer_id)
            return False

        channel = guild.get_channel(channel_id)
        if not isinstance(channel, (discord.TextChannel, discord.abc.Messageable)):
            log_sync(f"[timer] Rehydrate: channel {channel_id} not found, deleting timer", level="warn")
            await self._delete_timer_from_db(timer_id)
            return False

        voice_channel = guild.get_channel(voice_channel_id)
        if not isinstance(voice_channel, discord.VoiceChannel):
            log_sync(f"[timer] Rehydrate: voice channel {voice_channel_id} not found, deleting timer", level="warn")
            await self._delete_timer_from_db(timer_id)
            return False

        # Extract timer data
        durations = {
            "main": float(doc.get("durations_main", 0)),
            "easter_egg": float(doc.get("durations_easter_egg", 0)),
            "extra": float(doc.get("durations_extra", 0)),
        }
        messages = {
            "turns": str(doc.get("msg_turns", "")),
            "final": str(doc.get("msg_final", "")),
        }
        audio = {
            "turns": str(doc.get("audio_turns", "")),
            "final": str(doc.get("audio_final", "")),
            "easter_egg": str(doc.get("audio_easter_egg", "")),
        }
        ignore_autostop = bool(doc.get("ignore_autostop", False))

        # Update voice_channel_timers seq counter
        vc_id = voice_channel_id
        parts = timer_id.split("_")
        if len(parts) == 2:
            try:
                seq = int(parts[1])
                current_seq = self.voice_channel_timers.get(vc_id, 0)
                if seq > current_seq:
                    self.voice_channel_timers[vc_id] = seq
            except ValueError:
                pass

        if status == "paused":
            # Rehydrate as paused
            remaining = {
                "main": float(doc.get("remaining_main") or 0),
                "easter_egg": float(doc.get("remaining_easter_egg") or 0),
                "extra": float(doc.get("remaining_extra") or 0),
            }
            self.paused_timers[timer_id] = {
                "remaining": remaining,
                "messages": messages,
                "audio": audio,
                "voice_channel_id": voice_channel_id,
                "ignore_autostop": ignore_autostop,
                "pause_message": None,  # Can't restore Discord message objects
                "ctx": None,  # Can't restore ctx
            }
            if message_id:
                self.timer_messages[timer_id] = (channel_id, int(message_id))
            log_sync(f"[timer] Rehydrated paused timer {timer_id}")
            return True

        # Rehydrate as active timer
        start_time_utc = doc.get("start_time_utc")
        if not isinstance(start_time_utc, datetime):
            log_sync(f"[timer] Rehydrate: invalid start_time for {timer_id}, deleting", level="warn")
            await self._delete_timer_from_db(timer_id)
            return False

        # Motor/PyMongo often returns UTC datetimes as offset-naive.
        # Normalize to tz-aware UTC so math vs now_utc() never crashes.
        if start_time_utc.tzinfo is None:
            start_time_utc = start_time_utc.replace(tzinfo=timezone.utc)
        else:
            start_time_utc = start_time_utc.astimezone(timezone.utc)

        # Calculate remaining time
        elapsed = (now_utc() - start_time_utc).total_seconds()
        main_remaining = max(0, durations["main"] - elapsed)
        total_remaining = max(0, durations["main"] + durations["extra"] - elapsed)
        easter_egg_remaining = max(0, durations["easter_egg"] - elapsed)

        # If timer has fully expired, clean up
        if total_remaining <= 0:
            log_sync(f"[timer] Rehydrate: timer {timer_id} has expired, cleaning up", level="warn")
            await self._delete_timer_from_db(timer_id)
            return False

        # Store in active_timers
        self.active_timers[timer_id] = {
            "start_time": start_time_utc,
            "durations": durations,
            "messages": messages,
            "audio": audio,
            "voice_channel_id": voice_channel_id,
            "ignore_autostop": ignore_autostop,
        }
        if message_id:
            self.timer_messages[timer_id] = (channel_id, int(message_id))

        # Initialize task list
        self.timer_tasks[timer_id] = []

        # Schedule remaining events
        # We need to calculate what events are still pending
        main_seconds = durations["main"]
        extra_seconds = durations["extra"]
        easter_egg_delay = durations["easter_egg"]

        # Event 1: Easter egg (10 min warning)
        if easter_egg_remaining > 0:
            self.timer_tasks[timer_id].append(asyncio.create_task(
                self._rehydrated_play_voice(
                    guild,
                    audio["easter_egg"],
                    easter_egg_remaining,
                    voice_channel_id,
                    timer_id,
                )
            ))

        # Event 2: Main time end (turns message + audio)
        if main_remaining > 0:
            self.timer_tasks[timer_id].append(asyncio.create_task(
                self._rehydrated_timer_end(
                    guild,
                    channel,
                    main_remaining,
                    messages["turns"],
                    audio["turns"],
                    timer_id,
                    voice_channel_id,
                )
            ))
        elif main_remaining <= 0 and total_remaining > 0:
            # Main time passed but extra time remains - don't re-send turns message
            pass

        # Event 3: Final (extra time end)
        if total_remaining > 0:
            self.timer_tasks[timer_id].append(asyncio.create_task(
                self._rehydrated_final_timer_end(
                    guild,
                    channel,
                    total_remaining,
                    messages["final"],
                    audio["final"],
                    timer_id,
                    voice_channel_id,
                )
            ))

        log_sync(f"[timer] Rehydrated active timer {timer_id} with {len(self.timer_tasks[timer_id])} remaining events")
        return True

    async def _rehydrated_play_voice(
        self,
        guild: discord.Guild,
        audio_path: str,
        delay_seconds: float,
        voice_channel_id: int,
        timer_id: str,
    ) -> None:
        """Play voice file after delay (rehydrated timer)."""
        await asyncio.sleep(max(0, delay_seconds))
        if timer_id not in self.active_timers:
            return
        await self._play(guild, audio_path, channel_id=voice_channel_id, leave_after=True)

    async def _rehydrated_timer_end(
        self,
        guild: discord.Guild,
        channel: discord.abc.Messageable,
        delay_seconds: float,
        message: str,
        audio_path: str,
        timer_id: str,
        voice_channel_id: int,
    ) -> None:
        """Handle main timer end (rehydrated timer)."""
        await asyncio.sleep(max(0, delay_seconds))
        if timer_id not in self.active_timers:
            return

        # Edit or send message
        if timer_id in self.timer_messages:
            ch_id, m_id = self.timer_messages[timer_id]
            ch = self.bot.get_channel(ch_id)
            if ch:
                try:
                    msg = await ch.fetch_message(m_id)
                    await msg.edit(content=message)
                except Exception as e:
                    log_warn(f"[timer] Rehydrated: failed to edit message: {e}")
        else:
            try:
                msg = await channel.send(message)
                self.timer_messages[timer_id] = (channel.id, msg.id)
            except Exception as e:
                log_warn(f"[timer] Rehydrated: failed to send message: {e}")

        # Play audio
        await self._play(guild, audio_path, channel_id=voice_channel_id, leave_after=True)

    async def _rehydrated_final_timer_end(
        self,
        guild: discord.Guild,
        channel: discord.abc.Messageable,
        delay_seconds: float,
        message: str,
        audio_path: str,
        timer_id: str,
        voice_channel_id: int,
    ) -> None:
        """Handle final timer end (rehydrated timer)."""
        await asyncio.sleep(max(0, delay_seconds))
        if timer_id not in self.active_timers:
            return

        # Edit or send message
        msg_obj = None
        if timer_id in self.timer_messages:
            ch_id, m_id = self.timer_messages[timer_id]
            ch = self.bot.get_channel(ch_id)
            if ch:
                try:
                    msg_obj = await ch.fetch_message(m_id)
                    await msg_obj.edit(content=message)
                except Exception:
                    pass
        
        if msg_obj is None:
            try:
                msg_obj = await channel.send(message)
            except Exception:
                pass

        # Play audio
        await self._play(guild, audio_path, channel_id=voice_channel_id, leave_after=True)

        # Delete message after 1 minute
        if msg_obj:
            await asyncio.sleep(60)
            with contextlib.suppress(Exception):
                await msg_obj.delete()

        # Final cleanup
        self._cleanup_timer_structs(timer_id)
        await self._delete_timer_from_db(timer_id)

    # ---------- mod helpers ----------

    def _is_mod_member(self, member: Optional[discord.Member]) -> bool:
        if not member:
            return False
        for role in getattr(member, "roles", []) or []:
            if (ECL_MOD_ROLE_ID and role.id == ECL_MOD_ROLE_ID) or (
                ECL_MOD_ROLE_NAME and role.name == ECL_MOD_ROLE_NAME
            ):
                return True
        return False

    def _ignore_autostop_for_start(
        self,
        member: Optional[discord.Member],
        voice_channel: discord.VoiceChannel,
    ) -> bool:
        """
        Only ignore auto-stop for "testing":
        - caller is a mod
        - and VC has 1 or 2 non-bot members at start
        """
        if not self._is_mod_member(member):
            return False
        return len(_non_bot_members(voice_channel)) <= 2

    # ---------- voice utils ----------

    def _vlock(self, gid: int) -> asyncio.Lock:
        return self._voice_locks.setdefault(gid, asyncio.Lock())

    async def _hard_reset_voice(self, guild: discord.Guild):
        with contextlib.suppress(Exception):
            if guild.voice_client:
                await guild.voice_client.disconnect(force=True)
        await asyncio.sleep(0.5)

    async def _ensure_connected(
        self,
        guild: discord.Guild,
        target_ch: Optional[discord.VoiceChannel],
    ) -> Optional[discord.VoiceClient]:
        if not target_ch:
            return None

        vc = guild.voice_client
        if vc and vc.is_connected():
            if not _same_channel(vc, target_ch):
                log_sync(
                    f"[voice] Moving VC in guild {guild.id} "
                    f"from {getattr(vc.channel, 'id', None)} to {target_ch.id}"
                )
                with contextlib.suppress(Exception):
                    await vc.move_to(target_ch)
            return guild.voice_client

        log_sync(
            f"[voice] Connecting new VC in guild {guild.id} "
            f"to channel {target_ch.id}"
        )
        return await target_ch.connect(reconnect=True, timeout=VOICE_CONNECT_TIMEOUT)

    async def _play(
        self,
        guild: discord.Guild,
        source_path: Optional[str],
        *,
        channel_id: Optional[int] = None,
        leave_after: bool = True,
    ) -> bool:
        """Connect/move, play a file, optionally leave after. Queued per guild."""
        if not source_path or not guild:
            return False

        if not _voice_prereqs_ok():
            log_warn("[voice] Prereqs not OK; skipping playback")
            return False

        if not os.path.exists(source_path):
            log_warn(f"[voice] File not found: {source_path}")
            return False

        async with self._vlock(guild.id):
            ch = guild.get_channel(channel_id) if channel_id else None
            if not isinstance(ch, discord.VoiceChannel):
                log_warn(
                    f"[voice] Target channel is not a VoiceChannel "
                    f"(guild={guild.id}, channel_id={channel_id})"
                )
                return False

            log_sync(
                f"[voice] _play called: guild={guild.id}, "
                f"source_path={source_path}, channel_id={ch.id}, leave_after={leave_after}"
            )

            async def connect_and_play() -> bool:
                vc = await self._ensure_connected(guild, ch)
                if not vc:
                    log_warn("[voice] Failed to obtain VoiceClient")
                    return False

                log_sync(
                    f"[voice] Starting playback in guild {guild.id}, "
                    f"channel {ch.id}, file={source_path}"
                )
                try:
                    task = vc.play(_ffmpeg_src(source_path, FFMPEG_EXE), wait_finish=True)
                except Exception as e:
                    log_error(f"[voice] vc.play() raised: {e}")
                    return False

                if task is not None:
                    try:
                        err = await task
                        if err:
                            raise err
                    except Exception as e:
                        log_error(f"[voice] Playback error: {e}")
                        return False

                log_sync(
                    f"[voice] Finished playback in guild {guild.id}, channel {ch.id}"
                )
                return True

            try:
                ok = await connect_and_play()
            except asyncio.TimeoutError:
                log_warn(
                    f"[voice] Timeout while connecting/playing in "
                    f"guild={guild.id}, channel_id={channel_id}"
                )
                ok = False
            except discord.errors.ConnectionClosed:
                log_warn(
                    "[voice] ConnectionClosed during playback; "
                    "hard-resetting and retrying once"
                )
                await self._hard_reset_voice(guild)
                try:
                    ok = await connect_and_play()
                except asyncio.TimeoutError:
                    log_warn(
                        f"[voice] Timeout again after hard reset in "
                        f"guild={guild.id}, channel_id={channel_id}"
                    )
                    ok = False

            if leave_after:
                log_sync(f"[voice] Disconnecting from guild {guild.id} voice")
                with contextlib.suppress(Exception):
                    if guild.voice_client and guild.voice_client.is_connected():
                        await guild.voice_client.disconnect(force=True)

            log_debug(f"[voice] _play returning {ok}")
            return ok

    # ---------- channel/timer helpers ----------

    def _get_game_channel(
        self, guild: discord.Guild, game_number: int
    ) -> Optional[discord.VoiceChannel]:
        """Find the voice channel named 'ECL Game <game_number>'."""
        target_name = f"ECL Game {game_number}".lower()
        for ch in guild.voice_channels:
            if ch.name.lower() == target_name:
                return ch
        return None

    def _current_timer_id_for_channel(self, channel_id: int) -> Optional[str]:
        seq = self.voice_channel_timers.get(channel_id)
        if not seq:
            return None
        tid = make_timer_id(channel_id, seq)
        if tid in self.active_timers or tid in self.paused_timers:
            return tid
        return None

    def _timer_owner_id(self, timer_id: str) -> Optional[int]:
        """Return the user ID of whoever started this timer (if we can)."""
        data = self.active_timers.get(timer_id) or self.paused_timers.get(timer_id)
        if not data:
            return None

        ctx = data.get("ctx")
        if ctx is None or not hasattr(ctx, "author"):
            return None

        return getattr(ctx.author, "id", None)

    def _cleanup_timer_structs(self, timer_id: str) -> None:
        self.active_timers.pop(timer_id, None)
        self.paused_timers.pop(timer_id, None)
        self.timer_messages.pop(timer_id, None)
        self.timer_tasks.pop(timer_id, None)


    def _caller_in_vc(self, member: Optional[discord.Member], vc: discord.VoiceChannel) -> bool:
        if not member or not member.voice or not isinstance(member.voice.channel, discord.VoiceChannel):
            return False
        return member.voice.channel.id == vc.id


    async def _caller_is_pod_player(
        self,
        member: discord.Member,
        vc: discord.VoiceChannel,
    ) -> Optional[bool]:
        """
        Returns:
        True  -> verified pod player
        False -> verified NOT a pod player
        None  -> couldn't resolve pod (TopDeck mismatch / fetch error)
        """
        try:
            pod = await self.topdeck_tagger.match_vc_to_pod(vc, _non_bot_members(vc))
        except Exception as e:
            log_warn(f"[timer/topdeck] pod check failed: {type(e).__name__}: {e}")
            return None

        if not pod:
            return None

        pod_handles = {h for h in (getattr(pod, "entrant_discords_norm", []) or []) if h}
        caller_handles = _norm_member_handles(member)
        return bool(pod_handles.intersection(caller_handles))




    # TopDeck online tagging now handled by self.topdeck_tagger (see timer/topdeck.py)

    # ---------- core actions ----------

    async def timer_end(
        self,
        ctx: discord.ApplicationContext,
        minutes: float,
        message: str,
        voice_file_path: Optional[str] = None,
        *,
        timer_id: Optional[str] = None,
        edit: bool = False,
        delete_after: Optional[float] = None,  # minutes
        final_cleanup: bool = False,
    ):
        delay_sec = max(0.0, minutes) * 60
        log_sync(
            f"[timer_end] Scheduled fire: timer_id={timer_id}, minutes={minutes}, "
            f"delay_sec={delay_sec}, voice_file_path={voice_file_path}, edit={edit}, "
            f"final_cleanup={final_cleanup}"
        )
        await asyncio.sleep(delay_sec)

        channel = ctx.channel
        msg_obj: Optional[discord.Message] = None

        if edit and timer_id and timer_id in self.timer_messages:
            ch_id, m_id = self.timer_messages[timer_id]
            ch = self.bot.get_channel(ch_id) or channel
            try:
                msg_obj = await ch.fetch_message(m_id)
                await msg_obj.edit(content=message)
            except Exception as e:
                log_warn(f"[timer_end] Failed to edit message: {e}")
        else:
            try:
                msg_obj = await channel.send(message)
                if timer_id:
                    self.timer_messages[timer_id] = (channel.id, msg_obj.id)
            except Exception as e:
                log_warn(f"[timer_end] Failed to send message: {e}")

        vcid: Optional[int] = None
        if timer_id and timer_id in self.active_timers:
            vcid = self.active_timers[timer_id].get("voice_channel_id")
        if (
            vcid is None
            and ctx.guild
            and ctx.guild.voice_client
            and ctx.guild.voice_client.channel
        ):
            vcid = ctx.guild.voice_client.channel.id  # type: ignore[assignment]

        if voice_file_path and ctx.guild:
            await self._play(
                ctx.guild, voice_file_path, channel_id=vcid, leave_after=True
            )

        if delete_after is not None and msg_obj is not None:
            await asyncio.sleep(max(0.0, delete_after) * 60)
            with contextlib.suppress(Exception):
                await msg_obj.delete()

        if final_cleanup and timer_id:
            log_sync(f"[timer_end] Final stage complete, cleaning up timer_id={timer_id}")
            self._cleanup_timer_structs(timer_id)
            # Delete from DB
            await self._delete_timer_from_db(timer_id)

    async def play_voice_file(
        self,
        ctx: discord.ApplicationContext,
        voice_file_path: str,
        delay_seconds: float,
        *,
        timer_id: Optional[str] = None,
    ):
        delay = max(0.0, delay_seconds)
        log_sync(
            f"[play_voice_file] Scheduled: timer_id={timer_id}, "
            f"delay_seconds={delay}, path={voice_file_path}"
        )
        await asyncio.sleep(delay)

        vcid: Optional[int] = None
        if timer_id and timer_id in self.active_timers:
            vcid = self.active_timers[timer_id].get("voice_channel_id")
        if (
            vcid is None
            and ctx.guild
            and ctx.guild.voice_client
            and ctx.guild.voice_client.channel
        ):
            vcid = ctx.guild.voice_client.channel.id  # type: ignore[assignment]

        if ctx.guild:
            await self._play(
                ctx.guild, voice_file_path, channel_id=vcid, leave_after=True
            )

    async def _cancel_tasks(self, timer_id: str):
        for task in self.timer_tasks.get(timer_id, []):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self.timer_tasks[timer_id] = []

    async def set_timer_stopped(self, timer_id: str, reason: str = "manual"):
        """Stop a timer early (manual/end/auto/replace)."""
        if timer_id not in self.active_timers and timer_id not in self.paused_timers:
            return

        reason_map = {
            "manual": "manually.",
            "endtimer": "via /endtimer.",
            "auto": "automatically because the table dropped below 2 players.",
            "replace": "because a new timer was started for this table.",
        }
        reason_text = reason_map.get(reason, "manually.")

        await self._cancel_tasks(timer_id)

        if timer_id in self.timer_messages:
            ch_id, m_id = self.timer_messages[timer_id]
            ch = self.bot.get_channel(ch_id)
            if ch:
                try:
                    msg = await ch.fetch_message(m_id)
                    await msg.edit(content=f"Timer was stopped {reason_text}")

                    async def _del(m: discord.Message):
                        await asyncio.sleep(60)
                        with contextlib.suppress(Exception):
                            await m.delete()

                    asyncio.create_task(_del(msg))
                except Exception as e:
                    log_warn(f"[set_timer_stopped] Failed to edit/delete message: {e}")

        self._cleanup_timer_structs(timer_id)
        
        # Delete from DB
        await self._delete_timer_from_db(timer_id)
        
        log_sync(f"[set_timer_stopped] Cleaned up timer_id={timer_id}, reason={reason}")

    # ---------- core timer start ----------

    async def _start_timer(
        self,
        ctx: discord.ApplicationContext,
        voice_channel: discord.VoiceChannel,
        *,
        game_number: int,
        ignore_autostop: bool = False,
    ):
        guild = ctx.guild
        if guild is None:
            await safe_ctx_followup(ctx,
                "This command can only be used in a server.", ephemeral=True
            )
            return

        main_minutes = TIMER_MINUTES
        extra_minutes = EXTRA_TURNS_MINUTES
        offset = OFFSET_MINUTES

        vc_id = voice_channel.id
        self.voice_channel_timers[vc_id] = self.voice_channel_timers.get(vc_id, 0) + 1
        seq = self.voice_channel_timers[vc_id]
        timer_id = make_timer_id(vc_id, seq)

        # Always create fresh list for this timer_id (prevents KeyError race)
        self.timer_tasks[timer_id] = []

        log_sync(f"[timer] Using timer_id={timer_id}")

        main_seconds = max(0.0, main_minutes * 60.0)
        to_end_delay_sec = max(0.0, (main_minutes - offset) * 60.0)
        extra_seconds = max(0.0, extra_minutes * 60.0)

        log_sync(
            f"[timer] Calculated to_end_delay_sec={to_end_delay_sec} "
            f"({(main_minutes - offset):.2f} minutes from start)"
        )

        end_time_main = now_utc() + timedelta(minutes=main_minutes)
        end_ts_main = ts(end_time_main)
        final_time = end_time_main + timedelta(minutes=extra_minutes)

        turns_msg = (
            f"Time is over. You have **{int(extra_minutes)} minutes** to reach a "
            f"conclusion. Good luck! - <t:{ts(final_time)}:R>."
        )
        final_msg = "If no one won until now, the game is a draw. Well Played."

        sent = await safe_ctx_followup(ctx,
            f"Timer for **{voice_channel.name}** (Game in room {game_number}) will start now and end "
            f"<t:{end_ts_main}:R>. Play to win and to your outs.",
            ephemeral=False,  # force public
        )

        self.timer_messages[timer_id] = (sent.channel.id, sent.id)

        self.active_timers[timer_id] = {
            "start_time": now_utc(),
            "durations": {
                "main": main_seconds,
                "easter_egg": to_end_delay_sec,
                "extra": extra_seconds,
            },
            "ctx": ctx,
            "voice_channel_id": voice_channel.id,
            "ignore_autostop": bool(ignore_autostop),
            "messages": {
                "turns": turns_msg,
                "final": final_msg,
            },
            "audio": {
                "turns": EXTRA_TIME_AUDIO,
                "final": FINAL_DRAW_AUDIO,
                "easter_egg": TEN_TO_END_AUDIO,
            },
        }

        # Schedule tasks BEFORE awaiting voice playback (prevents autostop race KeyError)
        # main end -> extra time msg + audio
        self.timer_tasks[timer_id].append(
            asyncio.create_task(
                self.timer_end(
                    ctx,
                    main_minutes,
                    turns_msg,
                    EXTRA_TIME_AUDIO,
                    timer_id=timer_id,
                    edit=True,
                )
            )
        )
        # offset before end
        self.timer_tasks[timer_id].append(
            asyncio.create_task(
                self.play_voice_file(
                    ctx,
                    TEN_TO_END_AUDIO,
                    to_end_delay_sec,
                    timer_id=timer_id,
                )
            )
        )
        # final draw
        self.timer_tasks[timer_id].append(
            asyncio.create_task(
                self.timer_end(
                    ctx,
                    main_minutes + extra_minutes,
                    final_msg,
                    FINAL_DRAW_AUDIO,
                    timer_id=timer_id,
                    edit=True,
                    delete_after=1,
                    final_cleanup=True,
                )
            )
        )

        log_sync(
            f"[timer] Scheduled tasks for timer_id={timer_id}: "
            f"{len(self.timer_tasks[timer_id])} tasks, "
            f"delay_sec={to_end_delay_sec}"
        )

        # Persist timer to DB
        await self._save_timer_to_db(
            timer_id=timer_id,
            guild_id=guild.id,
            channel_id=ctx.channel.id,
            voice_channel_id=voice_channel.id,
            message_id=sent.id,
            status="active",
            start_time_utc=self.active_timers[timer_id]["start_time"],
            durations=self.active_timers[timer_id]["durations"],
            remaining=None,
            ignore_autostop=bool(ignore_autostop),
            messages=self.active_timers[timer_id]["messages"],
            audio=self.active_timers[timer_id]["audio"],
        )

        # intro audio
        ok = await self._play(
            guild,
            TIMER_START_AUDIO,
            channel_id=voice_channel.id,
            leave_after=True,
        )

        # If we got auto-stopped / replaced while playing intro, just stop here.
        if timer_id not in self.active_timers:
            return

        if not ok:
            # We continue with the text timers, but tell the caller audio failed.
            await safe_ctx_followup(ctx,
                f"Started timer for **{voice_channel.name}**, but I couldn't "
                f"connect to voice in time. Text timers will still run, but "
                f"no audio will play.",
                ephemeral=False,
            )

    # ---------- slash commands ----------

    @commands.slash_command(
        name="timer",
        description="Start a match timer for an ECL game voice channel.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def timer(
        self,
        ctx: discord.ApplicationContext,
        game: int = Option(int, "Game number (e.g. 1 for 'ECL Game 1')", min_value=1),
    ):
        # --- basic guild / channel checks (errors → ephemeral) ---
        if ctx.guild is None:
            await safe_ctx_respond(ctx,
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        guild = ctx.guild
        voice_channel = self._get_game_channel(guild, game)
        if not voice_channel:
            await safe_ctx_respond(ctx,
                f"Could not find a voice channel named `ECL Game {game}`.",
                ephemeral=True,
            )
            return

        # --- caller must be in that exact VC (errors → ephemeral) ---
        member = ctx.author if isinstance(ctx.author, discord.Member) else None
        caller_vc: Optional[discord.VoiceChannel] = None
        if member and member.voice and isinstance(member.voice.channel, discord.VoiceChannel):
            caller_vc = member.voice.channel

        if caller_vc is None:
            await safe_ctx_respond(ctx,
                f"You must be in **{voice_channel.name}** to start a timer for that room "
                f"(you're not in any voice channel).",
                ephemeral=True,
            )
            return

        if caller_vc.id != voice_channel.id:
            await safe_ctx_respond(ctx,
                f"You must be in **{voice_channel.name}** to start a timer for that room "
                f"(you're currently in **{caller_vc.name}**).",
                ephemeral=True,
            )
            return

        non_bot = _non_bot_members(voice_channel)
        is_mod = self._is_mod_member(member)

        # --- ECL MOD backdoor + 3-player requirement (errors → ephemeral) ---
        if len(non_bot) < 3 and not is_mod:
            await safe_ctx_respond(ctx,
                f"Cannot start a timer for **{voice_channel.name}**: "
                f"need at least 3 players in the channel (currently {len(non_bot)}). ",
                ephemeral=True,
            )
            return

        ignore_autostop = self._ignore_autostop_for_start(member, voice_channel)

        # --- existing timer? → show buttons (public) ---
        existing_timer_id = self._current_timer_id_for_channel(voice_channel.id)
        if existing_timer_id:
            view = ReplaceTimerView(
                self,
                ctx,
                voice_channel,
                game_number=game,
                existing_timer_id=existing_timer_id,
            )
            await safe_ctx_respond(ctx,
                f"There is already an active or paused timer for **{voice_channel.name}**.\n"
                "Do you want to stop it and start a new one?",
                view=view,
                ephemeral=False,  # visible to table
            )
            return

        # --- from here on we're doing heavier work → defer non-ephemeral ---
        await safe_ctx_defer(ctx, ephemeral=False, label="timer")  # non-ephemeral; followups will be public

        # Try to tag this pod as an online TopDeck game (or warn if not found)
        try:
            await self.topdeck_tagger.tag_online_game_for_timer(ctx, voice_channel, non_bot)
        except Exception as e:
            log_warn(
                "[timer/topdeck] Unexpected error in tag_online_game_for_timer: "
                f"{type(e).__name__}: {e}"
            )

        # start timer (schedules tasks + plays audio)
        await self._start_timer(ctx, voice_channel, game_number=game, ignore_autostop=ignore_autostop)

    @commands.slash_command(
        name="endtimer",
        description="Manually ends the active timer for a given ECL game.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def endtimer(
        self,
        ctx: discord.ApplicationContext,
        game: int = Option(int, "Game number (e.g. 1 for 'ECL Game 1')", min_value=1),
    ):
        if ctx.guild is None:
            await safe_ctx_respond(ctx,
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        voice_channel = self._get_game_channel(ctx.guild, game)
        if not voice_channel:
            await safe_ctx_respond(ctx,
                f"Could not find a voice channel named `ECL Game {game}`.",
                ephemeral=True,
            )
            return

        timer_id = self._current_timer_id_for_channel(voice_channel.id)
        if not timer_id:
            await safe_ctx_respond(ctx,
                f"No active or paused timer found for **{voice_channel.name}**.",
                ephemeral=True,
            )
            return

        owner_id = self._timer_owner_id(timer_id)
        if owner_id is not None and owner_id != ctx.author.id:
            await safe_ctx_respond(ctx,
                "Only the person who started the timer for this table can end it.",
                ephemeral=True,
            )
            return

        await self.set_timer_stopped(timer_id, reason="endtimer")
        await safe_ctx_respond(ctx,
            f"Timer for **{voice_channel.name}** has been manually ended.",
            ephemeral=False,
        )

    @commands.slash_command(
        name="pausetimer",
        description="Pauses the current timer for a given ECL game.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def pausetimer(self, ctx: discord.ApplicationContext, game: int = Option(int, "...", min_value=1)):
        if ctx.guild is None:
            await safe_ctx_respond(ctx, "This command can only be used in a server.", ephemeral=True)
            return

        voice_channel = self._get_game_channel(ctx.guild, game)
        if not voice_channel:
            await safe_ctx_respond(ctx, f"Could not find a voice channel named `ECL Game {game}`.", ephemeral=True)
            return

        timer_id = self._current_timer_id_for_channel(voice_channel.id)
        if not timer_id or timer_id not in self.active_timers:
            await safe_ctx_respond(ctx, f"There's no active timer to pause for **{voice_channel.name}**.", ephemeral=True)
            return

        # owner_id = self._timer_owner_id(timer_id)
        # if owner_id is not None and owner_id != ctx.author.id:
        #     await safe_ctx_respond(ctx, "Only the person who started the timer for this table can pause it.", ephemeral=True)
        #     return
        
        member = ctx.author if isinstance(ctx.author, discord.Member) else None
        if not member:
            await safe_ctx_respond(ctx, "Only server members can use this.", ephemeral=True)
            return

        if not self._is_mod_member(member) and not self._caller_in_vc(member, voice_channel):
            await safe_ctx_respond(
                ctx,
                f"You must be in **{voice_channel.name}** to pause that timer.",
                ephemeral=True,
            )
            return

        # ✅ ACK FAST (prevents 10062)
        await safe_ctx_defer(ctx, ephemeral=False, label="pausetimer")
        
        if not self._is_mod_member(member):
            is_player = await self._caller_is_pod_player(member, voice_channel)

            if is_player is False:
                await safe_ctx_followup(ctx, "Only players in the current TopDeck pod can pause this timer.", ephemeral=True)
                return

            if is_player is None:
                # Fallback: if we can't verify TopDeck pod, only the timer starter can control
                owner_id = self._timer_owner_id(timer_id)
                if owner_id is not None and owner_id != member.id:
                    await safe_ctx_followup(
                        ctx,
                        "I couldn't verify the TopDeck pod for this table right now. "
                        "Only the timer starter (or a mod) can pause it.",
                        ephemeral=True,
                    )
                    return


        await self._cancel_tasks(timer_id)

        timer_data = self.active_timers.pop(timer_id)
        elapsed = (now_utc() - timer_data["start_time"]).total_seconds()
        durations = timer_data["durations"]

        remaining = {
            "main": max(durations["main"] - elapsed, 0.0),
            "easter_egg": max(durations["easter_egg"] - elapsed, 0.0),
            "extra": max(durations["extra"] - elapsed + durations["main"], 0.0),
        }

        # delete old timer msg (can be slow)
        try:
            ch_id, m_id = self.timer_messages.get(timer_id, (None, None))
            if ch_id and m_id:
                ch = self.bot.get_channel(ch_id)
                if ch:
                    orig = await ch.fetch_message(m_id)
                    await orig.delete()
        except Exception as e:
            log_warn(f"[pausetimer] Error deleting original timer message: {e}")

        remaining_minutes = int(remaining["main"] // 60)

        pause_msg = await safe_ctx_followup(
            ctx,
            f"⏸️ Timer for **{voice_channel.name}** paused – **{remaining_minutes} minutes** remaining.",
            ephemeral=False,
        )

        self.paused_timers[timer_id] = {
            "ctx": timer_data["ctx"],
            "remaining": remaining,
            "messages": timer_data["messages"],
            "audio": timer_data["audio"],
            "pause_message": pause_msg,
            "voice_channel_id": timer_data.get("voice_channel_id"),
            "ignore_autostop": bool(timer_data.get("ignore_autostop", False)),
        }

        # Persist paused state to DB
        ch_id, m_id = self.timer_messages.get(timer_id, (ctx.channel.id, pause_msg.id))
        await self._save_timer_to_db(
            timer_id=timer_id,
            guild_id=ctx.guild.id,
            channel_id=ch_id,
            voice_channel_id=timer_data.get("voice_channel_id", 0),
            message_id=pause_msg.id,
            status="paused",
            start_time_utc=None,
            durations=timer_data.get("durations", {"main": 0, "easter_egg": 0, "extra": 0}),
            remaining=remaining,
            ignore_autostop=bool(timer_data.get("ignore_autostop", False)),
            messages=timer_data["messages"],
            audio=timer_data["audio"],
        )


    @commands.slash_command(
        name="resumetimer",
        description="Resumes a paused timer for a given ECL game.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def resumetimer(
        self,
        ctx: discord.ApplicationContext,
        game: int = Option(int, "Game number (e.g. 1 for 'ECL Game 1')", min_value=1),
    ):
        if ctx.guild is None:
            await safe_ctx_respond(ctx,"This command can only be used in a server.", ephemeral=True)
            return

        voice_channel = self._get_game_channel(ctx.guild, game)
        if not voice_channel:
            await safe_ctx_respond(ctx,f"Could not find a voice channel named `ECL Game {game}`.", ephemeral=True)
            return

        timer_id = self._current_timer_id_for_channel(voice_channel.id)
        if not timer_id or timer_id not in self.paused_timers:
            await safe_ctx_respond(ctx,f"No paused timer found for **{voice_channel.name}**.", ephemeral=True)
            return

        # owner_id = self._timer_owner_id(timer_id)
        # if owner_id is not None and owner_id != ctx.author.id:
        #     await safe_ctx_respond(ctx,"Only the person who started the timer for this table can resume it.", ephemeral=True)
        #     return
        
        member = ctx.author if isinstance(ctx.author, discord.Member) else None
        if not member:
            await safe_ctx_respond(ctx, "Only server members can use this.", ephemeral=True)
            return

        if not self._is_mod_member(member) and not self._caller_in_vc(member, voice_channel):
            await safe_ctx_respond(
                ctx,
                f"You must be in **{voice_channel.name}** to resume that timer.",
                ephemeral=True,
            )
            return


        # ✅ ACK FAST (prevents 10062)
        await safe_ctx_defer(ctx, ephemeral=False, label="resumetimer")
        
        if not self._is_mod_member(member):
            is_player = await self._caller_is_pod_player(member, voice_channel)

            if is_player is False:
                await safe_ctx_followup(ctx, "Only players in the current TopDeck pod can resume this timer.", ephemeral=True)
                return

            if is_player is None:
                # Fallback: if we can't verify TopDeck pod, only the timer starter can control
                owner_id = self._timer_owner_id(timer_id)
                if owner_id is not None and owner_id != member.id:
                    await safe_ctx_followup(
                        ctx,
                        "I couldn't verify the TopDeck pod for this table right now. "
                        "Only the timer starter (or a mod) can resume it.",
                        ephemeral=True,
                    )
                    return


        paused = self.paused_timers.pop(timer_id)

        pm = paused.get("pause_message")
        if pm:
            with contextlib.suppress(Exception):
                await pm.delete()

        self.active_timers[timer_id] = {
            "start_time": now_utc(),
            "durations": paused["remaining"],
            "messages": paused["messages"],
            "audio": paused["audio"],
            "voice_channel_id": paused.get("voice_channel_id"),
            "ignore_autostop": bool(paused.get("ignore_autostop", False)),
        }
        self.timer_tasks[timer_id] = []

        old_ctx = paused["ctx"]
        turns_msg = paused["messages"]["turns"]
        final_msg = paused["messages"]["final"]
        turns_audio = paused["audio"]["turns"]
        final_audio = paused["audio"]["final"]
        egg_audio = paused["audio"]["easter_egg"]

        main = paused["remaining"]["main"]
        egg = paused["remaining"]["easter_egg"]
        extra = paused["remaining"]["extra"]

        self.timer_tasks[timer_id].append(asyncio.create_task(
            self.timer_end(old_ctx, main / 60.0, turns_msg, turns_audio, timer_id=timer_id, edit=True)
        ))
        self.timer_tasks[timer_id].append(asyncio.create_task(
            self.play_voice_file(old_ctx, egg_audio, egg, timer_id=timer_id)
        ))
        self.timer_tasks[timer_id].append(asyncio.create_task(
            self.timer_end(old_ctx, extra / 60.0, final_msg, final_audio, timer_id=timer_id, edit=True, delete_after=1, final_cleanup=True)
        ))

        end_time = now_utc() + timedelta(seconds=main)

        # ✅ AFTER defer: use followup
        msg = await safe_ctx_followup(ctx,
            f"▶️ Timer for **{voice_channel.name}** has been resumed and will end <t:{ts(end_time)}:R>."
        )
        self.timer_messages[timer_id] = (ctx.channel.id, msg.id)

        # Persist resumed timer to DB
        # IMPORTANT: Store the ORIGINAL durations concept (main, extra as separate)
        # not the confusingly-named paused["remaining"] where "extra" is total remaining.
        # For rehydration, we need to store the actual remaining times clearly.
        #
        # After resume, the new "durations" should represent the time windows from now:
        # - main_duration = remaining main time
        # - extra_duration = remaining extra time (total_remaining - main_remaining)
        #
        main_remaining_sec = paused["remaining"]["main"]
        total_remaining_sec = paused["remaining"]["extra"]  # this is actually total, not extra
        extra_only_sec = max(0, total_remaining_sec - main_remaining_sec)
        
        await self._save_timer_to_db(
            timer_id=timer_id,
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
            voice_channel_id=paused.get("voice_channel_id", 0),
            message_id=msg.id,
            status="active",
            start_time_utc=self.active_timers[timer_id]["start_time"],
            durations={
                "main": main_remaining_sec,
                "extra": extra_only_sec,  # store JUST extra, not total
                "easter_egg": paused["remaining"]["easter_egg"],
            },
            remaining=None,
            ignore_autostop=bool(paused.get("ignore_autostop", False)),
            messages=paused["messages"],
            audio=paused["audio"],
        )

    @commands.slash_command(
        name="checktimer",
        description="Check how much time is left on an ECL game timer.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def checktimer(
        self,
        ctx: discord.ApplicationContext,
        game: int = Option(int, "Game number (e.g. 1 for 'ECL Game 1')", min_value=1),
    ):
        # everyone can use it, always ephemeral
        if ctx.guild is None:
            await safe_ctx_respond(ctx,
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        voice_channel = self._get_game_channel(ctx.guild, game)
        if not voice_channel:
            await safe_ctx_respond(ctx,
                f"Could not find a voice channel named `ECL Game {game}`.",
                ephemeral=True,
            )
            return

        timer_id = self._current_timer_id_for_channel(voice_channel.id)
        if not timer_id:
            await safe_ctx_respond(ctx,
                f"No active or paused timer found for **{voice_channel.name}**.",
                ephemeral=True,
            )
            return

        MAIN_TOTAL = TIMER_MINUTES * 60.0
        EXTRA_TOTAL = EXTRA_TURNS_MINUTES * 60.0
        now = now_utc()

        # ---------- active timer ----------
        if timer_id in self.active_timers:
            data = self.active_timers[timer_id]

            elapsed = (now - data["start_time"]).total_seconds()
            remaining_main = max(MAIN_TOTAL - elapsed, 0.0)
            remaining_total = max(MAIN_TOTAL + EXTRA_TOTAL - elapsed, 0.0)

            bar = _build_progress_bar(
                MAIN_TOTAL,
                EXTRA_TOTAL,
                remaining_main,
                remaining_total,
            )

            if remaining_main > 0:
                mins = remaining_main / 60.0
                await safe_ctx_respond(ctx,
                    f"Timer for **{voice_channel.name}** is running.\n"
                    f"≈ **{mins:.1f} minutes** of main time remaining.\n"
                    f"```{bar}```",
                    ephemeral=True,
                )
                return

            # in extra time
            extra_remaining = remaining_total
            mins = extra_remaining / 60.0
            await safe_ctx_respond(ctx,
                f"Main time is already over for **{voice_channel.name}**.\n"
                f"≈ **{mins:.1f} minutes** of extra time remaining.\n"
                f"```{bar}```",
                ephemeral=True,
            )
            return

        # ---------- paused timer ----------
        if timer_id in self.paused_timers:
            data = self.paused_timers[timer_id]
            remaining = data["remaining"]

            remaining_main = float(remaining.get("main", 0.0))
            # we stored 'extra' as time until final (main+extra) from now
            remaining_total = float(remaining.get("extra", 0.0))

            bar = _build_progress_bar(
                MAIN_TOTAL,
                EXTRA_TOTAL,
                remaining_main,
                remaining_total,
            )

            if remaining_main > 0:
                mins = remaining_main / 60.0
                await safe_ctx_respond(ctx,
                    f"Timer for **{voice_channel.name}** is **paused**.\n"
                    f"≈ **{mins:.1f} minutes** of main time remaining.\n"
                    f"```{bar}```",
                    ephemeral=True,
                )
                return

            if remaining_total > 0:
                extra_remaining = remaining_total
                mins = extra_remaining / 60.0
                await safe_ctx_respond(ctx,
                    f"Timer for **{voice_channel.name}** is **paused** "
                    f"during extra time.\n"
                    f"≈ **{mins:.1f} minutes** of extra time remaining.\n"
                    f"```{bar}```",
                    ephemeral=True,
                )
                return

        # ---------- weird edge ----------
        await safe_ctx_respond(ctx,
            f"Couldn't determine remaining time for **{voice_channel.name}** "
            f"(timer exists but has no remaining duration).",
            ephemeral=True,
        )

    # ---------- auto-stop on low player count ----------

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ):
        guild = member.guild
        if guild is None or (GUILD_ID and guild.id != GUILD_ID):
            return

        affected_channels = set()
        if isinstance(before.channel, discord.VoiceChannel):
            affected_channels.add(before.channel)
        if isinstance(after.channel, discord.VoiceChannel):
            affected_channels.add(after.channel)

        for ch in affected_channels:
            if not ch.name.lower().startswith("ecl game"):
                continue

            timer_id = self._current_timer_id_for_channel(ch.id)
            if not timer_id:
                continue

            non_bot = _non_bot_members(ch)
            data = self.active_timers.get(timer_id) or self.paused_timers.get(timer_id) or {}

            # If timer was started in mod-testing mode:
            # - keep it alive while at least 1 person is in the room
            # - but if room is completely empty, still stop
            # - if it ever reaches 3+ players, flip back to normal behavior
            if data.get("ignore_autostop"):
                if len(non_bot) >= 3:
                    data["ignore_autostop"] = False
                    log_sync(f"[auto-stop] Re-enabled for {timer_id} (table reached 3+ players).")
                elif len(non_bot) >= 1:
                    # At least 1 person still in room - keep timer alive (mod testing)
                    log_sync(f"[auto-stop] Skipped for {timer_id} (mod testing mode, {len(non_bot)} player(s)).")
                    continue
                # len(non_bot) == 0 falls through to stop the timer

            if len(non_bot) < 2:
                log_sync(
                    f"[auto-stop] Channel {ch.name} ({ch.id}) dropped to "
                    f"{len(non_bot)} non-bot members; stopping timer {timer_id}"
                )
                await self.set_timer_stopped(timer_id, reason="auto")


def setup(bot: commands.Bot):
    bot.add_cog(ECLTimerCog(bot))
