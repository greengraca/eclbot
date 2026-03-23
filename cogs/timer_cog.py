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

from utils.interactions import safe_ctx_defer, safe_ctx_respond, safe_ctx_followup
from utils.persistence import (
    save_timer as db_save_timer,
    delete_timer as db_delete_timer,
    get_all_active_timers as db_get_all_active_timers,
    cleanup_expired_timers as db_cleanup_expired_timers,
)
from utils.logger import log_sync, log_ok, log_warn, log_error, log_debug
from utils.mod_check import is_mod

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
    build_timer_embed as _build_timer_embed,
    game_color as _game_color,
    VOICE_CONNECT_TIMEOUT,
    ReplaceTimerView,
    TopDeckTagger,
)


from utils.settings import GUILD_ID

# ---------------- env / config ----------------


# Main round duration in minutes
TIMER_MINUTES: float = _env_float("TIMER_MINUTES", 75.0)

# Extra time for turns in minutes
EXTRA_TURNS_MINUTES: float = _env_float("EXTRA_TURNS_MINUTES", 15.0)

# offset: minutes BEFORE main time end when it should play
OFFSET_MINUTES: float = _env_float("OFFSET_MINUTES", 10.0)

# How often the embed updates (minutes)
TIMER_UPDATE_INTERVAL_MINUTES: float = _env_float("TIMER_UPDATE_INTERVAL_MINUTES", 5.0)

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
        player_mention_ids: Optional[List[int]] = None,
        game_number: int = 0,
        original_durations: Optional[Dict[str, float]] = None,
    ) -> None:
        """Persist timer state to MongoDB."""
        try:
            # Calculate when the timer fully expires
            if status == "active" and start_time_utc:
                total_duration = durations.get("main", 0) + durations.get("extra", 0)
                expires_at = start_time_utc + timedelta(seconds=total_duration + 60)  # +1 min buffer
            elif status == "paused" and remaining:
                total_remaining = remaining.get("main", 0) + remaining.get("extra", 0)
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
                player_mention_ids=player_mention_ids,
                game_number=game_number,
                expires_at=expires_at,
                original_durations=original_durations,
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

        # Load original durations (for progress bar across pause/resume).
        # Falls back to durations for timers saved before this field existed.
        if doc.get("original_durations_main") is not None:
            original_durations = {
                "main": float(doc["original_durations_main"]),
                "extra": float(doc.get("original_durations_extra", 0)),
            }
        else:
            original_durations = {"main": durations["main"], "extra": durations["extra"]}

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
                "original_durations": original_durations,
                "messages": messages,
                "audio": audio,
                "voice_channel_id": voice_channel_id,
                "ignore_autostop": ignore_autostop,
                "pause_message": None,
                "ctx": None,
                "player_mention_ids": [int(x) for x in (doc.get("player_mention_ids") or [])],
                "game_number": int(doc.get("game_number", 0)),
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

        if start_time_utc.tzinfo is None:
            start_time_utc = start_time_utc.replace(tzinfo=timezone.utc)
        else:
            start_time_utc = start_time_utc.astimezone(timezone.utc)

        elapsed = (now_utc() - start_time_utc).total_seconds()
        main_remaining = max(0, durations["main"] - elapsed)
        total_remaining = max(0, durations["main"] + durations["extra"] - elapsed)
        easter_egg_remaining = max(0, durations["easter_egg"] - elapsed)

        if total_remaining <= 0:
            log_sync(f"[timer] Rehydrate: timer {timer_id} has expired, cleaning up", level="warn")
            await self._delete_timer_from_db(timer_id)
            return False

        player_ids = [int(x) for x in (doc.get("player_mention_ids") or [])]
        game_number = int(doc.get("game_number", 0))
        draw_event = asyncio.Event()

        self.active_timers[timer_id] = {
            "start_time": start_time_utc,
            "durations": durations,
            "original_durations": original_durations,
            "messages": messages,
            "audio": audio,
            "voice_channel_id": voice_channel_id,
            "ignore_autostop": ignore_autostop,
            "player_mention_ids": player_ids,
            "game_number": game_number,
            "phase_override": None,
            "draw_event": draw_event,
        }
        if message_id:
            self.timer_messages[timer_id] = (channel_id, int(message_id))

        self.timer_tasks[timer_id] = []

        # 1. Embed update loop
        self.timer_tasks[timer_id].append(asyncio.create_task(
            self._embed_update_loop(timer_id, game_number)
        ))
        # 2. Easter egg audio
        if easter_egg_remaining > 0:
            self.timer_tasks[timer_id].append(asyncio.create_task(
                self._audio_at(easter_egg_remaining, audio["easter_egg"], timer_id, voice_channel_id)
            ))
        # 3. Main-end audio
        if main_remaining > 0:
            self.timer_tasks[timer_id].append(asyncio.create_task(
                self._audio_at(main_remaining, audio["turns"], timer_id, voice_channel_id)
            ))
        # 4. Final audio
        if total_remaining > 0:
            self.timer_tasks[timer_id].append(asyncio.create_task(
                self._final_audio(total_remaining, audio["final"], timer_id, voice_channel_id, draw_event)
            ))

        log_sync(f"[timer] Rehydrated active timer {timer_id} with {len(self.timer_tasks[timer_id])} tasks")
        return True

    # ---------- mod helpers ----------

    def _is_mod_member(self, member: Optional[discord.Member]) -> bool:
        """Check if member is a mod. Delegates to utils.mod_check.is_mod."""
        return is_mod(member)

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

    # ---------- audio-only tasks ----------

    async def _audio_at(
        self,
        delay_sec: float,
        audio_path: str,
        timer_id: str,
        voice_channel_id: int,
    ) -> None:
        """Sleep then play audio. No message editing."""
        await asyncio.sleep(max(0.0, delay_sec))
        if timer_id not in self.active_timers:
            return
        for g in self.bot.guilds:
            ch = g.get_channel(voice_channel_id)
            if ch:
                await self._play(g, audio_path, channel_id=voice_channel_id, leave_after=True)
                return

    async def _final_audio(
        self,
        delay_sec: float,
        audio_path: str,
        timer_id: str,
        voice_channel_id: int,
        draw_event: asyncio.Event,
    ) -> None:
        """Sleep, play draw audio, then signal the embed loop to show draw phase."""
        await asyncio.sleep(max(0.0, delay_sec))
        if timer_id not in self.active_timers:
            return

        # Play final audio
        guild = None
        for g in self.bot.guilds:
            ch = g.get_channel(voice_channel_id)
            if ch:
                guild = g
                break
        if guild:
            await self._play(guild, audio_path, channel_id=voice_channel_id, leave_after=True)

        # Signal the embed loop
        data = self.active_timers.get(timer_id)
        if data:
            data["phase_override"] = "draw"
        draw_event.set()

    # ---------- embed update loop ----------

    async def _embed_update_loop(
        self,
        timer_id: str,
        game_number: int,
    ) -> None:
        """Periodically update the timer embed. Sole owner of message editing."""
        interval = max(30.0, TIMER_UPDATE_INTERVAL_MINUTES * 60.0)

        while True:
            # Exit if timer was stopped/cancelled
            if timer_id not in self.active_timers:
                return

            data = self.active_timers[timer_id]
            now = now_utc()
            elapsed = (now - data["start_time"]).total_seconds()
            durations = data["durations"]

            main_dur = durations["main"]
            extra_dur = durations["extra"]
            remaining_main = max(0.0, main_dur - elapsed)
            remaining_total = max(0.0, main_dur + extra_dur - elapsed)

            # Use original durations for progress bar totals (survives pause/resume)
            orig = data.get("original_durations") or durations
            main_total = orig["main"]
            extra_total = orig["extra"]

            end_ts_main = ts(data["start_time"] + timedelta(seconds=main_dur))
            end_ts_final = ts(data["start_time"] + timedelta(seconds=main_dur + extra_dur))

            # Determine phase
            if data.get("phase_override") == "draw":
                phase = "draw"
            elif remaining_main > 0:
                phase = "running"
            elif remaining_total > 0:
                phase = "extra"
            else:
                phase = "draw"

            player_ids = data.get("player_mention_ids", [])

            embed = _build_timer_embed(
                game_number=game_number,
                phase=phase,
                main_total=main_total,
                extra_total=extra_total,
                remaining_main=remaining_main,
                remaining_total=remaining_total,
                end_ts_main=end_ts_main,
                end_ts_final=end_ts_final,
                player_ids=player_ids,
            )

            # Fetch and edit the message
            msg_info = self.timer_messages.get(timer_id)
            if not msg_info:
                log_warn(f"[timer/loop] No message tracked for {timer_id}, exiting loop")
                return

            ch_id, m_id = msg_info
            try:
                ch = self.bot.get_channel(ch_id)
                if ch is None:
                    log_warn(f"[timer/loop] Channel {ch_id} not found, cleaning up {timer_id}")
                    self._cleanup_timer_structs(timer_id)
                    await self._delete_timer_from_db(timer_id)
                    return
                msg = await ch.fetch_message(m_id)
                await msg.edit(embed=embed, content=msg.content)
            except discord.NotFound:
                log_warn(f"[timer/loop] Message deleted externally for {timer_id}, cleaning up")
                self._cleanup_timer_structs(timer_id)
                await self._delete_timer_from_db(timer_id)
                return
            except Exception as e:
                log_warn(f"[timer/loop] Failed to edit message for {timer_id}: {type(e).__name__}: {e}")

            # Draw phase: final edit done, wait 1 min, delete, cleanup
            if phase == "draw":
                await asyncio.sleep(60)
                try:
                    ch = self.bot.get_channel(ch_id)
                    if ch:
                        msg = await ch.fetch_message(m_id)
                        await msg.delete()
                except Exception:
                    pass
                self._cleanup_timer_structs(timer_id)
                await self._delete_timer_from_db(timer_id)
                return

            # Wait for draw event or interval
            draw_event = data.get("draw_event")
            if draw_event and not draw_event.is_set():
                try:
                    await asyncio.wait_for(draw_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass  # normal tick
            else:
                await asyncio.sleep(interval)

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
                    await msg.edit(
                        content=f"Timer was stopped {reason_text}",
                        embed=None,
                    )

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
        matched_players: Optional[List[discord.Member]] = None,
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

        self.timer_tasks[timer_id] = []

        log_sync(f"[timer] Using timer_id={timer_id}")

        main_seconds = max(0.0, main_minutes * 60.0)
        to_end_delay_sec = max(0.0, (main_minutes - offset) * 60.0)
        extra_seconds = max(0.0, extra_minutes * 60.0)

        start_time = now_utc()
        end_ts_main = ts(start_time + timedelta(seconds=main_seconds))
        end_ts_final = ts(start_time + timedelta(seconds=main_seconds + extra_seconds))

        player_ids = [m.id for m in (matched_players or [])]

        # Build initial embed
        embed = _build_timer_embed(
            game_number=game_number,
            phase="running",
            main_total=main_seconds,
            extra_total=extra_seconds,
            remaining_main=main_seconds,
            remaining_total=main_seconds + extra_seconds,
            end_ts_main=end_ts_main,
            end_ts_final=end_ts_final,
            player_ids=player_ids,
        )

        # Content with mentions for ping
        mentions = " ".join(f"<@{uid}>" for uid in player_ids) if player_ids else None

        sent = await safe_ctx_followup(ctx,
            content=mentions,
            embed=embed,
            ephemeral=False,
        )

        self.timer_messages[timer_id] = (sent.channel.id, sent.id)

        draw_event = asyncio.Event()

        self.active_timers[timer_id] = {
            "start_time": start_time,
            "durations": {
                "main": main_seconds,
                "easter_egg": to_end_delay_sec,
                "extra": extra_seconds,
            },
            "original_durations": {
                "main": main_seconds,
                "extra": extra_seconds,
            },
            "ctx": ctx,
            "voice_channel_id": voice_channel.id,
            "ignore_autostop": bool(ignore_autostop),
            "messages": {
                "turns": "",
                "final": "",
            },
            "audio": {
                "turns": EXTRA_TIME_AUDIO,
                "final": FINAL_DRAW_AUDIO,
                "easter_egg": TEN_TO_END_AUDIO,
            },
            "player_mention_ids": player_ids,
            "game_number": game_number,
            "phase_override": None,
            "draw_event": draw_event,
        }

        # Schedule 4 tasks: embed loop + 3 audio-only
        # 1. Embed update loop
        self.timer_tasks[timer_id].append(
            asyncio.create_task(
                self._embed_update_loop(timer_id, game_number)
            )
        )
        # 2. Easter egg audio (10 min warning)
        self.timer_tasks[timer_id].append(
            asyncio.create_task(
                self._audio_at(to_end_delay_sec, TEN_TO_END_AUDIO, timer_id, voice_channel.id)
            )
        )
        # 3. Main-end audio
        self.timer_tasks[timer_id].append(
            asyncio.create_task(
                self._audio_at(main_seconds, EXTRA_TIME_AUDIO, timer_id, voice_channel.id)
            )
        )
        # 4. Final draw audio + signal
        self.timer_tasks[timer_id].append(
            asyncio.create_task(
                self._final_audio(
                    main_seconds + extra_seconds,
                    FINAL_DRAW_AUDIO,
                    timer_id,
                    voice_channel.id,
                    draw_event,
                )
            )
        )

        log_sync(
            f"[timer] Scheduled tasks for timer_id={timer_id}: "
            f"{len(self.timer_tasks[timer_id])} tasks"
        )

        # Persist timer to DB
        await self._save_timer_to_db(
            timer_id=timer_id,
            guild_id=guild.id,
            channel_id=ctx.channel.id,
            voice_channel_id=voice_channel.id,
            message_id=sent.id,
            status="active",
            start_time_utc=start_time,
            durations=self.active_timers[timer_id]["durations"],
            remaining=None,
            ignore_autostop=bool(ignore_autostop),
            messages=self.active_timers[timer_id]["messages"],
            audio=self.active_timers[timer_id]["audio"],
            player_mention_ids=player_ids,
            game_number=game_number,
            original_durations=self.active_timers[timer_id]["original_durations"],
        )

        # Intro audio (non-blocking for the timer tasks)
        ok = await self._play(
            guild,
            TIMER_START_AUDIO,
            channel_id=voice_channel.id,
            leave_after=True,
        )

        if timer_id not in self.active_timers:
            return

        if not ok:
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
        matched_players: List[discord.Member] = []
        try:
            matched_players = await self.topdeck_tagger.tag_online_game_for_timer(ctx, voice_channel, non_bot, game_number=game)
        except Exception as e:
            log_warn(
                "[timer/topdeck] Unexpected error in tag_online_game_for_timer: "
                f"{type(e).__name__}: {e}"
            )

        # start timer (schedules tasks + plays audio)
        await self._start_timer(
            ctx, voice_channel,
            game_number=game,
            ignore_autostop=ignore_autostop,
            matched_players=matched_players,
        )

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
            "extra": max(durations["extra"] - elapsed, 0.0),
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

        remaining_main_val = float(remaining["main"])
        remaining_total_val = remaining_main_val + float(remaining["extra"])

        # Use original durations for progress bar totals (survives pause/resume)
        orig = timer_data.get("original_durations") or timer_data["durations"]
        MAIN_TOTAL = orig["main"]
        EXTRA_TOTAL = orig["extra"]

        player_ids = timer_data.get("player_mention_ids", [])
        game_number = timer_data.get("game_number", 0)

        embed = _build_timer_embed(
            game_number=game_number,
            phase="paused",
            main_total=MAIN_TOTAL,
            extra_total=EXTRA_TOTAL,
            remaining_main=remaining_main_val,
            remaining_total=remaining_total_val,
            end_ts_main=0,
            end_ts_final=0,
            player_ids=player_ids,
        )

        pause_msg = await safe_ctx_followup(
            ctx,
            embed=embed,
            ephemeral=False,
        )

        self.paused_timers[timer_id] = {
            "ctx": timer_data["ctx"],
            "remaining": remaining,
            "original_durations": orig,
            "messages": timer_data["messages"],
            "audio": timer_data["audio"],
            "pause_message": pause_msg,
            "voice_channel_id": timer_data.get("voice_channel_id"),
            "ignore_autostop": bool(timer_data.get("ignore_autostop", False)),
            "player_mention_ids": player_ids,
            "game_number": game_number,
        }

        # Update timer_messages to point to the new pause message
        self.timer_messages[timer_id] = (ctx.channel.id, pause_msg.id)

        # Persist paused state to DB
        await self._save_timer_to_db(
            timer_id=timer_id,
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
            voice_channel_id=timer_data.get("voice_channel_id", 0),
            message_id=pause_msg.id,
            status="paused",
            start_time_utc=None,
            durations=timer_data.get("durations", {"main": 0, "easter_egg": 0, "extra": 0}),
            remaining=remaining,
            ignore_autostop=bool(timer_data.get("ignore_autostop", False)),
            messages=timer_data["messages"],
            audio=timer_data["audio"],
            player_mention_ids=player_ids,
            game_number=game_number,
            original_durations=orig,
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

        player_ids = paused.get("player_mention_ids", [])
        game_number = paused.get("game_number", 0)
        draw_event = asyncio.Event()

        # Preserve original durations for progress bar across pause/resume cycles
        orig = paused.get("original_durations") or {
            "main": TIMER_MINUTES * 60.0,
            "extra": EXTRA_TURNS_MINUTES * 60.0,
        }

        self.active_timers[timer_id] = {
            "start_time": now_utc(),
            "durations": paused["remaining"],
            "original_durations": orig,
            "messages": paused["messages"],
            "audio": paused["audio"],
            "voice_channel_id": paused.get("voice_channel_id"),
            "ignore_autostop": bool(paused.get("ignore_autostop", False)),
            "player_mention_ids": player_ids,
            "game_number": game_number,
            "phase_override": None,
            "draw_event": draw_event,
        }
        self.timer_tasks[timer_id] = []

        turns_audio = paused["audio"]["turns"]
        final_audio = paused["audio"]["final"]
        egg_audio = paused["audio"]["easter_egg"]

        main = paused["remaining"]["main"]
        egg = paused["remaining"]["easter_egg"]
        extra = paused["remaining"]["extra"]

        # 1. Embed loop
        self.timer_tasks[timer_id].append(asyncio.create_task(
            self._embed_update_loop(timer_id, game_number)
        ))
        # 2. Easter egg audio
        if egg > 0:
            self.timer_tasks[timer_id].append(asyncio.create_task(
                self._audio_at(egg, egg_audio, timer_id, paused.get("voice_channel_id", 0))
            ))
        # 3. Main-end audio
        if main > 0:
            self.timer_tasks[timer_id].append(asyncio.create_task(
                self._audio_at(main, turns_audio, timer_id, paused.get("voice_channel_id", 0))
            ))
        # 4. Final audio (delay = main + extra = total remaining)
        total_remaining = main + extra
        self.timer_tasks[timer_id].append(asyncio.create_task(
            self._final_audio(total_remaining, final_audio, timer_id, paused.get("voice_channel_id", 0), draw_event)
        ))

        MAIN_TOTAL = orig["main"]
        EXTRA_TOTAL = orig["extra"]

        phase = "running" if main > 0 else "extra"
        end_ts_main = ts(now_utc() + timedelta(seconds=main))
        end_ts_final = ts(now_utc() + timedelta(seconds=total_remaining))

        embed = _build_timer_embed(
            game_number=game_number,
            phase=phase,
            main_total=MAIN_TOTAL,
            extra_total=EXTRA_TOTAL,
            remaining_main=main,
            remaining_total=total_remaining,
            end_ts_main=end_ts_main,
            end_ts_final=end_ts_final,
            player_ids=player_ids,
        )

        msg = await safe_ctx_followup(ctx, embed=embed)
        self.timer_messages[timer_id] = (ctx.channel.id, msg.id)

        # Persist
        main_remaining_sec = main
        extra_only_sec = extra

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
                "extra": extra_only_sec,
                "easter_egg": paused["remaining"]["easter_egg"],
            },
            remaining=None,
            ignore_autostop=bool(paused.get("ignore_autostop", False)),
            messages=paused["messages"],
            audio=paused["audio"],
            player_mention_ids=player_ids,
            game_number=game_number,
            original_durations=orig,
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

        now = now_utc()

        # ---------- active timer ----------
        if timer_id in self.active_timers:
            data = self.active_timers[timer_id]
            dur = data["durations"]
            main_dur = dur["main"]
            extra_dur = dur["extra"]

            elapsed = (now - data["start_time"]).total_seconds()
            remaining_main = max(main_dur - elapsed, 0.0)
            remaining_total = max(main_dur + extra_dur - elapsed, 0.0)

            orig = data.get("original_durations") or dur
            bar = _build_progress_bar(orig["main"], orig["extra"], remaining_main, remaining_total)

            game_number = data.get("game_number", game)

            color = _game_color(game_number)
            if remaining_main > 0:
                m, s = int(remaining_main // 60), int(remaining_main % 60)
                embed = discord.Embed(
                    title=f"⏱️ ECL Game {game_number} — Running",
                    description=f"```{bar}```",
                    color=color,
                )
                embed.add_field(name="Main Time", value=f"**{m}:{s:02d}** remaining", inline=False)
            elif remaining_total > 0:
                m, s = int(remaining_total // 60), int(remaining_total % 60)
                embed = discord.Embed(
                    title=f"⏱️ ECL Game {game_number} — Extra Time",
                    description=f"```{bar}```",
                    color=color,
                )
                embed.add_field(name="Extra Time", value=f"**{m}:{s:02d}** remaining", inline=False)
            else:
                embed = discord.Embed(
                    title=f"⏱️ ECL Game {game_number} — Game Over",
                    description=f"```{bar}```",
                    color=color,
                )

            await safe_ctx_respond(ctx, embed=embed, ephemeral=True)
            return

        # ---------- paused timer ----------
        if timer_id in self.paused_timers:
            data = self.paused_timers[timer_id]
            remaining = data["remaining"]

            remaining_main = float(remaining.get("main", 0.0))
            remaining_extra = float(remaining.get("extra", 0.0))
            remaining_total = remaining_main + remaining_extra

            orig = data.get("original_durations") or {
                "main": TIMER_MINUTES * 60.0,
                "extra": EXTRA_TURNS_MINUTES * 60.0,
            }
            bar = _build_progress_bar(orig["main"], orig["extra"], remaining_main, remaining_total)

            game_number = data.get("game_number", game)

            embed = discord.Embed(
                title=f"⏸️ ECL Game {game_number} — Paused",
                description=f"```{bar}```",
                color=_game_color(game_number),
            )
            if remaining_main > 0:
                m, s = int(remaining_main // 60), int(remaining_main % 60)
                embed.add_field(name="Main Time", value=f"**{m}:{s:02d}** remaining", inline=False)
            elif remaining_total > 0:
                m, s = int(remaining_total // 60), int(remaining_total % 60)
                embed.add_field(name="Extra Time", value=f"**{m}:{s:02d}** remaining", inline=False)

            await safe_ctx_respond(ctx, embed=embed, ephemeral=True)
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
