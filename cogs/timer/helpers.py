# cogs/timer/helpers.py
"""Pure utility functions for the timer cog."""

import os
from datetime import datetime, timezone
from typing import List, Optional

import discord

from utils.logger import log_warn


# ---------------- env helpers ----------------

def env_float(name: str, default: float) -> float:
    """Get a float from environment variable with fallback."""
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# ---------------- time helpers ----------------

def now_utc() -> datetime:
    """Get current time in UTC."""
    return datetime.now(timezone.utc)


def ts(dt: datetime) -> int:
    """Convert datetime to Unix timestamp (seconds)."""
    return int(dt.timestamp())


def month_start_utc() -> datetime:
    """Get the first day of the current month in UTC."""
    now = datetime.now(timezone.utc)
    return datetime(now.year, now.month, 1, tzinfo=timezone.utc)


def make_timer_id(voice_channel_id: int, seq: int) -> str:
    """Generate a unique timer ID from voice channel ID and sequence number."""
    return f"{voice_channel_id}_{seq}"


# ---------------- handle normalization ----------------

def norm_handle(s: str) -> str:
    """Normalize a Discord handle for fuzzy matching (lowercase alphanumeric only)."""
    return "".join(ch for ch in s.lower() if ch.isalnum()) if s else ""


def norm_member_handles(m: discord.Member) -> set[str]:
    """Get all normalized handles for a Discord member.
    
    Returns a set of all possible handles (username, display_name, global_name).
    Username (m.name) is the primary match target.
    """
    out: set[str] = set()

    for cand in (m.name, getattr(m, "global_name", None), getattr(m, "display_name", None)):
        if isinstance(cand, str):
            h = norm_handle(cand)
            if h:
                out.add(h)

    discrim = getattr(m, "discriminator", None)
    if discrim and discrim != "0":
        h = norm_handle(f"{m.name}#{discrim}")
        if h:
            out.add(h)

    return out


# ---------------- voice helpers ----------------

VOICE_CONNECT_TIMEOUT = 10.0


def same_channel(
    vc: Optional[discord.VoiceClient],
    ch: Optional[discord.VoiceChannel],
) -> bool:
    """Check if a voice client is connected to a specific channel."""
    return bool(vc and vc.channel and ch and vc.channel.id == ch.id)


def voice_prereqs_ok() -> bool:
    """Check if voice prerequisites (Opus, PyNaCl) are available."""
    if not discord.opus.is_loaded():
        log_warn("[voice] Opus is not loaded")
        return False
    try:
        import nacl  # noqa: F401
    except Exception:
        log_warn("[voice] PyNaCl is not installed; voice cannot work")
        return False
    return True


def ffmpeg_src(path: str, ffmpeg_exe: str = "ffmpeg") -> discord.AudioSource:
    """Create an FFmpeg audio source for Discord playback."""
    return discord.FFmpegOpusAudio(
        path,
        executable=ffmpeg_exe,
        before_options="-nostdin",
        options="-vn",
    )


def non_bot_members(ch: discord.VoiceChannel) -> List[discord.Member]:
    """Get all non-bot members in a voice channel."""
    return [m for m in ch.members if not m.bot]


# ---------------- progress bar ----------------

def build_progress_bar(
    main_total: float,
    extra_total: float,
    remaining_main: float,
    remaining_total: float,
    *,
    width: int = 30,
) -> str:
    """
    Build a text progress bar:

    [██████░░░░░░|██░░░░░░░░]

    Left side = main time, right side = extra time.
    remaining_main / remaining_total are in seconds.
    """
    main_total = max(float(main_total), 0.0)
    extra_total = max(float(extra_total), 0.0)
    total = main_total + extra_total
    if total <= 0:
        return "[----------]"

    width = max(width, 10)

    # how many chars belong to main vs extra
    main_slots = max(1, int(round(width * (main_total / total))))
    extra_slots = max(1, width - main_slots)

    # elapsed amounts (clamped)
    elapsed_total = main_total + extra_total - remaining_total
    elapsed_total = max(0.0, min(elapsed_total, main_total + extra_total))

    elapsed_main = main_total - remaining_main
    elapsed_main = max(0.0, min(elapsed_main, main_total))

    elapsed_extra = max(0.0, elapsed_total - elapsed_main)
    elapsed_extra = max(0.0, min(elapsed_extra, extra_total))

    # convert to filled slots
    if main_total > 0:
        main_fill = int(round(main_slots * (elapsed_main / main_total)))
    else:
        main_fill = main_slots
    main_fill = max(0, min(main_fill, main_slots))

    if extra_total > 0:
        extra_fill = int(round(extra_slots * (elapsed_extra / extra_total)))
    else:
        extra_fill = 0
    extra_fill = max(0, min(extra_fill, extra_slots))

    filled_main = "█" * main_fill + "░" * (main_slots - main_fill)
    filled_extra = "█" * extra_fill + "░" * (extra_slots - extra_fill)

    return f"[{filled_main}|{filled_extra}]"
