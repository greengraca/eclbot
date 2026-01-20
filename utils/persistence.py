# utils/persistence.py
"""
MongoDB persistence layer for timers and LFG lobbies.

Provides serialization, deserialization, and CRUD operations
to survive Heroku restarts.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, TypedDict

from db import persistent_timers, persistent_lobbies


# ============================================================================
# TIMER PERSISTENCE
# ============================================================================

class TimerDoc(TypedDict, total=False):
    """Schema for persistent_timers documents."""
    timer_id: str  # unique key: "{voice_channel_id}_{seq}"
    guild_id: int
    channel_id: int  # text channel where timer messages go
    voice_channel_id: int
    message_id: Optional[int]  # the timer message to edit
    status: str  # "active" | "paused"
    
    # For active timers: when timer was started (UTC)
    start_time_utc: Optional[datetime]
    
    # Original durations in seconds
    durations_main: float
    durations_easter_egg: float
    durations_extra: float
    
    # For paused timers: remaining time in seconds
    remaining_main: Optional[float]
    remaining_easter_egg: Optional[float]
    remaining_extra: Optional[float]
    
    # Timer settings
    ignore_autostop: bool
    
    # Messages to send at each phase
    msg_turns: str
    msg_final: str
    
    # Audio file paths
    audio_turns: str
    audio_final: str
    audio_easter_egg: str
    
    # Absolute time when the timer fully expires (for cleanup)
    expires_at: datetime
    
    # Tracking
    created_at: datetime
    updated_at: datetime


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def save_timer(
    timer_id: str,
    guild_id: int,
    channel_id: int,
    voice_channel_id: int,
    message_id: Optional[int],
    status: str,
    start_time_utc: Optional[datetime],
    durations: Dict[str, float],
    remaining: Optional[Dict[str, float]],
    ignore_autostop: bool,
    messages: Dict[str, str],
    audio: Dict[str, str],
    expires_at: datetime,
) -> None:
    """Upsert a timer document."""
    now = _now_utc()
    
    doc: Dict[str, Any] = {
        "timer_id": str(timer_id),
        "guild_id": int(guild_id),
        "channel_id": int(channel_id),
        "voice_channel_id": int(voice_channel_id),
        "message_id": int(message_id) if message_id else None,
        "status": str(status),
        "start_time_utc": start_time_utc,
        "durations_main": float(durations.get("main", 0)),
        "durations_easter_egg": float(durations.get("easter_egg", 0)),
        "durations_extra": float(durations.get("extra", 0)),
        "remaining_main": float(remaining["main"]) if remaining else None,
        "remaining_easter_egg": float(remaining["easter_egg"]) if remaining else None,
        "remaining_extra": float(remaining["extra"]) if remaining else None,
        "ignore_autostop": bool(ignore_autostop),
        "msg_turns": str(messages.get("turns", "")),
        "msg_final": str(messages.get("final", "")),
        "audio_turns": str(audio.get("turns", "")),
        "audio_final": str(audio.get("final", "")),
        "audio_easter_egg": str(audio.get("easter_egg", "")),
        "expires_at": expires_at,
        "updated_at": now,
    }
    
    await persistent_timers.update_one(
        {"timer_id": timer_id},
        {"$set": doc, "$setOnInsert": {"created_at": now}},
        upsert=True,
    )


async def delete_timer(timer_id: str) -> None:
    """Remove a timer document."""
    await persistent_timers.delete_one({"timer_id": timer_id})


async def get_timer(timer_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a single timer by ID."""
    return await persistent_timers.find_one({"timer_id": timer_id})


async def get_all_active_timers() -> List[Dict[str, Any]]:
    """Fetch all timers that haven't expired yet."""
    now = _now_utc()
    cursor = persistent_timers.find({
        "expires_at": {"$gt": now},
        "status": {"$in": ["active", "paused"]},
    })
    return await cursor.to_list(length=1000)


async def get_guild_timers(guild_id: int) -> List[Dict[str, Any]]:
    """Fetch all active/paused timers for a guild."""
    now = _now_utc()
    cursor = persistent_timers.find({
        "guild_id": int(guild_id),
        "expires_at": {"$gt": now},
        "status": {"$in": ["active", "paused"]},
    })
    return await cursor.to_list(length=100)


async def cleanup_expired_timers() -> int:
    """Delete timers that have fully expired. Returns count deleted."""
    now = _now_utc()
    result = await persistent_timers.delete_many({"expires_at": {"$lte": now}})
    return result.deleted_count


# ============================================================================
# LFG LOBBY PERSISTENCE
# ============================================================================

class LobbyDoc(TypedDict, total=False):
    """Schema for persistent_lobbies documents."""
    guild_id: int
    lobby_id: int  # compound key with guild_id
    channel_id: int
    message_id: Optional[int]
    host_id: int
    player_ids: List[int]
    invited_ids: List[int]
    max_seats: int
    
    # SpellTable link (empty until pod is full)
    link: str
    link_creating: bool
    
    # Elo mode settings
    elo_mode: bool
    host_elo: Optional[float]
    elo_base_range: Optional[int]
    elo_range_step: Optional[int]
    elo_max_steps: int
    
    # Player points snapshot (for Elo display)
    player_pts: Dict[str, float]  # str keys because MongoDB
    
    # Timing
    created_at: datetime
    almost_full_at: Optional[datetime]
    last_seat_open: bool
    
    # Expiration for inactivity
    expires_at: datetime
    
    updated_at: datetime


async def save_lobby(
    guild_id: int,
    lobby_id: int,
    channel_id: int,
    message_id: Optional[int],
    host_id: int,
    player_ids: List[int],
    invited_ids: List[int],
    max_seats: int,
    link: str,
    link_creating: bool,
    elo_mode: bool,
    host_elo: Optional[float],
    elo_base_range: Optional[int],
    elo_range_step: Optional[int],
    elo_max_steps: int,
    player_pts: Dict[int, float],
    created_at: datetime,
    almost_full_at: Optional[datetime],
    last_seat_open: bool,
    expires_at: datetime,
) -> None:
    """Upsert a lobby document."""
    now = _now_utc()
    
    # Convert player_pts keys to strings (MongoDB requirement)
    pts_doc = {str(k): float(v) for k, v in (player_pts or {}).items()}
    
    doc: Dict[str, Any] = {
        "guild_id": int(guild_id),
        "lobby_id": int(lobby_id),
        "channel_id": int(channel_id),
        "message_id": int(message_id) if message_id else None,
        "host_id": int(host_id),
        "player_ids": [int(x) for x in player_ids],
        "invited_ids": [int(x) for x in invited_ids],
        "max_seats": int(max_seats),
        "link": str(link or ""),
        "link_creating": bool(link_creating),
        "elo_mode": bool(elo_mode),
        "host_elo": float(host_elo) if host_elo is not None else None,
        "elo_base_range": int(elo_base_range) if elo_base_range is not None else None,
        "elo_range_step": int(elo_range_step) if elo_range_step is not None else None,
        "elo_max_steps": int(elo_max_steps),
        "player_pts": pts_doc,
        "created_at": created_at,
        "almost_full_at": almost_full_at,
        "last_seat_open": bool(last_seat_open),
        "expires_at": expires_at,
        "updated_at": now,
    }
    
    await persistent_lobbies.update_one(
        {"guild_id": int(guild_id), "lobby_id": int(lobby_id)},
        {"$set": doc},
        upsert=True,
    )


async def delete_lobby(guild_id: int, lobby_id: int) -> None:
    """Remove a lobby document."""
    await persistent_lobbies.delete_one({
        "guild_id": int(guild_id),
        "lobby_id": int(lobby_id),
    })


async def get_lobby(guild_id: int, lobby_id: int) -> Optional[Dict[str, Any]]:
    """Fetch a single lobby."""
    return await persistent_lobbies.find_one({
        "guild_id": int(guild_id),
        "lobby_id": int(lobby_id),
    })


async def get_all_active_lobbies() -> List[Dict[str, Any]]:
    """Fetch all lobbies that haven't expired and don't have a link yet."""
    now = _now_utc()
    cursor = persistent_lobbies.find({
        "expires_at": {"$gt": now},
        "link": "",  # only active lobbies without SpellTable link
    })
    return await cursor.to_list(length=1000)


async def get_guild_lobbies(guild_id: int) -> List[Dict[str, Any]]:
    """Fetch all active lobbies for a guild."""
    now = _now_utc()
    cursor = persistent_lobbies.find({
        "guild_id": int(guild_id),
        "expires_at": {"$gt": now},
        "link": "",
    })
    return await cursor.to_list(length=100)


async def cleanup_expired_lobbies() -> int:
    """Delete lobbies that have expired or have links. Returns count deleted."""
    now = _now_utc()
    # Delete expired OR completed (have link)
    result = await persistent_lobbies.delete_many({
        "$or": [
            {"expires_at": {"$lte": now}},
            {"link": {"$ne": ""}},
        ]
    })
    return result.deleted_count


async def update_lobby_expires_at(guild_id: int, lobby_id: int, expires_at: datetime) -> None:
    """Update just the expiration time (called on interactions to reset timeout)."""
    await persistent_lobbies.update_one(
        {"guild_id": int(guild_id), "lobby_id": int(lobby_id)},
        {"$set": {"expires_at": expires_at, "updated_at": _now_utc()}},
    )


async def get_max_lobby_id(guild_id: int) -> int:
    """Get the highest lobby_id used for a guild (for ID allocation after restart)."""
    cursor = persistent_lobbies.find(
        {"guild_id": int(guild_id)},
        {"lobby_id": 1},
    ).sort("lobby_id", -1).limit(1)
    
    docs = await cursor.to_list(length=1)
    if docs:
        return int(docs[0].get("lobby_id", 0))
    return 0
