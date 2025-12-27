from __future__ import annotations

import asyncio
from typing import Dict, Optional

from .models import LFGLobby


class LobbyStore:
    """In-memory lobby registry keyed by guild_id and lobby_id.

    Kept intentionally small: just storage + locking + id allocation.
    All behavioral rules (Elo, timeouts, etc.) live elsewhere.
    """

    def __init__(self) -> None:
        self._guild_lobbies: Dict[int, Dict[int, LFGLobby]] = {}
        self._lock = asyncio.Lock()
        self._next_lobby_id: int = 1

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    def alloc_lobby_id(self) -> int:
        lid = self._next_lobby_id
        self._next_lobby_id += 1
        return lid

    def get_guild_lobbies(self, guild_id: int) -> Dict[int, LFGLobby]:
        """Return the mutable lobby dict for a guild (creates if missing)."""
        return self._guild_lobbies.setdefault(int(guild_id), {})

    def peek_guild_lobbies(self, guild_id: int) -> Dict[int, LFGLobby]:
        """Return the lobby dict for a guild (does not create)."""
        return self._guild_lobbies.get(int(guild_id), {})

    def find_user_lobby(
        self,
        guild_id: int,
        user_id: int,
        *,
        exclude_lobby_id: Optional[int] = None,
    ) -> Optional[LFGLobby]:
        lobbies = self._guild_lobbies.get(int(guild_id), {})
        for lid, lob in lobbies.items():
            if exclude_lobby_id is not None and lid == exclude_lobby_id:
                continue
            if int(user_id) in lob.player_ids:
                return lob
        return None

    def get_lobby(self, guild_id: int, lobby_id: int) -> Optional[LFGLobby]:
        return self._guild_lobbies.get(int(guild_id), {}).get(int(lobby_id))

    def is_lobby_active(self, lobby: LFGLobby) -> bool:
        lobbies = self._guild_lobbies.get(int(lobby.guild_id), {})
        return lobbies.get(int(lobby.lobby_id)) is lobby

    def remove_lobby(self, guild_id: int, lobby_id: int) -> Optional[LFGLobby]:
        lobbies = self._guild_lobbies.get(int(guild_id))
        if not lobbies:
            return None
        lob = lobbies.pop(int(lobby_id), None)
        if not lobbies:
            self._guild_lobbies.pop(int(guild_id), None)
        return lob
