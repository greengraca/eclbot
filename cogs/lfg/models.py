from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, TYPE_CHECKING


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


if TYPE_CHECKING:
    from .views import LFGJoinView


class LFGLobby:
    """In-memory state for a single LFG lobby."""

    def __init__(
        self,
        guild_id: int,
        channel_id: int,
        host_id: int,
        max_seats: int = 4,
        invited_ids: Optional[List[int]] = None,
        *,
        elo_mode: bool = False,
        host_elo: Optional[float] = None,
        elo_max_steps: int = 4,
    ) -> None:
        self.guild_id = int(guild_id)
        self.channel_id = int(channel_id)
        self.host_id = int(host_id)
        self.max_seats = int(max_seats)
        self.player_ids: List[int] = [int(host_id)]  # host always first
        self.invited_ids: List[int] = invited_ids or []
        self.message_id: Optional[int] = None   # set after we send the embed
        self.link: str = ""                     # SpellTable link once lobby is full

        # True while we're creating a SpellTable room (prevents duplicate creation)
        self.link_creating: bool = False

        # TopDeck points snapshot for display in Elo lobbies
        self.player_pts: Dict[int, float] = {}
        if host_elo is not None:
            self.player_pts[int(host_id)] = float(host_elo)

        # Internal id (supports multiple lobbies per guild)
        self.lobby_id: int = 0

        # Elo-specific
        self.elo_mode: bool = bool(elo_mode)
        self.host_elo: Optional[float] = host_elo
        self.created_at: datetime = now_utc()   # used for Elo window expansion & inactivity info

        # Per-lobby dynamic window params
        self.elo_base_range: Optional[int] = None
        self.elo_range_step: Optional[int] = None
        self.elo_max_steps: int = int(elo_max_steps)

        # Last-seat behaviour
        self.almost_full_at: Optional[datetime] = None  # set when lobby reaches 3/4
        self.last_seat_open: bool = False               # host override

        # Runtime refs
        self.view: Optional[LFGJoinView] = None
        self.update_task: Optional[asyncio.Task] = None

    def is_full(self) -> bool:
        return len(self.player_ids) >= self.max_seats

    def remaining_slots(self) -> int:
        return max(0, self.max_seats - len(self.player_ids))

    def has_link(self) -> bool:
        return bool(self.link)
