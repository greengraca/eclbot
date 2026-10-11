# game_rooms.py
# Creates the online room for a full LFG pod: Convoke when CONVOKE_API_KEY is set,
# otherwise the TopDeck SpellTable proxy.
from dataclasses import dataclass
from typing import List, Optional

import discord

from convoke_client import convoke_enabled, create_convoke_game
from spelltable_client import create_spelltable_game

GAME_NAME = "ECL DragonShield"


@dataclass(frozen=True)
class GameRoom:
    link: str
    password: Optional[str] = None


def service_name(link: str = "") -> str:
    """Human name of the room service, from a link if we have one."""
    if link:
        return "Convoke" if "convoke" in link else "SpellTable"
    return "Convoke" if convoke_enabled() else "SpellTable"


async def create_game_room(
    guild: discord.Guild,
    channel_id: int,
    player_ids: List[int],
) -> GameRoom:
    """Create a room for these players. Raises on failure."""
    if convoke_enabled():
        players = []
        for uid in player_ids:
            member = guild.get_member(int(uid))
            players.append((int(uid), member.display_name if member else str(uid)))
        url, password = await create_convoke_game(
            game_name=GAME_NAME,
            guild_id=guild.id,
            channel_id=channel_id,
            players=players,
            seats=max(4, len(player_ids)),
        )
        return GameRoom(link=url, password=password)

    link = await create_spelltable_game(game_name=GAME_NAME, format_name="Commander", is_public=False)
    return GameRoom(link=link)
