# online_games_store.py
import os
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# Where the JSON files will live
ONLINE_GAMES_DIR = os.getenv("ONLINE_GAMES_DIR", "./online_games")


@dataclass
class OnlineGameRecord:
    """
    Minimal info per TopDeck game we care about.

    - (season, tid) identify the TopDeck match.
    - start_ts: when the TopDeck game started (unix timestamp, seconds) if known.
    - entrant_ids: TopDeck entrant IDs (4 players).
    - discord_ids: Discord user IDs we matched to these entrants (4 players).
    - online: True = SpellTable / online game, False = in-person game.
    """
    season: int
    tid: int
    start_ts: Optional[float]
    entrant_ids: List[int]
    discord_ids: List[int]
    online: bool  # True = online / SpellTable, False = in-person


def _file_path(bracket_id: str, year: int, month: int) -> str:
    os.makedirs(ONLINE_GAMES_DIR, exist_ok=True)
    return os.path.join(ONLINE_GAMES_DIR, f"{bracket_id}_{year:04d}-{month:02d}.json")


def load_index(bracket_id: str, year: int, month: int) -> List[OnlineGameRecord]:
    """
    Load all OnlineGameRecord entries for a given bracket + month.
    Returns [] if the file doesn't exist yet.
    """
    path = _file_path(bracket_id, year, month)
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Support both {"games": [...]} and bare list formats
    raw_games = data.get("games", data) if isinstance(data, dict) else data

    records: List[OnlineGameRecord] = []
    for obj in raw_games:
        records.append(
            OnlineGameRecord(
                season=int(obj["season"]),
                tid=int(obj["tid"]),
                start_ts=obj.get("start_ts"),
                entrant_ids=list(obj.get("entrant_ids", [])),
                discord_ids=list(obj.get("discord_ids", [])),
                online=bool(obj.get("online", True)),
            )
        )
    return records


def save_index(
    bracket_id: str,
    year: int,
    month: int,
    records: List[OnlineGameRecord],
) -> None:
    """
    Overwrite the JSON file for this bracket + month with the given records.
    """
    path = _file_path(bracket_id, year, month)
    as_list = [asdict(r) for r in records]
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"games": as_list}, f, indent=2, sort_keys=True)


def upsert_record(
    bracket_id: str,
    year: int,
    month: int,
    record: OnlineGameRecord,
) -> None:
    """
    Add or replace one record based on (season, tid).
    This lets you safely re-run the script or add from /timer later.
    """
    records = load_index(bracket_id, year, month)
    key = (record.season, record.tid)
    seen: Dict[Tuple[int, int], OnlineGameRecord] = {
        (r.season, r.tid): r for r in records
    }
    seen[key] = record
    save_index(bracket_id, year, month, list(seen.values()))


def count_online_games_by_entrant(
    records: List[OnlineGameRecord],
    *,
    online_only: bool = True,
) -> Dict[int, int]:
    """
    Return {entrant_id -> number of games}.

    If online_only=True, counts only records where record.online is True.
    """
    counts: Dict[int, int] = {}
    for r in records:
        if online_only and not r.online:
            continue
        for eid in r.entrant_ids:
            counts[eid] = counts.get(eid, 0) + 1
    return counts
