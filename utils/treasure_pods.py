# utils/treasure_pods.py
"""Configurable Treasure Pods system.

Supports multiple pod types per month (e.g. "Bring a Friend" + "Card Prize").
Pod types are configured via TREASURE_POD_TYPES env var (JSON array).
Falls back to legacy TREASURE_PODS_PER_MONTH bring-a-friend pods if unset.

The table numbers are encrypted so moderators (who are also players) cannot
know which pods will be treasure pods in advance.

The treasure table numbers correspond directly to TopDeck table numbers (1, 2, 3, ...)
which increment throughout each monthly league event.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import random
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from cryptography.fernet import Fernet

from utils.dates import month_key
from utils.logger import log_ok, log_warn


# Env var for encryption key
TREASURE_POD_SECRET = os.getenv("TREASURE_POD_SECRET", "").strip()

# Games per player estimate (based on historical data)
# December: 540/240 = 2.25, January: 315/96 = 3.28
GAMES_PER_PLAYER_ESTIMATE = float(os.getenv("TREASURE_GAMES_PER_PLAYER", "2.75"))

# Number of treasure pods per month (legacy fallback)
TREASURE_PODS_PER_MONTH = int(os.getenv("TREASURE_PODS_PER_MONTH", "5"))

# Minimum table number before first treasure pod can appear
MIN_TABLE_OFFSET = 10

# Default bring-a-friend metadata (used for legacy/old-format schedules)
_DEFAULT_BAF = {
    "type": "bring_a_friend",
    "title": "Bring a Friend Treasure Pod!",
    "description": (
        "**Congratulations!** This game is a **Treasure Pod**!\n\n"
        "The **winner** of this game will receive **free ECL access** "
        "for an unregistered friend for the current or next league!\n\n"
        "Please **open a ticket** to claim your prize! 🍀"
    ),
    "image_url": "",
}


def _parse_pod_types() -> List[Dict[str, Any]]:
    """Parse TREASURE_POD_TYPES JSON from env; fallback to legacy config."""
    raw = os.getenv("TREASURE_POD_TYPES", "").strip()
    if raw:
        try:
            types = json.loads(raw)
            if isinstance(types, list) and types:
                return types
        except (json.JSONDecodeError, TypeError):
            log_warn("[treasure] Failed to parse TREASURE_POD_TYPES; using legacy fallback")
    # Legacy fallback: all pods are bring-a-friend
    return [{
        "type": "bring_a_friend",
        "count": TREASURE_PODS_PER_MONTH,
        "title": _DEFAULT_BAF["title"],
        "description": _DEFAULT_BAF["description"],
        "image_url": "",
    }]


TREASURE_POD_TYPES: List[Dict[str, Any]] = _parse_pod_types()


def _get_fernet() -> Optional[Fernet]:
    """Get Fernet cipher from secret. Returns None if not configured."""
    if not TREASURE_POD_SECRET:
        return None
    key = hashlib.sha256(TREASURE_POD_SECRET.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(key)
    return Fernet(fernet_key)


def _encrypt_data(data: Any) -> Optional[str]:
    """Encrypt arbitrary JSON-serializable data. Returns base64 string or None."""
    fernet = _get_fernet()
    if not fernet:
        return None
    raw = json.dumps(data).encode()
    return fernet.encrypt(raw).decode()


def _decrypt_data(encrypted: str) -> Any:
    """Decrypt to arbitrary JSON data. Returns None if failed."""
    fernet = _get_fernet()
    if not fernet:
        return None
    try:
        raw = fernet.decrypt(encrypted.encode())
        return json.loads(raw.decode())
    except Exception:
        return None


def encrypt_table_numbers(table_numbers: List[int]) -> Optional[str]:
    """Encrypt a list of table numbers (legacy compat wrapper)."""
    return _encrypt_data(table_numbers)


def decrypt_table_numbers(encrypted: str) -> Optional[List[int]]:
    """Decrypt to a flat list of table numbers (legacy compat wrapper)."""
    data = _decrypt_data(encrypted)
    if data is None:
        return None
    # Old format: plain list
    if isinstance(data, list):
        return data
    # New dict format: flatten all types
    if isinstance(data, dict):
        all_tables: List[int] = []
        for tables in data.values():
            if isinstance(tables, list):
                all_tables.extend(tables)
        return sorted(all_tables)
    return None


def _decrypt_table_map(encrypted: str) -> Optional[Dict[str, List[int]]]:
    """Decrypt to a typed table map {type_id: [table_numbers]}.

    Auto-upgrades old list format to {"bring_a_friend": [...]}.
    """
    data = _decrypt_data(encrypted)
    if data is None:
        return None
    if isinstance(data, list):
        return {"bring_a_friend": data}
    if isinstance(data, dict):
        return data
    return None


def _encrypt_table_map(table_map: Dict[str, List[int]]) -> Optional[str]:
    """Encrypt a typed table map."""
    return _encrypt_data(table_map)


def estimate_total_tables(player_count: int) -> int:
    """Estimate total tables for a month based on player count."""
    if player_count <= 0:
        return 100
    estimated = int(player_count * GAMES_PER_PLAYER_ESTIMATE)
    return max(estimated, 50)


def generate_treasure_table_numbers(
    estimated_total: int,
    count: int = TREASURE_PODS_PER_MONTH,
    exclude: Optional[set] = None,
) -> List[int]:
    """
    Generate treasure table numbers spread across the estimated total.

    First ~28% of tables have LESS weight (at most 1 treasure).
    Remaining treasures spread across rest of the month.

    ``exclude`` contains table numbers already claimed by other types.
    """
    if estimated_total < 50:
        estimated_total = 50

    if exclude is None:
        exclude = set()

    low_weight_cutoff = max(MIN_TABLE_OFFSET + 10, int(estimated_total / 3.5))

    table_numbers: List[int] = []

    # 50% chance of 1 treasure in early zone, 50% chance of 0
    early_zone_count = 1 if random.random() < 0.5 else 0
    late_zone_count = count - early_zone_count

    if early_zone_count > 0:
        early_start = MIN_TABLE_OFFSET
        early_end = low_weight_cutoff
        for _ in range(50):
            candidate = random.randint(early_start, early_end)
            if candidate not in exclude:
                table_numbers.append(candidate)
                break
        else:
            # Fallback: pick anything in range
            table_numbers.append(random.randint(early_start, early_end))

    if late_zone_count > 0:
        late_start = low_weight_cutoff + 1
        late_end = estimated_total
        late_range = late_end - late_start

        if late_range < late_zone_count * 5:
            late_range = max(late_zone_count * 5, late_range)

        bucket_size = late_range // late_zone_count
        if bucket_size < 5:
            bucket_size = 5

        for i in range(late_zone_count):
            bucket_start = late_start + (i * bucket_size)
            bucket_end = bucket_start + bucket_size - 1

            if i == late_zone_count - 1:
                bucket_end = estimated_total

            for _ in range(50):
                candidate = random.randint(bucket_start, bucket_end)
                if candidate not in exclude and candidate not in table_numbers:
                    break
            table_numbers.append(candidate)

    return sorted(table_numbers)


def generate_all_treasure_tables(
    estimated_total: int,
    pod_types: List[Dict[str, Any]],
) -> Dict[str, List[int]]:
    """Generate non-overlapping table numbers for each pod type."""
    result: Dict[str, List[int]] = {}
    exclude: set[int] = set()

    for pt in pod_types:
        type_id = pt["type"]
        count = int(pt.get("count", 1))
        tables = generate_treasure_table_numbers(estimated_total, count, exclude=exclude)
        result[type_id] = tables
        exclude.update(tables)

    return result


class TreasurePodManager:
    """
    Manages treasure pod schedule and checking.

    Treasure table numbers correspond directly to TopDeck table numbers.
    """

    def __init__(self, db_schedule_collection, db_results_collection):
        self.schedule_col = db_schedule_collection
        self.results_col = db_results_collection

    async def get_schedule(self, guild_id: int, month: str) -> Optional[Dict[str, Any]]:
        """Get the schedule document for a month."""
        return await self.schedule_col.find_one({"guild_id": guild_id, "month": month})

    def _get_type_meta(
        self,
        type_id: str,
        pod_types_config: Optional[List[Dict[str, Any]]],
    ) -> Dict[str, str]:
        """Look up title/description/image for a type_id from the config snapshot."""
        if pod_types_config:
            for pt in pod_types_config:
                if pt.get("type") == type_id:
                    return {
                        "pod_type": type_id,
                        "pod_title": pt.get("title", type_id),
                        "pod_description": pt.get("description", ""),
                        "pod_image_url": pt.get("image_url", ""),
                    }
        # Fallback for old schedules or unknown type
        if type_id == "bring_a_friend":
            return {
                "pod_type": "bring_a_friend",
                "pod_title": _DEFAULT_BAF["title"],
                "pod_description": _DEFAULT_BAF["description"],
                "pod_image_url": "",
            }
        return {
            "pod_type": type_id,
            "pod_title": type_id.replace("_", " ").title(),
            "pod_description": "",
            "pod_image_url": "",
        }

    async def create_schedule(
        self,
        guild_id: int,
        month: str,
        player_count: int,
    ) -> Dict[str, Any]:
        """Create a new treasure pod schedule for a month."""
        estimated_total = estimate_total_tables(player_count)
        pod_types = TREASURE_POD_TYPES

        table_map = generate_all_treasure_tables(estimated_total, pod_types)
        encrypted = _encrypt_table_map(table_map)

        doc = {
            "guild_id": guild_id,
            "month": month,
            "encrypted_tables": encrypted,
            "estimated_total": estimated_total,
            "player_count_at_creation": player_count,
            "fired_tables": [],
            "pod_types_config": pod_types,
            "created_at": datetime.now(timezone.utc),
        }

        await self.schedule_col.update_one(
            {"guild_id": guild_id, "month": month},
            {"$set": doc},
            upsert=True,
        )

        total_count = sum(len(v) for v in table_map.values())
        type_summary = ", ".join(f"{k}={len(v)}" for k, v in table_map.items())
        log_ok(
            f"[treasure] Created schedule for {month}: "
            f"estimated_total={estimated_total}, player_count={player_count}, "
            f"types=[{type_summary}], total_pods={total_count}"
        )

        return doc

    async def check_if_treasure_pod(
        self,
        guild_id: int,
        month: str,
        table: int,
        player_discord_ids: List[int],
        player_topdeck_uids: List[str],
    ) -> Optional[Dict[str, Any]]:
        """
        Check if this table number is a treasure pod.

        Returns treasure pod info dict if it is, None otherwise.
        """
        schedule = await self.get_schedule(guild_id, month)
        if not schedule:
            return None

        encrypted = schedule.get("encrypted_tables")
        if not encrypted:
            return None

        table_map = _decrypt_table_map(encrypted)
        if not table_map:
            log_warn(f"[treasure] Failed to decrypt schedule for {month}")
            return None

        fired = set(schedule.get("fired_tables", []))
        if table in fired:
            return None

        # Find which type this table belongs to
        matched_type: Optional[str] = None
        for type_id, tables in table_map.items():
            if table in tables:
                matched_type = type_id
                break

        if matched_type is None:
            return None

        # It's a treasure pod!
        log_ok(f"[treasure] 🎁 TREASURE POD table #{table} triggered! (type={matched_type})")

        await self.schedule_col.update_one(
            {"guild_id": guild_id, "month": month},
            {"$push": {"fired_tables": table}},
        )

        pod_types_config = schedule.get("pod_types_config")
        type_meta = self._get_type_meta(matched_type, pod_types_config)

        treasure_doc = {
            "guild_id": guild_id,
            "month": month,
            "table": table,
            "player_discord_ids": player_discord_ids,
            "player_topdeck_uids": player_topdeck_uids,
            "status": "pending",
            "winner_entrant_id": None,
            "winner_topdeck_uid": None,
            "winner_discord_handle": None,
            "created_at": datetime.now(timezone.utc),
            **type_meta,
        }

        await self.results_col.insert_one(treasure_doc)

        return treasure_doc

    async def get_pending_treasure_pods(self, guild_id: int, month: str) -> List[Dict[str, Any]]:
        """Get all pending treasure pods for a month."""
        cursor = self.results_col.find({
            "guild_id": guild_id,
            "month": month,
            "status": "pending",
        })
        return await cursor.to_list(length=None)

    async def update_treasure_pod_result(
        self,
        guild_id: int,
        month: str,
        table: int,
        winner_entrant_id: Optional[int],
        winner_topdeck_uid: Optional[str],
        winner_discord_handle: Optional[str],
        is_draw: bool,
        current_max_table: int = 0,
        new_player_count: Optional[int] = None,
    ) -> bool:
        """
        Update a treasure pod with the game result.

        If draw, marks as draw and adds replacement pod.
        """
        query = {
            "guild_id": guild_id,
            "month": month,
            "table": table,
            "status": "pending",
        }

        if is_draw:
            update = {
                "$set": {
                    "status": "draw",
                    "updated_at": datetime.now(timezone.utc),
                }
            }
        else:
            update = {
                "$set": {
                    "status": "won",
                    "winner_entrant_id": winner_entrant_id,
                    "winner_topdeck_uid": winner_topdeck_uid,
                    "winner_discord_handle": winner_discord_handle,
                    "updated_at": datetime.now(timezone.utc),
                }
            }

        # Look up the pod_type before updating status
        pod_doc = await self.results_col.find_one(query)
        pod_type = pod_doc.get("pod_type", "bring_a_friend") if pod_doc else "bring_a_friend"

        result = await self.results_col.update_one(query, update)

        if result.modified_count > 0 and is_draw:
            await self._add_replacement_table(
                guild_id, month, current_max_table, new_player_count,
                pod_type=pod_type,
            )

        return result.modified_count > 0

    async def _add_replacement_table(
        self,
        guild_id: int,
        month: str,
        current_max_table: int,
        new_player_count: Optional[int] = None,
        pod_type: str = "bring_a_friend",
    ) -> None:
        """Add a replacement treasure table number after a draw."""
        schedule = await self.get_schedule(guild_id, month)
        if not schedule:
            return

        encrypted = schedule.get("encrypted_tables")
        if not encrypted:
            return

        table_map = _decrypt_table_map(encrypted)
        if not table_map:
            return

        if new_player_count and new_player_count > 0:
            estimated_total = estimate_total_tables(new_player_count)
        else:
            estimated_total = schedule.get("estimated_total", 250)

        # Collect ALL tables across all types to avoid cross-type collisions
        all_tables: set[int] = set()
        for tables in table_map.values():
            all_tables.update(tables)

        # Add replacement 1-30 tables after current max (can appear immediately!)
        min_new = current_max_table + 1
        max_new = current_max_table + 30

        for _ in range(20):
            candidate = random.randint(min_new, max_new)
            if candidate not in all_tables:
                break
        else:
            candidate = random.randint(min_new, max_new)

        # Add to the correct type's list
        if pod_type not in table_map:
            table_map[pod_type] = []
        table_map[pod_type] = sorted(table_map[pod_type] + [candidate])

        new_encrypted = _encrypt_table_map(table_map)

        if new_encrypted:
            await self.schedule_col.update_one(
                {"guild_id": guild_id, "month": month},
                {"$set": {
                    "encrypted_tables": new_encrypted,
                    "estimated_total": estimated_total,
                }},
            )
            log_ok(f"[treasure] Added replacement table #{candidate} ({pod_type}) for {month} after draw")

    async def get_stats(self, guild_id: int, month: str) -> Dict[str, Any]:
        """Get treasure pod stats (safe for mods - doesn't reveal table numbers)."""
        schedule = await self.get_schedule(guild_id, month)
        if not schedule:
            return {
                "month": month,
                "scheduled": False,
                "treasures_fired": 0,
                "treasures_remaining": 0,
            }

        fired = set(schedule.get("fired_tables", []))
        encrypted = schedule.get("encrypted_tables")

        total_scheduled = 0
        type_stats: Dict[str, Dict[str, int]] = {}

        if encrypted:
            table_map = _decrypt_table_map(encrypted)
            if table_map:
                for type_id, tables in table_map.items():
                    type_fired = len([t for t in tables if t in fired])
                    type_total = len(tables)
                    type_stats[type_id] = {
                        "total": type_total,
                        "fired": type_fired,
                        "remaining": type_total - type_fired,
                    }
                    total_scheduled += type_total

        # Get type metadata from schedule config
        pod_types_config = schedule.get("pod_types_config")
        if pod_types_config:
            for type_id in type_stats:
                meta = self._get_type_meta(type_id, pod_types_config)
                type_stats[type_id]["title"] = meta["pod_title"]

        return {
            "month": month,
            "scheduled": True,
            "treasures_fired": len(fired),
            "treasures_remaining": total_scheduled - len(fired),
            "estimated_total": schedule.get("estimated_total", 0),
            "type_stats": type_stats,
        }

    async def get_won_pods(self, guild_id: int, month: str) -> List[Dict[str, Any]]:
        """Get all won treasure pods for a month."""
        cursor = self.results_col.find({
            "guild_id": guild_id,
            "month": month,
            "status": "won",
        })
        return await cursor.to_list(length=None)

    async def redistribute_skipped_pods(
        self,
        guild_id: int,
        month: str,
        current_max_table: int,
    ) -> bool:
        """
        Detect unfired pods whose table numbers are already below current_max_table
        (i.e. skipped because the game was played without the bot timer) and
        redistribute them to upcoming table numbers.

        Returns True if any pods were redistributed.
        """
        schedule = await self.get_schedule(guild_id, month)
        if not schedule:
            return False

        encrypted = schedule.get("encrypted_tables")
        if not encrypted:
            return False

        table_map = _decrypt_table_map(encrypted)
        if not table_map:
            return False

        fired = set(schedule.get("fired_tables", []))

        # Collect ALL tables across all types (for collision avoidance)
        all_tables: set[int] = set()
        for tables in table_map.values():
            all_tables.update(tables)

        # Find skipped pods per type: unfired AND below current_max_table
        any_skipped = False
        for type_id, tables in table_map.items():
            skipped = [t for t in tables if t not in fired and t < current_max_table]
            if not skipped:
                continue

            any_skipped = True
            remaining = [t for t in tables if t not in skipped]

            # Generate replacement table numbers in [current_max_table+1, current_max_table+30]
            min_new = current_max_table + 1
            max_new = current_max_table + 30
            replacements: List[int] = []

            for old_table in skipped:
                for _ in range(50):
                    candidate = random.randint(min_new, max_new)
                    if candidate not in all_tables and candidate not in replacements:
                        break
                else:
                    # Fallback: pick sequentially
                    for fallback in range(min_new, max_new + 1):
                        if fallback not in all_tables and fallback not in replacements:
                            candidate = fallback
                            break
                replacements.append(candidate)

            all_tables.update(replacements)
            table_map[type_id] = sorted(remaining + replacements)

            skipped_str = ", ".join(f"#{t}" for t in skipped)
            log_ok(
                f"[treasure] Redistributed {len(skipped)} skipped {type_id} pod(s): "
                f"{skipped_str} (max_table={current_max_table})"
            )

        if not any_skipped:
            total_unfired = sum(
                1 for tables in table_map.values()
                for t in tables if t not in fired
            )
            log_ok(
                f"[treasure] redistribute check: 0 skipped "
                f"(max_table={current_max_table}, unfired={total_unfired})"
            )
            return False

        new_encrypted = _encrypt_table_map(table_map)
        if not new_encrypted:
            return False

        await self.schedule_col.update_one(
            {"guild_id": guild_id, "month": month},
            {"$set": {"encrypted_tables": new_encrypted}},
        )

        return True

    async def check_and_recalculate_if_needed(
        self,
        guild_id: int,
        month: str,
        days_until_close: float,
        current_max_table: int,
        new_player_count: Optional[int] = None,
    ) -> bool:
        """
        Check if treasure pods need recalculation and do it if needed.

        current_max_table is the highest table number seen so far (from TopDeck).
        """
        schedule = await self.get_schedule(guild_id, month)
        if not schedule:
            return False

        encrypted = schedule.get("encrypted_tables")
        if not encrypted:
            return False

        table_map = _decrypt_table_map(encrypted)
        if not table_map:
            return False

        fired = set(schedule.get("fired_tables", []))

        # Flatten all tables to find nearest unfired across ALL types
        all_tables: List[int] = []
        for tables in table_map.values():
            all_tables.extend(tables)
        unfired = sorted([n for n in all_tables if n not in fired])

        if not unfired:
            return False

        # Determine max distance based on days until close
        if days_until_close <= 3:
            max_distance = 5
        elif days_until_close <= 5:
            max_distance = 15
        elif days_until_close <= 11:
            max_distance = 30
        else:
            return False

        nearest = min(unfired)
        distance = nearest - current_max_table

        if distance <= max_distance:
            return False

        # Need to recalculate! Redistribute each type's unfired tables independently
        range_start = current_max_table + 1
        range_end = current_max_table + max_distance

        used: set[int] = set(fired)  # Don't reuse fired tables
        new_map: Dict[str, List[int]] = {}

        for type_id, tables in table_map.items():
            type_fired = [t for t in tables if t in fired]
            type_unfired = [t for t in tables if t not in fired]
            num_unfired = len(type_unfired)

            new_tables: List[int] = []
            for _ in range(num_unfired):
                for _ in range(50):
                    candidate = random.randint(range_start, range_end)
                    if candidate not in used and candidate not in new_tables:
                        new_tables.append(candidate)
                        break
                else:
                    for fallback in range(range_start, range_end + 1):
                        if fallback not in used and fallback not in new_tables:
                            new_tables.append(fallback)
                            break

            used.update(new_tables)
            new_map[type_id] = sorted(type_fired + new_tables)

        total_unfired = sum(
            len([t for t in tables if t not in fired])
            for tables in table_map.values()
        )

        new_encrypted = _encrypt_table_map(new_map)
        if not new_encrypted:
            return False

        update_doc: Dict[str, Any] = {"$set": {"encrypted_tables": new_encrypted}}

        if new_player_count and new_player_count > 0:
            new_estimated = estimate_total_tables(new_player_count)
            update_doc["$set"]["estimated_total"] = new_estimated

        await self.schedule_col.update_one(
            {"guild_id": guild_id, "month": month},
            update_doc,
        )

        log_ok(f"[treasure] Schedule recalculated to sooner ({total_unfired} unfired remaining)")

        return True

    async def check_pending_results(
        self,
        guild_id: int,
        month: str,
        matches: List[Any],
        entrant_to_uid: Dict[int, Optional[str]],
        player_map: Dict[str, Dict],
        current_max_table: int = 0,
        new_player_count: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Check pending treasure pods against TopDeck match results.

        Uses cached match data to determine winners/draws.
        """
        pending = await self.get_pending_treasure_pods(guild_id, month)
        if not pending:
            return {"checked": 0, "won": 0, "draw": 0, "still_pending": 0}

        match_lookup: Dict[int, Any] = {}
        for m in matches:
            match_lookup[m.id] = m

        results = {"checked": 0, "won": 0, "draw": 0, "still_pending": 0}

        for pod in pending:
            table = pod.get("table")
            if table is None:
                continue

            results["checked"] += 1

            match = match_lookup.get(table)
            if not match:
                results["still_pending"] += 1
                continue

            winner = match.winner

            if winner is None:
                results["still_pending"] += 1
                continue

            if winner == "_DRAW_":
                await self.update_treasure_pod_result(
                    guild_id=guild_id,
                    month=month,
                    table=table,
                    winner_entrant_id=None,
                    winner_topdeck_uid=None,
                    winner_discord_handle=None,
                    is_draw=True,
                    current_max_table=current_max_table,
                    new_player_count=new_player_count,
                )
                results["draw"] += 1
                log_ok(f"[treasure] Treasure pod table #{table} was a DRAW - replacement scheduled")
            else:
                try:
                    winner_entrant_id = int(winner)
                except (ValueError, TypeError):
                    winner_entrant_id = None

                winner_uid = entrant_to_uid.get(winner_entrant_id) if winner_entrant_id else None

                winner_discord = None
                if winner_uid:
                    player_data = player_map.get(str(winner_uid), {})
                    winner_discord = player_data.get("discord", "")

                await self.update_treasure_pod_result(
                    guild_id=guild_id,
                    month=month,
                    table=table,
                    winner_entrant_id=winner_entrant_id,
                    winner_topdeck_uid=winner_uid,
                    winner_discord_handle=winner_discord,
                    is_draw=False,
                )
                results["won"] += 1
                log_ok(f"[treasure] Treasure pod table #{table} WON by {winner_discord or winner_uid or winner_entrant_id}")

        return results
