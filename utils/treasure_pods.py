# utils/treasure_pods.py
"""Bring a Friend Treasure Pods system.

Randomly selects 5 table numbers per month to be "treasure pods" where the winner
gets free ECL access for an unregistered friend.

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

# Number of treasure pods per month
TREASURE_PODS_PER_MONTH = int(os.getenv("TREASURE_PODS_PER_MONTH", "5"))

# Minimum table number before first treasure pod can appear
MIN_TABLE_OFFSET = 10


def _get_fernet() -> Optional[Fernet]:
    """Get Fernet cipher from secret. Returns None if not configured."""
    if not TREASURE_POD_SECRET:
        return None
    key = hashlib.sha256(TREASURE_POD_SECRET.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(key)
    return Fernet(fernet_key)


def encrypt_table_numbers(table_numbers: List[int]) -> Optional[str]:
    """Encrypt a list of table numbers. Returns base64 string or None if no secret."""
    fernet = _get_fernet()
    if not fernet:
        return None
    data = json.dumps(table_numbers).encode()
    return fernet.encrypt(data).decode()


def decrypt_table_numbers(encrypted: str) -> Optional[List[int]]:
    """Decrypt an encrypted table numbers string. Returns None if failed."""
    fernet = _get_fernet()
    if not fernet:
        return None
    try:
        data = fernet.decrypt(encrypted.encode())
        return json.loads(data.decode())
    except Exception:
        return None


def estimate_total_tables(player_count: int) -> int:
    """Estimate total tables for a month based on player count."""
    if player_count <= 0:
        return 100
    estimated = int(player_count * GAMES_PER_PLAYER_ESTIMATE)
    return max(estimated, 50)


def generate_treasure_table_numbers(estimated_total: int, count: int = TREASURE_PODS_PER_MONTH) -> List[int]:
    """
    Generate treasure table numbers spread across the estimated total.
    
    First ~28% of tables have LESS weight (at most 1 treasure).
    Remaining treasures spread across rest of the month.
    """
    if estimated_total < 50:
        estimated_total = 50
    
    low_weight_cutoff = max(MIN_TABLE_OFFSET + 10, int(estimated_total / 3.5))
    
    table_numbers = []
    
    # 50% chance of 1 treasure in early zone, 50% chance of 0
    early_zone_count = 1 if random.random() < 0.5 else 0
    late_zone_count = count - early_zone_count
    
    if early_zone_count > 0:
        early_start = MIN_TABLE_OFFSET
        early_end = low_weight_cutoff
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
            
            table_numbers.append(random.randint(bucket_start, bucket_end))
    
    return sorted(table_numbers)


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
    
    async def create_schedule(
        self,
        guild_id: int,
        month: str,
        player_count: int,
    ) -> Dict[str, Any]:
        """Create a new treasure pod schedule for a month."""
        estimated_total = estimate_total_tables(player_count)
        table_numbers = generate_treasure_table_numbers(estimated_total)
        
        encrypted = encrypt_table_numbers(table_numbers)
        
        doc = {
            "guild_id": guild_id,
            "month": month,
            "encrypted_tables": encrypted,
            "estimated_total": estimated_total,
            "player_count_at_creation": player_count,
            "fired_tables": [],
            "created_at": datetime.now(timezone.utc),
        }
        
        await self.schedule_col.update_one(
            {"guild_id": guild_id, "month": month},
            {"$set": doc},
            upsert=True,
        )
        
        log_ok(
            f"[treasure] Created schedule for {month}: "
            f"estimated_total={estimated_total}, player_count={player_count}"
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
        
        table_numbers = decrypt_table_numbers(encrypted)
        if not table_numbers:
            log_warn(f"[treasure] Failed to decrypt schedule for {month}")
            return None
        
        fired = set(schedule.get("fired_tables", []))
        if table in fired:
            return None
        
        if table not in table_numbers:
            return None
        
        # It's a treasure pod!
        log_ok(f"[treasure] 🎁 TREASURE POD table #{table} triggered!")
        
        await self.schedule_col.update_one(
            {"guild_id": guild_id, "month": month},
            {"$push": {"fired_tables": table}},
        )
        
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
        
        result = await self.results_col.update_one(query, update)
        
        if result.modified_count > 0 and is_draw:
            await self._add_replacement_table(guild_id, month, current_max_table, new_player_count)
        
        return result.modified_count > 0
    
    async def _add_replacement_table(
        self,
        guild_id: int,
        month: str,
        current_max_table: int,
        new_player_count: Optional[int] = None,
    ) -> None:
        """Add a replacement treasure table number after a draw."""
        schedule = await self.get_schedule(guild_id, month)
        if not schedule:
            return
        
        encrypted = schedule.get("encrypted_tables")
        if not encrypted:
            return
        
        table_numbers = decrypt_table_numbers(encrypted)
        if not table_numbers:
            return
        
        if new_player_count and new_player_count > 0:
            estimated_total = estimate_total_tables(new_player_count)
        else:
            estimated_total = schedule.get("estimated_total", 250)
        
        # Add replacement 1-30 tables after current max (can appear immediately!)
        min_new = current_max_table + 1
        max_new = current_max_table + 30
        
        for _ in range(20):
            candidate = random.randint(min_new, max_new)
            if candidate not in table_numbers:
                break
        else:
            candidate = random.randint(min_new, max_new)
        
        new_list = sorted(table_numbers + [candidate])
        new_encrypted = encrypt_table_numbers(new_list)
        
        if new_encrypted:
            await self.schedule_col.update_one(
                {"guild_id": guild_id, "month": month},
                {"$set": {
                    "encrypted_tables": new_encrypted,
                    "estimated_total": estimated_total,
                }},
            )
            log_ok(f"[treasure] Added replacement table #{candidate} for {month} after draw")
    
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
        
        fired = schedule.get("fired_tables", [])
        encrypted = schedule.get("encrypted_tables")
        
        total_scheduled = 0
        if encrypted:
            table_numbers = decrypt_table_numbers(encrypted)
            if table_numbers:
                total_scheduled = len(table_numbers)
        
        return {
            "month": month,
            "scheduled": True,
            "treasures_fired": len(fired),
            "treasures_remaining": total_scheduled - len(fired),
            "estimated_total": schedule.get("estimated_total", 0),
        }
    
    async def get_won_pods(self, guild_id: int, month: str) -> List[Dict[str, Any]]:
        """Get all won treasure pods for a month."""
        cursor = self.results_col.find({
            "guild_id": guild_id,
            "month": month,
            "status": "won",
        })
        return await cursor.to_list(length=None)

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
        
        table_numbers = decrypt_table_numbers(encrypted)
        if not table_numbers:
            return False
        
        fired = set(schedule.get("fired_tables", []))
        unfired = sorted([n for n in table_numbers if n not in fired])
        
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
        
        # Need to recalculate!
        num_unfired = len(unfired)
        
        # Pick random table numbers within range 1 to max_distance from current
        # Can appear immediately on the next table!
        range_start = current_max_table + 1
        range_end = current_max_table + max_distance
        
        new_tables = []
        used = set(fired)  # Don't reuse fired tables
        
        for _ in range(num_unfired):
            # Try to find a unique random position
            for _ in range(50):  # max attempts
                candidate = random.randint(range_start, range_end)
                if candidate not in used and candidate not in new_tables:
                    new_tables.append(candidate)
                    break
            else:
                # Fallback: find any available slot
                for fallback in range(range_start, range_end + 1):
                    if fallback not in used and fallback not in new_tables:
                        new_tables.append(fallback)
                        break
        
        new_tables = sorted(new_tables)
        new_table_numbers = sorted(list(fired) + new_tables)
        
        new_encrypted = encrypt_table_numbers(new_table_numbers)
        if not new_encrypted:
            return False
        
        update_doc = {"$set": {"encrypted_tables": new_encrypted}}
        
        if new_player_count and new_player_count > 0:
            new_estimated = estimate_total_tables(new_player_count)
            update_doc["$set"]["estimated_total"] = new_estimated
        
        await self.schedule_col.update_one(
            {"guild_id": guild_id, "month": month},
            update_doc,
        )
        
        log_ok(f"[treasure] Schedule recalculated to sooner ({num_unfired} unfired remaining)")
        
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
