"""
MongoDB (Motor) setup for the ECL bot.

Subscriptions/free-entry data is important and should persist across restarts.

Env vars:
  - MONGO_URI (or MONGODB_URI)
  - MONGO_DB_NAME (optional)
  - IS_DEV=1 (optional)
"""

from __future__ import annotations

import os

import motor.motor_asyncio
from pymongo import ASCENDING, DESCENDING, IndexModel


MONGO_URI = (os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") or "").strip()
IS_DEV = os.getenv("IS_DEV", "0") == "1"

_default_name = "eclbot_dev" if IS_DEV else "eclbot"
DB_NAME = os.getenv("MONGO_DB_NAME", _default_name)

if not MONGO_URI:
    raise RuntimeError("Missing MONGO_URI/MONGODB_URI env var. Subscriptions require MongoDB.")

_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)

try:
    db = _client.get_default_database() or _client[DB_NAME]
except Exception:
    db = _client[DB_NAME]


# ---------------------------- Collections ---------------------------------

# One document per (guild_id, user_id, month)
subs_access = db.subs_access

# De-dupe Ko-fi events by transaction id
subs_kofi_events = db.subs_kofi_events

# One doc per (guild_id, user_id, month)
subs_free_entries = db.subs_free_entries

# Small job lock collection to avoid duplicate reminder/cleanup runs
subs_jobs = db.subs_jobs

# One doc per (bracket_id, year, month, season, tid)
online_games = db.online_games

# TopDeck exports / dumps
topdeck_pods = db["topdeck_pods"]
topdeck_month_dump_runs = db["topdeck_month_dump_runs"]
topdeck_month_dump_chunks = db["topdeck_month_dump_chunks"]

# Persistent state for timers and lobbies (survive restarts)
persistent_timers = db["persistent_timers"]
persistent_lobbies = db["persistent_lobbies"]


async def ping() -> bool:
    await _client.admin.command("ping")
    return True


async def ensure_indexes() -> None:
    await subs_access.create_indexes(
        [
            IndexModel(
                [("guild_id", ASCENDING), ("user_id", ASCENDING), ("month", ASCENDING)],
                unique=True,
                name="uniq_guild_user_month",
            ),
            IndexModel(
                [("guild_id", ASCENDING), ("user_id", ASCENDING), ("kind", ASCENDING), ("expires_at", ASCENDING)],
                name="by_guild_user_kind_expires",
            ),
        ]
    )

    await subs_kofi_events.create_indexes(
        [
            IndexModel([("txn_id", ASCENDING)], unique=True, name="uniq_kofi_txn"),
        ]
    )

    await subs_free_entries.create_indexes(
        [
            IndexModel(
                [("guild_id", ASCENDING), ("user_id", ASCENDING), ("month", ASCENDING)],
                unique=True,
                name="uniq_free_guild_user_month",
            )
        ]
    )

    await online_games.create_indexes(
        [
            IndexModel(
                [
                    ("bracket_id", ASCENDING),
                    ("year", ASCENDING),
                    ("month", ASCENDING),
                    ("season", ASCENDING),
                    ("tid", ASCENDING),
                ],
                unique=True,
                name="uniq_bracket_month_match",
            ),
            IndexModel(
                [("bracket_id", ASCENDING), ("year", ASCENDING), ("month", ASCENDING)],
                name="by_bracket_month",
            ),
            IndexModel([("entrant_ids", ASCENDING)], name="by_entrant_ids"),
            IndexModel([("topdeck_uids", ASCENDING)], name="by_topdeck_uids"),
            IndexModel(
                [("bracket_id", ASCENDING), ("year", ASCENDING), ("month", ASCENDING), ("online", ASCENDING)],
                name="by_bracket_month_online",
            ),
        ]
    )

    # ---- TopDeck exports ----

    await topdeck_pods.create_indexes(
        [
            IndexModel(
                [("bracket_id", ASCENDING), ("month", ASCENDING)],
                name="by_bracket_month",
            ),
            IndexModel(
                [("bracket_id", ASCENDING), ("season", ASCENDING), ("pod_id", ASCENDING)],
                name="by_bracket_season_pod",
            ),
            IndexModel(
                [("bracket_id", ASCENDING), ("pod_id", ASCENDING)],
                name="by_bracket_pod",
            ),
            IndexModel(
                [("month", ASCENDING)],
                name="by_month",
            ),
            # multikey (array of entrants objects)
            IndexModel(
                [("entrants.uid", ASCENDING)],
                name="by_entrants_uid",
            ),
        ]
    )

    await topdeck_month_dump_runs.create_indexes(
        [
            IndexModel(
                [("bracket_id", ASCENDING), ("month", ASCENDING), ("created_at", DESCENDING)],
                name="by_bracket_month_created_desc",
            ),
            # optional but recommended: prevent accidental duplicate run_id for same bracket/month
            IndexModel(
                [("bracket_id", ASCENDING), ("month", ASCENDING), ("run_id", ASCENDING)],
                unique=True,
                name="uniq_bracket_month_run_id",
            ),
        ]
    )

    await topdeck_month_dump_chunks.create_indexes(
        [
            IndexModel([("run_doc_id", ASCENDING)], name="by_run_doc_id"),
            IndexModel(
                [("bracket_id", ASCENDING), ("month", ASCENDING), ("run_id", ASCENDING)],
                name="by_bracket_month_run",
            ),
            # recommended: stable ordering + de-dupe safety for chunk assembly
            IndexModel(
                [("run_doc_id", ASCENDING), ("chunk_index", ASCENDING)],
                unique=True,
                name="uniq_run_doc_chunk_index",
            ),
        ]
    )

    # NOTE:
    # MongoDB already has a unique _id index on every collection.
    # Do NOT try to create a "unique" index on _id; Atlas will error.
    # We keep subs_jobs using _id as the job id without creating extra indexes.

    # ---- Persistent timers (survive restarts) ----
    await persistent_timers.create_indexes(
        [
            IndexModel(
                [("timer_id", ASCENDING)],
                unique=True,
                name="uniq_timer_id",
            ),
            IndexModel(
                [("guild_id", ASCENDING), ("status", ASCENDING)],
                name="by_guild_status",
            ),
            IndexModel(
                [("voice_channel_id", ASCENDING), ("status", ASCENDING)],
                name="by_vc_status",
            ),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="by_expires_at",
            ),
        ]
    )

    # ---- Persistent lobbies (survive restarts) ----
    await persistent_lobbies.create_indexes(
        [
            IndexModel(
                [("guild_id", ASCENDING), ("lobby_id", ASCENDING)],
                unique=True,
                name="uniq_guild_lobby",
            ),
            IndexModel(
                [("guild_id", ASCENDING)],
                name="by_guild",
            ),
            IndexModel(
                [("message_id", ASCENDING)],
                name="by_message",
            ),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="by_expires_at",
            ),
        ]
    )

    return
