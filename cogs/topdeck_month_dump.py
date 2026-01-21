# cogs/topdeck_month_dump.py
import os
import re
import json
import uuid
import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import discord
from discord.ext import commands

from pymongo import UpdateOne
from db import topdeck_month_dump_runs, topdeck_month_dump_chunks, topdeck_pods

from utils.settings import LISBON_TZ
from utils.logger import log_sync, log_warn
from utils.mod_check import is_mod

from topdeck_fetch import (
    Match,
    _fetch_json,
    _get_firestore_doc_url,
    _parse_tournament_fields,
    _extract_matches_all_seasons,
    _is_in_progress_match,
    _is_valid_completed_match,
)

# ---------- ENV / CONFIG ----------

GUILD_ID = int(os.getenv("GUILD_ID", "0"))

TOPDECK_BRACKET_ID = os.getenv("TOPDECK_BRACKET_ID", "").strip()
FIREBASE_ID_TOKEN = os.getenv("FIREBASE_ID_TOKEN", None)

# Keep chunks safely under Mongo's 16MB document limit (1MB-ish is comfy)
MONGO_CHUNK_BYTES = int(os.getenv("TOPDECK_DUMP_CHUNK_BYTES", "900000"))  # ~0.9MB


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat(timespec="seconds")


def _normalize_ts(ts: Optional[float]) -> Optional[float]:
    if ts is None:
        return None
    try:
        x = float(ts)
    except Exception:
        return None
    # ms vs s
    return x / 1000.0 if x > 10_000_000_000 else x


def _current_month_str() -> str:
    return datetime.now(LISBON_TZ).strftime("%Y-%m")


def _month_bounds_utc_ts(month_str: str) -> Tuple[float, float]:
    """
    Return (start_ts_utc, end_ts_utc) for the given month_str "YYYY-MM",
    where the month boundaries are defined in LISBON_TZ (same as your month-flip logic),
    converted to UTC timestamps.
    """
    y_s, m_s = month_str.split("-", 1)
    y = int(y_s)
    m = int(m_s)

    start_local = datetime(y, m, 1, 0, 0, 0, tzinfo=LISBON_TZ)
    start_utc_ts = start_local.astimezone(timezone.utc).timestamp()

    if m == 12:
        y2, m2 = y + 1, 1
    else:
        y2, m2 = y, m + 1

    end_local = datetime(y2, m2, 1, 0, 0, 0, tzinfo=LISBON_TZ)
    end_utc_ts = end_local.astimezone(timezone.utc).timestamp()

    return float(start_utc_ts), float(end_utc_ts)


def _chunk_bytes(b: bytes, chunk_size: int) -> List[bytes]:
    return [b[i : i + chunk_size] for i in range(0, len(b), chunk_size)]


async def _store_dump_in_mongo(*, bracket_id: str, month_str: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Store the full dump JSON in MongoDB using:
      - topdeck_month_dump_runs: 1 doc per run (metadata)
      - topdeck_month_dump_chunks: N docs per run (chunked JSON string)
    """
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    sha = hashlib.sha256(raw).hexdigest()
    run_id = str(payload.get("run_id") or uuid.uuid4().hex)
    created_at = _now_utc()

    chunks = _chunk_bytes(raw, MONGO_CHUNK_BYTES)

    run_doc = {
        "bracket_id": bracket_id,
        "month": month_str,
        "run_id": run_id,
        "created_at": created_at,
        "sha256": sha,
        "bytes": len(raw),
        "chunks": len(chunks),
        "schema_version": int(payload.get("schema_version") or 1),
        "counts": payload.get("counts") or {},
    }
    ins = await topdeck_month_dump_runs.insert_one(run_doc)
    run_doc_id = ins.inserted_id

    chunk_docs = []
    for idx, c in enumerate(chunks):
        chunk_docs.append(
            {
                "run_doc_id": run_doc_id,
                "bracket_id": bracket_id,
                "month": month_str,
                "run_id": run_id,
                "chunk_index": idx,
                "created_at": created_at,
                "data": c.decode("utf-8"),
            }
        )

    if chunk_docs:
        await topdeck_month_dump_chunks.insert_many(chunk_docs)

    return {"run_id": run_id, "sha256": sha, "chunks": len(chunks), "bytes": len(raw)}

async def _upsert_topdeck_pods(
    *,
    bracket_id: str,
    guild_id: int,
    month_str: str,
    run_id: str,
    matches: List[Match],
) -> Dict[str, int]:
    """
    Upsert completed matches into topdeck_pods (1 doc per pod).
    Keyed by _id = "{bracket_id}:{season}:{podId}".
    """
    try:
        year = int(month_str.split("-")[0])
        month = int(month_str.split("-")[1])
    except Exception:
        year, month = 0, 0

    now = _now_utc()
    ops: List[UpdateOne] = []

    for m in matches:
        pod_id = int(m.id)
        season = int(m.season)

        _id = f"{bracket_id}:{season}:{pod_id}"

        doc_set = {
            "bracket_id": bracket_id,
            "guild_id": int(guild_id),
            "year": int(year),
            "month": int(month),
            "month_str": month_str,

            "season": season,
            "podId": pod_id,            # main key field (what you called "table")
            "table": pod_id,            # keep alias for convenience

            "start_ts": _normalize_ts(m.start),
            "end_ts": _normalize_ts(m.end),
            "entrants": list(m.es or []),
            "winner": m.winner,

            # raw Firestore map for the match
            "raw": m.raw,

            # provenance
            "last_dump_run_id": run_id,
            "last_dump_at": now,
            "updated_at": now,
        }

        ops.append(
            UpdateOne(
                {"_id": _id},
                {
                    "$setOnInsert": {
                        "_id": _id,
                        "created_at": now,
                    },
                    "$set": doc_set,
                },
                upsert=True,
            )
        )

    if not ops:
        return {"pods_upserted": 0, "pods_matched": 0, "pods_modified": 0}

    res = await topdeck_pods.bulk_write(ops, ordered=False)

    upserted = getattr(res, "upserted_count", None)
    if upserted is None:
        upserted = len(getattr(res, "upserted_ids", {}) or {})

    return {
        "pods_upserted": int(upserted or 0),
        "pods_matched": int(getattr(res, "matched_count", 0) or 0),
        "pods_modified": int(getattr(res, "modified_count", 0) or 0),
    }


async def dump_topdeck_month_to_mongo(
    *,
    guild_id: int,
    month_str: str,
    bracket_id: str,
    firebase_id_token: Optional[str],
) -> Dict[str, Any]:
    """
    Reusable helper for:
      - manual command (month-to-date if current month)
      - month-flip automation (full previous month)

    Behavior:
      - Fetch fresh TopDeck API data (players + firestore doc)
      - Filter matches to month window [start, min(end, now))
      - Exclude in-progress + invalid-completed
      - Store in Mongo (runs + chunks)
    """
    if not bracket_id:
        raise RuntimeError("bracket_id is required")

    start_cutoff, end_cutoff = _month_bounds_utc_ts(month_str)
    now_ts = _now_utc().timestamp()
    effective_end = min(end_cutoff, now_ts)  # month-to-date for current month; full month for past months

    players_url = f"https://topdeck.gg/PublicPData/{bracket_id}"
    doc_url = _get_firestore_doc_url(bracket_id)

    async with aiohttp.ClientSession() as session:
        players = await _fetch_json(session, players_url, token=None)
        doc = await _fetch_json(session, doc_url, token=firebase_id_token)

    fields = _parse_tournament_fields(doc)
    matches: List[Match] = _extract_matches_all_seasons(fields)

    month_matches: List[Match] = []
    excluded_in_progress = 0
    excluded_invalid_completed = 0

    for m in matches:
        start_ts = _normalize_ts(m.start)
        if start_ts is None:
            continue
        if start_ts < start_cutoff or start_ts >= effective_end:
            continue

        if _is_in_progress_match(m):
            excluded_in_progress += 1
            continue
        if not _is_valid_completed_match(m):
            excluded_invalid_completed += 1
            continue

        month_matches.append(m)

    run_id = uuid.uuid4().hex

    payload: Dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "bracket_id": bracket_id,
        "guild_id": int(guild_id),
        "month": month_str,
        "built_at": int(_now_utc().timestamp()),
        "window": {
            "start_utc_ts": start_cutoff,
            "end_utc_ts": end_cutoff,
            "effective_end_utc_ts": effective_end,
        },
        "counts": {
            "completed_saved": len(month_matches),
            "excluded_in_progress": excluded_in_progress,
            "excluded_invalid_completed": excluded_invalid_completed,
        },
        "players": players,
        "matches": [
            {
                "season": m.season,
                "table": m.id,
                "start": _normalize_ts(m.start),
                "end": _normalize_ts(m.end),
                "es": list(m.es or []),
                "winner": m.winner,
                "raw": m.raw,
            }
            for m in month_matches
        ],
    }

    mongo_meta = await _store_dump_in_mongo(bracket_id=bracket_id, month_str=month_str, payload=payload)
    pods_meta = await _upsert_topdeck_pods(
        bracket_id=bracket_id,
        guild_id=guild_id,
        month_str=month_str,
        run_id=run_id,
        matches=month_matches,
    )

    mongo_meta["counts"] = payload["counts"]
    mongo_meta["pods"] = pods_meta
    return mongo_meta


class TopdeckMonthDumpCog(commands.Cog):
    """
    /topdeckdumpmonth (mod-only):
      - dumps month-to-date (or specific month) using fresh TopDeck API data
      - stores only in Mongo (chunked)
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._lock = asyncio.Lock()

    @staticmethod
    def _is_mod(member: discord.Member) -> bool:
        """Check if member is a mod. Delegates to utils.mod_check.is_mod."""
        return is_mod(member)

    @commands.slash_command(
        name="topdeckdumpmonth",
        description="MOD: Save a fresh TopDeck month dump (completed matches only) to Mongo (chunked).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def topdeckdumpmonth(self, ctx: discord.ApplicationContext, month: Optional[str] = None):
        if ctx.guild is None:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        member = ctx.author
        if not isinstance(member, discord.Member) or not self._is_mod(member):
            await ctx.respond("You must be an ECL MOD to use this command.", ephemeral=True)
            return

        if self._lock.locked():
            await ctx.respond("A TopDeck dump is already running. Please wait.", ephemeral=True)
            return

        month_str = (month or "").strip() or _current_month_str()
        if not re.match(r"^\d{4}-\d{2}$", month_str):
            await ctx.respond('Month must be in "YYYY-MM" format.', ephemeral=True)
            return

        if not TOPDECK_BRACKET_ID:
            await ctx.respond("TOPDECK_BRACKET_ID is not configured.", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)

        async with self._lock:
            try:
                log_sync(f"[topdeck-dump] {_now_iso()} Starting dump for {month_str} (bracket={TOPDECK_BRACKET_ID}).")

                mongo_meta = await dump_topdeck_month_to_mongo(
                    guild_id=ctx.guild.id,
                    month_str=month_str,
                    bracket_id=TOPDECK_BRACKET_ID,
                    firebase_id_token=FIREBASE_ID_TOKEN,
                )

                c = mongo_meta.get("counts") or {}
                log_sync(
                    f"[topdeck-dump] {_now_iso()} Finished dump for {month_str}. "
                    f"saved={c.get('completed_saved')} excluded_in_progress={c.get('excluded_in_progress')} "
                    f"excluded_invalid_completed={c.get('excluded_invalid_completed')}"
                )

            except Exception as e:
                log_warn(f"[topdeck-dump] Error: {type(e).__name__}: {e}")
                await ctx.followup.send("Dump failed. Check bot logs for details.", ephemeral=True)
                return

        c = mongo_meta.get("counts") or {}
        pods = (mongo_meta.get("pods") or {})
        await ctx.followup.send(
            (
                f"TopDeck dump saved for **{month_str}**.\n"
                f"- Completed saved: **{c.get('completed_saved', 0)}**\n"
                f"- Excluded in-progress: **{c.get('excluded_in_progress', 0)}**\n"
                f"- Pods upserted: **{pods.get('pods_upserted', 0)}** (modified: {pods.get('pods_modified', 0)})\n"
                f"- Excluded invalid-completed: **{c.get('excluded_invalid_completed', 0)}**\n\n"
                f"Mongo run_id: `{mongo_meta['run_id']}` "
                f"(chunks: {mongo_meta['chunks']}, sha256: {mongo_meta['sha256'][:12]}…)"
            ),
            ephemeral=True,
        )


def setup(bot: commands.Bot):
    bot.add_cog(TopdeckMonthDumpCog(bot))
