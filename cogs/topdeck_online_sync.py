# cogs/topdeck_online_sync.py

import os
import re
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

import aiohttp
import discord
from discord.ext import commands

from db import online_games
from online_games_store import OnlineGameRecord, upsert_record

from topdeck_fetch import (
    Match,
    _fetch_json,
    _parse_tournament_fields,
    _extract_entrant_to_uid,
    _extract_matches_all_seasons,
)

# ---------- ENV / CONFIG ----------

GUILD_ID = int(os.getenv("GUILD_ID", "0"))

TOPDECK_BRACKET_ID = os.getenv("TOPDECK_BRACKET_ID", "")
FIREBASE_ID_TOKEN = os.getenv("FIREBASE_ID_TOKEN", None)

FIRESTORE_TOURNAMENT_URL_TEMPLATE = (
    os.getenv("FIRESTORE_DOC_URL_TEMPLATE", "")
    .strip()
    .strip('"')
    .strip("'")
)

SPELLBOT_LFG_CHANNEL_ID = int(os.getenv("SPELLBOT_LFG_CHANNEL_ID", "0"))
SPELLBOT_USER_ID = int(os.getenv("SPELLBOT_USER_ID", "0"))


ECL_MOD_ROLE_ID = int(os.getenv("ECL_MOD_ROLE_ID", "0"))
ECL_MOD_ROLE_NAME = os.getenv("ECL_MOD_ROLE_NAME", "ECL MOD")

# Max allowed time difference between SpellBot "ready" and TopDeck Start
ONLINE_MATCH_MAX_TIME_DIFF_SECONDS = int(
    os.getenv("ONLINE_MATCH_MAX_TIME_DIFF_SECONDS", str(5 * 60 * 60))  # default 5h
)


def _month_start_utc() -> datetime:
    now = datetime.now(timezone.utc)
    return datetime(now.year, now.month, 1, tzinfo=timezone.utc)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _norm_handle(s: str) -> str:
    """Normalize a Discord handle for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _extract_topdeck_handle(discord_raw: str) -> str:
    """
    Take the TopDeck 'discord' field and turn it into something
    that should match the real Discord username.

    Examples:
    - 'Zerox#1234'       -> 'zerox'
    - 'Zerox (Zerox)'    -> 'zerox'
    - 'Zerox some stuff' -> 'zerox'
    """
    if not discord_raw:
        return ""
    s = discord_raw.strip()
    s = re.split(r"[\s(]", s, 1)[0]
    if "#" in s:
        s = s.split("#", 1)[0]
    return _norm_handle(s)


@dataclass
class SpellbotReadyGame:
    message_id: int
    channel_id: int
    ready_ts: float
    player_ids: List[int]
    handles_norm: List[str]


@dataclass
class TopdeckMatchInfo:
    season: int
    table: int
    start_ts: float   # ALWAYS seconds (normalized)
    entrant_ids: List[int]
    uids: List[str]
    discords_norm: List[str]


# ---------- TopDeck helpers ----------


async def _fetch_topdeck_matches_for_month() -> List[TopdeckMatchInfo]:
    """
    Fetch TopDeck matches for the configured bracket and return
    only matches that started on/after the first day of this month.
    We keep ALL such matches; some may later be marked online.
    """
    if not TOPDECK_BRACKET_ID:
        raise RuntimeError("TOPDECK_BRACKET_ID is not configured.")

    if not FIRESTORE_TOURNAMENT_URL_TEMPLATE:
        raise RuntimeError(
            "FIRESTORE_TOURNAMENT_URL_TEMPLATE is not configured in environment variables."
        )

    month_start = _month_start_utc()
    print(
        f"[online-sync] {_now_iso()} Starting TopDeck fetch for bracket "
        f"{TOPDECK_BRACKET_ID!r} from {month_start.isoformat()}."
    )

    players_url = f"https://topdeck.gg/PublicPData/{TOPDECK_BRACKET_ID}"

    raw_doc_url = FIRESTORE_TOURNAMENT_URL_TEMPLATE.format(
        bracket_id=TOPDECK_BRACKET_ID
    )
    doc_url = raw_doc_url.strip().strip('"').strip("'")
    if raw_doc_url != doc_url:
        print(
            "[online-sync] Normalized Firestore URL template from "
            f"{raw_doc_url!r} to {doc_url!r}."
        )

    async with aiohttp.ClientSession() as session:
        players = await _fetch_json(session, players_url, token=None)
        doc = await _fetch_json(session, doc_url, token=FIREBASE_ID_TOKEN)

    fields = _parse_tournament_fields(doc)
    entrant_to_uid = _extract_entrant_to_uid(fields)
    matches: List[Match] = _extract_matches_all_seasons(fields)

    # Normalize players into uid -> dict
    player_map: Dict[str, Dict] = {}
    if isinstance(players, dict):
        for uid, pdata in players.items():
            if isinstance(pdata, dict):
                player_map[str(uid)] = pdata
    elif isinstance(players, list):
        for idx, pdata in enumerate(players):
            if isinstance(pdata, dict):
                player_map[str(idx)] = pdata

    month_start_ts = _month_start_utc().timestamp()
    infos: List[TopdeckMatchInfo] = []

    example_logged = False

    for m in matches:
        if m.start is None:
            continue

        raw_start = float(m.start)

        # Detect ms vs s and normalize to seconds
        if raw_start > 10_000_000_000:  # treat as ms
            start_ts = raw_start / 1000.0
            unit = "ms"
        else:
            start_ts = raw_start
            unit = "s"

        if start_ts < month_start_ts:
            continue

        if not example_logged:
            print(
                "[online-sync] Example TopDeck start time normalisation:",
                f"raw={raw_start} ({unit}), normalized={start_ts}",
            )
            example_logged = True

        entrant_ids = list(m.es)
        uids: List[str] = []
        discords_norm: List[str] = []

        for eid in entrant_ids:
            uid = entrant_to_uid.get(eid)
            uid_str = str(uid) if uid is not None else f"E{eid}"
            uids.append(uid_str)

            pdata = player_map.get(str(uid)) or {}
            disc_raw = str(pdata.get("discord") or "").strip()
            discords_norm.append(_extract_topdeck_handle(disc_raw))

        infos.append(
            TopdeckMatchInfo(
                season=m.season,
                table=m.id,
                start_ts=start_ts,
                entrant_ids=entrant_ids,
                uids=uids,
                discords_norm=discords_norm,
            )
        )

    print(
        f"[online-sync] {_now_iso()} TopDeck fetch complete. "
        f"Matches this month (after filtering by date): {len(infos)}."
    )
    if infos:
        print(
            "[online-sync] Example TopDeck normalized handles for first match:",
            infos[0].discords_norm,
        )

    return infos


# ---------- SpellBot helpers ----------


async def _scan_spellbot_ready_games(guild: discord.Guild) -> List[SpellbotReadyGame]:
    """
    Scan SPELLBOT_LFG_CHANNEL_ID for 'Your game is ready!' embeds since
    the first day of the current month.
    """
    if not SPELLBOT_LFG_CHANNEL_ID:
        raise RuntimeError("SPELLBOT_LFG_CHANNEL_ID is not configured.")

    channel = guild.get_channel(SPELLBOT_LFG_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        raise RuntimeError(
            f"Channel id {SPELLBOT_LFG_CHANNEL_ID} is not a text channel in this guild."
        )

    month_start = _month_start_utc()
    print(
        f"[online-sync] {_now_iso()} Scanning SpellBot channel "
        f"{SPELLBOT_LFG_CHANNEL_ID} for ready games since {month_start.isoformat()}."
    )

    games: List[SpellbotReadyGame] = []

    async for msg in channel.history(limit=None, after=month_start):
        if SPELLBOT_USER_ID:
            if msg.author.id != SPELLBOT_USER_ID:
                continue
        else:
            if not msg.author.bot:
                continue

        if not msg.embeds:
            continue

        embed = msg.embeds[0]
        title = (embed.title or "").lower()
        if "your game is ready" not in title:
            continue

        players_field = next(
            (f for f in embed.fields if "player" in f.name.lower()),
            None,
        )
        if not players_field:
            continue

        value = players_field.value or ""
        ids = [int(x) for x in re.findall(r"<@!?(\d+)>", value)]
        if not ids:
            continue

        handles_norm: List[str] = []
        for uid in ids:
            member = guild.get_member(uid)
            if member is None:
                try:
                    member = await guild.fetch_member(uid)
                except discord.NotFound:
                    member = None
            if member is None:
                handles_norm = []
                break
            handles_norm.append(_norm_handle(member.name))

        if not handles_norm:
            continue

        dt = msg.edited_at or msg.created_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        ready_ts = dt.timestamp()

        games.append(
            SpellbotReadyGame(
                message_id=msg.id,
                channel_id=msg.channel.id,
                ready_ts=ready_ts,
                player_ids=ids,
                handles_norm=handles_norm,
            )
        )

    print(
        f"[online-sync] {_now_iso()} SpellBot scan complete. "
        f"Ready games found: {len(games)}."
    )
    if games:
        print(
            "[online-sync] Example SpellBot normalized handles for first ready game:",
            games[0].handles_norm,
        )

    return games


# ---------- Matching ----------


def _match_spellbot_to_topdeck(
    spellbot_games: List[SpellbotReadyGame],
    matches: List[TopdeckMatchInfo],
    *,
    max_time_diff_seconds: float = ONLINE_MATCH_MAX_TIME_DIFF_SECONDS,
) -> Tuple[Dict[Tuple[int, int], bool], Dict[str, int]]:
    """
    Mark TopDeck matches as online/offline, matching by:
    - exact set of normalized handles (ignoring duplicates and blanks)
    - and closest timestamp within max_time_diff_seconds.

    Each SpellBot ready game can match at most one TopDeck match.
    Additionally, for each handle-set cluster, any extra TopDeck
    matches close in time to a known-online match are also treated
    as online (to catch multiple games on the same SpellTable link).
    """
    print(
        f"[online-sync] {_now_iso()} Matching SpellBot ↔ TopDeck "
        f"with max time diff {max_time_diff_seconds} seconds."
    )

    # default everything to offline
    match_online: Dict[Tuple[int, int], bool] = {
        (m.season, m.table): False for m in matches
    }
    per_player_online: Dict[str, int] = {}

    # group by handle set
    sb_by_key: Dict[frozenset[str], List[SpellbotReadyGame]] = {}
    for sb in spellbot_games:
        key = frozenset(sb.handles_norm)
        if len(key) < 2:
            continue
        sb_by_key.setdefault(key, []).append(sb)

    td_by_key: Dict[frozenset[str], List[TopdeckMatchInfo]] = {}
    for mi in matches:
        handles = frozenset(h for h in mi.discords_norm if h)
        if len(handles) < 2:
            continue
        td_by_key.setdefault(handles, []).append(mi)

    print(
        f"[online-sync] {_now_iso()} SpellBot handle clusters: {len(sb_by_key)}; "
        f"TopDeck handle clusters (with non-empty handles): {len(td_by_key)}."
    )

    clusters_with_overlap = 0
    total_online = 0
    debug_unmatched_printed = 0
    debug_matched_printed = 0

    # --- First pass: direct SpellBot ↔ TopDeck matches ---

    for key, sb_list in sb_by_key.items():
        td_list = td_by_key.get(key)
        if not td_list:
            continue
        clusters_with_overlap += 1

        sb_list = sorted(sb_list, key=lambda g: g.ready_ts)
        td_list = sorted(td_list, key=lambda m: m.start_ts)

        used_td_indices: set[int] = set()

        for sb in sb_list:
            best_idx: Optional[int] = None
            best_dt: float = max_time_diff_seconds + 1.0

            for idx, mi in enumerate(td_list):
                if idx in used_td_indices:
                    continue
                dt = abs(mi.start_ts - sb.ready_ts)
                if dt <= max_time_diff_seconds and dt < best_dt:
                    best_dt = dt
                    best_idx = idx

            if best_idx is None:
                # debug a few examples where handles match but time window fails
                if debug_unmatched_printed < 5 and td_list:
                    closest_dt = min(
                        abs(mi.start_ts - sb.ready_ts) for mi in td_list
                    )
                    print(
                        "[online-sync] DEBUG no time match for handle-set "
                        f"{list(key)}; closest dt ~= {closest_dt:.0f}s; "
                        f"sb_ready_ts={sb.ready_ts}, "
                        f"sample_td_start_ts={td_list[0].start_ts}",
                    )
                    debug_unmatched_printed += 1
                continue

            used_td_indices.add(best_idx)
            mi = td_list[best_idx]
            key_match = (mi.season, mi.table)
            if not match_online[key_match]:
                match_online[key_match] = True
                total_online += 1
                for uid in mi.uids:
                    per_player_online[uid] = per_player_online.get(uid, 0) + 1

            if debug_matched_printed < 5:
                dt_dbg = abs(mi.start_ts - sb.ready_ts)
                print(
                    "[online-sync] DEBUG matched online game for handles "
                    f"{list(key)}; dt={dt_dbg:.0f}s; "
                    f"season={mi.season}, table={mi.table}",
                )
                debug_matched_printed += 1

    # --- Second pass: extra TopDeck matches in same handle-set near online ones ---

    extra_online = 0
    extra_debug_printed = 0
    reuse_window = max_time_diff_seconds  # reuse same window for simplicity

    for key, td_list in td_by_key.items():
        td_sorted = sorted(td_list, key=lambda m: m.start_ts)
        online_indices = [
            idx
            for idx, mi in enumerate(td_sorted)
            if match_online.get((mi.season, mi.table), False)
        ]
        if not online_indices:
            continue

        for idx, mi in enumerate(td_sorted):
            match_key = (mi.season, mi.table)
            if match_online.get(match_key, False):
                continue

            dt_to_nearest = min(
                abs(mi.start_ts - td_sorted[j].start_ts) for j in online_indices
            )
            if dt_to_nearest <= reuse_window:
                match_online[match_key] = True
                extra_online += 1
                for uid in mi.uids:
                    per_player_online[uid] = per_player_online.get(uid, 0) + 1

                if extra_debug_printed < 5:
                    print(
                        "[online-sync] DEBUG marked extra online game in same handle-set "
                        f"{list(key)}; dt_to_nearest={dt_to_nearest:.0f}s; "
                        f"season={mi.season}, table={mi.table}",
                    )
                    extra_debug_printed += 1

    total_online += extra_online

    print(
        f"[online-sync] {_now_iso()} Matching complete (handles + time). "
        f"Clusters with overlap: {clusters_with_overlap}, "
        f"Online TopDeck games: {total_online}, "
        f"Players with ≥1 online game: {len(per_player_online)}."
    )

    return match_online, per_player_online


# ---------- Persist helper (SpellBot view + timer view) ----------


async def _save_online_stats_to_db(new_payload: Dict[str, Any]) -> None:
    """Persist /synconline results to Mongo.

    We store one document per match, and we **never downgrade** online=True → False.
    """
    bracket_id = str(new_payload.get("bracket_id") or "")
    month_str = str(new_payload.get("month") or "")
    if not bracket_id or not month_str:
        return

    try:
        year = int(month_str.split("-")[0])
        month = int(month_str.split("-")[1])
    except Exception:
        return

    existing_online: set[tuple[int, int]] = set()
    async for doc in online_games.find(
        {"bracket_id": bracket_id, "year": year, "month": month, "online": True},
        projection={"_id": 0, "season": 1, "tid": 1},
    ):
        try:
            existing_online.add((int(doc["season"]), int(doc["tid"])))
        except Exception:
            continue

    for m in (new_payload.get("matches") or []):
        try:
            season = int(m.get("season") or 0)
            tid = int(m.get("table") or 0)
            if not season or not tid:
                continue

            entrant_ids: List[int] = []
            for x in (m.get("player_entrants") or []):
                try:
                    entrant_ids.append(int(x))
                except Exception:
                    continue

            topdeck_uids: List[str] = []
            for u in (m.get("player_uids") or []):
                if u is None:
                    continue
                s = str(u).strip()
                if s:
                    topdeck_uids.append(s)

            seen = set()
            topdeck_uids = [x for x in topdeck_uids if not (x in seen or seen.add(x))]

            online_flag = bool(m.get("online"))
            if (season, tid) in existing_online:
                online_flag = True

            rec = OnlineGameRecord(
                season=season,
                tid=tid,
                start_ts=float(m.get("start_ts") or 0.0) or None,
                entrant_ids=entrant_ids,
                topdeck_uids=topdeck_uids,
                online=online_flag,
            )
            await upsert_record(bracket_id, year, month, rec)
        except Exception:
            continue



class TopdeckOnlineSyncCog(commands.Cog):
    """
    Mod-only command to rebuild 'online game' stats for the current month.

    - Scans SpellBot 'Your game is ready!' embeds in SPELLBOT_LFG_CHANNEL_ID
    - Fetches all TopDeck matches for TOPDECK_BRACKET_ID this month
    - Marks each TopDeck match as online/offline using handles + timestamps
    - Writes results to MongoDB (collection: online_games).

    No automatic hook is registered; this is manual-only.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._lock = asyncio.Lock()

    @staticmethod
    def _is_mod(member: discord.Member) -> bool:
        for role in getattr(member, "roles", []):
            if ECL_MOD_ROLE_ID and role.id == ECL_MOD_ROLE_ID:
                return True
            if ECL_MOD_ROLE_NAME and role.name == ECL_MOD_ROLE_NAME:
                return True
        return False

    @commands.slash_command(
        name="synconline",
        description=(
            "MOD: Rebuild online-game stats from SpellBot 'Your game is ready!' "
            "embeds for this month."
        ),
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def synconline(self, ctx: discord.ApplicationContext):
        if ctx.guild is None:
            await ctx.respond(
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        member = ctx.author
        if not isinstance(member, discord.Member) or not self._is_mod(member):
            await ctx.respond(
                "You must be an ECL MOD to use this command.",
                ephemeral=True,
            )
            return

        if self._lock.locked():
            await ctx.respond(
                "An online-games sync is already running. Please wait for it to finish.",
                ephemeral=True,
            )
            return

        month_str = _month_start_utc().strftime("%Y-%m")
        print(f"[online-sync] {_now_iso()} /synconline started for month {month_str}.")

        await ctx.defer(ephemeral=True)

        async with self._lock:
            try:
                guild = ctx.guild

                # 1) Scan SpellBot ready games in this guild
                spellbot_games = await _scan_spellbot_ready_games(guild)

                # 2) Fetch TopDeck matches for this month
                topdeck_matches = await _fetch_topdeck_matches_for_month()

                # 3) Mark which TopDeck matches are online (handles + time)
                match_online, per_player_online = _match_spellbot_to_topdeck(
                    spellbot_games,
                    topdeck_matches,
                )

                # 4) Build entries for ALL TopDeck matches
                entries: List[Dict[str, Any]] = []
                online_count = 0
                for mi in topdeck_matches:
                    key = (mi.season, mi.table)
                    online = bool(match_online.get(key, False))
                    if online:
                        online_count += 1

                    entries.append(
                        {
                            "season": mi.season,
                            "table": mi.table,
                            "topdeck_match_key": f"S{mi.season}:T{mi.table}",
                            "start_ts": mi.start_ts,
                            "player_entrants": mi.entrant_ids,
                            "player_uids": mi.uids,
                            "online": online,
                        }
                    )

                # 5) Save JSON (merged with existing/timer-written data)
                payload = {
                    "bracket_id": TOPDECK_BRACKET_ID,
                    "guild_id": guild.id,
                    "month": month_str,
                    "built_at": int(datetime.now(timezone.utc).timestamp()),
                    "spellbot_lfg_channel_id": SPELLBOT_LFG_CHANNEL_ID,
                    "matches": entries,
                    "per_player_online": per_player_online,
                }
                await _save_online_stats_to_db(payload)

            except Exception as e:
                print(f"[online-sync] Error during sync: {type(e).__name__}: {e}")
                await ctx.followup.send(
                    "Something went wrong while rebuilding online-game stats. "
                    "Check the bot logs for details.",
                    ephemeral=True,
                )
                return

        print(
            f"[online-sync] {_now_iso()} /synconline finished. "
            f"SpellBot ready games: {len(spellbot_games)}, "
            f"TopDeck matches: {len(topdeck_matches)}, "
            f"Online TopDeck games: {online_count}, "
            f"Players with ≥1 online game: {len(per_player_online)}."
        )

        await ctx.followup.send(
            (
                f"Online-game stats rebuilt for **{month_str}**.\n"
                f"SpellBot ready games found: **{len(spellbot_games)}**\n"
                f"TopDeck matches this month: **{len(topdeck_matches)}**\n"
                f"Matched *online* TopDeck games (by players + time): **{online_count}**\n"
                f"Players with ≥1 online game: **{len(per_player_online)}**\n\n"
                "Data saved to MongoDB (collection: online_games)."
            ),
            ephemeral=True,
        )


def setup(bot: commands.Bot):
    bot.add_cog(TopdeckOnlineSyncCog(bot))
