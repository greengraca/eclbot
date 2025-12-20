# cogs/topdeck_league.py
import os
import json
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple, Dict

import discord
from discord.ext import commands

from topdeck_fetch import get_league_rows, PlayerRow

GUILD_ID = int(os.getenv("GUILD_ID", "0"))
TOPDECK_BRACKET_ID = os.getenv("TOPDECK_BRACKET_ID", "")
FIREBASE_ID_TOKEN = os.getenv("FIREBASE_ID_TOKEN", None)

TOPDECK_ONLINE_JSON = os.getenv("TOPDECK_ONLINE_JSON", "topdeck_online_games.json")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


TOPDECK_CACHE_MINUTES = _env_int("TOPDECK_CACHE_MINUTES", 30)  # default 30 min


def _ts(dt: datetime) -> int:
    return int(dt.timestamp())


def _load_online_counts() -> Dict[str, int]:
    """
    Load per-player online game counts from the JSON built by /synconline.

    Returns a dict: uid (str) -> online_games (int).
    If file is missing or invalid, returns empty dict and logs to console.
    """
    try:
        with open(TOPDECK_ONLINE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(
            "[topdeck/top16] No online-games JSON found "
            f"('{TOPDECK_ONLINE_JSON}'); treating everyone as 0 online games."
        )
        return {}
    except Exception as e:
        print(
            f"[topdeck/top16] Error reading online-games JSON "
            f"('{TOPDECK_ONLINE_JSON}'): {type(e).__name__}: {e}"
        )
        return {}

    per_player_online = data.get("per_player_online")
    if not isinstance(per_player_online, dict):
        print(
            "[topdeck/top16] online-games JSON has no valid "
            "'per_player_online' dict; treating everyone as 0 online games."
        )
        return {}

    month = data.get("month")
    bracket = data.get("bracket_id")
    print(
        "[topdeck/top16] Loaded online-games JSON: "
        f"month={month!r}, bracket={bracket!r}, "
        f"players_with_online_games={len(per_player_online)}."
    )

    cleaned: Dict[str, int] = {}
    for k, v in per_player_online.items():
        try:
            cleaned[str(k)] = int(v)
        except (TypeError, ValueError):
            continue
    return cleaned


class TopdeckLeagueCog(commands.Cog):
    """Slash commands that expose TopDeck league stats inside Discord."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # cache
        self._cached_rows: Optional[List[PlayerRow]] = None
        self._cached_at: Optional[datetime] = None
        self._cache_ttl = timedelta(minutes=TOPDECK_CACHE_MINUTES)

    # ------------- helpers -------------

    async def _load_rows(
        self,
        force_refresh: bool = False,
    ) -> Tuple[List[PlayerRow], datetime]:
        """
        Returns (rows, fetched_at).

        Uses an in-memory cache with TTL (TOPDECK_CACHE_MINUTES).
        Only hits the Topdeck / Firestore endpoints when:
        - there's no cache yet, or
        - cache is older than TTL, or
        - force_refresh=True.
        """
        now = datetime.now(timezone.utc)

        if (
            not force_refresh
            and self._cached_rows is not None
            and self._cached_at is not None
            and now - self._cached_at < self._cache_ttl
        ):
            # Serve from cache
            return self._cached_rows, self._cached_at

        if not TOPDECK_BRACKET_ID:
            raise RuntimeError(
                "TOPDECK_BRACKET_ID is not configured in environment variables."
            )

        # 🔵 Only reaches here when it will actually call the remote endpoints
        print(
            f"[topdeck] Fetching fresh TopDeck data from API for bracket "
            f"{TOPDECK_BRACKET_ID!r} (cache miss or expired)."
        )

        rows = await get_league_rows(TOPDECK_BRACKET_ID, FIREBASE_ID_TOKEN)
        self._cached_rows = rows
        self._cached_at = now
        return rows, now

    @staticmethod
    def _find_author_row(
        member: discord.Member,
        rows: List[PlayerRow],
    ) -> Optional[PlayerRow]:
        """
        Match the Discord member to a TopDeck PlayerRow, primarily by username.

        - First, match member's username/global_name/display_name to row.discord
          (which is assumed to be the stored Discord username).
        - Then, fall back to matching those same names to row.name.
        """

        def norm(v: Optional[str]) -> Optional[str]:
            if not isinstance(v, str):
                return None
            v = v.strip()
            return v.lower() if v else None

        # Candidate names from Discord
        candidates_raw = {
            member.name,
            getattr(member, "global_name", None),
            getattr(member, "display_name", None),
        }

        # If the account still has a discriminator, include username#discrim too
        discrim = getattr(member, "discriminator", None)
        if discrim and discrim != "0":
            candidates_raw.add(f"{member.name}#{discrim}")

        candidates = {c for c in (norm(x) for x in candidates_raw) if c}

        if not candidates:
            return None

        # --- 1) Exact match against row.discord (preferred) ---
        for row in rows:
            row_disc = norm(row.discord)
            if row_disc and row_disc in candidates:
                return row

        # --- 2) Fallback: exact match against row.name ---
        for row in rows:
            row_name = norm(row.name)
            if row_name and row_name in candidates:
                return row

        return None

    # ------------- /mostgames -------------

    @commands.slash_command(
        name="mostgames",
        description="Show Top 5 players with the most games in the current bracket.",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def mostgames(self, ctx: discord.ApplicationContext):
        # Server only
        if ctx.guild is None:
            await ctx.respond(
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        # Defer as NON-ephemeral so we can send a public message first
        await ctx.defer(ephemeral=False)

        try:
            rows, fetched_at = await self._load_rows()
        except Exception as e:
            print(f"[topdeck] /mostgames fetch error: {type(e).__name__}: {e}")
            await ctx.followup.send(
                "I couldn't fetch TopDeck data right now. "
                "Please try again in a bit.",
                ephemeral=True,
            )
            return

        if not rows:
            await ctx.followup.send(
                "I couldn't find any players in this bracket.",
                ephemeral=True,
            )
            return

        # Top 5 by number of games, then by points
        top5 = sorted(rows, key=lambda r: (-r.games, -r.pts))[:5]

        # Public Top 5 (minimal info)
        ts_int = _ts(fetched_at)
        header = (
            "**Top 5 – Most Games Played**\n"
            # f"*(TopDeck data: last updated <t:{ts_int}:R>)*\n"
        )
        public_lines = [header]
        for i, r in enumerate(top5, start=1):
            dropped_suffix = " *(dropped)*" if r.dropped else ""
            public_lines.append(
                f"`#{i}` **{r.name}** – {r.games} games{dropped_suffix}"
            )

        # 1) Public message first
        await ctx.followup.send("\n".join(public_lines))

        # 2) Ephemeral personal message after
        member = ctx.author
        row_for_author = (
            self._find_author_row(member, rows)
            if isinstance(member, discord.Member)
            else None
        )

        if row_for_author:
            # Distance to Top 5 by games
            if top5:
                threshold_games = top5[-1].games  # #5's games
                # Games needed to strictly pass #5 by games
                games_diff = threshold_games - row_for_author.games + 1
            else:
                threshold_games = 0
                games_diff = 0

            if games_diff <= 0:
                gap_line = (
                    "You're already in, or tied with, the current Top 5 by games. Congrats!"
                )
            else:
                gap_line = (
                    f"You're **{games_diff}** game(s) away from entering the Top 5 by games."
                )

            personal_msg = (
                f"You have **{row_for_author.games}** games in this bracket "
                f"({row_for_author.wins}W / {row_for_author.draws}D / {row_for_author.losses}L).\n"
                f"{gap_line}\n\n"
                f"TopDeck data: last updated <t:{ts_int}:R>."
            )
        else:
            personal_msg = (
                "I couldn't find you in this bracket's data. "
                "Please check that your Discord on TopDeck is up to date.\n\n"
                f"TopDeck data: last updated <t:{ts_int}:R>."
            )

        await ctx.followup.send(personal_msg, ephemeral=True)

    # ------------- /top16 -------------

    @commands.slash_command(
        name="top16",
        description="Show Top 16 qualified players (>= 10 online games).",
        guild_ids=[GUILD_ID] if GUILD_ID else None,
    )
    async def top16(self, ctx: discord.ApplicationContext):
        if ctx.guild is None:
            await ctx.respond(
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        # Defer as NON-ephemeral (same approach as /mostgames),
        # then send public, then ephemeral.
        await ctx.defer(ephemeral=False)

        # Load league rows
        try:
            rows, fetched_at = await self._load_rows()
        except Exception as e:
            print(f"[topdeck] /top16 fetch error: {type(e).__name__}: {e}")
            await ctx.followup.send(
                "I couldn't fetch TopDeck data right now. "
                "Please try again in a bit.",
                ephemeral=True,
            )
            return

        if not rows:
            await ctx.followup.send(
                "I couldn't find any players in this bracket.",
                ephemeral=True,
            )
            return

        # Load per-player online game counts from JSON
        online_counts = _load_online_counts()

        # ---- Step 1: all active players with >=10 TOTAL games ----
        # rows is already sorted (active first, then by pts, OW%, win%).
        active_by_games = [r for r in rows if not r.dropped and r.games >= 10]
        print(
            f"[topdeck/top16] Active players with >=10 total games: "
            f"{len(active_by_games)}."
        )

        # Raw top16 by games/points for debugging
        top16_by_games = active_by_games[:16]
        print("[topdeck/top16] Raw Top 16 by total games/points (before online filter):")
        for i, r in enumerate(top16_by_games, start=1):
            uid = r.uid or ""
            online_games = online_counts.get(uid, 0)
            print(
                f"  seed #{i:02} | name={r.name!r}, uid={uid!r}, "
                f"pts={r.pts:.1f}, total_games={r.games}, online_games={online_games}"
            )

        # ---- Step 2: REAL qualified list = all active players with >=10 ONLINE games ----
        qualified_candidates: List[PlayerRow] = []
        for r in active_by_games:
            uid = r.uid or ""
            online_games = online_counts.get(uid, 0)
            if online_games >= 10:
                qualified_candidates.append(r)

        print(
            f"[topdeck/top16] Players with >=10 online games: "
            f"{len(qualified_candidates)}."
        )

        # The actual displayed Top 16 are just the first 16 of these
        qualified_top16: List[PlayerRow] = qualified_candidates[:16]

        if len(qualified_top16) < 16:
            print(
                "[topdeck/top16] WARNING: fewer than 16 players meet the "
                "10 online games requirement overall."
            )

        # ---- Step 3: console info about drops/promotions ----

        def row_key(r: PlayerRow) -> str:
            # Use uid when present, otherwise fall back to entrant_id
            return (r.uid or "").strip() or f"entrant:{r.entrant_id}"

        raw_keys = {row_key(r) for r in top16_by_games}
        qualified_keys = {row_key(r) for r in qualified_top16}

        # Who was in raw Top 16 but got dropped?
        for r in top16_by_games:
            if row_key(r) not in qualified_keys:
                uid = r.uid or ""
                online_games = online_counts.get(uid, 0)
                print(
                    f"[topdeck/top16] DROPPED from cut: {r.name!r} (uid={uid!r}) "
                    f"with only {online_games} online games."
                )

        # Who is in final Top 16 and where did they come from?
        for i, r in enumerate(qualified_top16, start=1):
            uid = r.uid or ""
            online_games = online_counts.get(uid, 0)
            origin = (
                "kept from raw top16"
                if row_key(r) in raw_keys
                else "PROMOTED from below"
            )
            print(
                f"[topdeck/top16] FINAL SLOT #{i:02}: {r.name!r} (uid={uid!r}), "
                f"pts={r.pts:.1f}, total_games={r.games}, online_games={online_games} "
                f"→ {origin}."
            )

        # ---- Step 4: personal info for the user (online games based) ----
        member = ctx.author
        row_for_author = (
            self._find_author_row(member, rows)
            if isinstance(member, discord.Member)
            else None
        )

        ts_int = _ts(fetched_at)

        if row_for_author:
            author_uid = row_for_author.uid or ""
            online_games_for_author = online_counts.get(author_uid, 0)
            total_games = row_for_author.games

            if online_games_for_author >= 10:
                missing_msg = (
                    f"You're already eligible: you have **{online_games_for_author}** online games.\n"
                    f"(TopDeck record: {row_for_author.wins}W / "
                    f"{row_for_author.draws}D / {row_for_author.losses}L "
                    f"across {total_games} total games.)\n\n"
                    f"TopDeck data: last updated <t:{ts_int}:R>."
                )
            else:
                missing = 10 - online_games_for_author
                missing_msg = (
                    f"You have **{online_games_for_author}** online games. "
                    f"You need **{missing}** more online game(s) to be eligible.\n"
                    f"(TopDeck record: {row_for_author.wins}W / "
                    f"{row_for_author.draws}D / {row_for_author.losses}L "
                    f"across {total_games} total games.)\n\n"
                    f"TopDeck data: last updated <t:{ts_int}:R>."
                )
        else:
            missing_msg = (
                "I couldn't find you in this bracket's data. "
                "Please check that your Discord on TopDeck is up to date.\n\n"
                f"TopDeck data: last updated <t:{ts_int}:R>."
            )

        # ---- Step 5: public Top 16 (really qualified) ----
        if not qualified_top16:
            await ctx.followup.send(
                "There are no players with at least 10 online games in this bracket yet.",
                ephemeral=False,
            )
        else:
            header = "**Top 16 – Qualified Players (>= 10 online games)**\n"
            lines = [header]
            for i, r in enumerate(qualified_top16, start=1):
                lines.append(
                    f"`#{i:02}` **{r.name}** – {int(round(r.pts))} pts"
                )

            await ctx.followup.send("\n".join(lines), ephemeral=False)

        # ---- Step 6: ephemeral message just for you, AFTER the public one ----
        await ctx.followup.send(missing_msg, ephemeral=True)



def setup(bot: commands.Bot):
    bot.add_cog(TopdeckLeagueCog(bot))
