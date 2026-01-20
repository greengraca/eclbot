# cogs/spellbot_watch.py
import os
import re
from typing import Optional, Set, List

import discord
from discord.ext import commands

from topdeck_fetch import get_league_rows_cached, PlayerRow, WAGER_RATE
from utils.topdeck_identity import find_row_for_member

GUILD_ID = int(os.getenv("GUILD_ID", "0"))

SPELLBOT_USER_ID = int(os.getenv("SPELLBOT_USER_ID", "0"))
SPELLBOT_LFG_CHANNEL_ID = int(os.getenv("SPELLBOT_LFG_CHANNEL_ID", "0"))

SPELLBOT_READY_TITLE_PREFIX = os.getenv(
    "SPELLBOT_READY_TITLE_PREFIX",
    "Your game is ready!",
)

DEBUG_SPELLBOT_WATCH = bool(int(os.getenv("DEBUG_SPELLBOT_WATCH", "0")))

TOPDECK_BRACKET_ID = os.getenv("TOPDECK_BRACKET_ID", "")
FIREBASE_ID_TOKEN = os.getenv("FIREBASE_ID_TOKEN", None)

# Minimum total pot for "high-stakes pod" announcement
HIGH_STAKES_THRESHOLD = float(os.getenv("HIGH_STAKES_THRESHOLD", "375"))


class SpellBotWatchCog(commands.Cog):
    """
    Watches SpellBot embeds and detects when a lobby becomes a ready SpellTable game.

    Logs to console whenever we see a SpellBot message whose embed title starts with
    'Your game is ready!' – both when it's first created and when it's edited.

    Additionally, when a pod becomes ready, it checks the 4 players' current
    TopDeck points and warns if the wager pot is high enough (>= HIGH_STAKES_THRESHOLD).
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._seen_ready_messages: Set[int] = set()

        # print(
        #     "[spellbot-watch] Cog loaded with config: "
        #     f"GUILD_ID={GUILD_ID}, "
        #     f"SPELLBOT_USER_ID={SPELLBOT_USER_ID}, "
        #     f"SPELLBOT_LFG_CHANNEL_ID={SPELLBOT_LFG_CHANNEL_ID}, "
        #     f"READY_PREFIX={SPELLBOT_READY_TITLE_PREFIX!r}, "
        #     f"HIGH_STAKES_THRESHOLD={HIGH_STAKES_THRESHOLD}, "
        #     f"DEBUG={DEBUG_SPELLBOT_WATCH}"
        # )

    # --------- helpers ---------

    @staticmethod
    def _extract_spelltable_link(embed: discord.Embed) -> Optional[str]:
        desc = embed.description or ""
        if not desc:
            return None

        for token in desc.split():
            if "spelltable.wizards.com/game" in token:
                return token.strip("()<>")
        return None

    def _debug(self, msg: str) -> None:
        if DEBUG_SPELLBOT_WATCH:
            print(f"[spellbot-watch] {msg}")

    def _is_spellbot_message(self, msg: discord.Message) -> bool:
        # Prefer ID match if provided
        if SPELLBOT_USER_ID:
            is_spellbot = msg.author and msg.author.id == SPELLBOT_USER_ID
            self._debug(
                f"_is_spellbot_message: by ID? {is_spellbot} "
                f"(author_id={getattr(msg.author, 'id', None)})"
            )
            return is_spellbot

        # Fallback to name check (less reliable, but OK)
        is_spellbot = msg.author and msg.author.bot and msg.author.name.lower() == "spellbot"
        self._debug(
            f"_is_spellbot_message: by name? {is_spellbot} "
            f"(author_name={getattr(msg.author, 'name', None)})"
        )
        return is_spellbot

    def _is_in_lfg_channel(self, msg: discord.Message) -> bool:
        """
        True if the message is in the configured LFG channel, or in a thread whose
        parent is that channel. If no channel configured, allow everything.
        """
        if not SPELLBOT_LFG_CHANNEL_ID:
            return True

        ch = msg.channel

        if isinstance(ch, discord.Thread):
            base_id = ch.parent_id
            self._debug(
                f"_is_in_lfg_channel: thread detected "
                f"(thread_id={ch.id}, parent_id={ch.parent_id})"
            )
        else:
            base_id = ch.id

        in_channel = base_id == SPELLBOT_LFG_CHANNEL_ID
        self._debug(
            f"_is_in_lfg_channel: {in_channel} "
            f"(base_id={base_id}, LFG_ID={SPELLBOT_LFG_CHANNEL_ID})"
        )
        return in_channel

    async def _handle_high_stakes(self, msg: discord.Message) -> None:
        """
        After a ready game is detected, look up the 4 players in TopDeck,
        compute the wager pot and announce if it's high stakes.
        """
        try:
            guild = msg.guild
            if guild is None:
                self._debug("_handle_high_stakes: message has no guild, skipping.")
                return

            if not msg.embeds:
                self._debug("_handle_high_stakes: no embeds, skipping.")
                return

            embed = msg.embeds[0]

            # Find the "Players" field and extract mentions
            players_field = next(
                (f for f in embed.fields if "player" in f.name.lower()),
                None,
            )
            if not players_field:
                self._debug("_handle_high_stakes: no players field, skipping.")
                return

            value = players_field.value or ""
            id_strs = re.findall(r"<@!?(\d+)>", value)
            player_ids = [int(x) for x in id_strs]

            if len(player_ids) < 2:
                self._debug(
                    f"_handle_high_stakes: found {len(player_ids)} player IDs (<2), skipping."
                )
                return

            # League pods are 4-player; hard check to avoid weirdness
            if len(player_ids) != 4:
                self._debug(
                    f"_handle_high_stakes: expected 4 players, got {len(player_ids)}, skipping."
                )
                return

            # Resolve members
            members: List[discord.Member] = []
            for uid in player_ids:
                member = guild.get_member(uid)
                if member is None:
                    try:
                        member = await guild.fetch_member(uid)
                    except discord.NotFound:
                        member = None
                if member is None or not isinstance(member, discord.Member):
                    self._debug(
                        f"_handle_high_stakes: could not resolve member for id={uid}, aborting."
                    )
                    print(
                        "[spellbot-watch] High-stakes check aborted: "
                        f"could not resolve member for id={uid}"
                    )
                    return
                members.append(member)

            if not TOPDECK_BRACKET_ID:
                self._debug("_handle_high_stakes: TOPDECK_BRACKET_ID not set, skipping.")
                print(
                    "[spellbot-watch] High-stakes check aborted: "
                    "TOPDECK_BRACKET_ID not configured."
                )
                return

            # Get league rows (cached) – shared cache in topdeck_fetch
            rows, fetched_at = await get_league_rows_cached(
                TOPDECK_BRACKET_ID,
                FIREBASE_ID_TOKEN,
                force_refresh=False,
            )
            self._debug(
                f"_handle_high_stakes: loaded {len(rows)} league rows "
                f"(fetched_at={fetched_at.isoformat()})."
            )

            matched_rows: List[PlayerRow] = []
            for m in members:
                match = find_row_for_member(rows, m)
                if not match:
                    self._debug(
                        f"_handle_high_stakes: no TopDeck row for member {m} ({m.id}), aborting."
                    )
                    print(
                        "[spellbot-watch] High-stakes check aborted: "
                        f"no TopDeck row found for member {m} ({m.id})."
                    )
                    return

                # Log confidence: discord_id / handle / name
                try:
                    self._debug(
                        "_handle_high_stakes: identity match "
                        f"member_id={m.id} conf={match.confidence} key={match.matched_key!r} detail={match.detail}"
                    )
                except Exception:
                    pass

                matched_rows.append(match.row)

            # Compute pot using current points and WAGER_RATE
            stakes = [(r, r.pts * WAGER_RATE) for r in matched_rows]
            pot = sum(stake for _, stake in stakes)
            approx_pot = int(round(pot))

            # Always print detailed per-player info when we evaluate a pod
            print(
                "[spellbot-watch] High-stakes calculation for message "
                f"{msg.id}: pot≈{approx_pot}, threshold={HIGH_STAKES_THRESHOLD}"
            )
            for row, stake in stakes:
                print(
                    "[spellbot-watch]   player={name!r}, uid={uid!r}, "
                    "pts={pts:.1f}, stake≈{stake:.1f}".format(
                        name=row.name,
                        uid=row.uid,
                        pts=row.pts,
                        stake=stake,
                    )
                )

            if pot < HIGH_STAKES_THRESHOLD:
                print(
                    "[spellbot-watch] Pod below high-stakes threshold; "
                    f"pot≈{approx_pot}, threshold={HIGH_STAKES_THRESHOLD} – no announcement."
                )
                return

            # High-stakes pod!
            player_names = [r.name for r, _ in stakes]
            print(
                "[spellbot-watch] HIGH-STAKES POD DETECTED: "
                f"message_id={msg.id}, pot≈{approx_pot}, players={player_names}"
            )

            await msg.channel.send(
                f"🚨 **HIGH-STAKES POD DETECTED!** 🚨\n"
                f"The winner will take home ~**{approx_pot}** points."
            )

        except Exception as e:
            print(f"[spellbot-watch] Error in _handle_high_stakes: {type(e).__name__}: {e}")

    def _maybe_log_ready_message(self, msg: discord.Message) -> None:
        """Shared logic used by both on_message and on_message_edit."""
        if not msg.embeds:
            self._debug(
                f"_maybe_log_ready_message: message_id={msg.id} has no embeds, skipping."
            )
            return

        embed = msg.embeds[0]
        title = (embed.title or "").strip()
        self._debug(
            f"_maybe_log_ready_message: message_id={msg.id}, "
            f"title={title!r}"
        )

        if not title:
            return

        # SpellBot tends to send '**Your game is ready!**'
        normalized_title = title.strip("* ").lower()
        normalized_prefix = SPELLBOT_READY_TITLE_PREFIX.strip("* ").lower()

        if not normalized_title.startswith(normalized_prefix):
            self._debug(
                f"_maybe_log_ready_message: normalized_title={normalized_title!r} "
                f"does not start with {normalized_prefix!r}, skipping."
            )
            return

        if msg.id in self._seen_ready_messages:
            self._debug(
                f"_maybe_log_ready_message: message_id={msg.id} already logged, skipping."
            )
            return
        self._seen_ready_messages.add(msg.id)

        link = self._extract_spelltable_link(embed)
        channel_name = (
            f"#{msg.channel.name}"
            if hasattr(msg.channel, "name")
            else str(msg.channel.id)
        )

        print(
            "[spellbot-watch] Detected ready game: "
            f"message_id={msg.id}, channel={channel_name}, "
            f"link={link or 'unknown'}"
        )

        # Kick off async high-stakes check in the background
        try:
            self.bot.loop.create_task(self._handle_high_stakes(msg))
        except Exception as e:
            self._debug(
                f"_maybe_log_ready_message: failed to schedule _handle_high_stakes: {e}"
            )

    # --------- event listeners ---------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        self._debug(
            f"on_message: id={message.id}, guild_id="
            f"{getattr(message.guild, 'id', None)}, "
            f"author_id={getattr(message.author, 'id', None)}, "
            f"channel_id={message.channel.id}"
        )

        if message.guild is None:
            self._debug("on_message: no guild, skipping.")
            return
        if GUILD_ID and message.guild.id != GUILD_ID:
            self._debug(
                f"on_message: guild mismatch (got {message.guild.id}, "
                f"expected {GUILD_ID}), skipping."
            )
            return
        if not self._is_spellbot_message(message):
            self._debug("on_message: not a SpellBot message, skipping.")
            return
        if not self._is_in_lfg_channel(message):
            self._debug("on_message: not in LFG channel, skipping.")
            return

        self._maybe_log_ready_message(message)

    @commands.Cog.listener()
    async def on_message_edit(
        self,
        before: discord.Message,
        after: discord.Message,
    ):
        self._debug(
            f"on_message_edit: id={after.id}, guild_id="
            f"{getattr(after.guild, 'id', None)}, "
            f"author_id={getattr(after.author, 'id', None)}, "
            f"channel_id={after.channel.id}"
        )

        if after.guild is None:
            self._debug("on_message_edit: no guild, skipping.")
            return
        if GUILD_ID and after.guild.id != GUILD_ID:
            self._debug(
                f"on_message_edit: guild mismatch (got {after.guild.id}, "
                f"expected {GUILD_ID}), skipping."
            )
            return
        if not self._is_spellbot_message(after):
            self._debug("on_message_edit: not a SpellBot message, skipping.")
            return
        if not self._is_in_lfg_channel(after):
            self._debug("on_message_edit: not in LFG channel, skipping.")
            return

        self._maybe_log_ready_message(after)


def setup(bot: commands.Bot):
    bot.add_cog(SpellBotWatchCog(bot))
