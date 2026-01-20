# utils/logger.py
from __future__ import annotations

import contextlib
from typing import Any, Optional, Tuple

from utils.console import c

# ---- central mapping (shared by all files) ----

PREFIX_COLORS = {
    "subs": "cyan",
    "voice": "magenta",
    "timer": "blue",
    "timer_end": "blue",
    "play_voice_file": "blue",
    "topdeck": "yellow",
    "timer/topdeck": "yellow",
    "online-sync": "yellow",
    "lfg": "green",
    "db": "white",
    "set_timer_stopped": "grey",
}

LEVEL_COLORS = {
    "debug": "grey",
    "info": "white",
    "ok": "green",
    "warn": "yellow",
    "error": "red",
}

LEVEL_EMOJIS = {
    "debug": "🔹",
    "info": "ℹ️",
    "ok": "✅",
    "warn": "⚠️",
    "error": "❌",
}


def split_prefix(text: str) -> Tuple[Optional[str], str]:
    t = (text or "").strip()
    if not t.startswith("["):
        return None, t
    end = t.find("]")
    if end <= 1:
        return None, t
    prefix = t[1:end].strip()
    rest = t[end + 1 :].lstrip()
    return prefix, rest


def format_console(text: str, *, level: str = "info") -> str:
    prefix, rest = split_prefix(text)
    lvl = (level or "info").lower()
    lvl_color = LEVEL_COLORS.get(lvl, "white")

    if prefix:
        p_color = PREFIX_COLORS.get(prefix.lower(), lvl_color)
        return f"{c(f'[{prefix}]', p_color, bold=True)} {c(rest, lvl_color)}" if rest else c(f"[{prefix}]", p_color, bold=True)

    return c(text, lvl_color)


def format_discord(text: str, *, level: str = "info") -> str:
    lvl = (level or "info").lower()
    emoji = LEVEL_EMOJIS.get(lvl, "ℹ️")
    msg = f"{emoji} {str(text or '')}"
    return msg[:1900] + "…" if len(msg) > 1900 else msg


class Logger:
    """
    Shared logger for all cogs:
      - colored console
      - plain Discord logging channel
    Expects cfg.guild_id and cfg.log_channel_id like your current code.
    """

    def __init__(self, bot: Any, cfg: Any):
        self.bot = bot
        self.cfg = cfg

    async def log(self, text: str, *, level: str = "info", send: bool = True, console: bool = True) -> None:
        raw = str(text or "")

        # console
        if console:
            try:
                print(format_console(raw, level=level))
            except Exception:
                print(raw)

        # discord
        if not send:
            return

        ch_id = int(getattr(self.cfg, "log_channel_id", 0) or 0)
        if not ch_id:
            return

        guild_id = int(getattr(self.cfg, "guild_id", 0) or 0)
        guild = self.bot.get_guild(guild_id) if guild_id else None
        if not guild:
            return

        ch = guild.get_channel(ch_id)
        if not ch:
            with contextlib.suppress(Exception):
                ch = await guild.fetch_channel(ch_id)
        if not ch:
            return

        with contextlib.suppress(Exception):
            await ch.send(format_discord(raw, level=level))

    # convenience level methods
    async def debug(self, text: str, **kw): return await self.log(text, level="debug", **kw)
    async def info(self, text: str, **kw):  return await self.log(text, level="info", **kw)
    async def ok(self, text: str, **kw):    return await self.log(text, level="ok", **kw)
    async def warn(self, text: str, **kw):  return await self.log(text, level="warn", **kw)
    async def error(self, text: str, **kw): return await self.log(text, level="error", **kw)


def get_logger(bot: Any, cfg: Any) -> Logger:
    return Logger(bot, cfg)
