"""Shared helpers for safely responding to Discord interactions.

These helpers are intentionally conservative:
- Try to respond/edit through the interaction first.
- If the interaction is expired (10062) or already responded, fall back.

This avoids hard crashes when the event loop lags and Discord expires an
interaction token before we can ack it.

Supports:
- discord.ApplicationContext (slash commands)
- discord.Interaction (component interactions, e.g. button clicks)
"""

from __future__ import annotations

import contextlib
from typing import Optional, Union

import discord


async def safe_ctx_defer(
    ctx: discord.ApplicationContext,
    *,
    ephemeral: bool = False,
    label: str = "",
) -> bool:
    """Try to ack a slash interaction. If it expired (10062), don't crash."""
    try:
        inter = getattr(ctx, "interaction", None)
        created = getattr(inter, "created_at", None)
        if created:
            age = (discord.utils.utcnow() - created).total_seconds()
            if age > 2.7:
                print(f"[{label}] ⚠️ interaction age before defer: {age:.2f}s (event-loop lag)")

        await ctx.defer(ephemeral=ephemeral)
        return True
    except discord.NotFound:
        print(f"[{label}] ❌ ctx.defer failed: Unknown interaction (10062). Falling back to channel messages.")
        return False
    except Exception as e:
        print(f"[{label}] ❌ ctx.defer failed: {type(e).__name__}: {e}")
        return False


async def safe_ctx_respond(ctx: discord.ApplicationContext, *args, **kwargs):
    """ctx.respond, but if interaction expired, fallback to channel.send."""
    try:
        return await ctx.respond(*args, **kwargs)
    except discord.NotFound:
        if ctx.channel:
            kwargs.pop("ephemeral", None)
            content = kwargs.get("content", None)

            if args and isinstance(args[0], str):
                content = args[0]
                args = (f"{ctx.author.mention} {content}",) + tuple(args[1:])
            elif isinstance(content, str) and content:
                kwargs["content"] = f"{ctx.author.mention} {content}"
            else:
                kwargs["content"] = ctx.author.mention

            return await ctx.channel.send(*args, **kwargs)


async def safe_ctx_followup(ctx: discord.ApplicationContext, *args, **kwargs):
    """ctx.followup.send, but if interaction expired, fallback to channel.send."""
    try:
        return await ctx.followup.send(*args, **kwargs)
    except discord.NotFound:
        if ctx.channel:
            kwargs.pop("ephemeral", None)
            content = kwargs.get("content", None)

            if args and isinstance(args[0], str):
                content = args[0]
                args = (f"{ctx.author.mention} {content}",) + tuple(args[1:])
            elif isinstance(content, str) and content:
                kwargs["content"] = f"{ctx.author.mention} {content}"
            else:
                kwargs["content"] = ctx.author.mention

            return await ctx.channel.send(*args, **kwargs)


async def safe_i_send(
    interaction: discord.Interaction,
    content: Optional[str] = None,
    *,
    ephemeral: bool = False,
    embed: Optional[discord.Embed] = None,
):
    """Send a response to a component interaction; fallback to channel if 10062."""
    try:
        if interaction.response.is_done():
            return await interaction.followup.send(content, ephemeral=ephemeral, embed=embed)
        return await interaction.response.send_message(content, ephemeral=ephemeral, embed=embed)
    except discord.NotFound:
        ch = interaction.channel
        if ch:
            with contextlib.suppress(Exception):
                msg = content or ""
                msg = f"{interaction.user.mention} {msg}".strip()
                return await ch.send(msg, embed=embed)
    except Exception:
        return


async def safe_i_edit(
    interaction: discord.Interaction,
    *,
    content: Optional[str] = None,
    embed: Optional[discord.Embed] = None,
    view: Optional[discord.ui.View] = None,
):
    """Edit the original message via interaction; fallback to editing message directly."""
    try:
        if interaction.response.is_done():
            return await interaction.edit_original_response(content=content, embed=embed, view=view)
        return await interaction.response.edit_message(content=content, embed=embed, view=view)
    except discord.InteractionResponded:
        with contextlib.suppress(Exception):
            return await interaction.edit_original_response(content=content, embed=embed, view=view)
    except discord.NotFound:
        msg = getattr(interaction, "message", None)
        if msg:
            with contextlib.suppress(Exception):
                return await msg.edit(content=content, embed=embed, view=view)
    except Exception:
        return


async def resolve_member(
    guild: discord.Guild,
    user_id: Union[int, str],
) -> Optional[discord.Member]:
    """Try cache first, then API fetch. Returns None on failure."""
    uid = int(user_id)
    member = guild.get_member(uid)
    if member is not None:
        return member
    try:
        return await guild.fetch_member(uid)
    except Exception:
        return None
