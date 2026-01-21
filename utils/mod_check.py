# utils/mod_check.py
"""Centralized mod permission checking.
"""

import os
from typing import Optional

import discord

from .settings import SUBS


# Environment-based config (used by cogs that don't have SUBS access)
ECL_MOD_ROLE_ID: int = int(os.getenv("ECL_MOD_ROLE_ID", "0"))
ECL_MOD_ROLE_NAME: str = os.getenv("ECL_MOD_ROLE_NAME", "ECL MOD")


def get_mod_role_id() -> int:
    """Get mod role ID from settings or env var."""
    # Prefer SUBS config if available
    cfg_id = int(getattr(SUBS, "ecl_mod_role_id", 0) or 0)
    if cfg_id:
        return cfg_id
    return ECL_MOD_ROLE_ID


def is_mod(
    member: Optional[discord.Member],
    *,
    check_manage_roles: bool = False,
) -> bool:
    """Check if a member is an ECL mod.

    Args:
        member: The Discord member to check
        check_manage_roles: If True, also grant mod status to users with Manage Roles permission

    Returns:
        True if the member is a mod, False otherwise
    """
    if member is None:
        return False

    # Optionally check Manage Roles permission
    if check_manage_roles:
        perms = getattr(member, "guild_permissions", None)
        if perms and perms.manage_roles:
            return True

    # Check by role ID (from settings or env)
    mod_role_id = get_mod_role_id()
    if mod_role_id:
        if any(r.id == mod_role_id for r in (member.roles or [])):
            return True

    # Check by role name (env only)
    if ECL_MOD_ROLE_NAME:
        if any(r.name == ECL_MOD_ROLE_NAME for r in (member.roles or [])):
            return True

    return False


def get_mod_members(guild: discord.Guild) -> list[discord.Member]:
    """Get all members with the mod role in a guild.

    Args:
        guild: The Discord guild to search

    Returns:
        List of members with the mod role (excluding bots)
    """
    mod_role_id = get_mod_role_id()
    if not mod_role_id:
        return []

    role = guild.get_role(mod_role_id)
    if not role:
        return []

    return [m for m in (role.members or []) if not m.bot]
