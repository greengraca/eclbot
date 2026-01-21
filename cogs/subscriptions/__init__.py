# cogs/subscriptions/__init__.py
"""Subscriptions submodule - helpers for the main SubscriptionsCog."""

from .kofi import (
    compute_one_time_window,
    extract_discord_user_id,
    extract_json_from_message_content,
)
from .views import SubsLinksView
from .embeds import (
    build_reminder_embed,
    build_flip_mods_embed,
    build_top16_online_reminder_embed,
    build_topcut_prize_reminder_embed,
)
from .month_flip import MonthFlipHandler

__all__ = [
    "compute_one_time_window",
    "extract_discord_user_id",
    "extract_json_from_message_content",
    "SubsLinksView",
    "build_reminder_embed",
    "build_flip_mods_embed",
    "build_top16_online_reminder_embed",
    "build_topcut_prize_reminder_embed",
    "MonthFlipHandler",
]
