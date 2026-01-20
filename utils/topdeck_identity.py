"""TopDeck <-> Discord identity resolver.

Goal: prefer Discord ID matching whenever possible, and only fall back to
handle/name matching when it's unique.

This module is intentionally dependency-light so it can be reused across cogs.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata
from typing import Dict, Iterable, List, Optional, Set, Tuple, TypeVar, Any

import discord

from topdeck_fetch import normalize_topdeck_discord


T = TypeVar("T")


CONF_DISCORD_ID = "discord_id"
CONF_HANDLE = "handle"
CONF_NAME = "name"
CONF_AMBIG_HANDLE = "ambiguous_handle"
CONF_AMBIG_NAME = "ambiguous_name"
CONF_NONE = "none"


@dataclass(frozen=True)
class MemberIndex:
    """Lookup tables for mapping TopDeck rows to Discord members."""

    id_to_member: Dict[int, discord.Member]
    handle_to_ids: Dict[str, Set[int]]
    name_to_ids: Dict[str, Set[int]]


@dataclass(frozen=True)
class Resolution:
    """Result of resolving a TopDeck row to a Discord user id."""

    discord_id: Optional[int]
    confidence: str
    matched_key: str = ""
    detail: str = ""


@dataclass(frozen=True)
class RowMatch:
    """Result of resolving a Discord member to a TopDeck row."""

    row: Any
    confidence: str
    matched_key: str = ""
    detail: str = ""


def extract_discord_id(text: str) -> Optional[int]:
    """Extract a Discord snowflake from a mention like <@123> or raw digits."""
    if not text:
        return None
    t = str(text).strip()

    m = re.search(r"<@!?(\d{15,25})>", t)
    if m:
        return int(m.group(1))

    m2 = re.search(r"\b(\d{15,25})\b", t)
    if m2:
        return int(m2.group(1))

    return None


def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm_name(s: str) -> str:
    """Normalize a display-name-ish string for last-resort matching."""
    if not s:
        return ""
    s2 = _strip_accents(str(s)).lower().strip()
    # Keep alnum only, similar spirit to normalize_topdeck_discord
    return re.sub(r"[^a-z0-9]", "", s2)


def member_handle_candidates(member: discord.Member) -> List[str]:
    """Return normalized handle candidates for TopDeck matching (stable order)."""
    cands: List[str] = []

    ordered = [
        getattr(member, "display_name", None),
        getattr(member, "global_name", None),
        getattr(member, "name", None),
    ]

    # add name#discriminator when it exists (older accounts / bots)
    discrim = getattr(member, "discriminator", None)
    if discrim and discrim != "0" and getattr(member, "name", None):
        ordered.append(f"{member.name}#{discrim}")

    for raw in ordered:
        if not raw:
            continue
        h = normalize_topdeck_discord(str(raw))
        if h and h not in cands:
            cands.append(h)

    return cands


def _member_name_candidates(member: discord.Member) -> List[str]:
    ordered = [
        getattr(member, "display_name", None),
        getattr(member, "global_name", None),
        getattr(member, "name", None),
    ]
    out: List[str] = []
    for raw in ordered:
        if not raw:
            continue
        k = norm_name(str(raw))
        if k and k not in out:
            out.append(k)
    return out


def build_member_index(members: Iterable[discord.Member]) -> MemberIndex:
    id_to_member: Dict[int, discord.Member] = {}
    handle_to_ids: Dict[str, Set[int]] = {}
    name_to_ids: Dict[str, Set[int]] = {}

    for m in members:
        if getattr(m, "bot", False):
            continue

        mid = int(m.id)
        id_to_member[mid] = m

        for h in member_handle_candidates(m):
            handle_to_ids.setdefault(h, set()).add(mid)

        for nk in _member_name_candidates(m):
            name_to_ids.setdefault(nk, set()).add(mid)

    return MemberIndex(
        id_to_member=id_to_member,
        handle_to_ids=handle_to_ids,
        name_to_ids=name_to_ids,
    )


def _unique_id(ids: Set[int]) -> Optional[int]:
    if not ids:
        return None
    if len(ids) == 1:
        return next(iter(ids))
    return None


def resolve_row_discord_id(row: Any, index: MemberIndex) -> Resolution:
    """Resolve a TopDeck row to a Discord ID with confidence classification."""
    row_discord = getattr(row, "discord", "") or ""
    row_name = getattr(row, "name", "") or ""

    # 1) Strong: Discord ID embedded in row.discord
    did = extract_discord_id(row_discord)
    if did:
        return Resolution(
            discord_id=int(did),
            confidence=CONF_DISCORD_ID,
            matched_key=str(did),
            detail="extracted from row.discord",
        )

    # 2) Handle match (unique only)
    h = normalize_topdeck_discord(row_discord)
    if h:
        ids = index.handle_to_ids.get(h, set())
        uid = _unique_id(ids)
        if uid is not None:
            return Resolution(
                discord_id=int(uid),
                confidence=CONF_HANDLE,
                matched_key=h,
                detail="unique handle match from row.discord",
            )
        if len(ids) > 1:
            return Resolution(
                discord_id=None,
                confidence=CONF_AMBIG_HANDLE,
                matched_key=h,
                detail=f"handle matches {len(ids)} members",
            )

    # 3) Name match (unique only) — last resort
    name_keys: List[Tuple[str, str]] = []
    # sometimes row.discord holds a display-ish string; consider it as name too
    if row_discord:
        k = norm_name(row_discord)
        if k:
            name_keys.append((k, "row.discord"))
    if row_name:
        k = norm_name(row_name)
        if k:
            name_keys.append((k, "row.name"))

    for nk, origin in name_keys:
        ids = index.name_to_ids.get(nk, set())
        uid = _unique_id(ids)
        if uid is not None:
            return Resolution(
                discord_id=int(uid),
                confidence=CONF_NAME,
                matched_key=nk,
                detail=f"unique name match from {origin}",
            )
        if len(ids) > 1:
            return Resolution(
                discord_id=None,
                confidence=CONF_AMBIG_NAME,
                matched_key=nk,
                detail=f"name key matches {len(ids)} members ({origin})",
            )

    return Resolution(
        discord_id=None,
        confidence=CONF_NONE,
        matched_key="",
        detail="no match",
    )


def find_row_for_member(rows: Iterable[Any], member: discord.Member) -> Optional[RowMatch]:
    """Resolve a Discord member to a TopDeck row. ID first, then unique handle/name."""
    target_id = int(member.id)

    # 1) Strong: ID match against row.discord
    for r in rows:
        did = extract_discord_id(getattr(r, "discord", "") or "")
        if did == target_id:
            return RowMatch(
                row=r,
                confidence=CONF_DISCORD_ID,
                matched_key=str(target_id),
                detail="matched by discord id in row.discord",
            )

    # 2) Handle match (unique row only)
    handle_to_rows: Dict[str, List[Any]] = {}
    for r in rows:
        h = normalize_topdeck_discord(getattr(r, "discord", "") or "")
        if not h:
            continue
        handle_to_rows.setdefault(h, []).append(r)

    for h in member_handle_candidates(member):
        rs = handle_to_rows.get(h, [])
        if len(rs) == 1:
            return RowMatch(
                row=rs[0],
                confidence=CONF_HANDLE,
                matched_key=h,
                detail="unique handle match",
            )

    # 3) Name match (unique row only)
    name_to_rows: Dict[str, List[Any]] = {}
    for r in rows:
        nk = norm_name(getattr(r, "name", "") or "")
        if not nk:
            continue
        name_to_rows.setdefault(nk, []).append(r)

    for nk in _member_name_candidates(member):
        rs = name_to_rows.get(nk, [])
        if len(rs) == 1:
            return RowMatch(
                row=rs[0],
                confidence=CONF_NAME,
                matched_key=nk,
                detail="unique name match",
            )

    return None
