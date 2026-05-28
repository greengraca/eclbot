# Domain: Top16 Eligibility

## Qualification Flow (total-games rule, effective 2026-05)

```
TopDeck API → topdeck_fetch:_compute_standings() → PlayerRow (pts, games, wins, losses)
    ↓
online_games_store:has_recent_game_by_topdeck_uid(online_only=False) → {uid: bool}
    ↓
utils/top16_eligibility:is_top16_eligible(...)  — single source of truth in this repo
```

A player qualifies for Top16 when ALL of:
1. In top 16 by points (not dropped)
2. `games >= TOP16_MIN_TOTAL_GAMES` (default 10) — total games (online + in-person)
3. Recency: either `games >= TOP16_MIN_ONLINE_GAMES_NO_RECENCY` (default 20), OR recency isn't active yet, OR has at least 1 game after day `TOP16_RECENCY_AFTER_DAY` (default 20)

All thresholds are env-configurable via `utils/settings.py:SubsConfig`. The "_online_" in `TOP16_MIN_ONLINE_GAMES_NO_RECENCY` is a documented misnomer — it applies to total games now; not renamed to avoid coordinated Heroku/Vercel env changes. `TOP16_MIN_ONLINE_GAMES` is no longer used for gating.

## Consumers

| Consumer | File | Trigger | Effect |
|----------|------|---------|--------|
| `/stats` | `stats_cog.py` | User command (current month only) | Displays eligibility embed |
| `/league` / `/top16` | `topdeck_league.py` | User command (current month only) | Shows Top16 list with bumps |
| Month-end cut + `_tick` | `subscriptions_cog.py:_eligible_top16_entries_for_month` | 10-min loop + month flip | Grants/revokes Top16 Discord role |
| Topcut prize reminder | `subscriptions_cog.py:_topcut_prize_reminder_targets` | 5d/1d before close | DM eligible players |

All four call `is_top16_eligible(...)` from `utils/top16_eligibility.py` — single source of truth.

The old "online-games top16 reminder" (`_top16_unqualified_for_month` / `_build_top16_online_reminder_embed` / `_run_top16_online_reminder_job`) was **retired** with the total-games switch: under total games the population it targeted (top-16-by-points but short on online games) is essentially empty, since points come from games.

## eclDashboard Parallel Implementation

`lib/top16-eligibility.ts` in eclDashboard mirrors this predicate. It is **freeze-aware**: months >= `TOP16_TOTAL_GAMES_FROM` (default `"2026-05"`) use the total-games rule; older months stay on the old online-games rule for historical display. The boundary constant must match this repo's effective month. This repo does not need a date-gate since the bot only computes current/just-closed months.

## Data Stores

| Store | Field | Format | Critical |
|-------|-------|--------|----------|
| `online_games` | `start_ts` | float, **seconds** since epoch | Dashboard depends on this |
| `online_games` | `topdeck_uids` | array of UID strings | Aggregation unwinds this |
| `online_games` | `online` | bool | **No longer gates eligibility.** Still populated by `/synconline`; informational only. |

## Handle Normalization Chain

```
TopDeck discord field → utils/topdeck_normalize:normalize_topdeck_discord() → lowercase alnum
Discord member.name   → same function → lowercase alnum
                        Match if equal
```

Both sides must use the same normalizer. `utils/topdeck_identity.py` handles multi-candidate matching with confidence ranking.

## Gotchas

- `has_recent_game_by_topdeck_uid()` is called with `online_only=False` for the current rule. It clamps `after_day` to month length (Feb safety).
- `normalize_ts()` in `month_dump_reader.py` converts ms→s when value > 10 billion — used by both dump storage and reading.
- Standings use wager model: `START_POINTS=1000`, `WAGER_RATE=0.07` — dashboard ports this in `lib/topdeck.ts`.
- Each consumer's bare `except` blocks log warnings (audit fix) but still return `None` on failure.
