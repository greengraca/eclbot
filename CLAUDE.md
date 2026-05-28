# eclBot — Claude Code Reference

## Project Overview

ECL (Eclipse League) Discord bot for a competitive Magic: The Gathering online league.
Manages subscriptions, game timers, looking-for-game matchmaking, TopDeck/SpellTable integrations,
player statistics, and graphs for a single Discord guild.

Deployed to Heroku as a worker dyno (`Procfile`). No web server — pure Discord bot.

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Discord API | py-cord 2.7.0rc2 (`discord.ext.commands`) |
| Database | MongoDB Atlas via Motor (async) |
| External APIs | TopDeck (Firebase/Firestore), SpellTable proxy, Ko-fi webhooks |
| Voice | FFmpeg + libopus (MP3 announcements) |
| Charts | Matplotlib (Agg backend, headless/Heroku-safe) |
| Encryption | Fernet (`cryptography`) for treasure pod table numbers |
| Config | `.env` / `python-dotenv` — no config files |

## Key Directories

```
eclBot/
├── main.py                  # Bot init, extension loading (main.py:33), Opus setup
├── db.py                    # Motor client, all collection handles, ensure_indexes()
├── topdeck_fetch.py         # TopDeck API client + 30-min cache; Match, PlayerRow, InProgressPod
├── online_games_store.py    # CRUD for online_games; OnlineGameRecord dataclass
├── spelltable_client.py     # SpellTable game creation via proxy
├── cogs/                    # Discord extensions (one cog = one feature domain)
│   ├── lfg/                 # LFG subsystem: models, service, state, embeds, views, elo, autojoin
│   ├── subscriptions/       # Subscriptions subsystem: ko-fi, month_flip, embeds, views
│   ├── timer/               # Timer subsystem: helpers, topdeck tagging, views
│   ├── graphs_cog.py        # /graphs — 6 chart types (daily + all-time)
│   ├── stats_cog.py         # /stats — player snapshot, rank, top16 eligibility
│   ├── topdeck_league.py    # /league — standings, most-games, top16 qualifiers
│   ├── topdeck_month_dump.py # /topdeckdumpmonth — store full monthly dumps (chunked)
│   ├── topdeck_online_sync.py # /synconline — mark games as online
│   ├── join_league_cog.py   # /join — league signup
│   ├── spellbot_watch.py    # SpellBot integration monitoring
│   ├── timestamp_cog.py     # /timestamp — Discord timestamp generator
│   ├── invite_roles.py      # Invite-based role assignment on join
│   └── debug_cog.py         # Mod-only dry-run tools
├── utils/                   # Shared helpers
│   ├── dates.py             # All timezone logic — always Lisbon (Europe/Lisbon)
│   ├── settings.py          # SubsConfig dataclass loaded from env
│   ├── logger.py            # Colored console + Discord channel logging
│   ├── interactions.py      # Safe defer/respond/followup wrappers (handles expired tokens)
│   ├── persistence.py       # MongoDB save/load wrappers for timers & lobbies
│   ├── topdeck_normalize.py # Shared Discord handle normalization for TopDeck data
│   ├── topdeck_identity.py  # Discord member ↔ TopDeck player mapping (confidence levels)
│   ├── treasure_pods.py     # Fernet-encrypted pod table numbers, multi-type support
│   ├── graph_renderer.py    # Matplotlib chart rendering (Discord dark theme, 150 DPI PNG)
│   ├── month_dump_reader.py # Reassemble chunked month dumps, compute player history
│   ├── mod_check.py         # is_mod() helper, mod role detection
│   └── console.py           # Colorized text helper (colorama)
└── timer/                   # MP3 audio assets for voice announcements
```

## Essential Commands

```bash
pip install -r requirements.txt     # Install dependencies
python main.py                      # Run bot (requires .env)
```

**Environment setup:** copy `.env.example` → `.env` and fill in tokens.
Key required vars: `DISCORD_TOKEN`, `GUILD_ID`, `MONGO_URI` (or `MONGODB_URI`).
See `.env.example` for the full list of configurable vars.

## MongoDB Collections (db.py)

| Collection | Purpose |
|---|---|
| `subs_access` | Monthly subscription records |
| `subs_kofi_events` | Ko-fi transaction de-duplication |
| `subs_free_entries` | Free-entry grants (Judge, Vanguard roles) |
| `subs_jobs` | Distributed job locking (reminder/cleanup runs) |
| `online_games` | TopDeck online/in-person game records |
| `topdeck_pods` | TopDeck pod exports (per match) |
| `topdeck_month_dump_runs` | Metadata per monthly dump (bracket, month, sha256, chunk count) |
| `topdeck_month_dump_chunks` | Chunked JSON payloads of full month dumps (<16 MB each) |
| `persistent_timers` | Timer state surviving restarts |
| `persistent_lobbies` | LFG lobby state surviving restarts |
| `treasure_pod_schedule` | Treasure pod monthly schedule |
| `treasure_pods` | Individual treasure pod records |
| `spellbot_scan_cache` | SpellBot incremental sync cache |
| `user_preferences` | User timezone preferences (keyed by user_id) |

## Slash Commands

| Command | Cog | Purpose |
|---|---|---|
| `/graphs` | `graphs_cog` | Player stat charts (record, activity, points/rank, win rate, all-time) |
| `/stats` | `stats_cog` | Player snapshot: rank, online games, top16 eligibility |
| `/league` | `topdeck_league` | Standings, most-games contenders, top16 qualifiers |
| `/lfg` | `lfg_cog` | Open matchmaking lobby |
| `/lfgelo` | `lfg_cog` | Elo-restricted matchmaking lobby |
| `/join` | `join_league_cog` | League signup |
| `/synconline` | `topdeck_online_sync` | (Mod) Mark games as online |
| `/topdeckdumpmonth` | `topdeck_month_dump` | (Mod) Save monthly TopDeck dump to Mongo |
| `/timestamp` | `timestamp_cog` | Generate Discord timestamps for cross-timezone coordination |

## Testing

No automated test suite. All testing is manual via Discord.
Use `IS_DEV=1` env var to target `eclbot_dev` MongoDB database.

## Git Conventions

- **Commit messages**: Keep them short and simple, as if the user wrote them. No `Co-Authored-By` lines.
- Examples: `"fix timer progress bar after pause/resume"`, `"per-game embed colors"`, `"remove dead code"`

## Related Projects

- **eclDashboard** (`D:\Projetos\eclDashboard`) — Next.js web dashboard that shares the same
  MongoDB Atlas cluster. Reads bot-owned collections (`online_games`, `subs_access`,
  `topdeck_month_dump_*`, `treasure_pods`) but never writes to them. Key contracts to preserve:
  - `online_games.start_ts` must remain in **seconds** (dashboard recency cutoff depends on this)
  - Staking model constants (`START_POINTS=1000`, `WAGER_RATE=0.07`) must stay in sync with
    `lib/topdeck.ts:computeStandings()` in the dashboard (a port of `topdeck_fetch.py:_compute_standings`)
  - Top16 eligibility uses the **total-games rule** (effective 2026-05). Pre-2026-05 months
    stay frozen on the old online-games rule in the dashboard; the bot only ever cuts
    current/just-closed months and needs no date-gate. The boundary value `2026-05` must
    match `TOP16_TOTAL_GAMES_FROM` in dashboard `lib/constants.ts`. `TOP16_RECENCY_AFTER_DAY`
    must also match. `TOP16_MIN_ONLINE_GAMES` is no longer used for gating.
  - Dump format in `topdeck_month_dump_chunks` must remain compatible

## Additional Documentation

Check these files when working on the relevant feature:

- `.claude/docs/architectural_patterns.md` — cog structure, subsystem layering, async conventions,
  caching, logging, persistence, identity mapping, Elo matching, encryption, chunked storage,
  chart rendering, safe interactions, cross-season queries
- **`.claude/docs/domain_top16_eligibility.md`** — Top16 qualification logic, online game counting,
  recency checks, standings computation, dashboard sync. **Read when working on**: top16, eligibility,
  online games, standings, recency, `/stats`, `/league`, wager model
- **`.claude/docs/domain_subscriptions.md`** — Ko-fi webhook flow, entitlement sources, `_tick` loop,
  month flip, role grants/revokes. **Read when working on**: subscriptions, ko-fi, entitlements,
  month flip, ECL role, free entries, `_tick`
