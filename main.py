import os
import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

# Mongo bootstrap (Motor)
try:
    from db import ping as mongo_ping, ensure_indexes as mongo_ensure_indexes
except Exception as e:
    mongo_ping = None
    mongo_ensure_indexes = None
    print(f"[boot] Mongo not ready: {e}")


TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))

intents = discord.Intents.default()
intents.members = True
intents.guilds = True
intents.message_content = True
intents.voice_states = True  # needed for timers + auto-stop

bot = commands.Bot(command_prefix="!", intents=intents)

INITIAL_EXTENSIONS = [
    "cogs.invite_roles",
    "cogs.subscriptions_cog",
    "cogs.timer_cog",
    "cogs.topdeck_league",
    "cogs.spellbot_watch",
    "cogs.topdeck_online_sync",
    "cogs.lfg_cog",
    "cogs.join_league_cog",
]

_MONGO_BOOTSTRAPPED = False


def load_opus():
    if discord.opus.is_loaded():
        print("[voice] Opus already loaded")
        return

    env_path = os.getenv("OPUS_PATH")
    candidates = [
        env_path,
        os.path.join(os.path.dirname(__file__), "libopus.dll"),   # local Windows
        "libopus-0.dll",
        "libopus.dll",
        "opus",
        "libopus.so.0",                                           # Linux/Heroku
    ]

    for cand in candidates:
        if not cand:
            continue
        try:
            discord.opus.load_opus(cand)
            print(f"[voice] Loaded Opus from {cand}")
            return
        except OSError:
            continue

    print("[voice] WARNING: could not load Opus. Voice will not work.")


@bot.event
async def on_ready():
    global _MONGO_BOOTSTRAPPED

    # MongoDB connectivity check + indexes
    if (not _MONGO_BOOTSTRAPPED) and mongo_ping and mongo_ensure_indexes:
        try:
            await mongo_ping()
            await mongo_ensure_indexes()
            print("[boot] MongoDB OK + indexes ensured")
            _MONGO_BOOTSTRAPPED = True
        except Exception as e:
            print(f"[boot] MongoDB ERROR: {e}")

    # Existing invite-role cache bootstrap (unchanged behavior)
    invite_cog = bot.get_cog("InviteRoles")
    if invite_cog is not None:
        guild = bot.get_guild(GUILD_ID)
        if guild is not None:
            await invite_cog.build_invite_cache(guild)

    print(f"Logged in as {bot.user} ({bot.user.id})")
    print(f"[boot] voice_states intent on? {bot.intents.voice_states}")


if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("Missing DISCORD_TOKEN env var.")

    load_opus()

    # load extensions (sync in py-cord)
    for ext in INITIAL_EXTENSIONS:
        try:
            bot.load_extension(ext)
        except Exception as e:
            print(f"[boot] ⚠️ Failed to load extension '{ext}': {e}")

    bot.run(TOKEN)
