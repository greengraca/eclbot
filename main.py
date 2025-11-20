import os
import asyncio
import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))

intents = discord.Intents.default()
intents.members = True
intents.guilds = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

INITIAL_EXTENSIONS = [
    "cogs.invite_roles",
]


@bot.event
async def on_ready():
    invite_cog = bot.get_cog("InviteRoles")
    if invite_cog is not None:
        guild = bot.get_guild(GUILD_ID)
        if guild is not None:
            await invite_cog.build_invite_cache(guild)

    await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
    print(f"Logged in as {bot.user} ({bot.user.id})")


async def main():
    # load cogs (now async)
    for ext in INITIAL_EXTENSIONS:
        await bot.load_extension(ext)

    # start the bot
    await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
