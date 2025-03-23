import sqlite3
import discord
import datetime
import time

from datetime import datetime
from discord.ext import commands
from dataclasses import dataclass

# IMPORTANT CONSTATNTS

BOT_TOKEN = redacted_token
CHANNEL_ID = redacted_channel_id #Map Bot

# DISCORD BOT

bot = commands.Bot(command_prefix="m", intents=discord.Intents.all())

@dataclass
class BotData:
    """
    General Data the bot keeps tracks of in a session.
    """
    bot_commands = ["madd [name-of-map] [won (True/False)]",
                    "mcmds",]    


# Instatiate data classes
bot_data = BotData()


# Bot Behaviour

@bot.event
async def on_ready():
    """
    Functionaliy when bot activated
    """
    channel = bot.get_channel(CHANNEL_ID)
    msg = "\n===MAP BOT RUNNING==="

    await channel.send(msg)


@bot.command()
async def add(ctx, map_name: str, result: bool):
    """
    Command to add map to the database
    Arguments:
        map_name: name of the map played
        won: wwhether the map was one or not True or False
    """
    cur_timestamp = time.time()
    added_at = datetime.fromtimestamp(cur_timestamp).strftime("%Y-%m-%d %H:%M:%S")

    #Connecting to database
    connection = sqlite3.connect(redacted_db_path)
    cursor = connection.cursor()

    try:
        cursor.execute("INSERT INTO maps (map_name, added_by, timestamp, result) VALUES (?, ?, ?, ?)", (map_name, ctx.author.name, added_at, result))
        connection.commit()
        await ctx.send(f"Map '{map_name}' added successfully by {ctx.author.name} @ {added_at}")
    except Exception as e:
        await ctx.send(f"An error occured: {e}")
    finally:
        connection.close()


@bot.command()
async def winrate(ctx, map_name :str):
    """
    Fetches winrate for a given map
    returns as a percentage
    Arguments: 
        map_name: the name of the map
    """

    query = f"SELECT COUNT(*) FROM maps WHERE map_name = '{map_name}' AND result = 1"
    query2 = f"SELECT COUNT(*) FROM maps WHERE map_name = '{map_name}'"

    connection = sqlite3.connect(redacted_db_path)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        won = cursor.fetchone()
        cursor.execute(query2)
        total = cursor.fetchone()
        wrate = round((int(won[0]) / int(total[0]) * 100), 2)
        await ctx.send(f"{wrate}%")
    except Exception as e:
        await ctx.send(f"An error occured: {e}")
    finally:
        connection.close()


@bot.command()
async def cmds(ctx):
    """
    Function that will print commands
    """
    cmds = "\n".join(bot_data.bot_commands)
    await ctx.send("=== Commands ===\n" + cmds)


@bot.command()
@commands.is_owner()
async def stop(ctx):
    """
    Powers off the bot
    """

    await ctx.send("Bot is shutting down")
    await bot.close() # CLose port to bot

bot.run(BOT_TOKEN)