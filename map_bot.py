import sqlite3
import discord
import datetime
import time
import os

from dotenv import load_dotenv
from datetime import datetime
from discord.ext import commands
from dataclasses import dataclass

# IMPORTANT CONSTATNTS

load_dotenv()

BOT_TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("ID"))
DB_PATH = os.getenv("DB")

# DISCORD BOT

bot = commands.Bot(command_prefix="m", intents=discord.Intents.all())

@dataclass
class MapNameData:
    """
    Stores the name of the maps
    """

    map_names = ["antarctic-peninsula",
                 "busan",
                 "ilios",
                 "lijang-tower",
                 "nepal",
                 "oasis",
                 "samoa",
                 "circuit-royale",
                 "dorado",
                 "havana",
                 "junkertown", 
                 "rialto",
                 "route-66",
                 "shambali-monastery",
                 "watchpoint-gibraltar",
                 "new-junk-city",
                 "suravasa",
                 "blizzard-world",
                 "eichenwalde",
                 "hollywood",
                 "kings-row",
                 "midtown",
                 "numbani",
                 "paraiso",
                 "colosseo",
                 "esperanca",
                 "new-queen-street",
                 "runasapi",
                 "hanaoka",
                 "throne-of-anubis"]
    

@dataclass
class BotData:
    """
    General Data the bot keeps tracks of in a session.
    """
    bot_commands = ["madd [map-name] [result]",
                    "mwinrate [map-map]",
                    "mlastwon [map-name]",
                    "mlastplayed [map_name]",
                    "mlast10",
                    "mbestmaps",
                    "mworstmaps",
                    "mmostplayed",
                    "mmaps"]   


# Instatiate data classes
bot_data = BotData()
map_name_data = MapNameData()

# Bot Behaviour

@bot.event
async def on_ready():
    """
    Functionaliy when bot activated
    """
    channel = bot.get_channel(CHANNEL_ID)
    msg = "\nMAP BOT RUNNING\n"
    instr = "\n Information on how to use the bot can be found by typing in 'mcmds'"
    
    await channel.send(msg)
    await channel.send(instr)


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
    connection = sqlite3.connect(DB_PATH)
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
async def lastwon(ctx, map_name: str):
    """
    Fetches the last time you won the specified map.
    """
    cur_time = time.time()
    curf_time = datetime.fromtimestamp(cur_time).strftime("%Y-%m-%d %H:%M:%S")
    cur_time_obj = datetime.strptime(curf_time, "%Y-%m-%d %H:%M:%S")

    query = f"SELECT MAX(timestamp) FROM maps WHERE map_name = '{map_name}' AND result = 1"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        last_time = cursor.fetchone()[0]

        if last_time != None:

            last_time_obj = datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
            elapsed_time = cur_time_obj - last_time_obj
            days = elapsed_time.days
            hours, remainder = divmod(elapsed_time.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            await ctx.send(f"Time Elapsed since last win on {map_name}: {days} days, {hours} hours, {minutes} minutes, {seconds} seconds")
        else:
            await ctx.send(f"No recorded win in database")

    except Exception as e:
        await ctx.send(f"An error occurred: {e}")
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

    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        won = cursor.fetchone()
        cursor.execute(query2)
        total = cursor.fetchone()
        wrate = round((int(won[0]) / int(total[0]) * 100), 2)
        await ctx.send(f"{wrate}% - {total[0]} maps played")
    except Exception as e:
        await ctx.send(f"An error occured: {e}")
    finally:
        connection.close()

@bot.command()
async def bestmaps(ctx):
    """
    Provides information on the maps with the best recordss
    """
    query = "SELECT map_name, COUNT(*) AS total_results," \
    "SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END) AS total_losses," \
    "SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS total_wins," \
    "SUM(CASE WHEN result = 2 THEN 1 ElSE 0 END) AS total_draws," \
    "(CAST(SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100) AS win_rate" \
    " FROM maps GROUP BY map_name ORDER BY win_rate DESC, total_wins DESC LIMIT 10"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()

        parsed_result = "=== BEST MAPS ===\n"
        
        for i in result:
            name = i[0].strip()
            winrate = round(i[5], 2)
            record = f"{i[3]}W {i[2]}L {i[4]}D"
            parsed_result += name + " - " + str(winrate)+"%" + " - " + record + "\n"

        await ctx.send(parsed_result)

    except Exception as e:
        await ctx.send(f"An error occurred: {e}")
    finally:
        connection.close()

@bot.command()
async def worstmaps(ctx):
    """
    Provides information on the maps with the worst records
    """
    query = "SELECT map_name, COUNT(*) AS total_results," \
    "SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END) AS total_losses," \
    "SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS total_wins," \
    "SUM(CASE WHEN result = 2 THEN 1 ElSE 0 END) AS total_draws," \
    "(CAST(SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100) AS win_rate" \
    " FROM maps GROUP BY map_name ORDER BY win_rate ASC, total_losses DESC LIMIT 10"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()

        parsed_result = "=== WORST MAPS ===\n"
        
        for i in result:
            name = i[0].strip()
            winrate = round(i[5], 2)
            record = f"{i[3]}W {i[2]}L {i[4]}D"
            parsed_result += name + " - " + str(winrate)+"%" + " - " + record + "\n"

        await ctx.send(parsed_result)

    except Exception as e:
        await ctx.send(f"An error occurred: {e}")
    finally:
        connection.close()


@bot.command()
async def maps(ctx):
    """
    Shows the possible maps to enter intot the database
    """
    await ctx.send("Trackable Maps")
    await ctx.send("\n".join(map_name_data.map_names))

@bot.command()
async def cmds(ctx):
    """
    Prints list of bots commands
    """
    await ctx.send("=== Commands ===") 
    await ctx.send("\n".join(bot_data.bot_commands))



@bot.command()
async def last10 (ctx): 
    """
    Last 10 Results
    """
    query = f"SELECT map_name, result, timestamp FROM maps ORDER BY timestamp DESC LIMIT 10"
    
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        msg = ""
        
        for i in result:
            
            line = ""
            if i[1] == 0:
                line += "❌ loss "
            elif i[1] == 1:
                line += "✅ win "
            elif i[1] == 2:
                line += "draw"

            line += i[0].strip()

            line += f" {i[2]}"
            msg += f"{line}\n"

        await ctx.send(msg)

    except Exception as e:
        await ctx.send(f"An error occurred: {e}")

    finally:
        connection.close()


@bot.command()
async def mostplayed(ctx):
    """
    Shows most played maps
    """
    query = f"SELECT map_name, count(*) FROM maps group by map_name order by count(*) DESC"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        msg = "=== Most Played Maps\n"

        for i in result:
            msg += f"{i[0]}: {i[1]} times\n"

        await ctx.send(msg)
    except Exception as e:
        await ctx.send(f"An error occurred: {e}")
    finally:
        connection.close()


@bot.command()
async def lastplayed(ctx, map_name):
    """
    Shows the last time we got the map
    """
    cur_time = time.time()
    curf_time = datetime.fromtimestamp(cur_time).strftime("%Y-%m-%d %H:%M:%S")
    cur_time_obj = datetime.strptime(curf_time, "%Y-%m-%d %H:%M:%S")

    query = f"SELECT MAX(timestamp) FROM maps WHERE map_name = '{map_name}'"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        last_time = cursor.fetchone()[0]

        if last_time != None:

            last_time_obj = datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
            elapsed_time = cur_time_obj - last_time_obj
            days = elapsed_time.days
            hours, remainder = divmod(elapsed_time.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            await ctx.send(f"Last played {map_name}: {days} days, {hours} hours, {minutes} minutes, {seconds} seconds ago")
        else:
            await ctx.send("Have not played this map.")

    except Exception as e:
        await ctx.send(f"An error occurred: {e}")
    finally:
        connection.close()


@bot.command()
@commands.is_owner()
async def stop(ctx):
    """
    Powers off the bot
    """
    await ctx.send("Bot is shutting down")
    await bot.close() # Close port to bot

bot.run(BOT_TOKEN)