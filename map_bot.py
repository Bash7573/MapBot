import sqlite3
import discord
import datetime
import time
import os
import re

from dotenv import load_dotenv
from datetime import datetime
from discord.ext import commands
from dataclasses import dataclass

from sqlalchemy import create_engine, Column, Integer, Text, DateTime, String, CheckConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

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
    map_names = ["antarctic-peninsula","busan","ilios",
                 "lijang-tower","nepal","oasis",
                 "samoa","circuit-royale","dorado",
                 "havana","junkertown", "rialto",
                 "route-66","shambali-monastery","watchpoint-gibraltar",
                 "new-junk-city","suravasa","blizzard-world",
                 "eichenwalde","hollywood","kings-row","midtown",
                 "numbani","paraiso","colosseo",
                 "esperanca","new-queen-street","runasapi",]
    

@dataclass
class BotData:
    """
    General Data the bot keeps tracks of in a session.
    """
    bot_commands = ["madd map_name result --stack --season",
                    "mwinrate season map_name",
                    "mlastwon map_name",
                    "mlastplayed map_name",
                    "mlast10",
                    "mseasonbestmaps",
                    "mseasonworstmaps",
                    "mseasonbestmaps",
                    "mseasonworstmaps",
                    "mmostplayed",
                    "mmostplayedall",
                    "mmaps"]   


Base = declarative_base()

class MapReference(Base):
    __tablename__ = 'map_ref'

    id = Column(Integer, primary_key=True, autoincrement=True)
    map_name = Column(String, unique=True, nullable=False)
    map_type = Column(String, nullable=False)


class OwMapsTable(Base):
    __tablename__ = 'owmaps'

    # SCHEMA

    id = Column(Integer, primary_key=True, autoincrement=True)
    season = Column(Text, nullable=False)
    map_name = Column(Text, ForeignKey("map_ref.map_name"), nullable=False)
    map_result = Column(Text, nullable=False)
    stack = Column(Text, nullable=True)
    timestamp = Column(DateTime, nullable=False)
    added_by = Column(Text, nullable=False)

    # CONSTRAINTS
    __table_args__ = (
        CheckConstraint("season LIKE 's__' AND CAST(SUBSTR(season, 2, 2) AS INTEGER) BETWEEN 0 and 99", name = 'check_season_format'),
        CheckConstraint("map_name IN ('antarctic-peninsula','busan','ilios','lijang-tower',"
                        "'nepal','oasis','samoa','circuit-royale','dorado','havana','junkertown',"
                        "'rialto','route-66','shambali-monastery','watchpoint-gibraltar','new-junk-city',"
                        "'suravasa','blizzard-world','eichenwalde','hollywood','kings-row','midtown',"
                        "'numbani','paraiso','colosseo','esperanca','new-queen-street','runasapi')", 
                        name ='check_map_name'),

        CheckConstraint("map_result IN ('w', 'l', 'd')", name = 'check_map_result'),
        CheckConstraint("LENGTH(stack) <= 5", name="check_stack_length"),        
        )
        

engine = create_engine(f'sqlite:///{DB_PATH}')
Base.metadata.create_all(engine)

bot_data = BotData()
map_name_data = MapNameData()


# Populate ref table

connection = sqlite3.connect(DB_PATH)
cursor = connection.cursor()

cursor.execute("SELECT COUNT(*) FROM map_ref")
count = cursor.fetchone()[0]

if count == 0:
    maps = [("antarctic-peninsula", "control"), ("busan", "control"),
            ("ilios", "control"), ("lijang-tower", "control"),
            ("nepal", "control"), ("oasis", "control"),
            ("samoa", "control"), ("circuit-royale", "escort"),
            ("dorado", "escort"), ("havana", "escort"),
            ("junkertown", "escort"), ("rialto", "escort"),
            ("route-66", "escort"), ("shambali-monastery", "escort"),
            ("watchpoint-gibraltar", "escort"),("new-junk-city", "flashpoint"),
            ("suravasa", "flashpoint"), ("blizzard-world", "hybrid"),
            ("eichenwalde", "hybrid"), ("hollywood", "hybrid"),
            ("kings-row", "hybrid"), ("midtown", "hybrid"),
            ("numbani", "hybrid"), ("paraiso", "hybrid"),
            ("colosseo", "push"), ("esperanca", "push"),
            ("new-queen-street", "push"), ("runasapi", "push"),]

    cursor.executemany("INSERT INTO map_ref (map_name, map_type) VALUES (?, ?)", maps)
    connection.commit()
    print("✅ Map reference table populated!")
else:
    print("ℹ️ Table already populated, skipping insert.")

connection.close()

# Helper Functions

def validate_stack(stack):
    allowed_chars = 'LWDEJC'
    return all(char in allowed_chars for char in stack)


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
async def add(ctx, map_name: str, map_result: str, season='s16', stack=None):
    """
    """
    cur_timestamp = time.time()
    added_at = datetime.fromtimestamp(cur_timestamp).strftime("%Y-%m-%d %H:%M:%S")
    
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:

        if stack is not None:
            check_stack = validate_stack(stack)
            if stack is not None and not check_stack:
                raise ValueError("Invalid stack")
        
        cursor.execute("INSERT INTO owmaps (season, map_name, map_result, stack, timestamp, added_by) VALUES (?, ?, ?, ?, ?, ?)", (season, map_name, map_result, stack, added_at, ctx.author.name))
        connection.commit()
        await ctx.send(f"✅ Map '{map_name}' added successfully by {ctx.author.name} @ {added_at}")

    except ValueError as e:
        await ctx.send(f"⚠️ ValueError: {e} (Invalid value)")
    except sqlite3.IntegrityError as e:
        await ctx.send(f"⚠️ Integrity Error: {e} (Invalid value or constraint failed)")
    except sqlite3.OperationalError as e:
        await ctx.send(f"⚠️ Operational Error: {e} (SQL syntax or connection issue)")
    except sqlite3.DatabaseError as e:
        await ctx.send(f"⚠️ Database Error: {e}")
    except Exception as e:
        await ctx.send(f"⚠️ Unexpected Error: {e}")
    finally:
        connection.close()


@bot.command()
async def bestmaptype(ctx, season):
    """
    """
   
    query =  "SELECT " \
    "mr.map_type, " \
    "om.season, " \
    "COUNT(*) AS total_games, " \
    "SUM(CASE WHEN om.map_result = 'w' THEN 1 ELSE 0 END) AS total_wins, " \
    "SUM(CASE WHEN om.map_result = 'l' THEN 1 ELSE 0 END) AS total_losses, " \
    "SUM(CASE WHEN om.map_result = 'd' THEN 1 ELSE 0 END) AS total_draws, " \
    "(CAST(SUM(CASE WHEN om.map_result = 'w' THEN 1 ELSE 0 END) AS FLOAT) / " \
    "NULLIF(SUM(CASE WHEN om.map_result IN ('w', 'l', 'd') THEN 1 ELSE 0 END), 0) * 100) AS win_rate " \
    "FROM owmaps om " \
    "JOIN map_ref mr ON om.map_name = mr.map_name " \
    f"WHERE om.season IN ('{season}') " \
    "GROUP BY mr.map_type " \
    "ORDER BY win_rate DESC"


    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        result = cursor.fetchall()

        parsed_result = f"=== BEST MAP TYPES - {season} ===\n"
        
        for i in result:
            type = i[0].strip()
            winrate = round(i[6], 2)
            record = f"{i[3]}W {i[4]}L {i[5]}D"
            parsed_result += type + " - " + str(winrate)+"%" + " - " + record + "\n"

        await ctx.send(parsed_result)

    except Exception as e:
        await ctx.send(f"An error occurred: {e}")
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

    query = f"SELECT MAX(timestamp), season FROM owmaps WHERE map_name = '{map_name}' AND map_result = 'w'"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchone()
        last_time = result[0]
        last_season = result[1]

        if last_time != None:

            last_time_obj = datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
            elapsed_time = cur_time_obj - last_time_obj
            days = elapsed_time.days
            hours, remainder = divmod(elapsed_time.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            await ctx.send(f"You last won {map_name} in {last_season} which was: {days} days, {hours} hours, {minutes} minutes, {seconds} seconds ago")
        else:
            await ctx.send(f"No recorded win in database")

    except Exception as e:
        await ctx.send(f"An error occurred: {e}")
    finally:
        connection.close()


@bot.command()
async def personal_wr(ctx, name, season):
    """
    """
    query = f"SELECT COUNT (*) FROM owmaps WHERE stack LIKE '%{name}%' AND map_result = 'w' AND season = '{season}'"
    query2 = f"SELECT COUNT (*) FROM owmaps WHERE stack LIKE '%{name}%' AND season = '{season}'"

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
        await ctx.send(f"An error occurred: {e}")
    finally:
        connection.close()


@bot.command()
async def group_stats(ctx, season):


    group_memebers = {"W":"Will", "L":"Liam", "D":"Dan", "E":"Ewan", "C":"Chelsea", "J":"Justin"}
    res_string = "=== Season 16 Group Stats ===\n"

    gstats = []

    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    for name in group_memebers.keys():

        query = "SELECT COUNT(*) AS total_games, " \
        "(CAST(SUM(CASE WHEN map_result = 'w' THEN 1 ELSE 0 END) AS FLOAT) / " \
        "NULLIF(SUM(CASE WHEN map_result IN ('w', 'l', 'd') THEN 1 ELSE 0 END), 0) * 100) AS win_rate " \
        f"FROM owmaps WHERE stack LIKE '%{name}%' AND season = '{season}'"

        cursor.execute(query)
        stats = cursor.fetchall()
        gstats.append({"name": name, "games": stats[0][0], "win-rate": stats[0][1]})

    gstats.sort(key=lambda x: x["win-rate"])
    
    for i in range(len(gstats) - 1, -1, -1) :
        res_string += f"{group_memebers[gstats[i]["name"]]} - {gstats[i]["games"]} Games Played - {round(gstats[i]["win-rate"], 2)}% win rate\n"

    await ctx.send(res_string)


@bot.command()
async def winrate(ctx, season: str, map_name :str):
    """
    Fetches winrate for a given map
    returns as a percentage
    Arguments: 
        map_name: the name of the map
    """

    query = f"SELECT COUNT(*) FROM owmaps WHERE map_name = '{map_name}' AND map_result = 'w' AND season = '{season}'"
    query2 = f"SELECT COUNT(*) FROM owmaps WHERE map_name = '{map_name}' AND season = '{season}'"

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
async def seasonbestmaps(ctx, season):
    """
    Provides information on the maps with the best recordss
    """
    query = "SELECT map_name, COUNT(*) AS total_results," \
    "SUM(CASE WHEN map_result = 'l' THEN 1 ELSE 0 END) AS total_losses," \
    "SUM(CASE WHEN map_result = 'w' THEN 1 ELSE 0 END) AS total_wins," \
    "SUM(CASE WHEN map_result = 'd' THEN 1 ElSE 0 END) AS total_draws," \
    "(CAST(SUM(CASE WHEN map_result = 'w' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100) AS win_rate" \
    f" FROM owmaps WHERE season = '{season}' GROUP BY map_name ORDER BY win_rate DESC, total_wins DESC"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()

        parsed_result = f"=== BEST MAPS - {season} ===\n"
        
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
async def seasonworstmaps(ctx, season):
    """
    Provides information on the maps with the worst records
    """
    query = "SELECT map_name, COUNT(*) AS total_results," \
    "SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END) AS total_losses," \
    "SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS total_wins," \
    "SUM(CASE WHEN result = 2 THEN 1 ElSE 0 END) AS total_draws," \
    "(CAST(SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100) AS win_rate" \
    f"FROM owmaps WWHRE season = '{season}' GROUP BY map_name ORDER BY win_rate ASC, total_losses DESC"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()

        parsed_result = f"=== WORST MAPS - {season} ===\n"
        
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
async def bestmaps(ctx):
    """
    Provides information on the maps with the best recordss
    """
    query = "SELECT map_name, COUNT(*) AS total_results," \
    "SUM(CASE WHEN map_result = 'l' THEN 1 ELSE 0 END) AS total_losses," \
    "SUM(CASE WHEN map_result = 'w' THEN 1 ELSE 0 END) AS total_wins," \
    "SUM(CASE WHEN map_result = 'd' THEN 1 ElSE 0 END) AS total_draws," \
    "(CAST(SUM(CASE WHEN map_result = 'w' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100) AS win_rate" \
    f" FROM owmaps GROUP BY map_name ORDER BY win_rate DESC, total_wins DESC"
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
    " FROM maps GROUP BY map_name ORDER BY win_rate ASC, total_losses DESC"
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
async def last10 (ctx): 
    """
    Last 10 Results
    """
    query = f"SELECT map_name, map_result, timestamp FROM owmaps ORDER BY timestamp DESC LIMIT 10"
    
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        msg = ""
        
        for i in result:
            
            line = ""
            if i[1] == 'l':
                line += "❌ loss "
            elif i[1] == 'w':
                line += "✅ win "
            elif i[1] == 'd':
                line += "⛔ draw"

            line += i[0].strip()
            line += f" {i[2]}"
            msg += f"{line}\n"

        await ctx.send(msg)

    except Exception as e:
        await ctx.send(f"An error occurred: {e}")

    finally:
        connection.close()


@bot.command()
async def mostplayed(ctx, season):
    """
    Shows most played maps
    """
    query = f"SELECT map_name, COUNT(*) FROM owmaps WHERE season = '{season}' GROUP BY map_name ORDER BY COUNT(*) DESC"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        msg = f"=== Most Played Maps - {season}\n"

        for i in result:
            msg += f"{i[0]}: {i[1]} times\n"

        await ctx.send(msg)
    except Exception as e:
        await ctx.send(f"An error occurred: {e}")
    finally:
        connection.close()


@bot.command()
async def mostplayedall(ctx):
    """
    Shows most played maps
    """
    query = f"SELECT map_name, COUNT(*) FROM owmaps GROUP BY map_name ORDER BY COUNT(*) DESC"
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        msg = f"=== Most Played Maps - All Time\n"

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

    query = f"SELECT MAX(timestamp) FROM owmaps WHERE map_name = '{map_name}'"
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
@commands.is_owner()
async def stop(ctx):
    """
    Powers off the bot
    """
    await ctx.send("Bot is shutting down")
    await bot.close() # Close port to bot

bot.run(BOT_TOKEN)
