import asyncio
import datetime
import logging
import os
import re
import signal
import subprocess
import sys
from typing import Optional

import discord
from dotenv import load_dotenv
import pandas as pd

from src.config import config
from src.connect import connect, disconnect, query
from src import graph
from src import ledger_gemini
from src import query_presets

load_dotenv()

logger = logging.getLogger(__name__)

has_dumped = False
db_conf = config()
DATABASE_URL = (
    f"postgresql://{db_conf['user']}:{db_conf['password']}"
    f"@{db_conf['host']}:{db_conf['port']}/{db_conf['database']}"
)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)

POKERNOW = 'https://www.pokernow.club/games/'
JUMP_URL_PREFIX = 'https://discord.com/channels/'

channels: dict[int: dict[str, int]] = {}
channel_query = """SELECT channel_name, channel_id FROM channels;"""
roles: dict[int: dict[str, int]] = {}
role_query = """SELECT role_name, role_id FROM roles;"""

CHANNELS_TEMPLATE = {
    'admin', 'commands', 'email', 'email-database', 'game', 'game-test', 'graph',
    'graph-test', 'ledgers', 'ledgers-test', 'manage', 'music', 'query', 'query-test', 'roles'
}
ROLES_TEMPLATE = {'star', 'admin', 'poker bot', 'email needed'}


async def populate_dictionaries():
    global channels, roles
    try:
        logger.info(f'Servers: {[(guild.name, guild.id) for guild in client.guilds]}')
        for guild in client.guilds:
            guild_channels = [c for c in await guild.fetch_channels() if
                              isinstance(c, discord.TextChannel) and c.name in CHANNELS_TEMPLATE]
            channels[guild.id] = {c.name: c.id for c in sorted(guild_channels, key=lambda x: x.id)}
            guild_roles = [r for r in await guild.fetch_roles() if r.name in ROLES_TEMPLATE]
            roles[guild.id] = {r.name: r.id for r in sorted(guild_roles, key=lambda x: x.id)}
    except Exception as err:
        logger.warning('Using Default Dictionary Values: %s', err)
        # This is to default hard-code dictionaries in primary server for necessary channels/roles
        roles = {1:
                     {'star': 1,
                      'admin': 2,
                      'poker bot': 3,
                      'email needed': 4,
                      }
                 }
        channels = {1:
                        {'admin': 1,
                         'commands': 2,
                         'email': 3,
                         'email-database': 4,
                         'game': 5,
                         'game-test': 6,
                         'graph': 7,
                         'graph-test': 8,
                         'ledgers': 9,
                         'ledgers-test': 10,
                         'manage': 11,
                         'music': 12,
                         'query': 13,
                         'query-test': 14,
                         'roles': 15,
                         }
                    }
    finally:
        for guild_id, mapping in channels.items():
            for name in CHANNELS_TEMPLATE:
                mapping.setdefault(name, 0)
        for guild_id, mapping in roles.items():
            for name in ROLES_TEMPLATE:
                mapping.setdefault(name, 0)


async def attachments_to_bytes(attachments_list: list[list[discord.Attachment]]) -> list[list[tuple[bytes, str]]]:
    images = []
    try:
        for sublist in attachments_list:
            row = []
            for attachment in sublist:
                img_bytes = await attachment.read()
                img_type = attachment.content_type
                img_pair = (img_bytes, img_type)
                row.append(img_pair)
            images.append(row)
    except Exception as err:
        logger.exception('Error Converting Attachments to Images: %s', err)
        images = []

    return images


async def game_jump(message: discord.Message) -> Optional[discord.Message]:
    game_jump_message = None
    given_link = [word for word in message.content.split() if JUMP_URL_PREFIX in word]
    game_channel = client.get_channel(channels[message.guild.id]['game'])
    if given_link:
        # this is specifically fetching within #game
        try:
            msg = await game_channel.fetch_message(int(given_link[0].rpartition('/')[2]))
            if POKERNOW in msg.content:
                game_jump_message = msg
        except Exception as err:
            logger.warning('Message Not Found in #game: %s', err)
            return None
    else:
        async for entry in game_channel.history(limit=5):
            if POKERNOW in entry.content:
                game_jump_message = entry
                break
    return game_jump_message


def dump_database():
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_file = f"db/dump_{timestamp}.sql"
    try:
        subprocess.run([
            "pg_dump",
            "--format=plain",
            "--section=pre-data",
            "--section=data",
            "--section=post-data",
            "--blobs",
            "--no-owner",
            DATABASE_URL,
            "-f",
            dump_file
        ], check=True)
        logger.info(f"Database dumped to {dump_file}")
    except subprocess.CalledProcessError as e:
        logger.info(f"Database dump failed: {e}")


def dump_database_once():
    global has_dumped
    if not has_dumped:
        dump_database()
        has_dumped = True
    else:
        logger.info('Database has already been dumped')


async def shutdown_message():
    for guild in client.guilds:
        channel = client.get_channel(channels[guild.id]['admin'])
        if channel is not None:
            await channel.send("FindBot Offline - Shutting down...")
        else:
            logger.info("Channel not found in cache.")


async def shutdown():
    logger.info("Shutting down bot, dumping database if not already done...")
    dump_database_once()
    if not client.is_closed():
        try:
            await shutdown_message()
        except Exception as e:
            logger.warning(f"Failed to send shutdown message: {e}")

    await client.close()


def handle_signal(sig, _):
    # _ is a stand in for frame
    logger.info(f"Received signal {sig}, shutting down...")
    asyncio.get_event_loop().create_task(shutdown())


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


@client.event
async def on_ready():
    await populate_dictionaries()
    logger.info('%s is now running!', client.user)
    for guild in client.guilds:
        admin_id = channels.get(guild.id, {}).get("admin")
        if admin_id is not None:
            channel = client.get_channel(admin_id)
            if channel:
                await channel.send('Poker Bot Online! At Your Service!')


def update_guild_channel(
    new_channel: Optional[discord.abc.GuildChannel] = None,
    old_channel: Optional[discord.abc.GuildChannel] = None
):
    if old_channel and isinstance(old_channel, discord.TextChannel) and old_channel.name in CHANNELS_TEMPLATE:
        logger.info(f"Removing #{old_channel.name} in {old_channel.guild.name}")
        guild_channels = channels.setdefault(old_channel.guild.id, {})
        guild_channels[old_channel.name] = 0

    if new_channel and isinstance(new_channel, discord.TextChannel) and new_channel.name in CHANNELS_TEMPLATE:
        logger.info(f"Updating #{new_channel.name} in {new_channel.guild.name}")
        guild_channels = channels.setdefault(new_channel.guild.id, {})
        guild_channels[new_channel.name] = new_channel.id


@client.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    update_guild_channel(channel)


@client.event
async def on_guild_channel_update(before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
    if before.name != after.name:
        update_guild_channel(after, before)


@client.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    update_guild_channel(old_channel=channel)


def update_guild_role(
    new_role: Optional[discord.Role] = None,
    old_role: Optional[discord.Role] = None
):
    if old_role and old_role.name in ROLES_TEMPLATE:
        logger.info(f"Removing @{old_role.name} in {old_role.guild.name}")
        guild_roles = roles.setdefault(old_role.guild.id, {})
        guild_roles[old_role.name] = 0

    if new_role and new_role.name in ROLES_TEMPLATE:
        logger.info(f"Updating @{new_role.name} in {new_role.guild.name}")
        guild_roles = roles.setdefault(new_role.guild.id, {})
        guild_roles[new_role.name] = new_role.id


@client.event
async def on_guild_role_create(role: discord.Role):
    update_guild_role(role)


@client.event
async def on_guild_role_update(before: discord.Role, after: discord.Role):
    if before.name != after.name:
        update_guild_role(after, before)


@client.event
async def on_guild_role_delete(role: discord.Role):
    update_guild_role(old_role=role)


@client.event
async def on_guild_join(guild: discord.Guild):
    await populate_dictionaries()
    logger.info('%s Just Joined %s', client.user, guild.name)
    await client.get_channel(channels[guild.id]['admin']).send('Poker Bot Online! At Your Service!')


@client.event
async def on_message(message: discord.Message):
    global channels
    global roles

    guild = message.guild
    if message.channel.id == channels[guild.id]['email']:
        email_database_channel = client.get_channel(channels[guild.id]['email-database'])
        # newest to oldest
        async for entry in email_database_channel.history():
            if message.author in entry.mentions:
                await entry.delete()
        await email_database_channel.send(f'<@{message.author.id}> {message.content}')
        return
    elif message.channel.id == channels[guild.id]['manage']:
        if message.author == client.user:
            return
        txt = message.content.strip()
        if txt and txt.startswith('!'):
            words = txt[1:].split()
            if words:
                option = words[0].lower()
                arguments = words[1:]
                if option == 'restart':
                    await message.channel.send("Restarting bot...")
                    logger.info(f'Restarting bot...')
                    await shutdown()
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                elif option == 'setup':
                    guild_channels = [c for c in await guild.fetch_channels() if
                              isinstance(c, discord.TextChannel) and c.name in CHANNELS_TEMPLATE]
                    guild_channel_names = [c.name for c in guild_channels]
                    for channel in CHANNELS_TEMPLATE:
                        if channel not in guild_channel_names:
                            new_channel = await guild.create_text_channel(channel)
                            perms = new_channel.permissions_for(guild.me)
                            if not perms.read_messages:
                                await new_channel.set_permissions(guild.me, read_messages=True, send_messages=True)
                            channels[guild.id][channel] = new_channel.id
                        else:
                            duplicate_channels = [d for d in guild_channels if d.name == channel]
                            for c in duplicate_channels:
                                if c.id == channels[guild.id][channel]:
                                    perms = c.permissions_for(guild.me)
                                    if not perms.read_messages:
                                        await c.set_permissions(guild.me, read_messages=True, send_messages=True)
                                else:
                                    await c.delete(reason=f'Removed Duplicate of #{channel}')
                elif option == 'add_games':
                    # message: !add_games MM DD YYYY
                    if len(arguments) > 1:
                        m = int(arguments[0])
                        d = int(arguments[1])
                        y = int(arguments[2])
                        logger.debug('Starting to Add Games to Database')
                        game_query = """INSERT INTO games (url, date) VALUES (%s, %s);"""
                        links = []
                        game_channel = client.get_channel(channels[guild.id]['game'])
                        # oldest to newest
                        async for entry in game_channel.history(after=datetime.datetime(year=y, month=m, day=d)):
                            if POKERNOW in entry.content:
                                words_with_url = [word for word in entry.content.split() if POKERNOW in word]
                                links.append([words_with_url[0], entry.created_at.strftime('%m-%d-%y'), entry.created_at])
                        try:
                            with connect() as connection:
                                for item in links:
                                    # unique part of pokernow url
                                    query(connection, game_query, item[0].split()[-1].rpartition('/')[2], item[-1])
                            disconnect(connection)
                        except Exception as err:
                            logger.warning('No Games Inserted: %s', err)
                        return
                    else:
                        await message.channel.send('!add_games MM DD YYYY')
                        return
                elif option == 'add_ledgers':
                    # message: !add_ledgers {game_id of 1st ledger} MM DD YYYY [MM DD YYY] <-- [optional end date]
                    if len(arguments) in (4 , 7):
                        logger.debug('Starting to Add Ledgers to Database')
                        game_id = int(arguments[0])
                        m = int(arguments[1])
                        d = int(arguments[2])
                        y = int(arguments[3])
                        after = datetime.datetime(month=m, day=d, year=y)
                        before = (
                            datetime.datetime(month=int(arguments[4]), day=int(arguments[5]), year=int(arguments[6]))
                            if len(arguments) == 7
                            else datetime.datetime.now()
                        )
                        i = 0
                        attachments_list = []
                        buffer = None
                        ledgers_channel = client.get_channel(channels[guild.id]['ledgers'])
                        # oldest to newest
                        async for entry in ledgers_channel.history(after=after, before=before):
                            if entry.attachments:
                                if not attachments_list or entry.created_at - buffer >= datetime.timedelta(minutes=2):
                                    i += 1
                                    attachments_list.append(entry.attachments)
                                    buffer = entry.created_at
                                else:
                                    attachments_list[-1] = attachments_list[-1] + entry.attachments
                        images_list = await attachments_to_bytes(attachments_list=attachments_list)
                        results = []
                        for index, sublist in enumerate(images_list):
                            results.append(await asyncio.to_thread(ledger_gemini.gemini, sublist, game_id=game_id + index))
                        ledgers_sum = ledger_gemini.insert_ledgers(ledger_gemini.format_ledgers(results), game_id=game_id)
                        logger.info('%s Ledgers Inserted', len(images_list))
                        dump_database()
                        if ledgers_sum:
                            await message.channel.send(f'Unbalanced Ledgers Sum: {ledgers_sum}')
                            logger.warning('Unbalanced Ledgers Sum: %s', ledgers_sum)
                        return
                    else:
                        await message.channel.send('!add_ledgers GID MM DD YYYY')
                        return
                else:
                    channel_pattern = r"^<#(\d+)>$"
                    message_pattern = fr"^{re.escape(JUMP_URL_PREFIX)}{message.guild.id}/(\d+)/(\d+)$"
                    channel_match = re.match(channel_pattern, option)
                    message_match = re.match(message_pattern, option)
                    if channel_match:
                        # ! #channel [body] <-- [body is optional if attachments are included]
                        # group[1] == {channel_id where new message to be sent}
                        new_content = txt.split(channel_match.group())[1].strip()
                        channel = message.channel_mentions[0]
                        attachments = message.attachments
                        if not new_content and not attachments:
                            await message.channel.send('Compose Error: Missing message text/attachments')
                            return
                        elif not new_content:
                            await channel.send(file=await attachments[0].to_file())
                        elif attachments:
                            await channel.send(new_content, files=[await attachments[0].to_file()])
                        else:
                            await channel.send(new_content)
                            return

                        if attachments:
                            for file in attachments[1:]:
                                await channel.send(file=await file.to_file())
                        await message.channel.send(f'*Message Sent In: {channel.jump_url}')
                        return
                    elif message_match:
                        # ! {message_link} [body] <-- [optional body if attachments included]
                        # group[1] == {channel_id for the location of the message to be edited}
                        # group[2] == {message_id of the message to be edited}
                        # Edits with text, but no attachments will not alter existing attachments, same thing vice versa
                        cid, mid = message_match.group(1), message_match.group(2)
                        if cid.isdigit() and mid.isdigit():
                            new_content = txt.split(message_match.group())[1].strip()
                            attachments = message.attachments
                            old = await client.get_channel(int(cid)).fetch_message(int(mid))
                            if not new_content and not attachments:
                                await message.channel.send('Edit Error: Missing message text/attachments')
                                return
                            elif not new_content:
                                await old.edit(attachments=[await att.to_file() for att in attachments])
                                return
                            elif attachments:
                                await old.edit(content=new_content,
                                               attachments=[await att.to_file() for att in attachments])
                                return
                            else:
                                await old.edit(content=new_content)
                                return
            else:
                await message.channel.send('!add_games, !add_ledgers, ![#channel] <- compose, ![message link] <- edit')
                return
        return
    elif message.channel.id == channels[guild.id]['commands']:
        if message.author == client.user:
            return
        txt = message.content.strip()
        if txt and txt.startswith('!'):
            words = txt[1:].split()
            if words:
                option = words[0]
                arguments = words[1:]
                if option == 'table':
                    if arguments:
                        with connect() as connection:
                            table_query = f"""Select * from {arguments[0]}"""
                            ans, cols = query(connection, table_query)
                            answer = pd.DataFrame(ans, columns=cols)
                            answer.index += 1
                            with pd.option_context('display.min_rows', 25, 'display.max_rows', 25):
                                await message.channel.send(f'```{answer}```')
                        disconnect(connection)
                    else:
                        await message.channel.send('!table requires 1 argument, the table name')
                    return
            await message.channel.send('!table, more commands soon')
        return
    elif message.channel.id in (channels[guild.id]['query'], channels[guild.id]['query-test']):
        if message.author == client.user:
            return
        txt = message.content.strip()
        if txt and txt.startswith('!'):
            words = txt[1:].split()
            if words:
                option = words[0].lower()
                arguments = words[1:]
                if option == 'players':
                    ans, columns = query_presets.players()
                    if ans:
                        answer = pd.DataFrame(ans, columns=columns)
                        answer.index += 1
                        await message.channel.send(', '.join(answer['name'].to_list()))
                    else:
                        await message.channel.send("!WTF")
                    return
                elif option in ('leaderboard', 'leaderboard_avg'):
                    ans, columns = query_presets.leaderboard(txt.split()[1:], option == 'leaderboard_avg')
                    if ans:
                        answer = pd.DataFrame(ans, columns=columns)
                        answer.index += 1
                        with pd.option_context('display.min_rows', 25, 'display.max_rows', 25):
                            await message.channel.send(f'```{answer}```')
                    else:
                        await message.channel.send("!WTF")
                    return
                elif option == 'career':
                    if len(txt.split()) == 2:
                        ans, columns = query_presets.career(txt.split()[1])
                        if ans:
                            answer = pd.DataFrame(ans, columns=columns)
                            answer.index += 1
                            with pd.option_context('display.min_rows', 25, 'display.max_rows', 25):
                                await message.channel.send(f'```{answer}```')
                            return
                    await message.channel.send("!Include exactly 1 player name. !career name. !players.")
                    return
                elif option == 'graph':
                    career_graph = query_presets.career_graph(arguments)
                    if career_graph:
                        graph_file = discord.File(career_graph, filename='career_graph.png')
                        await message.channel.send(file=graph_file)
                    else:
                        await message.channel.send('Error or No Career Graph')
                    return
                elif option == 'recent':
                    days = 30
                    if arguments and arguments[0].isdigit():
                        days = arguments[0]
                        arguments = arguments[1:]
                    recent_graph = query_presets.recent_graph(days, arguments)
                    if recent_graph:
                        recent_file = discord.File(recent_graph, filename='recent_graph.png')
                        await message.channel.send(file=recent_file)
                    else:
                        await message.channel.send(f'No games in the last {days} days')
                    return
            await message.channel.send("!leaderboard, !leaderboard_avg, !career, !graph, !recent, !players")
        return
    elif message.channel.id in (channels[guild.id]['ledgers'], channels[guild.id]['ledgers-test']):
        if message.author == client.user:
            return
        elif not message.attachments:
            return

        game_jump_message = await game_jump(message)
        game_jump_url = game_jump_message.jump_url if game_jump_message else 'Cannot find game'
        attachments = message.attachments
        await message.channel.send(f'Ledger for: {game_jump_url}', file=await attachments[0].to_file())
        for screen_shot in attachments[1:]:
            await message.channel.send(file=await screen_shot.to_file())
        await message.delete()

        if game_jump_message:
            if message.channel.id == channels[guild.id]['ledgers-test']:
                if '!' in message.content:
                    await message.channel.send('Inserting Ledger')
                else:
                    await message.channel.send('Not Inserting Ledger', delete_after=5)
                    return
            words_with_url = [word for word in game_jump_message.content.split() if POKERNOW in word]
            url = words_with_url[0].rpartition('/')[2]
            game_query = """INSERT INTO games (url, date) VALUES (%s, %s)
                            ON CONFLICT (url) DO NOTHING RETURNING game_id;"""
            try:
                with connect() as connection:
                    ans, columns = query(connection, game_query, url, game_jump_message.created_at)
                disconnect(connection)
            except Exception as err:
                logger.warning('Unable to Insert Game: %s\nurl = %s', err, url)

            if ans:
                images_list = await attachments_to_bytes([attachments])
                results = []
                for sublist in images_list:
                    results.append(await asyncio.to_thread(ledger_gemini.gemini, sublist, game_id=ans[0][0]))
                ledgers_sum = ledger_gemini.insert_ledgers(ledger_gemini.format_ledgers(results), game_id=ans[0][0])
                logger.info('%s Ledger(s) Inserted', len(images_list))
                dump_database()
                if ledgers_sum:
                    await message.channel.send(f'Unbalanced Ledgers Sum: {ledgers_sum}')
                    logger.warning('Unbalanced Ledgers Sum: %s', ledgers_sum)
            else:
                logger.info('Ledgers Skipped, Game Already Exists')
        return
    elif message.channel.id in (channels[guild.id]['graph'], channels[guild.id]['graph-test']):
        if message.author == client.user:
            return
        attachments = message.attachments
        if len(attachments) == 2:
            file_names = (attachments[0].filename, attachments[1].filename)
            file_types = tuple(file.split('.')[-1].lower() if '.' in file else '' for file in file_names)

            if file_types == ('csv', 'csv'):
                attachment_one = await attachments[0].read()
                attachment_two = await attachments[1].read()

                game_jump_message = await game_jump(message)
                game_jump_url = game_jump_message.jump_url if game_jump_message else 'Cannot find game'

                # This does not enforce or check if the log and ledgers are truly corresponding
                nets_graph = graph.graph_setup(attachment_one, attachment_two)

                if nets_graph:
                    try:
                        # According to Official Documentation, the File object is only to be used once
                        nets_file = discord.File(nets_graph, filename='nets.png')
                        await message.channel.send(f'Nets for: {game_jump_url}', file=nets_file)

                        await message.delete()
                        return
                    except Exception as err:
                        logger.exception('Error Generating Plot: %s', err)
                        await message.channel.send(f'error generating plots')
                else:
                    await message.channel.send(f'Please attach the log and ledger .csv files for the session, '
                                               'include game link in message')
            await message.delete()
        elif len(attachments) == 1:
            await message.channel.send(f'Please attach both log and ledger .csv files for the session')
            await message.delete()
        return
    elif message.channel.id == channels[guild.id]['email-database']:
        member = message.mentions[0]
        member_name = member.display_name
        member_id = member.id
        member_email = message.content.split()[1]

        email_needed_role = guild.get_role(roles[guild.id]['email needed'])
        await member.remove_roles(email_needed_role)

        insert_player_query = """INSERT INTO players (name, discord_id, email) 
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT ON CONSTRAINT players_discord_id_key DO NOTHING;
                                        UPDATE players SET email = %s WHERE discord_id = %s;"""
        try:
            with connect() as connection:
                query(connection, insert_player_query, member_name ,member_id, member_email, member_email, member_id)
            disconnect(connection)
            dump_database()
        except Exception as err:
            logger.exception('Unable to Update Player Email: %s', err)
        return
    elif message.channel.id in (channels[guild.id]['game'], channels[guild.id]['game-test']):
        if message.author == client.user:
            return
        elif not POKERNOW in message.content:
            return
        ping = f"<@&{roles[guild.id]['star']}>"
        link = [word for word in message.content.split() if POKERNOW in word][0]
        email_database_channel = client.get_channel(channels[guild.id]['email-database'])
        email = None
        # newest to oldest
        async for entry in email_database_channel.history():
            if message.author in entry.mentions:
                if email := [word for word in entry.content.split() if '@' in word and '<' not in word]:
                    bot_link = await message.channel.send(f'{ping} {email[0]} \n{link}')
                    await bot_link.create_thread(name="Notes", auto_archive_duration=1440)
                    await message.delete()
                    return
                break
        missing_email = f"Lobby creator must first register an email with the server." \
                        f"\nAdd one to <#{channels[guild.id]['email']}> " \
                        f"or contact an <@&{roles[guild.id]['admin']}> for access."
        await message.channel.send(missing_email)
        await message.delete()
        return


def run_discord_bot():
    client.run(os.getenv('DISCORD_BOT_TOKEN'), log_handler=None)
