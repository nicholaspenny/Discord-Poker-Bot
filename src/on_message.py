import asyncio
import datetime
import logging
import os
import re
import sys
from typing import Optional

import discord
import pandas as pd

from src import common
from src.connect import connect, query
from src import graph
from src import ledger_gemini
from src import query_presets

logger = logging.getLogger(__name__)

POKERNOW = 'https://www.pokernow.club/games/'
JUMP_URL_PREFIX = 'https://discord.com/channels/'

channels = common.channels
roles = common.roles
TABLES = common.TABLES
CHANNELS_TEMPLATE = common.CHANNELS_TEMPLATE
ROLES_TEMPLATE = common.ROLES_TEMPLATE

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


async def game_jump(message: discord.Message) -> Optional[discord.Message]:
    game_jump_message = None
    given_link = [word for word in message.content.split() if JUMP_URL_PREFIX in word]
    game_channel = message.guild.get_channel(channels[message.guild.id]['game'])
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


class OnMessageHandler:
    def __init__(self, shutdown_fn, prompt_fn, admin_fn, reset_sequences_fn, dump_fn):
        self.shutdown = shutdown_fn
        self.prompt = prompt_fn
        self.admin_message = admin_fn
        self.reset_sequences = reset_sequences_fn
        self.dump = dump_fn

    async def handle_admin(self, message: discord.Message):
        # currently admin is just logs, might add functionality later
        return

    async def handle_commands(self, message: discord.Message):
        # for future implementation of commands
        return

    async def handle_database(self, message: discord.Message):
        guild = message.guild
        txt = message.content.strip()
        if txt and txt.startswith('!'):
            words = txt[1:].split()
            if words:
                option = words[0].lower()
                arguments = words[1:]
                if option == 'reset':
                    await self.reset_sequences(guild)
                    return
                elif option == 'delete':
                    response = await self.prompt(
                        message,
                        "Enter: [table] [id] (e.g., 'players 115' or 'games 72'). You have 1 minute."
                    )
                    if response is None:
                        await message.channel.send('Too late!')
                        return

                    parts = response.split()
                    if len(parts) != 2:
                        await message.channel.send("Invalid format. Operation cancelled.")
                        return

                    table, id_str = parts
                    table = table.lower()
                    if table not in TABLES or not id_str.isdigit():
                        await message.channel.send("Invalid table or ID. Operation cancelled.")
                        return

                    id_value = int(id_str)
                    delete_query = f"""DELETE FROM {table} WHERE {TABLES[table]} = %s"""

                    try:
                        with connect() as connection:
                            query(connection, delete_query, id_value)
                    except Exception as err:
                        logger.exception('Error deleting database entry: %s', err)
                        await message.channel.send(f'An error occurred: {err}')
                    else:
                        await message.channel.send(f"Deleted {table} entry {id_value} successfully.")
                    await self.reset_sequences(guild)
                    return
                elif option == 'reassign':
                    response = await self.prompt(
                        message,
                        "Enter: [wrong_player_id] [correct_player_id] {correct_user_id} (e.g. '151 108 a1b2c3d4e5'). You have 1 minute."
                    )
                    if response is None:
                        await message.channel.send('Too late!')
                        return

                    parts = response.split()
                    if len(parts) not in [2, 3] or not parts[0].isdigit() or not parts[1].isdigit():
                        await message.channel.send("Invalid format. Operation cancelled.")
                        return

                    incorrect_player_id = int(parts[0])
                    correct_player_id = int(parts[1])
                    correct_user_id = parts[2] if len(parts) == 3 else None

                    user_id_text = ''
                    if correct_user_id:
                        user_id_text = f' ({correct_user_id} - updated)'
                    response = await self.prompt(
                        message,
                        f"Respond 'OK' to reassign player {incorrect_player_id} -> {correct_player_id}{user_id_text}"
                    )
                    if response is None or response.lower() != 'ok':
                        raise RuntimeError("User cancelled the operation — rolling back.")

                    try:
                        with connect() as connection:
                            if correct_user_id:
                                row = query(connection, "SELECT 1 FROM users WHERE user_id = %s", correct_user_id)
                                if not row:
                                    query(
                                        connection,
                                        "INSERT INTO users (user_id, player_id) VALUES (%s, %s)",
                                        correct_user_id, correct_player_id
                                    )
                                query(
                                    connection,
                                    "UPDATE ledgers SET user_id = %s WHERE user_id IN "
                                    "(SELECT user_id FROM users WHERE player_id = %s)",
                                    correct_user_id, incorrect_player_id
                                )
                                query(
                                    connection,
                                    "DELETE FROM users WHERE player_id = %s and user_id != %s",
                                    incorrect_player_id, correct_user_id
                                )
                            else:
                                query(
                                    connection,
                                    "UPDATE users SET player_id = %s WHERE player_id = %s",
                                    correct_player_id, incorrect_player_id
                                )
                            if correct_player_id != incorrect_player_id:
                                query(
                                    connection,
                                    "DELETE FROM players WHERE player_id = %s ",
                                    incorrect_player_id
                                )
                    except RuntimeError:
                        logger.info('Player Reassignment Cancelled')
                        await message.channel.send(f'Player Reassignment Cancelled')
                    except Exception as err:
                        logger.exception('Error reassigning database entry: %s', err)
                        await message.channel.send(f'An error occurred: {err}')
                    else:
                        await message.channel.send('Player Reassignment Successful.')
                    await self.reset_sequences(guild)
                    return
                elif option == 'table':
                    if not arguments:
                        await message.channel.send('!table requires 1 argument, the table name')
                        return

                    table = arguments[0].lower()
                    if table not in TABLES or not _IDENTIFIER_RE.fullmatch(table):
                        await message.channel.send(f'Table: {table} - does not exist')
                        return
                    safe_table = f'"{table}"'

                    try:
                        with connect() as connection:
                            ans, columns = query(connection,
                                                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                                                    table)
                            table_columns = [c[0].lower() for c in ans]
                            table_query = f"""Select * FROM {safe_table} ORDER BY """
                            if arguments[1:]:
                                for col in arguments[1:]:
                                    if col not in table_columns or not _IDENTIFIER_RE.fullmatch(col):
                                        await message.channel.send(f'Column: {col} - not in {table}')
                                        return
                                table_query += ', '.join(f'"{col}" DESC' for col in arguments[1:])
                            else:
                                table_query += f'"{TABLES[table]}" DESC'

                            table_query += ';'
                            ans, cols = query(connection, table_query)
                            answer = pd.DataFrame(ans, columns=cols)
                            answer.index += 1
                            with pd.option_context('display.min_rows', 25, 'display.max_rows', 25):
                                await message.channel.send(f'```{answer}```')
                    except Exception as err:
                        logger.exception('Unable to Connect to the Database: %s', err)
                        await message.channel.send('Unable to Connect to the Database')
                    return
                elif option == 'search':
                    if len(arguments) < 2:
                        await message.channel.send('Usage: !search <table> <value> [value2] [value3] ...')
                        return

                    table = arguments[0].lower()
                    values = arguments[1:]

                    if table not in TABLES or not _IDENTIFIER_RE.fullmatch(table):
                        await message.channel.send(f'Table: {table} - does not exist')
                        return
                    safe_table = f'"{table}"'

                    try:
                        with connect() as connection:
                            ans, columns = query(connection,
                                                 "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                                                 table)
                            table_columns = [c[0].lower() for c in ans]

                            conditions = []
                            params = []
                            for val in values:
                                col_checks = [f'CAST("{col}" AS TEXT) ILIKE %s' for col in table_columns]
                                conditions.append("(" + " OR ".join(col_checks) + ")")
                                params.extend([f"%{val}%"] * len(table_columns))

                            where_clause = " OR ".join(conditions)
                            search_query = f'SELECT * FROM {safe_table} WHERE {where_clause} ORDER BY {TABLES[table]} DESC;'

                            ans, cols = query(connection, search_query, *params)
                            if not ans:
                                await message.channel.send(f'No matches found for {", ".join(values)} in {table}')
                                return

                            answer = pd.DataFrame(ans, columns=cols)
                            answer.index += 1
                            with pd.option_context('display.min_rows', 25, 'display.max_rows', 25):
                                await message.channel.send(f'```{answer}```')
                    except Exception as err:
                        logger.exception('Search failed: %s', err)
                        await message.channel.send('Search failed')
                    return
            await message.channel.send('!delete, !reassign, !reset, !table, !search, more commands soon')
            return

    @staticmethod
    async def handle_email(message: discord.Message):
        guild = message.guild
        email_database_channel = guild.get_channel(channels[guild.id]['email-database'])
        # newest to oldest
        async for entry in email_database_channel.history():
            if message.author in entry.mentions:
                await entry.delete()
        await email_database_channel.send(f'<@{message.author.id}> {message.content}')
        return

    async def handle_email_database(self, message: discord.Message):
        guild = message.guild
        member = message.mentions[0]
        member_name = member.display_name
        member_id = member.id
        member_email = message.content.split()[1]

        email_needed_role = guild.get_role(roles[guild.id]['email needed'])
        try:
            await member.remove_roles(email_needed_role)
        except discord.Forbidden:
            logger.warning('Missing permissions to add role in %s', guild.name)
            await self.admin_message(guild, 'Missing permissions to remove roles')

        insert_player_query = """INSERT INTO players (name, discord_id, email) VALUES (%s, %s, %s)
                                 ON CONFLICT (discord_id) DO UPDATE SET email = EXCLUDED.email;"""
        try:
            with connect() as connection:
                query(connection, insert_player_query, member_name, member_id, member_email)
            self.dump()
        except Exception as err:
            logger.exception('Unable to Update Player Email: %s', err)
        return

    async def handle_game(self, message: discord.Message):
        if not POKERNOW in message.content:
            return
        guild = message.guild
        ping = f"<@&{roles[guild.id]['star']}>"
        link = [word for word in message.content.split() if POKERNOW in word][0]
        email = await self._get_email(message)
        if email:
            bot_link = await message.channel.send(f'{ping} {email}\n{link}')
            await bot_link.create_thread(name="Notes", auto_archive_duration=1440)
            await message.delete()
            return
        missing_email = f"Lobby creator must first register an email with the server.\n" \
                        f"Add one to <#{channels[guild.id]['email']}> " \
                        f"or contact an <@&{roles[guild.id]['admin']}> for access."
        await message.channel.send(missing_email)
        await message.delete()
        return

    @staticmethod
    async def handle_graph(message: discord.Message):
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

    async def handle_ledgers(self, message: discord.Message):
        if not message.attachments:
            return
        guild = message.guild
        game_jump_message = await game_jump(message)
        game_jump_url = 'Cannot find game'
        email_tag = ''
        if game_jump_message:
            game_jump_url = game_jump_message.jump_url
            email = await self._get_email(game_jump_message)
            email_tag = f' {email}' if email else ''
        attachments = message.attachments
        await message.channel.send(f'Ledger for: {game_jump_url}{email_tag}', file=await attachments[0].to_file())
        for screen_shot in attachments[1:]:
            await message.channel.send(file=await screen_shot.to_file())
        await message.delete()

        if game_jump_message:
            if message.channel.id == channels[guild.id]['ledgers-test']:
                if '!' not in message.content:
                    await message.channel.send('Not Inserting Ledger', delete_after=5)
                    return
                else:
                    await message.channel.send('Inserting Game/Ledger')

            matches = [word for word in game_jump_message.content.split() if POKERNOW in word]
            if not matches:
                logger.warning('No PokerNow URL found in message.')
                await self.admin_message('Ledger Not Inserted - Missing PokerNow URL')
                return
            url = matches[0].rpartition('/')[2]

            game_query = """INSERT INTO games (url, date) VALUES (%s, %s)
                            ON CONFLICT (url) DO NOTHING RETURNING game_id;"""
            game_id_query = """SELECT game_id, date FROM games WHERE url = %s;"""
            try:
                with connect() as connection:
                    new_insert, _ = query(connection, game_query, url, game_jump_message.created_at)
                    existing_game, _ = query(connection, game_id_query, url)
            except Exception as err:
                logger.warning('Unable to Insert Game: %s\nurl = %s', err, url)
                await self.admin_message(guild, 'Error Connecting with Database. Ledger(s) Skipped')
                return

            if existing_game and not new_insert:
                response = await self.prompt(
                    message,
                    "Existing games for the Ledger(s) exist. Reply 'Yes' to make the insertion(s)\nYou have 2 minutes.",
                    timeout=120,
                    admin=True
                )
                if response is None:
                    await self.admin_message(guild, 'too late')
                    return
                elif response.lower() not in ['yes', 'y']:
                    return
            if new_insert or existing_game:
                game_id = new_insert[0][0] if new_insert else existing_game[0][0]
                images_list = await attachments_to_bytes([attachments])
                results = []
                for sublist in images_list:
                    results.append(await asyncio.to_thread(ledger_gemini.gemini, sublist, game_id=game_id))
                await self._insert(guild, results, game_id)
                return
            else:
                logger.warning('Ledgers Skipped, Unexpected Error')
                await self.admin_message(guild, 'Ledgers Skipped, Unexpected Error')
        return

    @staticmethod
    async def handle_query(message: discord.Message):
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
                        logger.info('Unexpected #query Error: %s', txt)
                        await message.channel.send("Unexpected Error")
                    return
                elif option in ('leaderboard', 'leaderboard_avg'):
                    ans, columns = query_presets.leaderboard(arguments, option == 'leaderboard_avg')
                    if ans:
                        answer = pd.DataFrame(ans, columns=columns)
                        answer.index += 1
                        with pd.option_context('display.min_rows', 25, 'display.max_rows', 25):
                            await message.channel.send(f'```{answer}```')
                    else:
                        logger.info('Unexpected #query Error: %s', txt)
                        await message.channel.send("Unexpected Error")
                    return
                elif option == 'career':
                    if len(arguments) == 1:
                        ans, columns = query_presets.career(arguments[0])
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

    async def handle_manage(self, message: discord.Message):
        guild = message.guild
        txt = message.content.strip()
        if txt and txt.startswith('!'):
            words = txt[1:].split()
            if words:
                option = words[0].lower()
                arguments = words[1:]
                if option == 'restart':
                    await message.channel.send("Restarting bot...")
                    logger.info('Restarting bot...')
                    await self.shutdown()
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                elif option == 'setup':
                    guild_channels = [c for c in await guild.fetch_channels() if
                                      isinstance(c, discord.TextChannel) and c.name in CHANNELS_TEMPLATE]
                    guild_channel_names = [c.name for c in guild_channels]
                    for channel in CHANNELS_TEMPLATE:
                        if channel not in guild_channel_names:
                            new_channel = await guild.create_text_channel(name=channel, reason="Setup missing channel")
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

                    guild_roles = [r for r in await guild.fetch_roles() if r.name in ROLES_TEMPLATE]
                    guild_role_names = [r.name for r in guild_roles]

                    for role in ROLES_TEMPLATE:
                        if role not in guild_role_names:
                            new_role = await guild.create_role(name=role, reason="Setup missing role")
                            roles[guild.id][role] = new_role.id
                        else:
                            duplicate_roles = [d for d in guild_roles if d.name == role]
                            for r in duplicate_roles:
                                if not r.id == roles[guild.id][role]:
                                    await r.delete(reason=f"Removed Duplicate of @'{role}'")
                    return
                elif option == 'purge':
                    channel_mentions = message.channel_mentions
                    if len(channel_mentions) == 1:
                        channel = channel_mentions[0]
                        response = await self.prompt(
                            message,
                            f"Enter the number of messages you want to purge from {channel.name}.\nYou have 1 minute."
                        )
                        if response is None:
                            await message.channel.send('Too late!')
                            return
                        elif response.lower() in ['cancel', 'no', 'quit', 'exit', 'abandon']:
                            return
                        elif not response.isdigit() or int(response) < 1:
                            await message.channel.send('Enter a positive integer for the number of messages to purge.')
                            return
                        try:
                            await message.channel_mentions[0].purge(limit=int(response))
                        except discord.Forbidden:
                            logger.warning('Missing permissions to manage messages in %s', channel.name)
                            await message.channel.send(f'Missing permissions to manage messages in {channel.name}')
                        return
                    await message.channel.send('!purge required exactly 1 channel to be mentioned')
                    return
                elif option == 'add_games':
                    # message: !add_games MM DD YYYY
                    if len(arguments) > 1:
                        m = int(arguments[0])
                        d = int(arguments[1])
                        y = int(arguments[2])
                        logger.debug('Starting to Add Games to Database')
                        game_query = """INSERT INTO games (url, date) VALUES (%s, %s);"""
                        links = []
                        game_channel = guild.get_channel(channels[guild.id]['game'])
                        # oldest to newest
                        async for entry in game_channel.history(after=datetime.datetime(year=y, month=m, day=d)):
                            if POKERNOW in entry.content:
                                matches = [word for word in entry.content.split() if POKERNOW in word]
                                links.append([matches[0], entry.created_at.strftime('%m-%d-%y'), entry.created_at])
                        try:
                            with connect() as connection:
                                for item in links:
                                    # unique part of pokernow url
                                    query(connection, game_query, item[0].split()[-1].rpartition('/')[2], item[-1])
                        except Exception as err:
                            logger.warning('No Games Inserted: %s', err)
                        return
                    else:
                        await message.channel.send('!add_games MM DD YYYY')
                        return
                elif option == 'add_ledgers':
                    # message: !add_ledgers {game_id of 1st ledger} MM DD YYYY [MM DD YYY] <-- [optional end date]
                    if len(arguments) in (4, 7):
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
                        buffer = datetime.datetime.min
                        ledgers_channel = guild.get_channel(channels[guild.id]['ledgers'])
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
                        await self._insert(guild, results, game_id)
                        return
                    else:
                        await message.channel.send('!add_ledgers GID MM DD YYYY')
                        return
                else:
                    channel_pattern = r"^<#(\d+)>$"
                    message_pattern = fr"^{re.escape(JUMP_URL_PREFIX)}{guild.id}/(\d+)/(\d+)$"
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
                            old = await guild.get_channel(int(cid)).fetch_message(int(mid))
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
                await message.channel.send('!setup, !restart, !add_games, !add_ledgers, ![#channel], ![message link]')
                return
        return

    async def _get_email(self, message: Optional[discord.Message]) -> Optional[str]:
        if not message:
            return None
        email_matches = [word for word in message.content.split() if '@' in word and '<' not in word]
        if email_matches:
            return email_matches[0]

        email = None
        email_query = """SELECT email FROM players WHERE discord_id = %s;"""
        guild = message.guild
        try:
            with connect() as connection:
                rows, cols = query(connection, email_query, message.author.id)
            if rows:
                return rows[0][0]
            else:
                await self.admin_message(guild, f"{message.author.name} missing from database")
        except Exception as err:
            logger.exception('Failed to connect to database to fetch email: %s', err)
            await self.admin_message(guild, "Failed to connect to database")

        email_database_channel = guild.get_channel(channels[guild.id]['email-database'])
        # newest to oldest
        async for entry in email_database_channel.history():
            if message.author in entry.mentions:
                entry_email = [word for word in entry.content.split() if '@' in word and '<' not in word]
                if entry_email:
                    email = entry_email[0]
                break
        return email

    async def _insert(self, guild: discord.Guild, results: list[pd.DataFrame], game_id: int):
        ledgers_sum, new_users, errors, success = ledger_gemini.insert_ledgers(ledger_gemini.format_ledgers(results),
                                                                      game_id=game_id)
        if errors:
            error_text = "\n".join(errors[:5])
            if len(errors) > 5:
                error_text += f'\n... {len(errors)} errors'
            await self.admin_message(guild, f"⚠️ Ledger Insert Errors:\n{error_text}")

        if success:
            num_ledgers = sum(len(df) for df in results if not df.empty)
            logger.info('%s Ledger(s) Inserted', num_ledgers)
            await self.admin_message(guild, f"Inserted Ledger(s)")
        await self._after_insert(guild, ledgers_sum, new_users)

    async def _after_insert(self, guild: discord.Guild, ledgers_sum: int, new_users: list[str]) -> None:
        await self.reset_sequences(guild)
        self.dump()
        if ledgers_sum:
            await self.admin_message(guild, f'Unbalanced Ledgers Sum: {ledgers_sum}')
            logger.warning('Unbalanced Ledgers Sum: %s', ledgers_sum)
        if new_users:
            for user in new_users:
                await self.admin_message(guild,
                                         f"{user} has been created\n!reassign in <#{channels[guild.id]['database']}>")
        await self.reset_sequences(guild)