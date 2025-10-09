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

from src.ledger_download import scrape_ledger
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


def keyword_search(text: str, keyword: str) -> str:
    return next((word for word in text.split() if keyword in word), '')


def discord_markdown(lines: list[str], ratio=0.75, limit=2000, height=41) -> str:
    if not lines:
        return ""

    max_width = len(max(lines, key=len))
    max_rows = (limit - 6) // (max_width + 1)
    max_body = min(max_rows -1, height)  # includes all rows of data and divider if needed

    header = lines[:1]
    rows = lines[1:]
    if len(rows) > max_body:
        top_count = int(max_body * ratio)
        bottom_count = max_body - top_count - 1
        output = rows[:top_count] + ["." * max_width] + rows[-bottom_count:]
    else:
        output = rows
    content = "\n".join(header + output)

    return f"```{content}```"

async def game_jump(message: discord.Message) -> Optional[discord.Message]:
    game_jump_message = None
    given_game_jump = keyword_search(message.content, JUMP_URL_PREFIX)
    game_channel = message.guild.get_channel(channels[message.guild.id]['game'])
    if given_game_jump:
        # this is specifically fetching within #game
        try:
            msg = await game_channel.fetch_message(int(given_game_jump.rpartition('/')[2]))
            if POKERNOW in msg.content:
                game_jump_message = msg
        except Exception as err:
            logger.warning('Message Not Found in #game: %s', err)
            return None
    else:
        given_url = keyword_search(message.content, POKERNOW).strip(",.;:!()[]/")
        game_url = given_url if given_url else POKERNOW
        async for entry in game_channel.history(limit=10):
            if game_url in entry.content:
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
    def __init__(self, shutdown_fn, prompt_fn, admin_fn, game_email_fn, reset_sequences_fn, dump_fn):
        self.shutdown = shutdown_fn
        self.prompt = prompt_fn
        self.admin_message = admin_fn
        self.game_email = game_email_fn
        self.reset_sequences = reset_sequences_fn
        self.dump = dump_fn

    async def handle_admin(self, message: discord.Message):
        # currently admin is just logs, might add functionality later
        return

    async def handle_invites(self, message: discord.Message):
        # for future implementation of invites
        return

    async def handle_database(self, message: discord.Message):
        guild = message.guild
        channel = message.channel
        txt = message.content.strip()
        if txt and txt.startswith('!'):
            words = txt[1:].split()
            if words:
                option = words[0].lower()
                arguments = words[1:]
                #admin_ping = f"<@&{roles[guild.id]['admin']}>"
                if option == 'sequences':
                    await channel.send("\n".join(seq for seq in await self.reset_sequences(guild)))
                    return
                elif option == 'rename':
                    if arguments:
                        parts = arguments
                    else:
                        response = await self.prompt(
                            channel,
                            message.author,
                            ("@Admin: Enter: [old_player_id/user_id] [new_player_id/name*/user_id] | *1 Word Name.\n"
                             "You have 1 minute."),
                            admin=True
                        )
                        if response is None:
                            logger.info('Player Rename Was Not Given Arguments')
                            return
                        parts = response.split()

                    if len(parts) != 2:
                        await channel.send("Invalid input. Operation cancelled.")
                        return

                    if parts[0].isdigit():
                        table = 'players'
                        old = int(parts[0])
                        if parts[1].isdigit():
                            change = 'player_id'
                            new = int(parts[1])
                        elif parts[1].isalpha() and len(parts[1]) <= 10:
                            change = 'name'
                            new = parts[1]
                        else:
                            await channel.send("Name Can Be Up to 10 letters, No Special Chars. Operation Cancelled.")
                            return
                    elif re.fullmatch(r"[A-Za-z0-9\-_]{10}", parts[1]):
                        table = 'users'
                        old = parts[0]
                        change = 'user_id'
                        new = parts[1]
                    else:
                        await channel.send("Requires valid 10 character user_id. A-Za-z0-9-_. Operation Cancelled.")
                        return

                    try:
                        with connect() as conn:
                            old_name = None
                            existing_user_id = False

                            if table == 'users':
                                a, _ = query(conn, f"SELECT player_id FROM {table} WHERE {change} = %s", old)
                                if not a:
                                    await channel.send(f"Error. Could not find user with user_id - {old}")
                                    return
                                player_id = a[0][0]
                            else:
                                a, _ = query(conn, "SELECT name FROM players WHERE player_id = %s", old)
                                if len(a) != 1:
                                    logger.warning("Missing or multiple players with player_id %s", old)
                                    await channel.send(f"Error. Unable to Identify player id - {old}")
                                    return
                                player_id = old
                                old_name = a[0][0]

                            if change != 'player_id':
                                a, _ = query(conn, f"SELECT player_id FROM {table} WHERE {change} ILIKE %s", new)
                                if a:
                                    for row in a:
                                        if row[0] != player_id:
                                            await channel.send(f"Another player with this name/id exists - {a[0][0]}")
                                            return
                                        else:
                                            existing_user_id = True

                            if table == 'players':
                                summary = f"{player_id}: {old_name} -> "
                                if change == 'name':
                                    summary += f"{player_id}: {new}"
                                else:
                                    summary += f"{new}: {old_name}"
                            else:
                                summary = f"{player_id}: {old} -> {player_id}: {new}"

                            confirmation = await self.prompt(
                                channel,
                                message.author,
                                (f"@Admin: Reply 'OK' to Complete Adjustment | {summary}\n"
                                 f"You have 1 minute."),
                                admin=True
                            )
                            if confirmation is None or confirmation.lower() != 'ok':
                                logger.info('Player Rename Was Not Confirmed')
                                return

                            if table == 'users':
                                if existing_user_id:
                                    query(conn, "UPDATE ledgers SET user_id = %s WHERE user_id ILIKE %s", new, old)
                                    query(conn, "DELETE FROM users WHERE user_id ILIKE %s AND user_id != %s", old, new)
                                else:
                                    query(conn, f"UPDATE {table} SET {change} = %s WHERE user_id ILIKE %s", new, old)
                            else:
                                query(conn, f"UPDATE {table} SET {change} = %s WHERE player_id = %s", new, old)
                    except Exception as err:
                        logger.exception("Error During Rename/ReID | %s, %s, %s, %s: %s", table, change, old, new, err)
                        await channel.send("Error During Rename/ReID. Operation Canceled.")
                        return
                    else:
                        logger.info("Renamed/ReID Completed | %s", summary)
                        await channel.send(f"Rename/ReID Complete | {summary}")
                        return
                    finally:
                        await self.reset_sequences(guild)
                elif option == 'delete': # ledgers, games, players, not users (can use !rename, or edit here to allow)
                    if arguments:
                        parts = arguments
                    else:
                        response = await self.prompt(
                            channel,
                            message.author,
                            ("@Admin: Enter: [table] [id] (e.g., 'players 115' or 'games 72'). May delete multiple rows.\n"
                             "You have 1 minute."),
                            admin=True
                        )
                        if response is None:
                            logger.info('Entry Delete Was Not Given Arguments')
                            return
                        parts = response.split()

                    if len(parts) != 2:
                        await channel.send("Invalid format. Operation cancelled.")
                        return

                    table, id_str = parts
                    table = table.lower()
                    if table not in TABLES or not id_str.isdigit():
                        await channel.send("Invalid table or ID. Operation cancelled.")
                        return

                    try:
                        safe_table = f'"{table}"'
                        lookup = f'"{TABLES[table]["lookup_key"]}"'
                        key_id = int(id_str)
                        with connect() as conn:
                            ans, _ = query(conn, f"""SELECT * FROM {safe_table} WHERE {lookup} = %s""", key_id)

                            if not ans:
                                await channel.send("No Matching Entries Found. Operation cancelled.")
                                return


                            summary = '\n'.join(str(row) for row in ans)
                            confirmation = await self.prompt(
                                channel,
                                message.author,
                                (f"@Admin: Reply 'OK' to Delete {len(ans)} Rows. You have 1 minute:\n"
                                 f"{summary}"),
                                admin=True
                            )

                            if confirmation is None or confirmation.lower() != 'ok':
                                logger.info('Entry Deletion Was Not Confirmed')
                                return

                            delete_query = f"""DELETE FROM {safe_table} WHERE {lookup} = %s RETURNING *"""

                            deleted, _ = query(conn, delete_query, key_id)

                            if set(ans) != set(deleted): #requires hashable columns, database cannot include arrays etc.
                                await channel.send(f"Mismatch! Deleted rows don't match intended rows")
                                raise ValueError(f"Mismatch! Deleted rows don't match intended rows.\n{ans}\n{deleted}")
                    except ValueError as err:
                        logger.exception('Error! Mismatch in Confirmed and Deleted Rows: %s', err)
                        await channel.send(f"Error: Mismatch in Confirmed and Deleted Rows - {err}")
                        return
                    except Exception as err:
                        logger.exception('Error Deleting Database Entry(s): %s', err)
                        await channel.send(f"Unable to Delete Database Entry(s) - {err}")
                        return
                    else:
                        await channel.send(f"Deleted {len(ans)} Rows From {table} successfully.")
                        return
                    finally:
                        await self.reset_sequences(guild)
                elif option == 'merge':
                    if arguments:
                        parts = arguments
                    else:
                        response = await self.prompt(
                            channel,
                            message.author,
                            ("@Admin: Enter: [old_player_id] [new_player_id] {new_user_id} (e.g. '151 108 a1b2c3d4e5')\n"
                             "You have 1 minute."),
                            admin=True
                        )
                        if response is None:
                            logger.info('Player Merge Was Not Given Arguments')
                            return
                        parts = response.split()

                    if len(parts) not in [2, 3] or not parts[0].isdigit() or not parts[1].isdigit():
                        await channel.send("Invalid format. Operation cancelled.")
                        return

                    old_player_id = int(parts[0])
                    new_player_id = int(parts[1])
                    new_user_id = parts[2] if len(parts) == 3 else None
                    if old_player_id == new_player_id:
                        await channel.send(f"Invalid. Cannot Merge to the same ID: {old_player_id}")
                        return

                    try:
                        with connect() as conn:
                            ans, _ = query(
                                conn,
                                "SELECT name, email, discord_id FROM players WHERE player_id = %s",
                                old_player_id
                            )
                            if len(ans) != 1:
                                await channel.send(f"Old player_id {old_player_id} not found. Operation cancelled.")
                                return
                            old_name, old_email, old_discord = ans[0]

                            ans2, _ = query(
                                conn,
                                "SELECT name, email, discord_id FROM players WHERE player_id = %s",
                                new_player_id
                            )
                            if len(ans2) != 1:
                                await channel.send(f"New player_id {new_player_id} not found. Operation cancelled.")
                                await channel.send("Use !rename to adjust the player_id instead of !merge")
                                return
                            new_name, new_email, new_discord = ans2[0]

                            summary = f"{old_player_id}: {old_name} -> {new_player_id} - {new_name}"
                            if new_user_id:
                                summary += f" | {new_user_id} - updated"

                            fields = [("email", old_email, new_email), ("discord_id", old_discord, new_discord)]
                            resolved = {}
                            for field, old, new in fields:
                                if old and new and old != new:
                                    display_old = f"<@{old}>" if field == "discord_id" else old
                                    display_new = f"<@{new}>" if field == "discord_id" else new

                                    selection = await self.prompt(
                                        channel,
                                        message.author,
                                        (f"Conflict detected for {field}:\n"
                                         f"A) Old ({old_player_id}): {display_old}\n"
                                         f"B) New ({new_player_id}): {display_new}\n"
                                         "Type 'a' to keep old, 'b' to keep new."),
                                        admin=True
                                    )
                                    if not selection:
                                        logger.info('Player %s Merge Was Not Selected', field)
                                        return
                                    elif selection.lower() not in {'a', 'b'}:
                                        logger.info('Player %s Merge Selection Invalid', field)
                                        await channel.send("Invalid Selection. Operation Canceled")
                                        return
                                    if selection.lower() == 'a':
                                        resolved[field] = old
                                        summary += f" | {display_old}"
                                    else:
                                        resolved[field] = new
                                        summary += f" | {display_new}"
                                else:
                                    resolved[field] = old or new

                            confirmation = await self.prompt(
                                channel,
                                message.author,
                                (f"@Admin: Reply 'OK' to Merge Player | {summary} | OLD PLAYER WILL BE REMOVED\n"
                                 f"You have 1 minute."),
                                admin=True
                            )
                            if confirmation is None or confirmation.lower() != 'ok':
                                logger.info('Player Merge Was Not Confirmed')
                                return

                            if new_user_id:
                                query(
                                    conn,
                                    "INSERT INTO users (user_id, player_id) VALUES (%s, %s) "
                                    "ON CONFLICT (user_id) DO UPDATE SET player_id = EXCLUDED.player_id",
                                    new_user_id, new_player_id
                                )
                                query(
                                    conn,
                                    "UPDATE ledgers SET user_id = %s WHERE user_id IN "
                                    "(SELECT user_id FROM users WHERE player_id = %s)",
                                    new_user_id, old_player_id
                                )
                                query(
                                    conn,
                                    "DELETE FROM users WHERE player_id = %s AND user_id != %s",
                                    old_player_id, new_user_id
                                )
                            else:
                                query(
                                    conn,
                                    "UPDATE users SET player_id = %s WHERE player_id = %s",
                                    new_player_id, old_player_id
                                )

                            query(conn, "DELETE FROM players WHERE player_id = %s", old_player_id)
                            query(
                                conn,
                                "UPDATE players SET email = %s, discord_id = %s WHERE player_id = %s",
                                resolved['email'], resolved['discord_id'], new_player_id
                            )
                    except Exception as err:
                        logger.exception('Error Merging Database Entry: %s', err)
                        await channel.send(f"Unable to Complete Player Merge - {err}")
                        return
                    else:
                        logger.info("Player Merge Successful | %s", summary)
                        await channel.send(f"Player Merge Successful | {summary}")
                        return
                    finally:
                        await self.reset_sequences(guild)
                elif option == 'table':
                    if not arguments:
                        await channel.send("!table requires 1 argument, the table name")
                        return

                    table = arguments[0].lower()
                    if table not in TABLES or not _IDENTIFIER_RE.fullmatch(table):
                        await channel.send(f'Table: {table} - does not exist')
                        return

                    try:
                        safe_table = f'"{table}"'
                        lookup = f'"{TABLES[table]["lookup_key"]}"'
                        with connect() as conn:
                            ans, columns = query(
                                conn,
                                "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                                table
                            )
                            table_columns = [c[0].lower() for c in ans]
                            table_query = f"""Select * FROM {safe_table} ORDER BY """
                            if arguments[1:]:
                                for col in arguments[1:]:
                                    if col not in table_columns or not _IDENTIFIER_RE.fullmatch(col):
                                        await channel.send(f'Column: {col} - not in {table}')
                                        return
                                table_query += ', '.join(f'"{col}" DESC' for col in arguments[1:])
                            else:
                                table_query += f'{lookup} DESC'

                            ans, cols = query(conn, table_query)
                            answer = pd.DataFrame(ans, columns=cols)
                            answer.index += 1

                            table = discord_markdown(answer.to_string().splitlines())
                            await channel.send(f"{table}")
                    except Exception as err:
                        logger.exception('Unable to Connect to the Database: %s', err)
                        await channel.send(f"Unable to Connect to the Database - {err}")
                    return
                elif option == 'search':
                    if len(arguments) < 1:
                        await channel.send("Usage: !search <table> <value> [value2] [value3] ...")
                        return

                    table = arguments[0].lower()
                    values = arguments[1:]
                    if table not in TABLES or not _IDENTIFIER_RE.fullmatch(table):
                        await channel.send(f'Table: {table} - does not exist')
                        return

                    safe_table = f'"{table}"'
                    lookup = f'"{TABLES[table]["lookup_key"]}"'
                    sort = TABLES[table].get('sort_by') or ''
                    ordering = f'ORDER BY {lookup} DESC'
                    if sort:
                        ordering += f", {sort}"

                    try:
                        with connect() as conn:
                            ans, _ = query(
                                conn,
                                "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                                table
                            )
                            table_columns = [c[0].lower() for c in ans]

                            equality_filters = []
                            equality_params = []
                            inequality_filters = []
                            inequality_params = []
                            string_filters = [f'CAST("{col}" AS TEXT) ILIKE ANY(%s)' for col in table_columns]
                            string_words = []

                            for val in values:
                                comp_match = re.match(r'^(>=|<=|>|<|!=|=)(-?\d+)$', val)
                                if comp_match:
                                    operator = comp_match.group(1)
                                    number = int(comp_match.group(2))
                                    if operator == '=':
                                        equality_filters.append(f'{lookup} = %s')
                                        equality_params.append(number)
                                    else:
                                        inequality_filters.append(f'{lookup} {operator} %s')
                                        inequality_params.append(number)
                                else:
                                    string_words.append(val)
                            string_params = [[f"%{v}%" for v in string_words] for _ in table_columns] if string_words else []
                            query_params = equality_params + inequality_params + string_params

                            equality_clause = "(" + " OR ".join(equality_filters) + ")" if equality_params else ""
                            inequality_clause = "(" + " AND ".join(inequality_filters) + ")" if inequality_params else ""
                            string_clause = " OR ".join(string_filters) if string_params else ""

                            numeric_clause = " OR ".join((c for c in (equality_clause, inequality_clause) if c))
                            where_clause = " AND ".join((f'({c})' for c in (numeric_clause, string_clause) if c)) or "TRUE"

                            search_query = f"SELECT * FROM {safe_table} WHERE {where_clause} {ordering}"
                            ans2, cols = query(conn, search_query, *query_params)
                            if not ans2:
                                await channel.send(f'No matches found in {table}')
                                return

                            answer = pd.DataFrame(ans2, columns=cols)
                            answer.index += 1

                            table = discord_markdown(answer.to_string().splitlines())
                            await channel.send(f"{table}")
                    except Exception as err:
                        logger.exception('Unable to Complete the Search: %s', err)
                        await channel.send(f"Unable to Complete the Search - {err}")
                    return

            await channel.send("!delete, !merge, !sequences, !table, !search, more commands soon")
            return

    @staticmethod
    async def handle_email(message: discord.Message):
        guild = message.guild
        email_database_channel = guild.get_channel(channels[guild.id]['email-database'])
        # newest to oldest
        async for entry in email_database_channel.history():
            if message.author in entry.mentions:
                await entry.delete()
        await email_database_channel.send(f'{message.author.mention} {message.content}')
        return

    async def handle_email_database(self, message: discord.Message):
        guild = message.guild
        member = message.mentions[0]
        member_name = member.name.replace(' ', '')[:10] # uses username, might change to use after.display_name
        member_id = member.id
        member_email = message.content.split()[1].strip(",.;:!?()")

        email_needed_role = guild.get_role(roles[guild.id]['email needed'])
        try:
            await member.remove_roles(email_needed_role)
        except discord.Forbidden:
            logger.warning('Missing permissions to add role in %s', guild.name)
            await self.admin_message(guild, 'Missing permissions to remove roles')

        insert_player_query = """INSERT INTO players (name, discord_id, email) VALUES (%s, %s, %s)
                                 ON CONFLICT (discord_id) DO UPDATE SET email = EXCLUDED.email"""
        try:
            with connect() as conn:
                query(conn, insert_player_query, member_name, member_id, member_email)
            self.dump()
        except Exception as err:
            logger.exception('Unable to Update Player Email: %s', err)
        return

    async def handle_game(self, message: discord.Message):
        if not POKERNOW in message.content:
            return

        guild = message.guild
        channel = message.channel
        ping = f"<@&{roles[guild.id]['poker']}>"
        url = keyword_search(message.content, POKERNOW).strip(",.;:!()[]/")
        email = await self.game_email(message)
        if email:
            game_message = await channel.send(f'{ping} {email}\n{url}')
            await game_message.create_thread(name="Notes", auto_archive_duration=1440)
            await message.delete()
            return
        missing_email_msg = (f"Lobby creator must be a <@&{roles[guild.id]['game runner']}> with an email configured.\n"
                             f"Add one to <#{channels[guild.id]['email']}> "
                             f"or contact an <@&{roles[guild.id]['admin']}> for roles/access.")
        await channel.send(missing_email_msg)
        await message.delete()
        return

    @staticmethod
    async def handle_graph(message: discord.Message):
        channel = message.channel
        attachments = message.attachments
        if len(attachments) == 2:
            file_names = (attachments[0].filename, attachments[1].filename)
            file_types = tuple(file.split('.')[-1].lower() if '.' in file else '' for file in file_names)

            if all(file_type in ('csv', 'json') for file_type in file_types):
                attachment_one = await attachments[0].read()
                attachment_two = await attachments[1].read()

                game_jump_message = await game_jump(message)
                game_jump_url = game_jump_message.jump_url if game_jump_message else 'Cannot Find Game'

                # This does not enforce or check if the log and ledgers are truly corresponding
                nets_graph = graph.graph_setup(attachment_one, attachment_two)

                if nets_graph:
                    try:
                        # According to Official Documentation, the File object is only to be used once
                        nets_file = discord.File(nets_graph, filename='nets.png')
                        await channel.send(f'Nets for: {game_jump_url}', file=nets_file)

                        await message.delete()
                        return
                    except Exception as err:
                        logger.exception('Error Generating Plot: %s', err)
                        await channel.send(f'error generating plots')
                else:
                    await channel.send(f'Please attach the log and ledger .csv/json files for the session, '
                                               'include game link in message')
            else:
                await channel.send(f'Please attach the log and ledger .csv/json files for the session, ')
            await message.delete()
        elif len(attachments) == 1:
            await channel.send(f'Please attach both log and ledger .csv/json files for the session')
            await message.delete()
        return

    async def handle_ledgers(self, message: discord.Message):
        if not message.attachments:
            return
        guild = message.guild
        channel = message.channel

        game_jump_message = await game_jump(message)
        url = keyword_search(message.content, POKERNOW).strip(",.;:!()[]/")
        force_csv = bool(url)

        if game_jump_message:
            game_jump_url = game_jump_message.jump_url
            game_date = game_jump_message.created_at
            email = await self.game_email(game_jump_message)
            email_tag = f" {email}" if email else f" <@&{roles[guild.id]['admin']}>"
            url = keyword_search(game_jump_message.content, POKERNOW).strip(",.;:!()[]/")
        else:
            game_jump_url = url if url else 'Cannot Find Game'
            game_date = message.created_at
            email_tag = f" <@&{roles[guild.id]['admin']}>"

        attachments = message.attachments
        msg = await channel.send(f"Ledger for: {game_jump_url}{email_tag}", file=await attachments[0].to_file())
        for screen_shot in attachments[1:]:
            await channel.send(file=await screen_shot.to_file())
        await message.delete()

        if not url:
            logger.warning('No PokerNow URL Found in Message. No Ledgers Will be Inserted')
            await self.admin_message(guild, 'Ledgers Were Not Inserted - No PokerNow URL Found')
            return

        if channel.id == channels[guild.id]['ledgers-test']:
            if '!' not in message.content:
                await channel.send("Not Inserting Ledger", delete_after=5)
                return
            await channel.send("Inserting Game/Ledger")

        url_id = url.rpartition('/')[2]
        if not url_id:
            logger.warning('Mismatch in PokerNow URL found in message. %s', url)
            await self.admin_message(guild, f"Ledger Not Inserted - Mismatched PokerNow URL {url}")
            return

        game_query = """INSERT INTO games (url, date) VALUES (%s, %s)
                        ON CONFLICT (url) DO NOTHING RETURNING game_id"""
        game_id_query = "SELECT game_id, date FROM games WHERE url = %s"
        try:
            with connect() as conn:
                new_insert, _ = query(conn, game_query, url_id, game_date)
                if not new_insert:
                    existing_game, _ = query(conn, game_id_query, url_id)
                game_id = new_insert[0][0] if new_insert else existing_game[0][0]
                inserted = bool(new_insert)
        except Exception as err:
            logger.warning('Unable to Insert/Find Game - %s:\n %s', url_id, err)
            await self.admin_message(guild, 'Error Connecting with Database. Ledger(s) Skipped')
            return

        images_list = await attachments_to_bytes([attachments])
        images = images_list[0] if images_list else []

        scrape_working = True
        gemini_df = await asyncio.to_thread(ledger_gemini.gemini, images, game_id=game_id)
        try:
            csv_df = await scrape_ledger(url)
        except Exception as err:
            scrape_working = False
            csv_df = pd.DataFrame()
            logger.exception("Unable to Scrape Ledger: %s", err)
            await self.admin_message(guild, "Unable to Scrape Ledger, Playwright Browser Likely not Installed")

        corresponding = ledger_gemini.reconcile_ledgers(gemini_df, csv_df)
        if not force_csv and not corresponding and scrape_working:
            logger.warning("Ledger Images Did Not Match Scraped CSV For URL %s, (%s)", url, game_jump_url)

            game_id, correction_inserted, lookup_df = await self._game_finder(msg, gemini_df, game_id, inserted)
            if lookup_df is not None:
                inserted = correction_inserted
                csv_df = lookup_df

        if not inserted:
            await self.admin_message(guild, f"Game {game_id} for the Ledger Already Exists in the Database.")
            response = await self.prompt(
                guild.get_channel(channels[guild.id]['admin']),
                message.author,
                ("@Admin: Reply 'OK' to Insert the Ledger(s).\n"
                 "You have 2 minutes."),
                timeout=120,
                admin=True
            )
            if response is None or response.lower() != 'ok':
                logger.info('Ledgers Insertion Aborted')
                return

        if force_csv or corresponding:
            await self._insert(guild, [csv_df], game_id, msg.jump_url, csv=True)
        else:
            await self._insert(guild, [gemini_df], game_id, msg.jump_url)
        return

    async def handle_query(self, message: discord.Message):
        channel = message.channel
        txt = message.content.strip()
        if txt and txt.startswith('!'):
            words = txt[1:].split()
            if words:
                option = words[0].lower()
                arguments = words[1:]
                try:
                    if option == 'players':
                        ans, columns = query_presets.players()
                        if ans:
                            answer = pd.DataFrame(ans, columns=columns)
                            answer.index += 1
                            await channel.send(", ".join(answer['name'].to_list()))
                        else:
                            logger.info('Unexpected #query Error: %s', txt)
                            await channel.send("Unexpected Error")
                        return
                    elif option in ('leaderboard', 'leaderboard_avg'):
                        ans, columns = query_presets.leaderboard(arguments, option == 'leaderboard_avg')
                        if ans:
                            answer = pd.DataFrame(ans, columns=columns)
                            answer.index += 1
                            table = discord_markdown(answer.to_string().splitlines())
                            await channel.send(f"{table}")
                        else:
                            logger.info('Unexpected #query Error: %s', txt)
                            await channel.send("Unexpected Error")
                        return
                    elif option == 'career':
                        if not arguments:
                            await channel.send("!career: single_word_name {optional ordering (alias/net/career)}.")
                            return

                        name = arguments[0]
                        orderings = arguments[1:]

                        with connect() as conn:
                            matches, _ = query(
                                conn,
                                "SELECT player_id, name FROM players WHERE name ILIKE %s",
                                f"{name}%"
                            )

                        if not matches:
                            await channel.send(f"No players found matching '{name}'")
                            return
                        elif len(matches) > 1:
                            name_list = "\n".join(f"{chr(i+65)}: {m[1]} ({m[0]})" for i, m in enumerate(matches))
                            response = await self.prompt(
                                channel,
                                message.author,
                                (f"Multiple Players Found. Make an Alphabetic Selection. Or 'Cancel'\n"
                                 f"{name_list}\n"
                                 f"You have 1 minute.")
                            )
                            if response is None or len(response) != 1:
                                logger.info('!career Player Selection Time Expired')
                                return
                            elif (ord(response.upper()) - 65) not in range(len(matches)):
                                logger.info('!career Invalid Player Selection')
                                await channel.send("Invalid Player Selection.")
                                return
                            else:
                                player = matches[ord(response.upper()) - 65]
                        else:
                            player = matches[0]

                        player_id, player_name = player
                        ans, columns = query_presets.career(player_id)
                        if not ans:
                            await channel.send(f"No data found for {player_name}.")
                            return

                        answer = pd.DataFrame(ans, columns=columns)
                        answer.index += 1

                        seen = set()
                        orders = []
                        for word in orderings:
                            lw = word.lower()
                            if lw in ['net', 'career', 'alias'] and lw not in seen:
                                orders.append(lw)
                                seen.add(lw)
                        if orders:
                            answer = answer.sort_values(by=orders, ascending=False)

                        table = discord_markdown(answer.to_string().splitlines())
                        await channel.send(f"{table}")
                        return
                    elif option == 'graph':
                        career_buffer = query_presets.career_graph(arguments)
                        if career_buffer:
                            graph_file = discord.File(career_buffer, filename='career_graph.png')
                            await channel.send(file=graph_file)
                        else:
                            await channel.send("Error or No Career Graph")
                        return
                    elif option == 'recent':
                        if arguments and arguments[0].isdigit():
                            days = int(arguments[0])
                            arguments = arguments[1:]
                        else:
                            days = 30

                        recent_buffer = query_presets.recent_graph(arguments, days)
                        if recent_buffer:
                            recent_file = discord.File(recent_buffer, filename='recent_graph.png')
                            await channel.send(file=recent_file)
                        else:
                            await channel.send(f'No games in the last {days} days')
                        return
                except Exception as err:
                    logger.exception("An Error Occurred While Running Command: %s", err)
                    await channel.send("An Error Occurred While Producing Output")
                    return
            await channel.send("!leaderboard, !leaderboard_avg, !career, !graph, !recent, !players")
        return

    async def handle_manage(self, message: discord.Message):
        guild = message.guild
        channel = message.channel
        txt = message.content.strip()
        if txt and txt.startswith('!'):
            words = txt[1:].split()
            if words:
                option = words[0].lower()
                arguments = words[1:]
                if option == 'restart':
                    await channel.send("Restarting bot...")
                    logger.info('Restarting bot...')
                    await self.shutdown()
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                elif option == 'setup':
                    try:
                        guild_channels = [c for c in await guild.fetch_channels() if
                                          isinstance(c, discord.TextChannel) and c.name in CHANNELS_TEMPLATE]
                        guild_channel_names = [c.name for c in guild_channels]
                        for ch in CHANNELS_TEMPLATE:
                            if ch not in guild_channel_names:
                                new_channel = await guild.create_text_channel(name=ch, reason="Setup missing channel")
                                perms = new_channel.permissions_for(guild.me)
                                if not perms.read_messages:
                                    await new_channel.set_permissions(guild.me, read_messages=True, send_messages=True)
                                channels[guild.id][ch] = new_channel.id
                                await channel.send(f'Channel: {ch} - created')
                            else:
                                duplicate_channels = [d for d in guild_channels if d.name == ch]
                                for c in duplicate_channels:
                                    if c.id == channels[guild.id][ch]:
                                        perms = c.permissions_for(guild.me)
                                        if not perms.read_messages:
                                            await c.set_permissions(guild.me, read_messages=True, send_messages=True)
                                    else:
                                        await c.delete(reason=f'Removed Duplicate of #{ch}')

                        guild_roles = [r for r in await guild.fetch_roles() if r.name in ROLES_TEMPLATE]
                        guild_role_names = [r.name for r in guild_roles]

                        for role in ROLES_TEMPLATE:
                            if role not in guild_role_names:
                                new_role = await guild.create_role(name=role, reason="Setup missing role")
                                roles[guild.id][role] = new_role.id
                                await channel.send(f'Role: {role} - created')
                            else:
                                duplicate_roles = [d for d in guild_roles if d.name == role]
                                for r in duplicate_roles:
                                    if not r.id == roles[guild.id][role]:
                                        await r.delete(reason=f"Removed Duplicate of @'{role}'")
                    except Exception as err:
                        logger.exception('Missing Permissions to Setup %s: %s', guild.name, err)
                        await channel.send(f'Missing Permissions to Setup Channels/Roles in the Server')
                    else:
                        await channel.send("Setup Complete")
                    return
                elif option == 'purge':
                    channel_mentions = message.channel_mentions
                    if len(channel_mentions) == 1:
                        destination = channel_mentions[0]
                        response = await self.prompt(
                            channel,
                            message.author,
                            (f"@Admin: Enter the number of messages to be purged from {destination.name}. Or 'Cancel'\n"
                             f"You have 1 minute."),
                            admin=True
                        )
                        if response is None:
                            logger.info('Prompt Expired')
                            return
                        elif response.lower() in ['cancel', 'no', 'quit', 'exit', 'abandon']:
                            logger.info('Prompt Declined')
                            return
                        elif not response.isdigit() or int(response) < 1:
                            await channel.send("Aborted. Enter a positive integer amount of messages to purge.")
                            return
                        try:
                            await channel_mentions[0].purge(limit=int(response))
                        except Exception as err:
                            logger.exception('Missing permissions to manage messages in %s: %s', destination.name, err)
                            await channel.send(f'Missing permissions to manage messages in {destination.name}')
                        else:
                            await channel.send(f"Purge Complete")
                        return
                    await channel.send("!purge required exactly 1 destination channel to be mentioned")
                    return
                elif option == 'add_games':
                    # message: !add_games MM DD YYYY
                    if len(arguments) == 3:
                        m = int(arguments[0])
                        d = int(arguments[1])
                        y = int(arguments[2])
                        logger.debug('Starting to Add Games to Database')
                        game_query = "INSERT INTO games (url, date) VALUES (%s, %s)"
                        games = []
                        game_channel = guild.get_channel(channels[guild.id]['game'])
                        # oldest to newest
                        async for entry in game_channel.history(after=datetime.datetime(year=y, month=m, day=d)):
                            if POKERNOW in entry.content:
                                url = keyword_search(entry.content, POKERNOW).strip(",.;:!()[]/")
                                # unique part of pokernow url
                                url_id = url.rpartition('/')[2]
                                games.append((url_id, entry.created_at))

                        if not games:
                            await channel.send("No games found")
                            return

                        try:
                            with connect() as conn:
                                logger.info('Attempting to Insert Games With !add_games in %s', guild.name)
                                for game_url, game_date in games:
                                    query(conn, game_query, game_url, game_date)
                        except Exception as err:
                            logger.warning('No Games Inserted: %s', err)
                            await channel.send(f'Error While Attempting to Insert Games')
                        else:

                            logger.info('Insert Successful With !add_games in %s', guild.name)
                            await channel.send(f'Successfully Added {len(games)} Games to Database')
                        return
                    else:
                        await channel.send("!add_games MM DD YYYY")
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
                                    attachments_list[-1].extend(entry.attachments)
                        images_list = await attachments_to_bytes(attachments_list=attachments_list)
                        results = []
                        for index, sublist in enumerate(images_list):
                            results.append(await asyncio.to_thread(ledger_gemini.gemini, sublist, game_id=game_id + index))
                        await self._insert(guild, results, game_id, message.jump_url)
                        return
                    else:
                        await channel.send("!add_ledgers GID MM DD YYYY")
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
                        destination = message.channel_mentions[0]
                        attachments = message.attachments
                        if not new_content and not attachments:
                            await channel.send("Compose Error: Missing message text/attachments")
                            return
                        elif not new_content:
                            await destination.send(file=await attachments[0].to_file())
                        elif attachments:
                            await destination.send(new_content, files=[await attachments[0].to_file()])
                        else:
                            await destination.send(new_content)
                            return

                        if attachments:
                            for file in attachments[1:]:
                                await destination.send(file=await file.to_file())
                        await channel.send(f'*Message Sent In: {destination.jump_url}')
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
                                await channel.send("Edit Error: Missing message text/attachments")
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
                await channel.send("!setup, !restart, !purge, !add_games, !add_ledgers, ![#channel], ![message link]")
                return
        return

    async def _game_finder(self, msg: discord.Message, gemini_df: pd.DataFrame,
                           game_id: int, inserted: bool) -> tuple[int, bool, Optional[pd.DataFrame]]:
        guild = msg.guild
        game_channel = guild.get_channel(channels[guild.id]['game'])
        async for entry in game_channel.history(limit=10):
            if POKERNOW not in entry.content:
                continue
            url2 = keyword_search(entry.content, POKERNOW).strip(",.;:!()[]/")
            game_history_df = await scrape_ledger(url2)

            corresponding = ledger_gemini.reconcile_ledgers(gemini_df, game_history_df)
            if corresponding:
                logger.warning("Ledger Match With Scraped CSV For URL %s, (%s)", url2, entry.id)
                game_date = entry.created_at
                email = await self.game_email(entry)
                email_tag = f" {email}" if email else f" <@&{roles[guild.id]['admin']}>"

                url = url2
                url_id = url.rpartition('/')[2]
                try:
                    game_query = """INSERT INTO games (url, date) VALUES (%s, %s)
                                    ON CONFLICT (url) DO NOTHING RETURNING game_id"""
                    game_id_query = "SELECT game_id, date FROM games WHERE url = %s"
                    with connect() as conn:
                        if inserted:
                            delete_query = "DELETE FROM games WHERE game_id = %s"
                            query(conn, delete_query, game_id)

                        corrected_insert, _ = query(conn, game_query, url_id, game_date)
                        if not corrected_insert:
                            existing_game, _ = query(conn, game_id_query, url_id)
                        corrected_game_id = corrected_insert[0][0] if corrected_insert else existing_game[0][0]
                except Exception as err:
                    logger.exception('Unable to Insert/Find Game Correction - %s:\n %s', url_id, err)
                    if not inserted:
                        await self.admin_message(guild, f"Error Correcting {msg.jump_url} {game_id}. Ledger Skipped")
                        return game_id, False, None

                await msg.edit(content=f"Ledger for: {entry.jump_url}{email_tag}")
                await self.admin_message(guild, f"{msg.jump_url}: Game Corrected -> {entry.jump_url}")
                return corrected_game_id, bool(corrected_insert), game_history_df

        return game_id, False, None

    async def _insert(self, guild: discord.Guild, results: list[pd.DataFrame], game_id: int, tag='', csv = False):
        if not csv:
            results = ledger_gemini.format_ledgers(results)
        ledgers_sum, linked_users, new_users, errors, successes = ledger_gemini.insert_ledgers(results, game_id, csv)

        if errors:
            error_text = "\n".join(errors[:5])
            if len(errors) > 5:
                error_text += f'\n... {len(errors)} errors'
            await self.admin_message(guild, f"<@&{roles[guild.id]['admin']}>️ Ledger Insert Errors:\n{error_text}")

        if successes:
            logger.info('%s Ledger(s) Inserted', len(successes))
            message_tag = f" - {', '.join(str(insert) for insert in successes)}{': ' + tag if tag else ''}"
            message_tag += " (From CSV)" if csv else " (From Images)"
            await self.admin_message(guild, f"{len(successes)} Ledger(s) Inserted {message_tag}")

        await self._after_insert(guild, ledgers_sum, linked_users, new_users)

    async def _after_insert(self, guild: discord.Guild, ledgers_sum: int, linked_users: list[str],
                            new_users: list[str]) -> None:
        await self.reset_sequences(guild)
        self.dump()

        if ledgers_sum:
            await self.admin_message(guild, f'Unbalanced Ledgers Sum: {ledgers_sum}')
            logger.warning('Unbalanced Ledgers Sum: %s', ledgers_sum)

        if linked_users:
            linked_message = "\n".join(["Automatically Linked Users:"] + linked_users)
            if new_users:
                linked_message += '\n' + '-' * 30
            await self.admin_message(guild, linked_message)

        if new_users:
            for user in new_users:
                await self.admin_message(guild, f"{user} has been created")
            await self.admin_message(guild, f"!merge in <#{channels[guild.id]['database']}>")

