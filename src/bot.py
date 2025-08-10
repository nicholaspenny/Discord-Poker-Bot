import asyncio
import datetime
import io
import logging
import os
import re
from typing import Optional

import discord
from dotenv import load_dotenv
import pandas as pd
from PIL import Image

from src.connect import connect, disconnect, query
from src import graph
from src import ledger_gemini
from src import query_presets

load_dotenv()

logger = logging.getLogger(__name__)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)

POKERNOW = 'https://www.pokernow.club/games/'
JUMP_URL_PREFIX = 'https://discord.com/channels/'

channels: dict[str, int] = {}
channel_query = """SELECT channel_name, channel_id FROM channels;"""
roles: dict[str, int] = {}
role_query = """SELECT role_name, role_id FROM roles;"""
players: dict[int, int] = {}
player_query = """SELECT discord_id, player_id FROM players;"""


def populate_dictionaries():
    global channels, roles, players
    data = {}

    try:
        with connect() as connection:
            zipped = zip(['channels', 'roles', 'players'],
                         [channel_query, role_query, player_query])
            for dictionary, dict_query in zipped:
                result, columns = query(connection, dict_query)
                data[dictionary] = {key: value for key, value in result}
        disconnect(connection)

        channels = data['channels']
        roles = data['roles']
        players = data['players']
    except Exception as err:
        logger.warning('Using Default Dictionary Values: %s', err)
        # This is to default hard-code dictionaries for necessary channels/roles
        roles = {'admin': 1,
                 'email_needed': 2,
                 'poker_bot': 3,
                 'star': 4,}
        channels = {'graph': 1,
                    'ledgers': 2,
                    'query': 3,
                    'email': 4,
                    'email_database': 5,
                    'game': 6,
                    'admin': 7,}
        players = {}


async def attachments_to_images(attachments_list: list[list[discord.Attachment]]) -> list[list[Image.Image]]:
    images = []
    try:
        for sublist in attachments_list:
            row = []
            for a in sublist:
                img_bytes = await a.read()
                image = Image.open(io.BytesIO(img_bytes))
                row.append(image)
            images.append(row)
    except Exception as err:
        logger.exception('Error Converting Attachments to Images: %s', err)
        images = []

    return images


async def view_emails(message: discord.Message):
    email = [word for word in message.content.split() if '@' in word and '<' not in word]
    ids = [message.author.id]
    with connect() as connection:
        if email:
            ans, columns = query(connection, """Select discord_id
                                        from players
                                        where email ilike %s""", email[0])
            if ans:
                ids.append(ans[0][0])
    disconnect(connection)
    email_db_channel = client.get_channel(channels['email_database'])
    if email_db_channel.overwrites_for(message.guild.default_role).view_channel is False:
        for item in ids:
            if item != client.user.id:
                await email_db_channel.set_permissions(message.guild.get_member(item), view_channel=True)
    return


async def game_jump(content: str) -> Optional[discord.Message]:
    game_jump_message = None
    given_link = [word for word in content.split() if JUMP_URL_PREFIX in word]
    game_channel = client.get_channel(channels['game'])
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


@client.event
async def on_ready():
    populate_dictionaries()
    logger.info('%s is now running!', client.user)


@client.event
async def on_message(message: discord.Message):
    # MULTI-SERVER IMPLEMENTATION:
    #    If message.guild.id not in channels: return
    #    Implement nested dicts throughout: i.e., roles[message.guild.id]['admin']
    global channels
    global roles

    if message.channel.id == channels['email']:
        email_database_channel = client.get_channel(channels['email_database'])
        # loop through #email-database to remove old email if present
        # newest to oldest
        async for entry in email_database_channel.history():
            if message.author in entry.mentions:
                await entry.delete()
        await email_database_channel.send(f'<@{message.author.id}> {message.content}')
        return
    elif message.channel.id == channels['admin']:
        if message.author == client.user:
            return
        elif message.content.startswith('!'):
            txt = message.content[1:]
            words = txt.split()
            channel_pattern = r"^<#(\d+)>$"
            message_pattern = fr"^{re.escape(JUMP_URL_PREFIX)}{message.guild.id}/(\d+)/(\d+)$"
            channel_match = re.match(channel_pattern, words[0]) if words else None
            message_match = re.match(message_pattern, words[0]) if words else None

            if not words:
                await message.channel.send('!add_games, !add_ledgers, ![#channel] <- compose, ![message link] <- edit')
                return
            elif words[0] == 'add_games':
                # message: !add_games MM DD YYYY
                if len(words) > 1:
                    m = int(words[1])
                    d = int(words[2])
                    y = int(words[3])
                    logger.debug('Starting to Add Games to Database')
                    game_query = """INSERT INTO games (url, date) VALUES (%s, %s);"""
                    links = []
                    game_channel = client.get_channel(channels['game'])
                    # finding messages in #game with a pokernow url, oldest to newest
                    async for entry in game_channel.history(after=datetime.datetime(year=y, month=m, day=d)):
                        if POKERNOW in entry.content:
                            words_with_url = [word for word in entry.content.split() if POKERNOW in word]
                            links.append([words_with_url[0], entry.created_at.strftime('%m-%d-%y'), entry.created_at])
                    try:
                        with connect() as connection:
                            for item in links:
                                # unique part of pokernow_url
                                query(connection, game_query, item[0].split()[-1].rpartition('/')[2], item[-1])
                        disconnect(connection)
                    except Exception as err:
                        logger.warning('No Games Inserted: %s', err)
                    return
                else:
                    await message.channel.send('!add_games MM DD YYYY')
                    return
            elif words[0] == 'add_ledgers':
                # message: !add_ledgers {game_id of 1st ledger} MM DD YYYY [MM DD YYY] <-- [optional end date]
                if len(words) in (5 , 8):
                    logger.debug('Starting to Add Ledgers to Database')
                    game_id = int(words[1])
                    m = int(words[2])
                    d = int(words[3])
                    y = int(words[4])
                    after = datetime.datetime(month=m, day=d, year=y)
                    before = (
                        datetime.datetime(month=int(words[5]), day=int(words[6]), year=int(words[7]))
                        if len(words) == 8
                        else datetime.datetime.now()
                    )
                    i = 0
                    attachments_list = []
                    buffer = None
                    ledgers_channel = client.get_channel(channels['ledgers'])
                    # oldest to newest
                    async for entry in ledgers_channel.history(after=after, before=before):
                        if entry.attachments:
                            if not attachments_list or entry.created_at - buffer >= datetime.timedelta(minutes=2):
                                i += 1
                                attachments_list.append(entry.attachments)
                                buffer = entry.created_at
                            else:
                                attachments_list[-1] = attachments_list[-1] + entry.attachments
                    images_list = await attachments_to_images(attachments_list=attachments_list)
                    results = []
                    for index, sublist in enumerate(images_list):
                        results.append(await asyncio.to_thread(ledger_gemini.gemini, sublist, game_id=game_id + index))
                    ledger_gemini.insert_ledgers(ledger_gemini.format_ledgers(results), game_id=game_id)
                    logger.info('\n%s Ledgers Uploaded', len(images_list))
                    return
                else:
                    await message.channel.send('!add_ledgers GID MM DD YYYY')
                    return
            elif channel_match:
                # ! #channel [body] <-- [optional body if attachments included]
                # group[1] == {channel_id where new message to be sent}
                new_content = txt.split(channel_match.group())[1]
                channel = message.channel_mentions[0]
                attachments = message.attachments
                if not new_content and not attachments:
                    await message.channel.send('Compose Error: Missing message text/attachments')
                    return
                elif not new_content:
                    await channel.send(file=await attachments[0].to_file())
                else:
                    await channel.send(new_content, files=[await attachments[0].to_file()] if attachments else [])

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
                    new_content = txt.split(message_match.group())[1]
                    attachments = message.attachments
                    old = await client.get_channel(int(cid)).fetch_message(int(mid))
                    if not new_content and not attachments:
                        await message.channel.send('Edit Error: Missing message text/attachments')
                        return
                    elif not new_content:
                        await old.edit(attachments=[await att.to_file() for att in attachments])
                        return
                    elif not attachments:
                        await old.edit(content=new_content)
                        return
                    else:
                        await old.edit(content=new_content, attachments=[await att.to_file() for att in attachments])
                        return
            else:
                await message.channel.send('!add_games, !add_ledgers, ![#channel] <- compose, ![message link] <- edit')
                return
        return
    elif message.channel.id == channels['query']:
        if message.author == client.user:
            return
        elif not message.content.strip().startswith('!'):
            return

        txt = message.content.strip()[1:]
        if not txt or txt == 'help':
            await message.channel.send("!leaderboard, !leaderboard_avg, !career, !graph, !players")
        else:
            option = txt.split()[0]
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
                ans, columns = query_presets.leaderboard(txt.split()[1:], option=='leaderboard_avg')
                if ans:
                    answer = pd.DataFrame(ans, columns=columns)
                    answer.index += 1
                    with pd.option_context('display.min_rows', 25, 'display.max_rows', 25):
                        await message.channel.send(f'```{answer}```')
                    return
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
                career_graph = query_presets.career_graph(txt.split()[1:])
                graph_file = discord.File(career_graph, filename='career_graph.png')
                await message.channel.send(file=graph_file)
                return
            return
        return
    elif message.channel.id == channels['ledgers']:
        if message.author == client.user:
            return
        elif not message.attachments:
            return

        # game_jump = game_jump_url(message)
        game_jump_message = await game_jump(message.content)
        game_jump_url = game_jump_message.jump_url if game_jump_message else 'Cannot find game'
        attachments = message.attachments
        await message.channel.send(f'Ledger for: {game_jump_url}', file=await attachments[0].to_file())
        for screen_shot in attachments[1:]:
            await message.channel.send(file=await screen_shot.to_file())
        await message.delete()

        if game_jump_message:
            await view_emails(game_jump_message)

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
                images_list = await attachments_to_images([attachments])
                results = []
                for sublist in images_list:
                    results.append(await asyncio.to_thread(ledger_gemini.gemini, sublist, game_id=ans[0][0]))
                ledger_gemini.insert_ledgers(ledger_gemini.format_ledgers(results), game_id=ans[0][0])
                logger.info('%s Ledger(s) Inserted', len(images_list))
            else:
                logger.info('Ledgers Skipped, Game Already Exists')
        return
    elif message.channel.id == channels['graph']:
        if message.author == client.user:
            return

        # start to generate chart(s) of provided game
        attachments = message.attachments
        if len(attachments) == 2:
            file_names = (attachments[0].filename, attachments[1].filename)
            file_types = tuple(file.split('.')[-1].lower() if '.' in file else '' for file in file_names)

            if file_types == ('csv', 'csv'):
                attachment_one = await attachments[0].read()
                attachment_two = await attachments[1].read()

                game_jump_message = await game_jump(message.content)
                game_jump_url = game_jump_message.jump_url if game_jump_message else 'Cannot find game'

                # This does not enforce or check if the log and ledgers are truly corresponding
                nets_graph = graph.graph_setup(attachment_one, attachment_two)

                if nets_graph:
                    try:
                        # According to API, the File object is only to be used once
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
    elif message.channel.id == channels['email_database']:
        guild = message.guild
        member = message.mentions[0]
        member_name = member.display_name
        member_id = member.id
        member_email = message.content.split()[1]

        email_needed_role = guild.get_role(roles['email_needed'])
        await member.remove_roles(email_needed_role)

        insert_player_query = """INSERT INTO players (name, discord_id, email) 
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT ON CONSTRAINT players_discord_id_key DO NOTHING;
                                        UPDATE players SET email = %s WHERE discord_id = %s;"""
        try:
            with connect() as connection:
                query(connection, insert_player_query, member_name ,member_id, member_email, member_email, member_id)
            disconnect(connection)
        except Exception as err:
            logger.exception('Unable to Update Player Email: %s', err)
        else:
            populate_dictionaries()
        return
    elif message.channel.id == channels['game']:
        if message.author == client.user:
            return
        elif not POKERNOW in message.content:
            return
        ping = f"<@&{roles['star']}>"
        link = [word for word in message.content.split() if POKERNOW in word][0]
        email_database_channel = client.get_channel(channels['email_database'])
        email = None
        # newest to oldest
        async for entry in email_database_channel.history():
            if message.author in entry.mentions:
                if email := [word for word in message.content.split() if '@' in word and '<' not in word]:
                    bot_link = await message.channel.send(f'{ping} {email[0]} \n{link}')
                    await bot_link.create_thread(name="Notes", auto_archive_duration=1440)
                    await message.delete()
                    return
                break
        missing_email = f"Lobby creator must first register an email with the server." \
                        f"\nAdd one to <#{channels['email']}> or contact an <@&{roles['admin']}> for access."
        await message.channel.send(missing_email)
        await message.delete()
        return


def run_discord_bot():
    client.run(os.getenv('DISCORD_BOT_TOKEN'))
