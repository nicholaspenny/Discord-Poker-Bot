import asyncio
import datetime
import logging
import os
import signal
import subprocess
from typing import Optional

import discord
from dotenv import load_dotenv

from src import common
from src.config import config
from src.connect import connect, query
from src.on_message import OnMessageHandler

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

channels = common.channels
roles = common.roles
CHANNELS_TEMPLATE = common.CHANNELS_TEMPLATE
ROLES_TEMPLATE = common.ROLES_TEMPLATE

async def populate_dictionaries():
    global channels, roles
    try:
        logger.info(f'Servers: {[(guild.name, guild.id) for guild in client.guilds]}')
        for guild in client.guilds:
            guild_channels = [c for c in await guild.fetch_channels() if
                              isinstance(c, discord.TextChannel) and c.name in CHANNELS_TEMPLATE]
            channels[guild.id] = {c.name: c.id for c in sorted(guild_channels, key=lambda x: x.created_at, reverse=True)}
            guild_roles = [r for r in await guild.fetch_roles() if r.name in ROLES_TEMPLATE]
            roles[guild.id] = {r.name: r.id for r in sorted(guild_roles, key=lambda x: x.created_at, reverse=True)}
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
                         'database': 3,
                         'manage': 4,
                         }
                    }
    finally:
        for guild_id, mapping in channels.items():
            for name in CHANNELS_TEMPLATE:
                mapping.setdefault(name, 0)
        for guild_id, mapping in roles.items():
            for name in ROLES_TEMPLATE:
                mapping.setdefault(name, 0)


def reset_sequence(table: str, column: str) -> int:
    reset_query = f"""SELECT setval(
                                      pg_get_serial_sequence('{table}', '{column}'),
                                      COALESCE((SELECT MAX({column}) FROM {table}), 0), -- max existing ID or 0 if empty
                                      TRUE -- next nextval() will return max+1
                              );"""
    next_id_query = f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table};"
    try:
        with connect() as connection:
            query(connection, reset_query)
            next_id, _ = query(connection, next_id_query)
        return next_id
    except Exception as err:
        logger.exception('Unable to Connect to the Database: %s', err)
        raise


async def reset_database_sequences(guild: discord.Guild = None):
    try:
        next_player = reset_sequence('players', 'player_id')
        next_game = reset_sequence('games', 'game_id')
    except Exception as err:
        logger.exception('Failed to reset database sequences: %s', err)
        if guild:
            await admin_message(guild, 'Failed to reset database sequences')
    else:
        logger.info('Successfully reset database sequences')
        logger.info(f'Players - Next ID: {next_player}')
        logger.info(f'Games - Next ID: {next_game}')


def dump_database() -> Optional[str]:
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_path = f"db/dump_{timestamp}.sql"
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
            dump_path
        ], check=True)
        logger.info(f"Database dumped to {dump_path}")
        return dump_path
    except subprocess.CalledProcessError as e:
        logger.info(f"Database dump failed: {e}")
        return None


def dump_database_once() -> Optional[str]:
    global has_dumped
    if not has_dumped:
        dump_path = dump_database()
        has_dumped = True
        return dump_path
    else:
        logger.info('Database has already been dumped')
        return None


async def admin_message(guild: discord.Guild, content: str, file_path: str = None):
    try:
        admin_channel = guild.get_channel(channels[guild.id]['admin'])
        if admin_channel:
            if file_path:
                attachment = discord.File(file_path, filename=file_path)
                await admin_channel.send(content, file=attachment)
            else:
                await admin_channel.send(content)
        else:
            logger.warning('Admin channel missing in %s', guild.name)
    except Exception as err:
        logger.exception('Failed to send admin message in %s: %s', guild.name, err)


async def prompt(message: discord.Message, prompt_text: str, timeout: float = 60.0, admin = False) -> Optional[str]:
    if admin:
        channel = message.guild.get_channel(channels[message.guild.id]['admin'])
    else:
        channel = message.channel
    await channel.send(prompt_text)

    def check(m: discord.Message) -> bool:
        if admin:
            return m.author.get_role(roles[m.guild.id]['admin']) is not None and not m.author.bot and m.channel == channel
        else:
            return m.author == message.author and m.channel == channel

    try:
        reply = await client.wait_for('message', timeout=timeout, check=check)
        return reply.content.strip()
    except asyncio.TimeoutError:
        await message.channel.send("No response received in time. Operation cancelled.")
        return None


async def shutdown_message(file_path: Optional[str] = None):
    for guild in client.guilds:
        await admin_message(guild, "Poker Bot Offline - Shutting down...", file_path)


async def shutdown():
    logger.info("Shutting down bot, dumping database if not already done...")
    dump_path = dump_database_once()
    try:
        await shutdown_message(dump_path)
    except Exception as e:
        logger.warning(f"Failed to send shutdown message: {e}")

    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    if not client.is_closed():
        await client.close()


def handle_signal(sig, _):
    # _ is a stand in for frame
    logger.info(f"Received signal {sig}, shutting down...")
    asyncio.create_task(shutdown())


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

handler = OnMessageHandler(shutdown, prompt, admin_message, reset_database_sequences, dump_database)


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
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    update_guild_channel(channel)


@client.event
async def on_guild_channel_update(before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
    if before.name != after.name:
        update_guild_channel(after, before)


@client.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    update_guild_channel(old_channel=channel)


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
    await admin_message(guild, 'Poker Bot Online - At Your Service!')


@client.event
async def on_member_join(member: discord.Member):
    guild = member.guild
    email_needed_role = guild.get_role(roles[guild.id]['email needed'])

    if email_needed_role and not member.bot:
        try:
            await member.add_roles(email_needed_role)
        except discord.Forbidden:
            logger.warning('Missing permissions to add role in %s', guild.name)
            await admin_message(guild, 'Missing permissions to add roles')
        except Exception as err:
            logger.exception('Failed to assign role in %s: %s', guild.name, err)
    else:
        logger.warning('Missing email needed role in %s:', guild.name)
        await admin_message(guild, 'Please add a role named "email needed"')


@client.event
async def on_member_update(before: discord.Member, after: discord.Member):
    added_roles = [r for r in after.roles if r not in before.roles]
    guild = after.guild
    email_needed_role = guild.get_role(roles[guild.id]['email needed'])
    if email_needed_role in added_roles:
        try:
            with connect() as connection:
                existing_player_query = """SELECT * FROM players WHERE discord_id = %s;"""
                ans, cols = query(connection, existing_player_query, after.id)
                if ans:
                    logger.info('Member (%s) already exists in database. Removing email needed role.', ans)
                    try:
                        await after.remove_roles(email_needed_role)
                    except discord.Forbidden:
                        logger.warning('Missing permissions to add role in %s', guild.name)
                        await admin_message(guild, 'Missing permissions to remove roles')
                elif not after.bot:
                    insert_player_query = """INSERT INTO players (name, discord_id)
                                             VALUES (%s, %s) RETURNING player_id;"""
                    ans2, cols2 = query(connection, insert_player_query, after.name, after.id)
                    await admin_message(guild, f'{after.name} Inserted into Database - {ans2[0][0]}')
        except Exception as err:
            logger.exception('DB error checking existing player: %s', err)
            return


@client.event
async def on_ready():
    await populate_dictionaries()
    await reset_database_sequences()
    logger.info('%s is now running!', client.user)
    for guild in client.guilds:
        await admin_message(guild, 'Poker Bot Online - At Your Service!')


@client.event
async def on_message(message: discord.Message):
    global channels, roles
    guild = message.guild
    cid = message.channel.id

    if cid == channels[guild.id]['email']:
        await handler.handle_email(message)
    elif cid == channels[guild.id]['email-database']:
        await handler.handle_email_database(message)
    elif message.author != client.user:
        if cid == channels[guild.id]['admin']:
            await handler.handle_admin(message)
        elif cid == channels[guild.id]['manage']:
            await handler.handle_manage(message)
        elif cid == channels[guild.id]['database']:
            await handler.handle_database(message)
        elif cid == channels[guild.id]['commands']:
            await handler.handle_commands(message)
        elif cid in (channels[guild.id]['query'], channels[guild.id]['query-test']):
            await handler.handle_query(message)
        elif cid in (channels[guild.id]['ledgers'], channels[guild.id]['ledgers-test']):
            await handler.handle_ledgers(message)
        elif cid in (channels[guild.id]['graph'], channels[guild.id]['graph-test']):
            await handler.handle_graph(message)
        elif cid in (channels[guild.id]['game'], channels[guild.id]['game-test']):
            await handler.handle_game(message)


def run_discord_bot():
    client.run(os.getenv('DISCORD_BOT_TOKEN'), log_handler=None)
