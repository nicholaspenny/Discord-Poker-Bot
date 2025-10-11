import asyncio
import datetime
import logging
import os
import signal
import subprocess
from typing import Optional

import discord
from discord.ext import commands, tasks
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
bot = commands.Bot(command_prefix="!", intents=intents)

channels = common.channels
roles = common.roles
CHANNELS_TEMPLATE = common.CHANNELS_TEMPLATE
ROLES_TEMPLATE = common.ROLES_TEMPLATE

invites: dict[int, dict[str, tuple[int, Optional[datetime.datetime], int]]] = {}
MAX_INVITE_DURATION_HOURS = 72


async def cache_invites(guild: discord.Guild):
    global invites

    guild_invites = invites.setdefault(guild.id, {})
    for invite in await guild.invites():
        if not invite:
            continue

        if not invite.inviter:
            try:
                await invite.delete()
            except Exception as err:
                logger.exception('Failed to delete invite with empty inviter: %s', err)
        else:
            uses = invite.uses if invite.uses else 0
            guild_invites[invite.code] = (invite.inviter.id, invite.created_at, uses)

async def populate_dictionaries():
    global channels, roles
    try:
        logger.info(f'Servers: {[(guild.name, guild.id) for guild in bot.guilds]}')
        for guild in bot.guilds:
            guild_channels = [c for c in await guild.fetch_channels() if
                              isinstance(c, discord.TextChannel) and c.name in CHANNELS_TEMPLATE]
            channels[guild.id] = {c.name: c.id for c in sorted(guild_channels, key=lambda x: x.created_at, reverse=True)}
            guild_roles = [r for r in await guild.fetch_roles() if r.name in ROLES_TEMPLATE]
            roles[guild.id] = {r.name: r.id for r in sorted(guild_roles, key=lambda x: x.created_at, reverse=True)}
    except Exception as err:
        logger.warning('Using Default Dictionary Values: %s', err)
        # This is to default hard-code dictionaries in the primary server for necessary channels/roles
        roles = {1:
                     {'admin': 1,
                      'email needed': 2,
                      'game runner': 3,
                      'poker bot': 4,
                      }
                 }
        channels = {1:
                        {'admin': 1,
                         'database': 2,
                         'invites': 3,
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

        if not sweep_old_invites.is_running():
            sweep_old_invites.start()


def reset_sequence(table: str, column: str) -> int:
    reset_query = f"""SELECT setval(
                                      pg_get_serial_sequence('{table}', '{column}'),
                                      COALESCE((SELECT MAX({column}) FROM {table}), 0), -- max existing ID or 0 if empty
                                      TRUE -- next nextval() will return max+1
                              )"""
    next_id_query = f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table}"
    try:
        with connect() as conn:
            query(conn, reset_query)
            next_id, _ = query(conn, next_id_query)
        return next_id
    except Exception as err:
        logger.exception('Unable to Connect to the Database: %s', err)
        raise


async def reset_database_sequences(guild: discord.Guild = None) -> list[str]:
    try:
        next_player = reset_sequence('players', 'player_id')
        next_game = reset_sequence('games', 'game_id')
    except Exception as err:
        logger.exception('Failed to reset database sequences: %s', err)
        if guild:
            await admin_message(guild, 'Failed to reset database sequences')
        return []
    else:
        players_sequence = f"Players - Next ID: {next_player[0][0]}"
        games_sequence = f"Games - Next ID: {next_game[0][0]}"
        logger.info('Successfully Reset Database Sequences.\n%s | %s', players_sequence, games_sequence)
        return [players_sequence, games_sequence]


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
    except subprocess.CalledProcessError as err:
        logger.info(f"Database dump failed: {err}")
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


async def game_email(message: discord.Message) -> str:
    for word in message.content.split():
        if "@" in word and "." in word and "<" not in word and not word.startswith("!"):
            return word.strip(",.;:!?()")

    guild = message.guild
    if not guild:
        return ''

    email_query = "SELECT email FROM players WHERE discord_id = %s"
    if len(message.mentions) == 1:
        mention = message.mentions[0]
        if mention.get_role(roles[guild.id]['game runner']):
            try:
                with connect() as conn:
                    rows, cols = query(conn, email_query, mention.id)
                if rows:
                    return rows[0][0]
            except Exception as err:
                logger.warning("Failed to fetch email for mentioned member: %s", mention.name)

    if message.author.get_role(roles[guild.id]['game runner']):
        try:
            with connect() as conn:
                rows, cols = query(conn, email_query, message.author.id)
            if rows:
                return rows[0][0]
        except Exception as err:
            logger.warning("Failed to fetch email from database for message author: %s", message.author.name)

    await admin_message(guild, f"Email Fetch Request failed in {message.channel.mention}")
    return ''


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


async def prompt(channel: discord.TextChannel, member: discord.Member, prompt_text: str, timeout: float = 60.0,
                 admin=False) -> Optional[str]:
    p = await channel.send(prompt_text)

    def check(m: discord.Message) -> bool:
        if admin:
            return m.author.get_role(roles[m.guild.id]['admin']) is not None and m.channel == channel
        else:
            return m.author == member and m.channel == channel

    try:
        reply = await bot.wait_for("message", timeout=timeout, check=check)
        return reply.content.strip()
    except asyncio.TimeoutError:
        await channel.send("No Response Received In Time. Operation cancelled.")
        return None
    finally:
        await p.delete()


async def shutdown_message(file_path: Optional[str] = None):
    for guild in bot.guilds:
        await admin_message(guild, "Poker Bot Offline - Shutting down...", file_path)


async def shutdown():
    logger.info("Shutting down bot, dumping database if not already done...")
    os.makedirs("db/backup", exist_ok=True)
    dump_path = f"db/backup/dump_{timestamp}.sql"
    try:
        await shutdown_message(dump_path)
    except Exception as err:
        logger.warning(f"Failed to send shutdown message: {err}")

    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in pending:
        task.cancel()
    try:
        await asyncio.gather(*pending, return_exceptions=True)
    except asyncio.CancelledError as err:
        logger.warning(f"Error during task cleanup: {err}")

    if not bot.is_closed():
        await bot.close()


def handle_signal(sig, _):
    # _ is a stand in for frame
    logger.info(f"Received signal {sig}, shutting down...")
    asyncio.create_task(shutdown())


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

handler = OnMessageHandler(shutdown, prompt, admin_message, game_email, reset_database_sequences, dump_database)


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


@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    update_guild_channel(channel)


@bot.event
async def on_guild_channel_update(before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
    if before.name != after.name:
        update_guild_channel(after, before)


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    update_guild_channel(old_channel=channel)


@bot.event
async def on_guild_role_create(role: discord.Role):
    update_guild_role(role)


@bot.event
async def on_guild_role_update(before: discord.Role, after: discord.Role):
    if before.name != after.name:
        update_guild_role(after, before)


@bot.event
async def on_guild_role_delete(role: discord.Role):
    update_guild_role(old_role=role)


@bot.event
async def on_guild_join(guild: discord.Guild):
    await populate_dictionaries()
    logger.info('%s Just Joined %s', bot.user, guild.name)
    await admin_message(guild, 'Poker Bot Online - At Your Service!')


@bot.event
async def on_member_join(member: discord.Member):
    global invites
    guild = member.guild
    logger.info('Member Joined %s - %s', member.id, member.display_name)
    logger.info('Invites Before: %s', invites)

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
        logger.warning('Missing "email needed" role in %s', guild.name)
        await admin_message(guild, 'Please add a role named "email needed"')

    inner = invites.setdefault(guild.id, {})
    guild_invites = {i.code: i for i in await guild.invites()}
    found = False
    for code, (inviter_id, created_at, uses) in list(inner.items()):
        inv = guild_invites.get(code)
        if inv:
            current_uses = inv.uses or 0
            if current_uses > uses:
                found = True
                logger.info('Invite: (%s - %s) | Uses: %s', code, inviter_id, uses)
                invites[guild.id][code] = (inviter_id, created_at, current_uses)
                ch = await bot.fetch_channel(channels[guild.id]['invites'])
                await ch.send(f"{inv.inviter.mention} invited {member.mention}\n{inv.inviter.id} >>> {member.id}")
                break

    if not found:
        for code, inv in guild_invites.items():
            current_uses = inv.uses or 0
            if code not in inner and current_uses > 0:
                logger.info('Invite (Guess): (%s - %s) | Uses: %s', code, inv.inviter.id, current_uses)
                ch = await bot.fetch_channel(channels[guild.id]['invites'])
                await ch.send(f"{inv.inviter.mention} maybe invited {member.mention}\n{inv.inviter.id} >>> {member.id}")
                break

    await cache_invites(guild)
    logger.info('%s: %s', guild.name, invites[guild.id])


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    added_roles = [r for r in after.roles if r not in before.roles]
    guild = after.guild
    email_needed_role = guild.get_role(roles[guild.id]['email needed'])
    if email_needed_role in added_roles:
        try:
            with connect() as conn:
                existing_player_query = "SELECT * FROM players WHERE discord_id = %s"
                ans, cols = query(conn, existing_player_query, after.id)
                if ans:
                    logger.info('Member (%s) already exists in database. Removing email needed role.', ans)
                    try:
                        await after.remove_roles(email_needed_role)
                    except discord.Forbidden:
                        logger.warning('Missing permissions to add role in %s', guild.name)
                        await admin_message(guild, 'Missing permissions to remove roles')
                elif not after.bot:
                    name = after.name.replace(' ', '')[:10] # uses username, might change to use after.display_name
                    insert_player_query = """INSERT INTO players (name, discord_id)
                                             VALUES (%s, %s) RETURNING player_id"""
                    ans2, cols2 = query(conn, insert_player_query, name, after.id)
                    await admin_message(guild, f'{after.mention} Inserted into Database: {name} - {ans2[0][0]}')
        except Exception as err:
            logger.exception('DB error checking existing player: %s', err)
            return


@bot.event
async def on_invite_create(invite: discord.Invite):
    logger.info(f'%s created Invite - %s | %s', invite.inviter.display_name, invite.code, invite.guild.name)
    await cache_invites(invite.guild)
    logger.info('%s: %s', invite.guild.name, invites[invite.guild.id])

@bot.event
async def on_invite_delete(invite: discord.Invite):
    global invites
    if invites.get(invite.guild.id, {}):
        invites[invite.guild.id].pop(invite.code, None)


@tasks.loop(hours=12)
async def sweep_old_invites():
    global invites

    now = datetime.datetime.now(datetime.timezone.utc)
    for guild_id, inner in invites.items():
        guild = bot.get_guild(guild_id)
        if not guild:
            continue
        guild_invites = {i.code: i for i in await guild.invites()}

        for code, (inviter_id, created_at, uses) in list(inner.items()):
            if not created_at:
                continue

            age = (now - created_at).total_seconds() / 3600
            if age > MAX_INVITE_DURATION_HOURS:
                inv = guild_invites.get(code)
                try:
                    if inv:
                        await inv.delete()
                    else:
                        logger.info("Invite %s already gone in guild %s", code, guild_id)
                except discord.Forbidden:
                    logger.warning("Missing perms to delete invite %s in %s", code, guild_id)
                finally:
                    invites[guild_id].pop(code, None)

    for guild in bot.guilds:
        await cache_invites(guild)

    logger.info('Invites:\n%s', '\n'.join(str(g) for g in invites.items()))


@bot.event
async def on_ready():
    logger.info('%s is now running!', bot.user)
    await reset_database_sequences()
    await populate_dictionaries()
    for guild in bot.guilds:
        await admin_message(guild, 'Poker Bot Online - At Your Service!')


@bot.event
async def on_message(message: discord.Message):
    global channels, roles
    guild = message.guild
    cid = message.channel.id

    if isinstance(message.channel, discord.Thread):
        if message.channel.parent.id == channels[guild.id]['game']:
            if not message.content.strip():
                return
            elif message.content.strip().split()[0].lower() != "!settle":
                return
            if message.author.get_role(roles[guild.id]['admin']):
                logger.info(f"Attempting to Change Email for a New Game Settler: %s", message.content)
                new_email = await game_email(message)
                if new_email:
                    try:
                        async def replace_emails(m: discord.Message):
                            words = m.content.split()
                            new_content = m.content
                            emails = []
                            for i, word in enumerate(words):
                                if "@" in word and "." in word and "<" not in word and not word.startswith("!"):
                                    emails.append(word)
                            for email in emails:
                                new_content = new_content.replace(email, new_email)

                            if m.content != new_content:
                                await m.edit(content=new_content)

                        game_message = await message.channel.parent.fetch_message(message.channel.id)
                        await replace_emails(game_message)

                        ledgers_channel = guild.get_channel(channels[guild.id]['ledgers'])
                        async for entry in ledgers_channel.history(limit=10):
                            if f"/{game_message.channel.id}/{game_message.id}" in entry.content:
                                await replace_emails(entry)
                    except Exception as err:
                        logger.warning(f"Failed to change email for game settler %s", message.content)
                        await admin_message(guild, 'Failed to change email for game settler')
            return

    if cid == channels[guild.id]['email']:
        await handler.handle_email(message)
    elif cid == channels[guild.id]['email-database']:
        await handler.handle_email_database(message)
    elif message.author != bot.user:
        if cid == channels[guild.id]['admin']:
            await handler.handle_admin(message)
        elif cid == channels[guild.id]['manage']:
            await handler.handle_manage(message)
        elif cid == channels[guild.id]['database']:
            await handler.handle_database(message)
        elif cid == channels[guild.id]['invites']:
            await handler.handle_invites(message)
        elif cid in (channels[guild.id]['query'], channels[guild.id]['query-test']):
            await handler.handle_query(message)
        elif cid in (channels[guild.id]['ledgers'], channels[guild.id]['ledgers-test']):
            await handler.handle_ledgers(message)
        elif cid in (channels[guild.id]['graph'], channels[guild.id]['graph-test']):
            await handler.handle_graph(message)
        elif cid in (channels[guild.id]['game'], channels[guild.id]['game-test']):
            await handler.handle_game(message)


def run_discord_bot():
    bot.run(os.getenv('DISCORD_BOT_TOKEN'), log_handler=None)

