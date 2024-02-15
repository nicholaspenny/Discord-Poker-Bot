import discord
import os
import graph
import asyncio
from connect import connect, disconnect, query
import pandas as pd
from discord.ext import commands
import datetime


channels: dict[str, int] = {}
channels_query = """SELECT channel_name, channel_id FROM channels;"""
roles: dict[str, int] = {}
roles_query = """SELECT role_name, role_id FROM roles;"""
misc: dict[str, int] = {}
misc_query = """SELECT misc_name, misc_id FROM misc;"""
players: dict[int, int] = {}
players_query = """SELECT discord_id, user_id FROM players LEFT JOIN users ON players.player_id = users.player_id;"""


def run_discord_bot():
    intents = discord.Intents.default()
    intents.message_content = True
    intents.members = True
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        global channels
        global roles
        global misc
        global channels_query
        global roles_query
        global misc_query

        with connect() as connection:
            for d, q in [(channels, channels_query), (roles, roles_query), (misc, misc_query)]:
                result, columns = query(connection, q)

                for row in result:
                    d[row[0]] = row[1]

            connection.rollback()
        disconnect(connection)

        print(f'{client.user} is now running from PC')

        await monthly_purge()

    async def monthly_purge():
        global misc
        global misc_query
        global players
        global players_query

        while True:
            before = datetime.datetime.today()
            after = datetime.datetime(before.year, before.month, 28, 12)
            if before > after:
                after = datetime.datetime(before.year + before.month // 12, before.month % 12 + 1, 28, 12)

            wait_time = (after - before).total_seconds()

            await asyncio.sleep(wait_time)

            with connect() as connection:
                misc = {}
                players = {}
                for d, q in [(players, players_query), (misc, misc_query)]:
                    result, columns = query(connection, q)

                    for row in result:
                        d[row[0]] = row[1]

                connection.rollback()
            disconnect(connection)

            guild = client.get_guild(misc['server'])
            for member in guild.members:
                if not member.bot and member.id not in misc.values() and member.id not in players.keys():
                    await member.kick(reason='unregistered')
                elif not member.bot and member.id not in misc.values() and players[member.id] is None:
                    await member.kick(reason='inactive user')

    @client.event
    async def on_message(message: discord.Message):
        global channels
        global roles
        global misc

        # Active Servers
        # guild = client.get_guild(misc['server']) // must get guild to access role objects

        # used channels
        # game_channel = client.get_channel(channels['game'])
        # email_channel = client.get_channel(channels['email'])
        # email_database_channel = client.get_channel(channels['email_database'])
        # database_channel = client.get_channel(channels['database'])
        # ledgers_channel = client.get_channel(channels['ledgers'])
        # graph_test_channel = client.get_channel(channels['graph_test'])
        # graph_channel = client.get_channel(channels['game_graph'])
        # server_check_channel = client.get_channel(channels['server_check'])

        # role ids
        # email_needed_role = guild.get_role(roles['email needed'])
        # fiend_role = guild.get_role(roles['fiend'])
        # admin_role = guild.get_role(roles['admin'])
        if message.channel.id == channels['email']:
            email_database_channel = client.get_channel(channels['email_database'])

            # loop through email-database to remove old email if present
            async for entry in email_database_channel.history():
                name = entry.content.split()[0]

                if name == f'<@{message.author.id}>':
                    await entry.delete()

            # add new email to email_database
            await email_database_channel.send(f'<@{message.author.id}> {message.content}')
            return

        elif message.channel.id == channels['server_check']:
            # verifying the bot is running and from the correct location
            if message.author == client.user:
                return
            else:
                await message.channel.send('Operating out of PC')

                if message.content.startswith('!'):
                    await message.channel.send(message.content[1:])

                return

        elif message.channel.id == channels['database']:
            if message.author == client.user:
                return
            else:
                # change to have preset queries and user inputted values
                if message.content.strip().startswith('!') and message.content.strip().endswith(';'):
                    with connect() as connection:
                        ans, columns = query(connection, message.content.strip()[1:])
                        answer = pd.DataFrame(ans, columns=columns)
                        answer.index += 1
                        pd.set_option('display.max_rows', 10)
                        await message.channel.send(f"```{answer}```")
                        pd.reset_option('display.max_rows')

                        connection.rollback()
                    disconnect(connection)
                return

        elif message.channel.id == channels['ledgers']:
            game_channel = client.get_channel(channels['game'])

            if message.author == client.user:
                return
            elif not message.attachments:
                return
            else:
                game_link = 'Cannot find game'
                if message.content:
                    game_link = f"{message.content}"
                else:
                    async for entry in game_channel.history():
                        if entry.content.split()[-1].startswith('https'):
                            game_link = entry.jump_url
                        break

                await message.channel.send(f'Ledger for: {game_link}')

                for screen_shot in message.attachments:
                    await message.channel.send(file=await screen_shot.to_file())

                await message.delete()

                return

        elif message.channel.id == channels['graph_test'] or message.channel.id == channels['graph']:
            # prevent loop
            if message.author == client.user:
                return

            # start to generate chart(s) of provided game
            if len(message.attachments) == 2:
                attachment_one = message.attachments[0]
                attachment_two = message.attachments[1]

                game_channel = client.get_channel(channels['game'])

                # gathering #game message of last/given session
                game_link = 'Cannot find game'
                game_url = None

                if message.content:
                    game_link = message.content.split()[0]
                    game_link_message = await game_channel.fetch_message(int(game_link.rpartition('/')[2]))
                    game_url = game_link_message.content.split()[-1].rpartition('/')[2]
                else:
                    # loop through game to find last game
                    async for entry in game_channel.history():
                        if entry.content.split()[-1].startswith('https'):
                            game_link = entry.jump_url
                            game_url = entry.content.split()[-1].rpartition('/')[2]
                            break
                # using graph.graph_session to check if both are csv and order files
                # not sure how it would work if provided non-corresponding files
                # locally created profits.png and stacks.png
                # returns true if received a log and ledger file (just checking if both csv), false otherwise
                graph_session = await graph.graph_message(attachment_one, attachment_two, game_url)

                if graph_session:
                    # game_channel = client.get_channel(game)

                    # if os.path.isfile('profits.png') and os.path.isfile('stacks.png'):
                    if os.path.isfile('profits.png'):
                        # File object causes error if created and unused, so I commented out the unused stacks.png
                        # According to API the File object is only to be used once
                        profits_file = discord.File('profits.png')
                        # stacks_file = discord.File('stacks.png')

                        # Send images separately for full picture in channel
                        await message.channel.send(f'Profits for: {game_link}', file=profits_file)
                        # await message.channel.send(file=stacks_file) // see above
                        os.remove("profits.png")
                        # os.remove("stacks.png") // see above again
                        await message.delete()
                        return

                    else:
                        await message.channel.send(f'error generating plots')
                else:
                    await message.channel.send(f'Please attach the log and ledger .csv files for the session, '
                                               'include game link in message')
            else:
                await message.channel.send(f'Please attach the log and ledger .csv files for the session')

            await message.delete()
            return

        elif message.channel.id == channels['email_database']:
            guild = client.get_guild(misc['server'])
            member_id = message.content.split()[0][2:-1]
            email_needed_role = guild.get_role(roles['email needed'])
            member = await guild.fetch_member(int(member_id))

            # remove email needed role
            await member.remove_roles(email_needed_role)
            return

        elif message.channel.id == channels['game']:
            if message.author == client.user:
                return

            elif not message.content.startswith('https://www.pokernow.club/games/'):
                # message at the top
                # game_instructions = misc['game_instructions']

                wrong_format = f'Just paste a pokernow link in the channel.' \
                               '\ni.e. <https://www.pokernow.club/games/pg2Hn5EKpaXmJLy3dap0hfp4k>'
                wrong_format_message = await message.channel.send(wrong_format)

                await message.delete()
                await wrong_format_message.delete(delay=5)

                return

            else:
                role = f"<@&{roles['fiend']}>"
                link = message.content.split()[0]
                email_database_channel = client.get_channel(channels['email_database'])

                async for entry in email_database_channel.history():
                    entry_email = entry.content.split()

                    if len(entry_email) == 1:
                        if entry.author.id == message.author.id:
                            gmail = entry_email[0]
                            # send_message(message, f'{link} {gmail} \n', is_private=False, is_link=True)
                            bot_link = await message.channel.send(f'{role} {gmail} \n{link}')

                            thread = await bot_link.create_thread(name="Verification", auto_archive_duration=1440)

                            if len(message.content.split()) > 1:
                                await thread.send(f'{message.content.partition(" ")[2]}')

                        await message.delete()
                        return

                    elif entry_email[0] == f'<@{message.author.id}>':
                        gmail = entry_email[1]

                        bot_link = await message.channel.send(f'{role} {gmail} \n{link}')

                        thread = await bot_link.create_thread(name="Verification", auto_archive_duration=1440)

                        if len(message.content.split()) > 1:
                            await thread.send(f'{message.content.partition(" ")[2]}')

                        await message.delete()
                        return

                missing_email = f"Lobby creator must have an email for e-transferring in the server." \
                    f"\nAdd one to <#{channels['email']}> or contact an <@&{roles['admin']}> if you dont have access."
                await message.channel.send(missing_email)
                await message.delete()

                return

    client.run(os.getenv('DISCORD_BOT_TOKEN'))
