import discord
import os
import graph
import asyncio
from connect import connect, disconnect, query
import pandas as pd
from discord.ext import commands


def run_discord_bot():
    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        print(f'{client.user} is now running from PC')

    @client.event
    async def on_message(message):
        # Active Servers
        server_id = 1126305350736424980

        # used channels
        game = 1126317888991678516
        # game_channel = client.get_channel(game)
        email = 1129582722067726346
        # email_channel = client.get_channel(email)
        email_database = 1129583460223295630
        # email_database_channel = client.get_channel(email_database)
        database_channel_id = 1199552447279018074
        # database_channel = client.get_channel(database_channel_id)
        ledgers = 1126305351243944069
        #ledgers_channel = client.get_channel(ledgers)
        graph_test = 1130374032353665024
        # graph_test_channel = client.get_channel(graph_test)
        game_graph = 1185427411219779594
        # game_graph_channel = client.get_channel(game_graph)
        server_check = 1186201448502009896
        #server_check_channel = client.get_channel(server_check)

        # guild = client.get_guild(server_id) // must get guild to access role objects

        # role ids
        email_needed_role_id = 1129593395925499934
        # email_needed role = guild.get_role(email_needed_role_id)
        fiend_role_id = 1130361475270201424
        # fiend_role = guild.get_role(fiend_role_id)
        admin_role_id = 1126323029987831879
        # admin_role = guild.get_role(admin_role_id)

        username = str(message.author)
        user_message = str(message.content)
        channel = str(message.channel)

        if message.channel.id == email:
            email_database_channel = client.get_channel(email_database)

            # loop through email-database to remove old email if present
            async for entry in email_database_channel.history():
                name = entry.content.split()[0]

                if name == f'<@{message.author.id}>':
                    await entry.delete()

            # add new email to email_database
            await email_database_channel.send(f'<@{message.author.id}> {message.content}')
            return

        elif message.channel.id == server_check:
            # verifying the bot is running and from the correct location
            if message.author == client.user:
                return
            else:
                await message.channel.send('Operating out of PC')

                if message.content.startswith('!'):
                    await message.channel.send(message.content)

                return

        elif message.channel.id == database_channel_id:
            if message.author == client.user:
                return
            else:
                #change to have preset queries and user inputed values
                if message.content.strip().startswith('!') and message.content.strip().endswith(';'):
                    connection = connect()
                    ans, columns = query(connection, message.content.strip()[1:])
                    answer = pd.DataFrame(ans, columns=columns)
                    answer.index += 1
                    pd.set_option('display.max_rows', 10)
                    await message.channel.send(f"```{answer}```")
                    pd.reset_option('display.max_rows')
                    disconnect(connection)
                return

        elif message.channel.id == ledgers:
            game_channel = client.get_channel(game)

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

        elif message.channel.id == graph_test or message.channel.id == game_graph:
            # prevent loop
            if message.author == client.user:
                return
            error_message = None

            # start to generate chart(s) of provided game
            if len(message.attachments) == 2:
                attachment_one = message.attachments[0]
                attachment_two = message.attachments[1]

                game_channel = client.get_channel(game)

                # gathering #game message of last/given session
                game_link = 'Cannot find game'
                game_url = None

                if message.content:
                    game_link = message.content.split()[0]
                    game_link_message = await game_channel.fetch_message(game_link.rpartition('/')[2])
                    game_url = game_link_message.content.split()[-1].rpartition('/')[2]
                else:
                    # loop through game to find last game
                    async for entry in game_channel.history():
                        if entry.content.split()[-1].startswith('https'):
                            game_link = entry.jump_url
                            game_url = entry.content.split()[-1].rpartition('/')[2]
                            break
                # using graph.graph_session to check if both are csv and order files
                # not sure how it would work if provided uncorresponding files
                # locally created profits.png and stacks.png
                # returns true if received a log and ledger file (just checking if both csv), false otherwise
                graph_session = await graph.graph_message(attachment_one, attachment_two, game_url)

                if graph_session:
                    game_channel = client.get_channel(game)

                    # if os.path.isfile('profits.png') and os.path.isfile('stacks.png'):
                    if os.path.isfile('profits.png'):
                        # File object causes error if created and unused so I commented out the unused stacks.png
                        # According to API the File object is only to be used once
                        profits_file = discord.File('profits.png')
                        #stacks_file = discord.File('stacks.png')


                        # Send images separately for full picture in channel
                        profits_message = await message.channel.send(f'Profits for: {game_link}', file=profits_file)
                        # stacks_message = await message.channel.send(file=stacks_file) // see above
                        os.remove("profits.png")
                        # os.remove("stacks.png") // see above again
                        await message.delete()
                        return

                    else:
                        error_message = await message.channel.send(f'error generating plots')
                else:
                    error_message = await message.channel.send(f'Please attach the log and ledger .csv files for the session, '
                    'include game link in message')
            else:
                error_message = await message.channel.send(f'Please attach the log and ledger .csv files for the session')

            await message.delete()
            return

        elif message.channel.id == email_database:
            guild = client.get_guild(server_id)
            member_id = user_message.split()[0][2:-1]
            email_needed = guild.get_role(email_needed_role_id)
            member = await guild.fetch_member(int(member_id))

            # remove email needed role
            await member.remove_roles(email_needed)
            return

        elif message.channel.id == game:
            if message.author == client.user:
                return

            elif not user_message.startswith('https://www.pokernow.club/games/'):
                # message at the top
                game_instructions = 1126317978057703484

                # send_message(message, user_message, is_private=False,is_link=False)
                wrong_format_reply = await message.channel.send(f'Just paste a pokernow link in the channel. '
                '\ni.e. <https://www.pokernow.club/games/pg2Hn5EKpaXmJLy3dap0hfp4k>')
                await message.delete()
                await asyncio.sleep(5)
                await wrong_format_reply.delete()
                return

            else:
                role = f'<@&{fiend_role_id}>'
                link = user_message.split()[0]
                email_database_channel = client.get_channel(email_database)
                gmail = 'missing'

                async for entry in email_database_channel.history():
                    entry_email = entry.content.split()

                    if len(entry_email) == 1:
                        if entry.author.id == message.author.id:
                            gmail = entry_email[0]
                            # send_message(message, f'{link} {gmail} \n', is_private=False, is_link=True)
                            bot_link = await message.channel.send(f'{role} {gmail} \n{link}')

                            thread = await bot_link.create_thread(name="Verification", auto_archive_duration=1440)

                            if len(user_message.split()) > 1:
                                await thread.send(f'{user_message.partition(" ")[2]}')

                        await message.delete()
                        return

                    elif entry_email[0] == f'<@{message.author.id}>':
                        gmail = entry_email[1]

                        bot_link = await message.channel.send(f'{role} {gmail} \n{link}')

                        thread = await bot_link.create_thread(name="Verification", auto_archive_duration=1440)

                        if len(user_message.split()) > 1:
                            await thread.send(f'{user_message.partition(" ")[2]}')

                        await message.delete()
                        return

                await message.channel.send(f'Lobby creator must have an email for e-transfering in the server.'
                f'\nAdd one to <#{email}> or contact an <@&{admin_role_id}> if you dont have access.')
                await message.delete()

                return

    client.run(os.getenv('DISCORD_BOT_TOKEN'))
