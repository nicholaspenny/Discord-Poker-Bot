Discord Poker Bot

This is an original Discord bot that serves as a multipurpose tool to assist with the running of poker sessions on pokernow.com.

It serves as a message interpreter, as a graph generator, and accesses a database containing player information, and history of previous sessions.

Setup

This implementation is always running as it is not using commands but instead responding to each message.

The bot is hosted on an AWS EC2 Instance and the database is in an RDS instance.

bot.py:
DISCORD_BOT_TOKEN is an environment variable that should be assigned to the token for your specific Discord bot.

database.ini:
Insert the appropriate values for \<endpoint\>, \<port\>, \<username\>, \<password\>, \<database\>
