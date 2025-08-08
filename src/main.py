import logging
from logging.handlers import RotatingFileHandler
import os

from src import bot

os.makedirs("./logs", exist_ok=True)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            RotatingFileHandler("./logs/bot.log", maxBytes=1_000_000, backupCount=3),
            logging.StreamHandler()
        ]
    )


def main():
    # run the bot
    setup_logging()

    logger = logging.getLogger(__name__)
    logger.info("Launching Discord Poker Bot")

    bot.run_discord_bot()


if __name__ == '__main__':
    main()

