import logging

import psycopg2

from src.config import config

logger = logging.getLogger(__name__)


def connect():
    try:
        params = config()
        connection = psycopg2.connect(**params)
        connection.autocommit = False
        return connection
    except Exception as err:
        logger.exception('Unable to Connect to the Database: %s', err)
        raise RuntimeError("Database connection failed") from err


def query(connection, command: str, *args):
    try:
        cursor = connection.cursor()
        cursor.execute(command, args)

        if cursor.description is not None:
            value = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
        else:
            value = []
            columns = None
        cursor.close()
    except Exception as err:
        logger.exception('Unable to Complete Database Query: %s\nQuery: %s', err, command)
        value = []
        columns = None
    return value, columns


def disconnect(connection):
    if connection is not None:
        try:
            connection.close()
        except Exception as err:
            logger.exception('Unable to Disconnect from the Database: %s', err)
