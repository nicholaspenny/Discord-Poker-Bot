import psycopg2
from config import config


def connect():
    connection = None
    try:
        params = config()
        connection = psycopg2.connect(**params)
        connection.autocommit = False
    except(Exception, psycopg2.DataError) as error:
        print(error)

    return connection


def query(connection, command: str, *args):
    try:
        cursor = connection.cursor()
        if args:
            cursor.execute(command, args)
        else:
            cursor.execute(command)

        value = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
    except(Exception, psycopg2.DataError) as error:
        value = error
        columns = None

    return value, columns


def disconnect(connection):
    if connection is not None:
        connection.close()
