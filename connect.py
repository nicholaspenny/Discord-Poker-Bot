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


def query(connection, command, *args):
    value = None
    columns = None

    try:
        cursor = connection.cursor()
        cursor.execute(command, args)
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

