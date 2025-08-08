import argparse
import logging
import os
import threading
import time
from typing import Optional

import google.generativeai as genai
import pandas as pd
from PIL import Image
from rapidfuzz import fuzz, process

from src.connect import connect, disconnect, query

logger = logging.getLogger(__name__)
done = False
ERROR_CODE = 'Error-0001'

def gemini(images: list[Image.Image], game_id=None) -> tuple[pd.DataFrame, Optional[bool]]:
    global ERROR_CODE
    prefix = f'{game_id}: ' if game_id is not None else ''
    try:
        GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

        if GEMINI_API_KEY is None:
            logger.error('Error: GOOGLE_API_KEY environment variable not set.')

        genai.configure(api_key=GEMINI_API_KEY)

        # Using the gemini-2.5-pro model for multimodal input
        vision_model = genai.GenerativeModel('gemini-2.5-pro')

        # Prompt for the vision model
        prompt = ('Convert the single ledger in the photo(s) to a table. If there are multiple photos, '
                  'the ledger has been split and may or may not contain duplicated rows across the photos.'
                  '\nColumns: PLAYER, ID, BUY-IN, BUY-OUT, STACK, NET; '
                  'where player is the string before the @ sign, and id is the string after the @ sign.'
                  '\nWhen completed, if the absolute value of sum[NET] is greater than 0.02, start your response with:'
                  f' "{ERROR_CODE} [Imbalance]:".\nRespond with only the table and error above it if applicable.')
        request = [prompt] + images
        logger.info('%sSending %s image(s) with default prompt.', prefix, len(images))
        response = vision_model.generate_content(request)
    except Exception as e:
        logger.error('%sError generating Gemini response. %s', prefix, e, exc_info=True)
        return pd.DataFrame(), None
    try:
        lines = response.text.strip().splitlines()
        error = ERROR_CODE in response.text
        status = 'Error: Imbalance' if error else 'Passed'
        logger.info('%sResponse Complete -> %s!', prefix, status)

        # trimming leading and trailing whitespace | ignoring: blank lines, error/stub rows, line of dashes from table
        ledgers = [l for line in lines if (l := line.strip()) and len(line) > 30 and any(c.isalpha() for c in line)]
        rows = [line.strip('|').split('|') for line in ledgers]
        rows = [[cell.strip() for cell in row] for row in rows]
        df = pd.DataFrame(rows[1:], columns=rows[0])
        df.columns = df.columns.str.upper()
        df = df.drop(columns=['BUY-IN']).drop(columns=['BUY-OUT']).drop(columns=['STACK'])
        df = df.rename(columns={'PLAYER': 'alias'}).rename(columns={'ID': 'user_id'}).rename(columns={'NET': 'net'})
        df['net'] = (df['net'].astype(float) * 100).round().astype(int)
        return df, error
    except Exception as e:
        logger.error('Error formatting response from Gemini: %s', e, exc_info=True)
        return pd.DataFrame(), None


def format_ledgers(data: list[tuple[pd.DataFrame, bool]]) -> list[tuple[pd.DataFrame, bool]]:
    try:
        user_query = """SELECT user_id FROM users;"""
        with connect() as connection:
            ans, cols = query(connection, user_query)
        disconnect(connection)
    except Exception as e:
        logger.warning('Unable to Access Users from Database: %s', e)
    else:
        users = [item[0] for item in ans]
        try:
            threshold = 76.0
            for pair in data:
                for i in pair[0].index:
                    user_id_ocr = pair[0].at[i, 'user_id']
                    match = process.extractOne(user_id_ocr, users, scorer=fuzz.ratio)
                    if match and match[1] >= threshold:
                        pair[0].at[i, 'user_id'] = match[0]
        except Exception as e:
            logger.warning('Warning, Fuzzy Pattern Matching Failed: %s', e)

    return data


def insert_ledgers(results: list[tuple[pd.DataFrame, bool]], game_id: int):
    if results:
        logger.info('Ledgers Completed')

        game_query = """INSERT INTO games (game_id, url, date) VALUES (%s, %s, CURRENT_DATE) 
                            ON CONFLICT DO NOTHING RETURNING game_id;"""
        clear_query = """DELETE FROM ledgers WHERE game_id = %s;"""
        select_user_query = """SELECT user_id FROM users WHERE user_id = %s;"""
        create_player_query = """INSERT INTO players (name) VALUES (%s) RETURNING player_id;"""
        create_user_query = """INSERT INTO users (player_id, user_id) VALUES (%s, %s);"""
        ledger_query = """INSERT INTO ledgers (game_id, user_id, net, alias) 
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (game_id, user_id) DO NOTHING;"""
        try:
            with connect() as connection:
                # [alias, user_id, net]
                for df, error in results:
                    query(connection, game_query, game_id, game_id)
                    query(connection, clear_query, game_id)
                    for row in df.itertuples(index=False):
                        ans1, cols1 = query(connection, select_user_query, row.user_id)
                        if not ans1:
                            ans2, cols2 = query(connection, create_player_query, row.alias)
                            query(connection, create_user_query, ans2[0], row.user_id)

                        query(connection, ledger_query, game_id, row.user_id, row.net, row.alias)
                    game_id += 1
                logger.info('Finished Inserting at %s', game_id - 1)
            disconnect(connection)
        except Exception as e:
            logger.error('Unable to Insert Ledgers: %s', e)


def spinner():
    global done
    while not done:
        for ch in '|/-\\':
            print(f'\rComputing, please wait... {ch}', end='', flush=True)
            time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser(description="Process images and optionally a game_id.")

    # Positional argument for image paths (one or more)
    parser.add_argument('images', nargs='+', help='Image file paths')
    # Optional game_id argument
    parser.add_argument('--game_id', type=int, help='Game ID (integer)')
    args = parser.parse_args()

    images = args.images
    game_id = args.game_id
    image_list = []
    for img in images:
        image = Image.open(img)
        image_list.append(image)

    global done
    done = False
    t = threading.Thread(target=spinner)
    t.start()
    response = gemini(image_list)
    done = True
    t.join()

    if game_id is not None:
        insert_ledgers(format_ledgers([response]), game_id)
    else:
        print()
        if response[1]: print('Error!')
        print(response[0])


if __name__ == '__main__':
    main()
