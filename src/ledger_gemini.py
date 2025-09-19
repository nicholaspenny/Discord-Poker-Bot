import argparse
import logging
import mimetypes
import os
import threading
import time

from dotenv import load_dotenv
from google import genai
import pandas as pd
from rapidfuzz import fuzz, process

from src.connect import connect, query

load_dotenv()

logger = logging.getLogger(__name__)

done = False


def gemini(images: list[tuple[bytes, str]], game_id=None) -> pd.DataFrame:
    prefix = f'{game_id}: ' if game_id is not None else ''
    try:
        GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

        if GEMINI_API_KEY is None:
            logger.exception('Error: GOOGLE_API_KEY environment variable not set.')

        try:
            client = genai.Client()
        except Exception as e:
            logger.exception("Failed to create GenAI client. Ensure GEMINI_API_KEY is set. Error: %s", e)
            return pd.DataFrame()

        image_parts = []
        for img_bytes, mime_type in images:
            image_parts.append(
                genai.types.Part.from_bytes(data=img_bytes, mime_type=mime_type)
            )
        # Prompt for the vision model
        prompt = ('Convert the single ledger in the photo(s) to a table. If there are multiple photos, '
                  'the ledger has been split and may or may not contain duplicated rows across the photos.'
                  '\nColumns: PLAYER, ID, BUY-IN, BUY-OUT, STACK, NET; '
                  'where player is the string before the @ sign, and id is the string after the @ sign.'
                  f'\nRespond with only the table, continue with output regardless of any issue or error.')
        #prompt = 'What do you see in these photos'
        text_part = genai.types.Part.from_text(text=prompt)
        contents = [
            text_part,
            image_parts
        ]
        logger.info('%sSending %s image(s) with default prompt.', prefix, len(images))
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=contents
        )
    except Exception as e:
        logger.exception('%sError generating Gemini response. %s', prefix, e)
        return pd.DataFrame()
    df2 = None
    try:
        lines = response.text.strip().splitlines()
        logger.info('%sResponse Completed', prefix)

        # trimming leading and trailing whitespace | ignoring: blank lines, stub rows, line of dashes from table
        ledgers = [l for line in lines if (l := line.strip()) and len(line) > 30 and any(c.isalpha() for c in line)]
        rows = [line.strip('|').split('|') for line in ledgers]
        rows = [[cell.strip() for cell in row] for row in rows]
        df = pd.DataFrame(rows[1:], columns=rows[0])
        df.columns = df.columns.str.upper()
        df = df.drop(columns=['BUY-IN']).drop(columns=['BUY-OUT']).drop(columns=['STACK'])
        df = df.rename(columns={'PLAYER': 'alias'}).rename(columns={'ID': 'user_id'}).rename(columns={'NET': 'net'})
        df['net'] = (df['net'].astype(float) * 100).round().astype(int)
        return df
    except Exception as e:
        logger.exception('Error formatting response from Gemini: %s', e)
        logger.info('Gemini Response: %s', df2)
        return pd.DataFrame()


def format_ledgers(data: list[pd.DataFrame]) -> list[pd.DataFrame]:
    try:
        user_query = """SELECT user_id FROM users;"""
        with connect() as connection:
            ans, cols = query(connection, user_query)
    except Exception as e:
        logger.warning('Unable to Access Users from Database: %s', e)
    else:
        users = [item[0] for item in ans]
        try:
            threshold = 70.1
            for df in data:
                for i in df.index:
                    user_id_ocr = df.at[i, 'user_id']
                    match = process.extractOne(user_id_ocr, users, scorer=fuzz.ratio)
                    if match and match[1] >= threshold:
                        df.at[i, 'user_id'] = match[0]
        except Exception as e:
            logger.warning('Warning, Fuzzy Pattern Matching Failed: %s', e)

    return data


def insert_ledgers(results: list[pd.DataFrame], game_id: int) -> tuple[int, list[str], list[str], bool]:
    errors = []
    new_users = []
    success = True
    if results:
        game_query = """INSERT INTO games (game_id, url, date) VALUES (%s, %s, CURRENT_DATE) 
                            ON CONFLICT DO NOTHING RETURNING game_id;"""
        clear_query = """DELETE FROM ledgers WHERE game_id = %s;"""
        select_user_query = """SELECT user_id FROM users WHERE user_id = %s;"""
        create_player_query = """INSERT INTO players (name) VALUES (%s) RETURNING player_id;"""
        create_user_query = """INSERT INTO users (player_id, user_id) VALUES (%s, %s);"""
        ledger_query = """INSERT INTO ledgers (game_id, user_id, net, alias) 
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (game_id, user_id) DO NOTHING;"""
        sum_query = """SELECT SUM(net) FROM ledgers"""
        try:
            with connect() as connection:
                # [alias, user_id, net]
                for df in results:
                    if not df.empty:
                        query(connection, game_query, game_id, game_id)
                        query(connection, clear_query, game_id)
                        for row in df.itertuples(index=False):
                            ans1, cols1 = query(connection, select_user_query, row.user_id)
                            if not ans1:
                                ans2, cols2 = query(connection, create_player_query, row.alias)
                                query(connection, create_user_query, ans2[0], row.user_id)
                                new_users.append(f'{ans2[0]}: {row.alias} ({row.user_id})')
                            query(connection, ledger_query, game_id, row.user_id, row.net, row.alias)
                        logger.info('Inserting at %s', game_id)
                    else:
                        logger.info('Unable to Insert at %s', game_id)
                        errors.append(f'Unable to Insert at game_id: {game_id}')
                    game_id += 1
        except Exception as e:
            logger.exception('Unexpected Error While Attempting to Insert Ledgers: %s', e)
            errors.append(f'No Ledgers Inserted. Unexpected Error at game_id: {game_id}')
            success = False
        else:
            logger.info('Ledgers Completed')

        try:
            with connect() as connection:
                ledgers_sum, _ = query(connection, sum_query)
                ledgers_sum = ledgers_sum[0][0]
        except Exception as e:
            logger.warning('Unable to Retrieve Sum of Ledgers: %s', e)
            ledgers_sum = 0
        else:
            logger.info('Sum of Ledgers: %s', ledgers_sum)

        return ledgers_sum, new_users, errors, success
    else:
        errors.append("No ledgers provided.")
        success = False
        return -1, [], errors, success

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
        mime_type, _ = mimetypes.guess_type(img)
        with open(img, "rb") as f:
            img_bytes = f.read()
        image_list.append((img_bytes, mime_type))

    global done
    done = False
    t = threading.Thread(target=spinner)
    t.start()
    response = gemini(image_list)
    done = True
    t.join()

    if game_id is not None:
        ledgers_sum, _, _, _ = insert_ledgers(format_ledgers([response]), game_id)
        if ledgers_sum:
            print(ledgers_sum)
    else:
        print()
        if response[1]:
            print('Error!')
        print(response[0])


if __name__ == '__main__':
    main()
