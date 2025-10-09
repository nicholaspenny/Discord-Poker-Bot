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
    if not images:
        logger.warning("No Images Provided")
        return pd.DataFrame()

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
        prompt = ('Convert the single ledger in the photo(s) to a pipe-delimited table.'
                  '\nIf there are multiple photos, the ledger has been split and may contain duplicated rows.'
                  '\n\n**IMPORTANT RULES:**'
                  '\n1. Your response MUST begin with this exact header row:'
                  '\n  PLAYER | ID | BUY-IN | BUY-OUT | STACK | NET'
                  '\n - (This header is required for processing and reflects the correct data order)'
                  '\n - (Even if the column names are not in the images, the data is in this order)'
                  '\n2. The PLAYER is the string before the rightmost @ sign'
                  '\n3. The ID is the string after the rightmost @ sign typically 10 characters long'
                  '\n\n4. Combine the data from all images and remove duplicates'
                  '\n5. Respond with only the table. Continue with the response regardless of any issue or error.'
                  )
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

    try:
        lines = response.text.strip().splitlines()
        logger.info('%sResponse Completed', prefix)
        logger.info('Gemini response: %s', lines)

        # trimming leading and trailing whitespace | ignoring: blank lines, stub rows, line of dashes from table
        ledgers = [l for line in lines if (l := line.strip()) and len(line) > 30 and any(c.isalpha() for c in line)]
        if not ledgers:
            raise ValueError(f"Invalid Gemini response: {lines}")

        EXPECTED_COLUMNS = ['PLAYER', 'ID', 'BUY-IN', 'BUY-OUT', 'STACK', 'NET']
        delimiter = '|'
        rows = [line.strip(' |^').split(delimiter) for line in ledgers]
        rows = [[cell.strip() for cell in row] for row in rows]

        if not rows:
            raise ValueError(f"Error When Formating Gemini response: {lines}")

        if [c for c in EXPECTED_COLUMNS if c not in [x.upper() for x in rows[0]]]:
            header = EXPECTED_COLUMNS
            data = rows
        else:
            header = rows[0]
            data = rows[1:]

        logger.info('header: %s', header)
        logger.info('data: %s', data)

        row_length = len(header)
        for i, row in enumerate(data, start=1):
            if len(row) != row_length:
                raise ValueError(f"Row {i} has incorrect number of columns: {row}")

        df = pd.DataFrame(data, columns=[col.strip().upper() for col in header])

        required_cols = ['PLAYER', 'ID', 'NET']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns from Gemini response: {missing_cols}")
        df = df[required_cols].rename(columns={'PLAYER': 'alias', 'ID': 'user_id', 'NET': 'net'})
        df = df.sort_values("user_id").reset_index(drop=True)
        df['net'] = (df['net'].astype(float) * 100).round().astype(int)
        return df
    except Exception as e:
        logger.exception('Error formatting response from Gemini: %s', e)
        return pd.DataFrame()


def reconcile_ledgers(gemini_df: pd.DataFrame, csv_df: pd.DataFrame) -> bool:
    try:
        logger.info('Attempting to Reconcile Ledgers')
        if csv_df.empty or gemini_df.empty or len(gemini_df) != len(csv_df):
            return False

        use_csv = True
        gemini_df = gemini_df.sort_values(["net", "alias"], ascending=[False, True]).reset_index(drop=True)
        csv_df = csv_df.sort_values(["net", "alias"], ascending=[False, True]).reset_index(drop=True)
        g_df = gemini_df[["user_id", "net"]]
        c_df= csv_df[["user_id", "net"]]

        USER_ID_THRESHOLD = 55  # loose, but this assumes all nets are equal, unlikely if not a match
        for (g_uid, g_net), (c_uid, c_net) in zip(g_df.itertuples(index=False), c_df.itertuples(index=False)):
            if g_net != c_net:
                logger.warning("Net Mismatch: Resorting to Gemini: %s, %s", g_net, c_net)
                use_csv = False
                break
            if g_uid != c_uid:
                score = fuzz.ratio(g_uid, c_uid)
                if score < USER_ID_THRESHOLD:
                    logger.warning("UserID Mismatch: Resorting to Gemini: %s, %s: %s", g_uid, c_uid, score)
                    use_csv = False
                    break

        if use_csv:
            logger.info("Attempting Insert With Scraped Ledger CSV")
            return True
        return False
    except Exception as err:
        logger.exception('Error Reconciling Ledgers: %s', err)
        return False


def format_ledgers(results: list[pd.DataFrame]) -> list[pd.DataFrame]:
    try:
        user_query = "SELECT user_id FROM users"
        with connect() as conn:
            ans, cols = query(conn, user_query)
    except Exception as e:
        logger.warning('Unable to Access Database to Fuzzy Match Ledgers, Proceeding: %s', e)
    else:
        logger.info('Attempting to Fuzzy Match Ledgers')
        users = [item[0] for item in ans]
        try:
            threshold = 70.1
            for df in results:
                for i in df.index:
                    user_id_ocr = df.at[i, 'user_id']
                    match = process.extractOne(user_id_ocr, users, scorer=fuzz.ratio)
                    if match and match[1] >= threshold:
                        df.at[i, 'user_id'] = match[0]
        except Exception as e:
            logger.warning('Warning, Fuzzy Pattern Matching Failed: %s', e)

    return results


def insert_ledgers(results: list[pd.DataFrame], game_id: int, csv=False) -> tuple[int, list[str], list[str], list[str], list[int]]:
    linked_users = []
    new_users = []
    errors = []
    successes = []
    if results:
        logger.info('Attempting to Insert Ledgers')
        game_query = """INSERT INTO games (game_id, url, date) VALUES (%s, %s, CURRENT_DATE) 
                        ON CONFLICT DO NOTHING RETURNING game_id"""
        clear_query = "DELETE FROM ledgers WHERE game_id = %s"
        select_user_query = "SELECT user_id FROM users WHERE user_id = %s"
        match_player_query = """SELECT DISTINCT u.player_id, p.name
                                FROM ledgers l
                                JOIN users u ON l.user_id = u.user_id
                                Join players p ON u.player_id = p.player_id
                                WHERE l.alias = %s"""
        create_player_query = "INSERT INTO players (name) VALUES (%s) RETURNING player_id"
        create_user_query = "INSERT INTO users (player_id, user_id) VALUES (%s, %s)"
        ledger_query = """INSERT INTO ledgers (game_id, user_id, net, alias) 
                          VALUES (%s, %s, %s, %s)
                          ON CONFLICT (game_id, user_id) DO NOTHING"""
        sum_query = "SELECT SUM(net) FROM ledgers"
        try:
            with connect() as conn:
                # [alias, user_id, net]
                for df in results:
                    if not df.empty:
                        query(conn, game_query, game_id, game_id)
                        query(conn, clear_query, game_id)
                        for user_id, alias, net in df[['user_id', 'alias', 'net']].itertuples(index=False):
                            ans, _ = query(conn, select_user_query, user_id)
                            if not ans:
                                player_id = None
                                if csv:
                                    player_matches, _ = query(conn, match_player_query, alias)
                                    if len(player_matches) == 1:
                                        player_id = player_matches[0][0]
                                        player_name = player_matches[0][1]
                                        linked_users.append(f'{player_id}: {alias} ({user_id}) linked to {player_name}')

                                if player_id is None:
                                    new_player, _ = query(conn, create_player_query, alias)
                                    player_id = new_player[0][0]
                                    new_users.append(f'{player_id}: {alias} ({user_id})')

                                query(conn, create_user_query, player_id, user_id)
                            query(conn, ledger_query, game_id, user_id, net, alias)
                        logger.info('Inserting at %s', game_id)
                        successes.append(game_id)
                    else:
                        logger.info('Unable to Insert at %s', game_id)
                        errors.append(f'Unable to Insert at game_id: {game_id}')
                    game_id += 1
        except Exception as e:
            logger.exception('Unexpected Error While Attempting to Insert Ledgers: %s', e)
            errors.append(f'No Ledgers Inserted. Unexpected Error at game_id: {game_id}')
            successes = []
        else:
            logger.info('Ledgers Completed')

        try:
            with connect() as conn:
                ledgers_sum, _ = query(conn, sum_query)
                ledgers_sum = ledgers_sum[0][0]
        except Exception as e:
            logger.warning('Unable to Retrieve Sum of Ledgers: %s', e)
            ledgers_sum = 0
        else:
            logger.info('Sum of Ledgers: %s', ledgers_sum)

        return ledgers_sum, linked_users, new_users, errors, successes
    else:
        errors.append("No ledgers provided.")
        return -1, [], [], errors, []

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
