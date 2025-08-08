import io
import logging

import matplotlib.pyplot as plt
import pandas as pd

from src.connect import connect, disconnect, query

logger = logging.getLogger(__name__)


def players():
    players_query = """SELECT name from players Order By name;"""
    with connect() as connection:
        a, c = query(connection, players_query)
    disconnect(connection)
    return a, c


def leaderboard(names = None, order_avg=False):
    with connect() as connection:
        query_start = f"""
            SELECT u.player_id, 
                p.name,
                COUNT(*) AS appearances,
                round(SUM(l.net/100.0),2) AS total_net,
                ROUND(AVG(l.net/100.0), 2) AS avg_net_per_appearance
            FROM ledgers l
            JOIN users u ON l.user_id = u.user_id
            JOIN players p ON u.player_id = p.player_id
            """
        query_middle = '\n'
        params = []
        if names:
            query_middle = '\nWHERE p.name ILIKE ANY(%s)'
            params.append(names)
        query_end = f"""
            GROUP BY u.player_id, p.name
            ORDER BY {'avg_net_per_appearance' if order_avg else 'total_net'} desc;
            """

        a, c = query(connection, query_start + query_middle + query_end, params)
    disconnect(connection)
    return a, c


def career(name = None):
    career_query = """
        SELECT ledgers.alias,
            ROUND(ledgers.net / 100.0, 2) AS net,
            ROUND(SUM(ledgers.net / 100.0) OVER (ORDER BY games.date), 2) as YTD,
            TO_CHAR(games.date, 'YYYY-MM-DD') as date
        FROM ledgers
        JOIN users ON users.user_id = ledgers.user_id
        JOIN players ON players.player_id = users.player_id
        JOIN games ON ledgers.game_id = games.game_id
        WHERE players.name ILIKE %s
        ORDER BY games.date;
        """

    if name:
        with connect() as connection:
            a, c = query(connection, career_query,f'%{name}%')
        disconnect(connection)
        return a, c
    else:
        return [], None



def ytd(selected_players = None) -> io.BytesIO:
    ytd_query = """
        SELECT players.name AS name,
            ledgers.net / 100.0 AS net,
            games.date AS date,
            ledgers.game_id AS game_id
        FROM ledgers
        JOIN users ON users.user_id = ledgers.user_id
        JOIN players ON players.player_id = users.player_id
        JOIN games ON ledgers.game_id = games.game_id
        ORDER BY players.name, games.date;
        """

    with connect() as connection:
        df = pd.read_sql(ytd_query, connection)
    disconnect(connection)

    df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None)
    df['date'] = df['date'].dt.floor('D')

    if selected_players:
        df = df[df['name'].str.lower().isin([name.lower() for name in selected_players])]
    df = df.sort_values(['name', 'date', 'game_id'])
    df['YTD'] = df.groupby('name')['net'].cumsum()

    df = df.sort_values(['name', 'game_id'])

    fig, ax = plt.subplots(figsize=(12, 9))

    min_game = df['game_id'].min()
    max_game = df['game_id'].max()
    xticks = list(range(min_game, max_game, 10))
    ax.set_xlim(min_game, max_game + 5)

    for name, group in df.groupby('name'):
        group = group.sort_values('game_id')
        x = group['game_id'].tolist()
        y = group['YTD'].tolist()

        ax.plot(x, y, label=name)

    xtick_labels = []
    for x in xticks:
        closest_row = df.iloc[(df['game_id'] - x).abs().argsort().iloc[0]]
        xtick_labels.append(closest_row['date'].strftime('%b %Y'))

    ax.set_xticks(xticks)
    ax.set_xticklabels(xtick_labels, rotation=45)

    ax.grid(True, axis='x', which='major', linestyle='--')
    ax.grid(False, axis='y')
    ax.axhline(0, color='black', linewidth=1)
    ax.legend(title='Player', bbox_to_anchor=(1.02, 1), loc='upper left')

    ax.set_xlabel('Date (by game_id)')
    ax.set_ylabel('YTD')
    ax.set_title('Player Career Graphs')

    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format='png')
    buffer.seek(0)
    return buffer
