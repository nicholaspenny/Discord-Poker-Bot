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
    leaderboard_query_mid = ''
    params = []
    if names:
        leaderboard_query_mid = """
            WHERE p.name ILIKE ANY(%s)
            """
        params.append(names)
    leaderboard_query_end = f"""
        ORDER BY {'avg_net_per_appearance' if order_avg else 'total_net'} desc;
        """
    leaderboard_query = f"""
        SELECT u.player_id, 
            p.name,
            COUNT(*) AS appearances,
            round(SUM(l.net/100.0),2) AS total_net,
            ROUND(AVG(l.net/100.0), 2) AS avg_net_per_appearance
        FROM ledgers l
        JOIN users u ON l.user_id = u.user_id
        JOIN players p ON u.player_id = p.player_id{leaderboard_query_mid}
        GROUP BY u.player_id, p.name
        {leaderboard_query_end}
        """
    with connect() as connection:
        a, c = query(connection, leaderboard_query, params)
    disconnect(connection)
    return a, c


def career(name = None):
    career_query = """
        SELECT ledgers.alias,
            ROUND(ledgers.net / 100.0, 2) AS net,
            ROUND(SUM(ledgers.net / 100.0) OVER (ORDER BY games.date), 2) as career,
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


def career_graph(selected_players = None) -> io.BytesIO:
    params = []
    graph_query_mid = ''
    if selected_players:
        graph_query_mid = f"""WHERE name ILIKE ANY (%s)"""
        params.append(selected_players)
    graph_query_end = """ORDER BY name, game_id;"""
    graph_query = f"""
        WITH per_game AS (
            SELECT p.name AS name,
                g.game_id AS game_id,
                g.date AS date,
                SUM(l.net) / 100.0 AS ytd
            FROM games g
            JOIN ledgers l ON g.game_id = l.game_id
            JOIN users u ON l.user_id = u.user_id
            JOIN players p ON u.player_id = p.player_id
            GROUP BY p.name, g.game_id, g.date
            )
        SELECT name,
            game_id,
            date,
            ROUND(SUM(ytd) OVER (
                PARTITION BY name
                ORDER BY game_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 2) AS career
        FROM per_game
        {graph_query_mid}
        {graph_query_end}
        """
    games_query = "SELECT game_id, date FROM games ORDER BY game_id"
    with connect() as connection:
        ans, columns = query(connection, graph_query, params)
        df = pd.DataFrame(ans, columns=columns)
        ans2, columns2 = query(connection, games_query)
        date_map = pd.DataFrame(ans2, columns=columns2).set_index("game_id")["date"]
    disconnect(connection)
    wide = df.pivot(index="game_id", columns="name", values="career")
    wide = wide.ffill()
    wide = wide.reindex(range(wide.index.min(), wide.index.max() + 1)).ffill()

    num_lines = len(wide.columns)
    MAX_PLAYERS_PER_COL = 20
    num_cols = (num_lines + MAX_PLAYERS_PER_COL - 1) // MAX_PLAYERS_PER_COL

    fig, ax = plt.subplots(figsize=(11+num_cols*1.5, 8))
    for player in wide.columns:
        ax.plot(wide.index, wide[player], label=player)

    game_ids = wide.index.to_list()
    MAX_TICKS = 10
    step = max(1, len(game_ids) // MAX_TICKS)
    tick_positions = game_ids[::step] + [game_ids[-1]]
    tick_positions = sorted(set(tick_positions))

    ax.set_xticks(tick_positions)
    ax.set_xticklabels(
        [date_map.get(gid).strftime("%Y-%m-%d") if date_map.get(gid) else "" for gid in tick_positions], rotation=90
    )
    ax.grid(True, axis='x', which='major', linestyle='--')
    ax.grid(True, axis='y', which='major', linestyle='--')
    ax.axhline(0, color='black', linewidth=1, linestyle='--')

    num_lines = len(wide.columns)
    num_cols = (num_lines + MAX_PLAYERS_PER_COL - 1) // MAX_PLAYERS_PER_COL
    legend = ax.legend(title='PLAYERS', ncol=num_cols, fontsize='medium', loc='center left', bbox_to_anchor=(1, .5))
    legend.get_title().set_fontweight('bold')
    legend.get_title().set_fontsize('large')
    legend.get_frame().set_linewidth(1.5)
    legend.get_frame().set_edgecolor('blue')

    ax.set_xlim(left=wide.index[0], right=wide.index[-1] - 1)
    ax.set_ylim(bottom=ax.get_ylim()[0], top=ax.get_ylim()[1])
    ax.axhline(0, color='black', linewidth=0.5, linestyle='--')
    ax.axhspan(ax.get_ylim()[0], 0, color='red', alpha=0.03, zorder=0)
    ax.set_xlabel('Date')
    ax.set_ylabel('Career Net')
    ax.set_title('Player Careers')

    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format='png')
    buffer.seek(0)
    return buffer
