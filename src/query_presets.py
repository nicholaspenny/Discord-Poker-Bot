import io
import logging

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import pandas as pd

from src.connect import connect, query
from src.common import timezone

logger = logging.getLogger(__name__)


def players():
    players_query = "SELECT name from players ORDER BY LOWER(name)"
    with connect() as conn:
        a, c = query(conn, players_query)
    return a, c


def leaderboard(names = None, order_avg=False):
    leaderboard_query_mid = ''
    if names:
        leaderboard_query_mid = """
            WHERE p.name ILIKE ANY(%s)
            """
    leaderboard_query_end = f"""
        ORDER BY {'avg_net' if order_avg else 'total_net'} desc
        """
    leaderboard_query = f"""
        SELECT u.player_id,
            p.name,
            COUNT(*) AS sessions,
            round(SUM(l.net/100.0),2) AS total_net,
            ROUND(AVG(l.net/100.0), 2) AS avg_net
        FROM ledgers l
        JOIN users u ON l.user_id = u.user_id
        JOIN players p ON u.player_id = p.player_id{leaderboard_query_mid}
        GROUP BY u.player_id, p.name
        {leaderboard_query_end}
        """
    with connect() as conn:
        a, c = query(conn, leaderboard_query, *([names] if names else []))
    return a, c


def career(player_id: int):
    tz_clause = f" AT TIME ZONE '{timezone}'" if timezone else ''
    career_query = f"""
        SELECT ledgers.alias,
            ROUND(ledgers.net / 100.0, 2) AS net,
            ROUND(SUM(ledgers.net / 100.0) OVER (ORDER BY games.date), 2) AS career,
            TO_CHAR(games.date{tz_clause}, 'YYYY-MM-DD HH24:MI') AS date
        FROM ledgers
        JOIN users ON users.user_id = ledgers.user_id
        JOIN players ON players.player_id = users.player_id
        JOIN games ON ledgers.game_id = games.game_id
        WHERE players.player_id = %s
        ORDER BY games.date DESC
        """

    with connect() as conn:
        answer, columns = query(conn, career_query, player_id)
        narrowed = [(a[:13] + '...' if a and len(a) > 16 else a, n, c, d) for a, n, c, d in answer]
    return narrowed, columns


def grapher(grapher_query, title='', *args):
    games_query = "SELECT game_id, date FROM games ORDER BY game_id"
    with connect() as conn:
        ans, columns = query(conn, grapher_query, args)
        df = pd.DataFrame(ans, columns=columns)
        ans2, columns2 = query(conn, games_query)
        date_map = pd.DataFrame(ans2, columns=columns2).set_index("game_id")
        date_map['date'] = date_map['date'].astype('datetime64[ns, UTC]')
        if timezone:
            try:
                date_map['date'] = date_map['date'].dt.tz_convert(timezone)
            except Exception as err:
                logging.warning(f"Invalid timezone '{timezone}', keeping UTC. Error: {err}")

    if df.empty:
        return None

    game_counts = df['player_id'].value_counts()
    filtered_players = game_counts.head(14).index
    df_filtered = df[df['player_id'].isin(filtered_players)]
    wide = df_filtered.pivot(index="game_id", columns=["player_id", "name"], values="career").sort_index()
    min_gid, max_gid = wide.index.min(), wide.index.max()
    all_gids = sorted([gid for gid in date_map.index.to_list() if min_gid <= gid <= max_gid])
    x0_gid = min_gid - 1
    games = len(all_gids)

    wide.loc[wide.index[0] - 1] = 0
    wide = wide.sort_index()
    wide = wide.ffill().fillna(0)

    num_lines = len(wide.columns)
    MAX_PLAYERS_PER_COL = 20
    num_cols = (num_lines + MAX_PLAYERS_PER_COL - 1) // MAX_PLAYERS_PER_COL
    fig, ax = plt.subplots(figsize=(11 + num_cols * 1.5, 8))
    for player_id, name in wide.columns:
        count = df_filtered['player_id'].eq(player_id).sum()
        ax.plot(wide.index, wide[(player_id, name)], label=f'{name} - {count}')

    MAX_TICKS = 15
    step = (games - 1 + MAX_TICKS - 1) //(MAX_TICKS - 1)

    ticks = [x0_gid] + all_gids
    major_ticks = ticks[:-1:step] + [max_gid]
    tick_date_map = [min_gid] + major_ticks[1:]
    tick_dates = [
        date_map['date'].get(gid).strftime("%Y-%m-%d") if gid in date_map.index else f'Game {gid}'
        for gid in tick_date_map
    ]

    ax.set_xticks(major_ticks)
    ax.set_xticklabels(tick_dates, rotation=90)
    ax.grid(True, axis='x', which='major', linestyle='--')
    ax.grid(True, axis='y', which='major', linestyle='--')

    previous_year = None
    for game in ticks:
        date = date_map['date'].get(game if game != x0_gid else min_gid)
        if date and (previous_year is None or date.year > previous_year):
            ax.axvline(x=game, color='gray', linestyle='--', linewidth=1.5, zorder=0)
            ax.text(game, ax.get_ylim()[1] * 0.975 + ax.get_ylim()[0] * 0.025, str(date.year),
                    rotation=90, verticalalignment='top', horizontalalignment='left',
                    color='gray', fontsize=30)
            previous_year = date.year

    legend = ax.legend(title='PLAYERS', ncol=num_cols, fontsize='medium', loc='center left', bbox_to_anchor=(1, .5))
    legend.get_title().set_fontweight('bold')
    legend.get_title().set_fontsize('large')
    legend.get_frame().set_linewidth(1.5)
    legend.get_frame().set_edgecolor('blue')

    ax.xaxis.set_minor_locator(AutoMinorLocator(5))
    ax.yaxis.set_minor_locator(AutoMinorLocator(5))
    ax.set_xlim(left=wide.index[0], right=wide.index[-1])
    ax.set_ylim(bottom=ax.get_ylim()[0], top=ax.get_ylim()[1])
    ax.axhline(0, color='black', linewidth=0.8, linestyle='--')
    ax.axhspan(ax.get_ylim()[0], 0, color='red', alpha=0.03, zorder=0)
    ax.grid(True, linestyle='-', color='gray', alpha=0.5)
    ax.grid(True, which='minor', linestyle=':', linewidth=0.5, color='gray', alpha=0.6)
    ax.set_xlabel('Date')
    ax.set_ylabel('Career Net')
    ax.set_title(f'{title}: {games} Games')

    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format='png')
    buffer.seek(0)
    return buffer

def recent_graph(selected_players=None, days=30) -> io.BytesIO:
    tz_clause = f" AT TIME ZONE '{timezone}'" if timezone else ''

    graph_query_mid = ''
    if selected_players:
        graph_query_mid = "WHERE name ILIKE ANY (%s)"

    date_filter = f"g.date >= NOW() - INTERVAL '{days} days'"
    recent_query = f"""
        WITH recent_games AS (
            SELECT p.player_id AS player_id,
                   p.name AS name,
                   g.game_id AS game_id,
                   TO_CHAR(g.date{tz_clause}, 'YYYY-MM-DD HH24:MI') AS date,
                   SUM(l.net) / 100.0 AS ytd
            FROM games g
            JOIN ledgers l ON g.game_id = l.game_id
            JOIN users u ON l.user_id = u.user_id
            JOIN players p ON u.player_id = p.player_id
            WHERE {date_filter}
            GROUP BY p.player_id, g.game_id, g.date
        ),
        active_players AS (
            SELECT DISTINCT player_id, name
            FROM recent_games
            {graph_query_mid}
        )
        SELECT rg.player_id,
               rg.name,
               rg.game_id,
               rg.date,
               ROUND(SUM(rg.ytd) OVER (
                   PARTITION BY rg.player_id
                   ORDER BY rg.game_id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ), 2) AS career
        FROM recent_games rg
        JOIN active_players ap ON rg.player_id = ap.player_id
        ORDER BY player_id, game_id
    """
    return grapher(recent_query, f'Last {days} Days', *([selected_players] if selected_players else []))


def career_graph(selected_players = None) -> io.BytesIO:
    tz_clause = f" AT TIME ZONE '{timezone}'" if timezone else ''

    graph_query_mid = ''
    if selected_players:
        graph_query_mid = "WHERE name ILIKE ANY (%s)"

    graph_query_end = "ORDER BY player_id, game_id"
    graph_query = f"""
        WITH per_game AS (
            SELECT p.player_id as player_id,
                p.name AS name,
                g.game_id AS game_id,
                TO_CHAR(g.date{tz_clause}, 'YYYY-MM-DD HH24:MI') AS date,
                SUM(l.net) / 100.0 AS ytd
            FROM games g
            JOIN ledgers l ON g.game_id = l.game_id
            JOIN users u ON l.user_id = u.user_id
            JOIN players p ON u.player_id = p.player_id
            GROUP BY p.player_id, g.game_id, g.date
            )
        SELECT player_id,
            name,
            game_id,
            date,
            ROUND(SUM(ytd) OVER (
                PARTITION BY player_id
                ORDER BY game_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 2) AS career
        FROM per_game
        {graph_query_mid}
        {graph_query_end}
        """
    return grapher(graph_query, f'Domain', *([selected_players] if selected_players else []))
