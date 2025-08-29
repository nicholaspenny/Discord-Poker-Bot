import csv
import io
import logging
import os
import re
import sys
from typing import Optional

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

from src.connect import connect, disconnect, query

COLORS = [
    "Blue", "Red", "Lime", "Magenta", "Orange", "SaddleBrown", "Cyan",
    "DarkViolet", "Gray", "Green", "Gold", "Salmon", 'SkyBlue', 'Orchid',
]

plt.rcParams['axes.prop_cycle'] = plt.cycler(color=COLORS)

logger = logging.getLogger(__name__)


def graph_setup(csv1: bytes, csv2: bytes) -> Optional[io.BytesIO]:
    logger.info('Preparing CSVs For Graphing')
    try:
        csv_text_1 = csv1.decode('utf-8')
        csv_text_2 = csv2.decode('utf-8')
    except Exception as err:
        logger.warning("Failed to read CSVs: %s", err)
        return None

    csv_rows_1 = list(csv.reader(io.StringIO(csv_text_1)))
    csv_rows_2 = list(csv.reader(io.StringIO(csv_text_2)))

    ledger = None
    log = None
    if csv_rows_1 and csv_rows_1[0]:
        if csv_rows_1[0][0] == 'entry':
            log = csv_rows_1
        if csv_rows_1[0][0] == 'player_nickname':
            ledger = csv_rows_1
    if csv_rows_2 and csv_rows_2[0]:
        if csv_rows_2[0][0] == 'entry':
            log = csv_rows_2
        if csv_rows_2[0][0] == 'player_nickname':
            ledger = csv_rows_2

    if log and ledger:
        return graph(log, ledger)
    else:
        return None


def graph(log: list[list[str]], ledger: list[list[str]]) -> io.BytesIO:
    players = {}  # userid: alias
    transactions = {}  # userid: [pot net, stack change, stack change bool, street action]
    stack_sizes = {}  # userid: [stack at start of hand #]
    buy_ins = {}  # userid: [total net buy_in/buy_out at start of hand #]

    for row in ledger[1:]:
        players[row[1]] = row[0]
    for player in players.keys():
        transactions[player] = [0.0, 0.0, 0.0, 0.0]
        buy_ins[player] = [0.0]
        stack_sizes[player] = [0.0]

    # Process of parsing through the log
    logger.info('Starting Log Conversion')
    hand_number = 0
    line_number = 0
    for row in reversed(log):
        line_number += 1
        line = row[0]
        if line.startswith('-- starting'):
            hand_number += 1
        elif line.startswith('Player stacks:'):
            for name, quantity in transactions.items():
                if not (re.search(f'{name}', line)):
                    stack_sizes[name].append(stack_sizes[name][hand_number - 1] + quantity[0])
                    if quantity[2]:
                        stack_sizes[name][hand_number] += quantity[1]
                    buy_ins[name].append(buy_ins[name][hand_number - 1] + quantity[1])
                    quantity[0] = 0
                    quantity[1] = 0
                    quantity[2] = 0
                else:
                    dollar = re.search(fr'{name}" [(]\d+\.\d\d', line)
                    dollar = float(dollar.group().split()[1].lstrip('('))
                    stack_sizes[name].append(dollar)
                    buy_ins[name].append(quantity[1] + buy_ins[name][hand_number - 1])
                    quantity[0] = 0
                    if quantity[2]:
                        quantity[1] = 0
                        quantity[2] = 0
        elif line.startswith(('-- ending', 'Flop:', 'Turn:', 'River:')):
            for name, quantity in transactions.items():
                quantity[0] += quantity[3]
                quantity[3] = 0
        elif re.search(r"approved", line):
            for name, quantity in transactions.items():
                if re.search(name, line):
                    dollar = re.search(r" \d+\.\d\d", line)
                    dollar = float(dollar.group())
                    quantity[1] += dollar
                    quantity[2] = 1
        elif re.search(r"updated", line):
            for name, quantity in transactions.items():
                if re.search(name, line):
                    old_dollar = re.search(r" \d+\.\d\d ", line)
                    old_dollar = float(old_dollar.group())
                    new_dollar = re.search(r" \d+\.\d\d.$", line)
                    new_dollar = float(new_dollar.group().rstrip('.'))
                    quantity[1] += new_dollar - old_dollar
                    quantity[2] = 1
        elif re.search(r"quits the game", line):
            for name, quantity in transactions.items():
                if re.search(name, line):
                    dollar = re.search(r" \d+\.\d\d", line)
                    dollar = float(dollar.group())
                    quantity[1] -= dollar
                    quantity[2] = 1
        elif re.search(r"missing small blind", line):
            for name, quantity in transactions.items():
                if re.search(name, line):
                    dollar = re.search(r" \d+\.\d\d", line)
                    dollar = float(dollar.group())
                    quantity[0] -= dollar
        else:
            for name, quantity in transactions.items():
                if re.search(f'{name}', line):
                    if line.split()[0] == 'Uncalled':
                        quantity[0] += float(line.split()[3])
                    elif line.startswith('"') and line.split('@ ')[1].startswith(name):
                        if line.split('@ ')[1].split()[1] == 'collected':
                            quantity[0] += float(line.split('@ ')[1].split()[2])
                        elif re.search(r" \d+\.\d\d", line) and not (re.search(r"joined", line)):
                            dollar = re.search(r" \d+\.\d\d", line)
                            dollar = float(dollar.group())
                            quantity[3] = 0 - dollar

    # End of log final tabulations
    hand_number += 1
    for name, quantity in transactions.items():
        stack_sizes[name].append(stack_sizes[name][hand_number - 1] + quantity[0])
        if quantity[2]:
            stack_sizes[name][hand_number] += quantity[1]
        buy_ins[name].append(quantity[1] + buy_ins[name][hand_number - 1])
        quantity[0] = 0

    # Changing user_ids to names in each dictionary, to be reflected in the graph's legend
    stack_sizes = update_names(stack_sizes, players)
    transactions = update_names(transactions, players)
    buy_ins = update_names(buy_ins, players)
    nets = {}
    for player, values in transactions.items():
        nets[player] = [x1 - x2 for (x1, x2) in zip(stack_sizes[player], buy_ins[player])]

    # Graphing with matplotlib
    try:
        fig, ax = plt.subplots(figsize=(10, 6), dpi=450)
        for user, values in nets.items():
            numeric_values = values
            ax.plot(numeric_values, label=user)

        ax.set_xlabel("Hand #")
        ax.set_ylabel("Net $")
        legend = ax.legend(title='PLAYERS', bbox_to_anchor=(1, 0.5), loc='center left', frameon=True)
        legend.get_title().set_fontweight('bold')
        legend.get_title().set_fontsize('large')
        legend.get_frame().set_linewidth(1.5)
        legend.get_frame().set_edgecolor('blue')

        ax.xaxis.set_minor_locator(AutoMinorLocator(5))
        ax.yaxis.set_minor_locator(AutoMinorLocator(5))
        ax.set_xlim(left=0)
        ax.set_ylim(bottom=ax.get_ylim()[0], top=ax.get_ylim()[1])
        ax.axhline(0, color='black', linewidth=0.5, linestyle='--')
        ax.axhspan(ax.get_ylim()[0], 0, color='red', alpha=0.03, zorder=0)
        ax.grid(True, linestyle='-', color='gray', alpha=0.5)
        ax.grid(True, which='minor', linestyle=':', linewidth=0.5, color='gray', alpha=0.6)

        fig.tight_layout()
        buffer = io.BytesIO()
        fig.savefig(buffer, format='png')
        plt.close('all')
        buffer.seek(0)
        return buffer
    except Exception as err:
        logger.exception('Error Plotting Graph')


def update_names(dictionaries: dict[str, list[int]], players_dict: dict[str, str]) -> dict[str,list[int]]:
    # Searching user_ids to be replaced by the player's name when they exist in the database
    # Otherwise, replacing it with the alias used during the game when they do not exist in the database
    logger.info('Updating Names for Graph')
    new_dictionaries = {}
    with connect() as connection:
        for player in dictionaries:
            ans, cols = query(connection, """SELECT p.name FROM players p 
                                              JOIN users u ON
                                              u.player_id = p.player_id and u.user_id = %s;""", player)
            if ans:
                name = ans[0][0].title()
                if new_dictionaries.get(name):
                    # combining multiple instances of the same player across devices/user_ids
                    new_dictionaries[name] = [sum(x) for x in zip(new_dictionaries[name], dictionaries[player])]
                else:
                    new_dictionaries[name] = dictionaries[player]
            else:
                new_dictionaries[players_dict[player]+'*'] = dictionaries[player]
    disconnect(connection)
    return new_dictionaries


def main():
    if len(sys.argv) <= 2:
        return
    try:
        with open(sys.argv[1], "rb") as f1, open(sys.argv[2], "rb") as f2:
            csv1_bytes = f1.read()
            csv2_bytes = f2.read()

        result = graph_setup(csv1_bytes, csv2_bytes)
        if result:
            base_names = tuple(os.path.splitext(os.path.basename(path))[0] for path in [sys.argv[1], sys.argv[2]])
            extracted = [name[idx+1:] if (idx:= name.find('_pg')) != -1 else name for name in base_names]
            unique_sorted = sorted(set(extracted))
            output_filename = "graph_" + "_".join(unique_sorted) + ".png"
            with open(output_filename, "wb") as out_file:
                out_file.write(result.getvalue())
        else:
            logger.warning("Graph Setup Failed.")
    except FileNotFoundError as err:
        logger.critical('Incorrect or missing file path(s): %s', err, exc_info=True)
    except Exception as err:
        logger.exception('Unexpected Error: %s', err)


if __name__ == '__main__':
    main()
