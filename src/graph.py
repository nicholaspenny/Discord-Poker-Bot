import csv
import re
import sys
import os
import discord
import matplotlib.pyplot as plt
from connect import connect, disconnect, query
import logging

logger = logging.getLogger(__name__)


async def graph_setup(one: discord.Attachment, two: discord.Attachment) -> bool:
    ledger_path = None
    log_path = None
    one_path = one.filename
    two_path = two.filename

    if one_path.endswith('.csv') and two_path.endswith('.csv'):
        await one.save(one.filename)
        await two.save(two.filename)

        for path in [one_path, two_path]:
            with open(path, 'r') as csv_file:
                csv_reader = list(csv.reader(csv_file))

                if csv_reader and csv_reader[0]:
                    if csv_reader[0][0] == 'entry':
                        log_path = path
                    if csv_reader[0][0] == 'player_nickname':
                        ledger_path = path

    if ledger_path and log_path:
        graph(log_path, ledger_path)
        return True
    else:
        return False


def graph(full_log_path: str, ledger_path: str):
    players = {}  # userid: alias
    transactions = {}  # userid: [pot net, stack change, stack change bool, street action]
    stack_sizes = {}  # userid: [stack at start of hand #]
    buy_ins = {}  # userid: [total net buy_in/buy_out at start of hand #]

    with open(ledger_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)

        for row in csv_reader:
            players[row[1]] = row[0]

    for player in players.keys():
        transactions[player] = [0.0, 0.0, 0.0, 0.0]
        buy_ins[player] = [0.0]
        stack_sizes[player] = [0.0]

    with open(full_log_path, 'r') as csv_file:
        hand_number = 0
        line_number = 0
        csv_reader = list(csv.reader(csv_file))
        csv_reader.reverse()

        for row in csv_reader:
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

    stack_sizes = update_names(stack_sizes, players)
    transactions = update_names(transactions, players)
    buy_ins = update_names(buy_ins, players)

    profits = {}
    for player, values in transactions.items():
        profits[player] = [x1 - x2 for (x1, x2) in zip(stack_sizes[player], buy_ins[player])]

    plt.figure(figsize=(10, 6), dpi=450)
    plt.axhline(0, color='black', linewidth=0.5, linestyle='--')
    for user, values in profits.items():
        numeric_values = values
        plt.plot(numeric_values, label=user)
    plt.xlabel("Hand number")
    plt.ylabel("Profit $")
    plt.legend()
    plt.axhspan(plt.ylim()[0], 0, color='red', alpha=0.03)
    plt.xlim(left=0)
    plt.savefig('profits.png')


def update_names(dictionaries: dict, players_dict: dict):
    # searching user_ids within the given dictionary to update it to the player's name if they exist in the database
    # or replacing it with the alias used during the game if they do not exist in the database
    new_dictionaries = {}

    with connect() as connection:
        for player in dictionaries:
            ans, cols = query(connection, """SELECT p.name FROM players p 
                                              JOIN users u ON
                                              u.player_id = p.player_id and u.user_id = %s;""", player)
            if ans:
                name = ans[0][0].title()
                if new_dictionaries.get(name):
                    new_dictionaries[name] = [sum(x) for x in zip(new_dictionaries[name], dictionaries[player])]
                else:
                    new_dictionaries[name] = dictionaries[player]
            else:
                new_dictionaries[players_dict[player]] = dictionaries[player]

            connection.rollback()
    disconnect(connection)

    return new_dictionaries


def main():
    # arg1, arg2 should be log, ledger
    if os.path.getsize(sys.argv[1]) > os.path.getsize(sys.argv[2]):
        graph(sys.argv[1], sys.argv[2])
    else:
        graph(sys.argv[2], sys.argv[1])


if __name__ == '__main__':
    main()
