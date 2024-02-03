import csv
import re
import sys
import os
import matplotlib.pyplot as plt
from connect import connect, disconnect, query


async def graph_message(one, two, game_url):
    ledger_path = None
    log_path = None
    one_path = one.filename
    two_path = two.filename

    timestamp = None

    if one_path.endswith('.csv') and two_path.endswith('.csv'):
        await one.save(one.filename)
        await two.save(two.filename)

        for path in [one_path, two_path]:
            with open(path, 'r') as csv_file:
                csv_reader = list(csv.reader(csv_file))

                for row in csv_reader:
                    if row and row[0] == 'entry':
                        log_path = path
                        timestamp = csv_reader[-1][1].partition('T')
                        timestamp = f'{timestamp[0]} {timestamp[2][:8]}+0'
                        break
                    elif row and row[0] == 'player_nickname':
                        ledger_path = path
                        break

    if ledger_path and log_path:
        connection = connect()
        game_id = None

        g, columns = query(connection, """INSERT INTO games (date, url) VALUES 
                                           (%s, %s) 
                                           ON CONFLICT(url) DO NOTHING
                                           RETURNING game_id;""", timestamp, game_url)
        if g:
            game_id = g[0][0]
            connection.commit()
        disconnect(connection)

        graph(log_path, ledger_path, game_id)
        os.remove(one_path)
        os.remove(two_path)
        return True
    else:
        return False


def graph(full_log_path, ledger_path, game_id):
    players = {}  # userid: alias
    transactions = {}  # userid: [pot net, stack change, stack change bool, street action]
    stack_sizes = {}  # userid: [stack at start of hand #]
    buyins = {}  # userid: [total net buyin/buyout at start of hand #]

    with open(ledger_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)

        connection = connect()

        if game_id:
            query(connection, """DELETE FROM ledgers WHERE game_id = %s;""", game_id)

        for row in csv_reader:
            players[row[1]] = row[0]
            if game_id:
                print('here', game_id, row[1], row[7], row[0])
                p, pcolumns = query(connection, """INSERT INTO users (user_id, aliases) VALUES
                                                      (%s, '{}')
                                                      ON CONFLICT DO NOTHING;""", row[1])
                print(p)
                print('---')
                q, columns = query(connection, """INSERT INTO ledgers (game_id, user_id, net, alias) VALUES 
                                      (%s, %s, %s, %s)
                                      ON CONFLICT (game_id, user_id) DO
                                      UPDATE SET net = ledgers.net + EXCLUDED.net;""", game_id, row[1], row[7], row[0])
                print(q)

        if game_id:
            query(connection, """UPDATE users u 
                                 SET aliases = (SELECT ARRAY(SELECT DISTINCT l.alias FROM ledgers l
                                                             WHERE l.user_id = u.user_id));""")
            connection.commit()

        disconnect(connection)

    for player in players.keys():
        # list of stack sizes at the start of each hand [individual hand win/loss, buyin/buyout, ]
        transactions[player] = [0, 0, 0, 0]
        # list of total net buyin/buyout at the start of each hand
        buyins[player] = [0]
        # list of stack sizes at the start of each hand
        stack_sizes[player] = [0]

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
                        buyins[name].append(buyins[name][hand_number - 1] + quantity[1])
                        quantity[0] = 0
                        quantity[1] = 0
                        quantity[2] = 0

                    else:
                        dollar = re.search(fr'{name}" [(]\d+\.\d\d', line)
                        dollar = float(dollar.group().split()[1].lstrip('('))
                        stack_sizes[name].append(dollar)
                        buyins[name].append(quantity[1] + buyins[name][hand_number - 1])
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

        # End of log final calculations
        hand_number += 1
        for name, quantity in transactions.items():
            stack_sizes[name].append(stack_sizes[name][hand_number - 1] + quantity[0])

            if quantity[2]:
                stack_sizes[name][hand_number] += quantity[1]

            buyins[name].append(quantity[1] + buyins[name][hand_number - 1])
            quantity[0] = 0

    stack_sizes = update_name(stack_sizes, players)
    transactions = update_name(transactions, players)
    buyins = update_name(buyins, players)

    profits = {}
    for player, values in transactions.items():
        profits[player] = [x1 - x2 for (x1, x2) in zip(stack_sizes[player], buyins[player])]

    plt.figure(figsize=(10, 6), dpi=450)

    for user, values in profits.items():
        numeric_values = values
        plt.plot(numeric_values, label=user)
    plt.xlabel("Hand number")
    plt.ylabel("Profit $")
    plt.legend()
    plt.savefig('profits.png')

    plt.figure(figsize=(10, 6), dpi=450)
    for user, values in stack_sizes.items():
        numeric_values = values
        plt.plot(numeric_values, label=user)

    plt.xlabel("Hand number")
    plt.ylabel("Stack Size $")
    plt.legend()
    # plt.savefig('stacks.png')


def update_name(dictionaries, players_dict):
    # searching user_ids within the given dictionary to update it to the players name if they exist in database
    # or replacing it with the alias used during the game if they do not exist in the database

    new_dictionaries = {}
    connection = connect()
    for player in dictionaries:
        n, columns = query(connection, """SELECT p.name FROM players p 
                                          JOIN users u ON
                                          u.player_id = p.player_id and u.user_id = %s;""", player)
        if n:
            name = n[0][0]
            if new_dictionaries.get(name):
                new_dictionaries[name] = [sum(x) for x in zip(new_dictionaries[name], dictionaries[player])]
            else:
                new_dictionaries[name] = dictionaries[player]
        else:
            new_dictionaries[players_dict[player]] = dictionaries[player]
    disconnect(connection)
    return new_dictionaries


if __name__ == '__main__':
    graph_message(sys.argv[1], sys.argv[2], sys.argv[3])
