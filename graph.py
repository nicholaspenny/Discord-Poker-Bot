import csv
import re
import os
import matplotlib.pyplot as plt
# import json


async def graph_message(one, two):
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
                for row in csv_reader:
                    if row and row[0] == 'entry':
                        log_path = path
                        break
                    elif row and row[0] == 'player_nickname':
                        ledger_path = path
                        break

    if ledger_path and log_path:
        graph(log_path, ledger_path)
        os.remove(one_path)
        os.remove(two_path)
        return True
    else:
        return False


def graph(full_log_path, ledger_path):
    players = {}  # userid: username
    transactions = {}  # name: [pot net, stack change, stack change bool, street action]
    stack_sizes = {}  # name: [stack at start of hand #]
    buyins = {}  # name: [total net buyin/buyout at start of hand #]

    with open(ledger_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)

        for row in csv_reader:
            players[row[1]] = row[0]

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

    stack_sizes = update_name(stack_sizes)
    transactions = update_name(transactions)
    buyins = update_name(buyins)

    # with open("stack_sizes.json", "w") as f:
    #    json.dump(stack_sizes, f)

    profits = {}
    for player, values in transactions.items():
        profits[player] = [x1 - x2 for (x1, x2) in zip(stack_sizes[player], buyins[player])]

    # with open("profits.json", "w") as f:
    #    json.dump(profits, f)

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
    plt.savefig('stacks.png')


def update_name(dictionaries):
    players_database = {
        'Nick': ['UuOZVaVHBL', 'WIu0Z3l0r9', 'gseR2k-QyO'],
        'Dante': ['WE0IAE3QII', '8Jdc1XjMBj'],
        'Shreyas': ['cKD9AvkfLo', '5vhVDDut10'],
        'Nabil': ['8uCU-T89qK'],
        'Stefan': ['DyToyiPUwq', 'ikJq95sN1t', 'v7StLFeZTU'],
        'Austin': ['s-OfUZcUtY'],
        'Haiyang': ['_UjP1NCuKc'],
        'Christian': ['EjHsc0yLfZ'],
        'Lila': [],
        'Rohun': ['Tz3ys1wh8S'],
        'Andy': ['xibJ1QP4fl'],
        'Anurag': ['uY2gNzm7vS'],
        'Slater': ['LTOQs2xY6L'],
        'KevinL': [],
        'Jun': ['TCy7j0cT__']
    }
    inverse = {}
    new_dictionaries = {}
    for player, users_list in players_database.items():
        for user in users_list:
            inverse[user] = player
    for player in dictionaries:
        if player in inverse:
            for name in inverse:
                if name == player:
                    if new_dictionaries.get(inverse[name]):
                        new_dictionaries[inverse[name]] = [sum(x) for x in zip(new_dictionaries[inverse[name]], dictionaries[player])]
                    else:
                        new_dictionaries[inverse[name]] = dictionaries[player]
        else:
            new_dictionaries[player] = dictionaries[player]

    return new_dictionaries


if __name__ == '__main__':
    graph('log.csv', 'ledger.csv')
