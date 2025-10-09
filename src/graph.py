import csv
import io
import json
import logging
import os
import re
import sys
from typing import Optional, Union

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

from src.connect import connect, query

COLORS = [
    "Blue", "Red", "Lime", "Magenta", "Orange", "SaddleBrown", "Cyan",
    "DarkViolet", "Gray", "Green", "Gold", "Salmon", 'SkyBlue', 'Orchid',
]

plt.rcParams['axes.prop_cycle'] = plt.cycler(color=COLORS)

logger = logging.getLogger(__name__)


def classify_graph_file(text_content: str) -> Optional[tuple[str, Union[dict[str, object], list[Union[str, list[str]]]]]]:
    try:
        data = json.loads(text_content)
        if isinstance(data, dict) and 'hands' in data:
            return ('json_hands', data)
    except json.JSONDecodeError:
        pass

    try:
        csv_rows = list(csv.reader(io.StringIO(text_content)))
        if csv_rows and len(csv_rows) > 0 and len(csv_rows[0]) > 0:
            first_cell = csv_rows[0][0].strip()
            if first_cell == 'player_nickname':
                return 'csv_ledger', csv_rows
            elif first_cell == 'entry':
                return 'csv_log', [row[0] for row in csv_rows]
    except Exception:
        pass
    return None


def json_to_csv_list(json_data: dict[str, object]) -> list[str]:
    csv_rows = []
    hands = json_data.get('hands', [])

    # Map event types to readable actions
    event_type_map = {
        0: "checks",
        1: "bets",  # bomb pot
        2: "posts a big blind of",
        3: "posts a small blind of",
        4: "posts missed big blind of",
        5: "posts missed small blind of",
        6: "posts a straddle",
        7: "calls",
        8: "raises to",  # or "bets"
        10: "collected",
        11: "folds",
        12: "shows",
        13: "posts an ante",
        14: "runs it twice",
        15: None, #End of hand Marker
        16: "Uncalled bet of"
    }

    for hand in reversed(hands):  # Process in reverse order like original
        hand_id = hand.get('id', '')
        hand_number = hand.get('number', '')
        hand_actions = []
        # Starting hand line
        hand_actions.append(f'-- starting hand #{hand_number} (id: {hand_id}) --')

        # Player stacks line
        players = hand.get('players', [])
        stack_info = " | ".join([f'#{p["seat"]} "{p["name"]} @ {p["id"]}" ({p["stack"] / 100:.2f})'
                                 for p in players])
        hand_actions.append(f'Player stacks: {stack_info}')

        # Process events
        seat_to_player = {p['seat']: p for p in players}

        for event in hand.get('events', []):
            payload = event.get('payload', {})
            event_type = payload.get('type')
            seat = payload.get('seat')
            value = payload.get('value', 0) / 100.0

            if event_type == 9:  # Community cards
                cards = payload.get('cards', [])
                card_str = ", ".join(cards) if cards else ""
                turn = payload.get('turn', 1)
                if turn == 1:
                    hand_actions.append(f'Flop: [{card_str}]')
                elif turn == 2:
                    hand_actions.append(f'Turn: [{card_str}]')
                elif turn == 3:
                    hand_actions.append(f'River: [{card_str}]')
                # Add Turn/River logic as needed
            elif seat and seat in seat_to_player:
                player = seat_to_player[seat]
                player_name = f'"{player["name"]} @ {player["id"]}"'

                if event_type in event_type_map:
                    action = event_type_map[event_type]
                    if event_type in [1, 2, 3, 4, 5, 6, 7, 8]:  # Actions with values
                        hand_actions.append(f'{player_name} {action} {value:.2f}')
                    elif event_type == 10:  # Collect pot
                        hand_actions.append(f'{player_name} collected {value:.2f} from pot')
                    elif event_type == 16:  # Uncalled bet
                        hand_actions.append(f'Uncalled bet of {value:.2f} returned to {player_name}')
                    elif event_type in [0, 11]:  # Check, Fold
                        hand_actions.append(f'{player_name} {action}')
                    elif event_type == 12:  # Show cards
                        cards = payload.get('cards', [])
                        card_str = ", ".join([c for c in cards if c]) if cards else ""
                        hand_actions.append(f'{player_name} shows {card_str}')
                    elif event_type == 14:  # Run it twice
                        hand_actions.append(f'{player_name} {action}')

        # Ending hand line
        hand_actions.append(f'-- ending hand #{hand_number} --')
        csv_rows.extend(reversed(hand_actions))

    return csv_rows


def graph_setup(file1: bytes, file2: bytes) -> Optional[io.BytesIO]:
    try:
        file1_text = file1.decode('utf-8')
        file2_text = file2.decode('utf-8')

        f1 = classify_graph_file(file1_text)
        f2 = classify_graph_file(file2_text)

        if not f1 or not f2:
            logger.warning("Could not classify one or both files")
            return None

        if f1[0] == 'csv_ledger':
            if f2[0] == 'csv_log':
                return graph(f2[2], f1[1])
            elif f2[0] == 'json_hands':
                return graph(json_to_csv_list(f2[1]), f1[1])
        elif f2[0] == 'csv_ledger':
            if f1[0] == 'csv_log':
                return graph(f1[1], f2[1])
            elif f1[0] == 'json_hands':
                return graph(json_to_csv_list(f1[1]), f2[1])

        logger.warning(f"Incompatible file combination: {f1[0]} + {f2[0]}")
        return None
    except Exception as err:
        logger.warning("Failed to process files: %s", err)
        return None


def graph(log: list[list[str]], ledger: list[str]) -> io.BytesIO:
    players = {}  # userid: alias
    player_data = {}  # userid: { 'net': [], 'hand_net': float, 'street_invested': float }

    # Build player mapping from ledger
    for row in ledger[1:]:
        players[row[1]] = row[0]

    matches = []
    for entry in reversed(log):
        before, sep, after = entry.partition('changed the ID from ')
        if sep:
            a = after.split()
            matches.append((a[0], a[2]))

    for old, new in matches:
        log = [entry.replace(old, new) for entry in log]

    # Initialize player data
    for player_id in players.keys():
        player_data[player_id] = {
            'net': [0.0],  # Starting with a net of 0 at hand 0
            'hand_net': 0.0,  # Running net during hands
            'street_invested': 0.0  # Amount invested on current street (for raises)
        }

    # Process log in reverse (oldest first)
    hand_number = 0
    for line in reversed(log):
        if line.startswith('-- starting'):
            hand_number += 1
            # Reset street invested for new hand
            for player_id in player_data:
                player_data[player_id]['hand_net'] = 0.0
                player_data[player_id]['street_invested'] = 0.0

        elif line.startswith('-- ending'):
            for player_id in player_data:
                player_data[player_id]['net'][-1] += player_data[player_id]['hand_net'] - player_data[player_id]['street_invested']

        elif line.startswith('Player stacks:'):
            for player_id in player_data:
                player_data[player_id]['net'].append(player_data[player_id]['net'][-1])

        elif line.startswith(('Flop:', 'Turn:', 'River:')):
            # Street change or hand end - reset street invested
            for player_id in player_data:
                player_data[player_id]['hand_net'] -= player_data[player_id]['street_invested']
                player_data[player_id]['street_invested'] = 0.0

        elif line.startswith('"'):
            # This is a player action line
            for player_id in player_data:
                if player_id not in line:
                    continue

                # Parse the action
                parts = line.split(f'@ {player_id}"')
                if len(parts) < 2:
                    continue

                action_part = parts[1].strip()

                # Collect from pot (positive to net)
                if action_part.startswith('collected'):
                    match = re.search(r'collected ([0-9]+\.[0-9]+)', action_part)
                    if match:
                        amount = float(match.group(1))
                        player_data[player_id]['hand_net'] += amount

                # Posts blinds/straddles (negative to net, counts as street investment)
                elif action_part.startswith('posts a missed') or action_part.startswith('posts an ante'):
                    match = re.search(r' ([0-9]+\.[0-9]+)', action_part)
                    if match:
                        amount = float(match.group(1))
                        player_data[player_id]['hand_net'] -= amount

                elif any(action_part.startswith(a) for a in ['posts a', 'calls', 'raises to', 'bets']):
                    match = re.search(r' ([0-9]+\.[0-9]+)', action_part)
                    if match:
                        amount = float(match.group(1))
                        player_data[player_id]['street_invested'] = amount
        elif line.startswith('Uncalled bet of'):
            # Uncalled bet returned (positive to net)
            match_amount = re.search(r'Uncalled bet of ([0-9]+\.[0-9]+)', line)
            if match_amount:
                amount = float(match_amount.group(1))
                returnee = [word[:-1] for word in line.split() if word.endswith('"')][0]
                player_data[returnee]['hand_net'] += amount
    # Ensure all players have the same number of data points
    max_hands = max(len(data['net']) for data in player_data.values())
    for player_id in player_data:
        while len(player_data[player_id]['net']) < max_hands:
            player_data[player_id]['net'].append(player_data[player_id]['net'][-1])
    # Convert user_ids to names
    nets = {}
    with connect() as conn:
        for player_id, data in player_data.items():
            ans, cols = query(conn, """SELECT p.name
                                             FROM players p
                                                      JOIN users u ON
                                                 u.player_id = p.player_id and u.user_id = %s""", player_id)
            if ans:
                name = ans[0][0].title()
                if name in nets:
                    # Combine multiple instances of the same player
                    nets[name] = [sum(x) for x in zip(nets[name], data['net'])]
                else:
                    nets[name] = data['net']
            else:
                nets[players[player_id] + '*'] = data['net']

    # Graphing with matplotlib
    try:
        fig, ax = plt.subplots(figsize=(10, 6), dpi=450)
        for user, values in nets.items():
            ax.plot(values, label=user)

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
        logger.info('Completed Graphing')
        return buffer
    except Exception as err:
        logger.exception('Error Plotting Graph')


def update_names(dictionaries: dict[str, list[int]], players_dict: dict[str, str]) -> dict[str,list[int]]:
    # Searching user_ids to be replaced by the player's name when they exist in the database
    # Otherwise, replacing it with the alias used during the game when they do not exist in the database
    new_dictionaries = {}
    with connect() as conn:
        for player in dictionaries:
            ans, cols = query(conn, """SELECT p.name FROM players p 
                                              JOIN users u ON
                                              u.player_id = p.player_id and u.user_id = %s""", player)
            if ans:
                name = ans[0][0].title()
                if new_dictionaries.get(name):
                    # combining multiple instances of the same player across devices/user_ids
                    new_dictionaries[name] = [sum(x) for x in zip(new_dictionaries[name], dictionaries[player])]
                else:
                    new_dictionaries[name] = dictionaries[player]
            else:
                new_dictionaries[players_dict[player]+'*'] = dictionaries[player]
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
