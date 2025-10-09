TABLES = {
    'players': {
        'lookup_key': 'player_id',
        'sort_by': '"name" ASC'
    },
    'users': {
        'lookup_key': 'player_id',
        'sort_by': '"user_id" ASC'
    },
    'ledgers': {
        'lookup_key': 'game_id',
        'sort_by': '"net" DESC, "alias" ASC'
    },
    'games': {
        'lookup_key': 'game_id',
        'sort_by': '"date" DESC'
    }
}

CHANNELS_TEMPLATE = {
    'admin', 'database', 'email', 'email-database', 'game', 'game-test', 'graph', 'graph-test',
    'invites', 'ledgers', 'ledgers-test', 'manage', 'music', 'query', 'query-test', 'roles'
}
ROLES_TEMPLATE = {'admin', 'email needed', 'game runner', 'poker', 'poker bot'}

channels: dict[int, dict[str, int]] = {}
roles: dict[int, dict[str, int]] = {}

timezone = '' #tz identifier, ie 'America/Toronto' for EST/EDT