CHANNELS_TEMPLATE = {
    'admin', 'commands', 'database', 'email', 'email-database', 'game', 'game-test', 'graph',
    'graph-test', 'ledgers', 'ledgers-test', 'manage', 'music', 'query', 'query-test', 'roles'
}
ROLES_TEMPLATE = {'fiend', 'admin', 'fiend bot', 'email needed'}

channels: dict[int: dict[str, int]] = {}
roles: dict[int: dict[str, int]] = {}