#!/usr/bin/env bash
set -euo pipefail

# --- Update system packages ---
sudo apt update -y

# --- Settings ---
SERVICE_NAME="${SERVICE_NAME:-discord-poker-bot}"
PY_BIN="python3"
VENV_DIR="venv"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_EXAMPLE="$APP_DIR/.env.example"
ENV_FILE="$APP_DIR/.env"
DB_EXAMPLE="$APP_DIR/db/database.ini.example"
DB_INI="$APP_DIR/db/database.ini"
SCHEMA_FILE="$APP_DIR/db/schema.sql"

# --- Install essential system packages ---
echo "==> Installing Python, Git, and PostgreSQL 17..."

# Install Python, pip, and Git
sudo apt install -y ${PY_BIN} ${PY_BIN}-venv python3-pip git wget curl ca-certificates

# Add PostgreSQL official repository for latest 17.x
echo "==> Adding PostgreSQL 17 repository..."
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

# Update package list and install PostgreSQL 17
sudo apt update -y
sudo apt install -y postgresql-17 postgresql-client-17 postgresql-contrib-17

# --- Create Python virtual environment ---
echo "==> Creating Python virtualenv..."
if [[ ! -d "$APP_DIR/$VENV_DIR" ]]; then
    $PY_BIN -m venv "$APP_DIR/$VENV_DIR"
fi
source "$APP_DIR/$VENV_DIR/bin/activate"

# --- Upgrade pip and install requirements ---
echo "==> Upgrading pip and installing requirements..."
pip install --upgrade pip
if [[ -f "$APP_DIR/requirements.txt" ]]; then
    pip install --prefer-binary -r "$APP_DIR/requirements.txt"
fi

# --- Create .env and database.ini from examples if missing ---
[[ ! -f "$ENV_FILE" ]] && cp "$ENV_EXAMPLE" "$ENV_FILE" && echo "✓ Created .env from example"
[[ ! -f "$DB_INI" ]] && cp "$DB_EXAMPLE" "$DB_INI" && echo "✓ Created database.ini from example"

# --- Extract DB credentials ---
DB_USER=$(awk -F= '/^user[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); print $2}' "$DB_INI" | tail -n1)
DB_PASS=$(awk -F= '/^password[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); print $2}' "$DB_INI" | tail -n1)
DB_NAME=$(awk -F= '/^database[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); print $2}' "$DB_INI" | tail -n1)

# --- Start PostgreSQL service ---
sudo systemctl start postgresql
sudo systemctl enable postgresql

# --- Ensure PostgreSQL database and user exist safely ---
cd /tmp  # avoid "Permission denied" errors for postgres user
SAFE_DB_NAME="${DB_NAME// /_}"

# Create user if it doesn't exist
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1; then
    echo "Creating database user: $DB_USER"
    sudo -u postgres psql -c "CREATE USER \"${DB_USER}\" WITH PASSWORD '${DB_PASS}';"
fi


# Create database if it doesn't exist
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${SAFE_DB_NAME}'" | grep -q 1; then
    echo "Creating database: $SAFE_DB_NAME"
    sudo -u postgres createdb "$SAFE_DB_NAME"
fi

# Grant database-level privileges
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE \"$SAFE_DB_NAME\" TO \"${DB_USER}\";"

# --- Apply schema if present ---
if [[ -f "$SCHEMA_FILE" ]]; then
    echo "==> Applying schema.sql (non-fatal if objects already exist)..."
    sudo cp "$SCHEMA_FILE" /tmp/schema.sql
    sudo -u postgres psql -d "${SAFE_DB_NAME}" -f /tmp/schema.sql || true
    echo "==> Granting privileges on all tables and sequences to $DB_USER..."
    sudo -u postgres psql -d "$SAFE_DB_NAME" -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$DB_USER\";"
    sudo -u postgres psql -d "$SAFE_DB_NAME" -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"$DB_USER\";"
fi

# --- Install systemd service ---
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Discord Poker Bot
After=network.target postgresql.service

[Service]
Type=simple
User=$(id -un)
WorkingDirectory=$APP_DIR
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=$ENV_FILE
ExecStart=$APP_DIR/$VENV_DIR/bin/python $APP_DIR/main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# --- Enable & start the service ---
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

# --- Feedback ---
echo ""
echo "==> Setup complete!"
echo "Service status: sudo systemctl status ${SERVICE_NAME} --no-pager"
echo "Live logs:      journalctl -u ${SERVICE_NAME} -f"
echo ""
echo "NOTE: Check your .env and database.ini are correctly configured."
echo "Database name spaces were replaced with underscores for safety: $SAFE_DB_NAME"
