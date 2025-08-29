#!/usr/bin/env bash
set -euo pipefail

# --- Update/Upgrade ---
echo "==> Updating system packages..."
sudo apt update -y
sudo apt upgrade -y

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

# --- Update system packages ---
echo "==> Updating system packages..."
sudo apt update -y
sudo apt install -y ${PY_BIN} ${PY_BIN}-venv python3-pip postgresql postgresql-contrib git

# --- Create Python virtual environment ---
echo "==> Creating Python virtualenv..."
if [[ ! -d "$APP_DIR/$VENV_DIR" ]]; then
  ${PY_BIN} -m venv "$APP_DIR/$VENV_DIR"
fi
# shellcheck disable=SC1090
source "$APP_DIR/$VENV_DIR/bin/activate"

# --- Upgrade pip and install requirements ---
echo "==> Upgrading pip and installing requirements..."
pip install --upgrade pip
if [[ -f "$APP_DIR/requirements.txt" ]]; then
  pip install -r "$APP_DIR/requirements.txt"
fi

# --- Create or update .env dynamically ---
if [[ ! -f "$ENV_FILE" ]]; then
    echo "==> Creating .env from .env.example..."
    cp "$ENV_EXAMPLE" "$ENV_FILE"
fi

while IFS= read -r line; do
    [[ -z "$line" || "$line" =~ ^# ]] && continue
    key="${line%%=*}"
    if grep -q "^$key=" "$ENV_FILE"; then
        continue
    fi
    read -p "Enter value for $key: " value
    echo "$key=$value" >> "$ENV_FILE"
done < "$ENV_EXAMPLE"

# --- Create or update database.ini dynamically ---
if [[ ! -f "$DB_INI" ]]; then
    echo "==> Creating database.ini from example..."
    cp "$DB_EXAMPLE" "$DB_INI"
fi

while IFS= read -r line; do
    [[ -z "$line" || "$line" =~ ^\s*# || "$line" =~ ^\[.*\] ]] && continue
    key="${line%%=*}"
    if grep -q "^$key=" "$DB_INI"; then
        continue
    fi
    read -p "Enter value for $key in database.ini: " value
    echo "$key=$value" >> "$DB_INI"
done < "$DB_EXAMPLE"

# --- Extract DB credentials ---
DB_USER=$(awk -F= '/^user[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); print $2}' "$DB_INI" | tail -n1)
DB_PASS=$(awk -F= '/^password[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); print $2}' "$DB_INI" | tail -n1)
DB_NAME=$(awk -F= '/^database[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); print $2}' "$DB_INI" | tail -n1)

# --- Ensure PostgreSQL database and user exist ---
echo "==> Ensuring PostgreSQL database & user exist..."
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1; then
  sudo -u postgres psql -c "CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASS}';"
fi

if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1; then
  sudo -u postgres createdb "${DB_NAME}"
fi

sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};" >/dev/null

# --- Apply schema if present ---
if [[ -f "$SCHEMA_FILE" ]]; then
  echo "==> Applying schema.sql (non-fatal if objects already exist)..."
  sudo -u postgres psql -d "${DB_NAME}" -f "$SCHEMA_FILE" || true
else
  echo "==> WARNING: $SCHEMA_FILE not found; skipping schema import."
fi

# --- Install systemd service ---
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
echo "==> Installing systemd service: $SERVICE_NAME"
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

echo "==> Enabling and starting the service..."
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

echo "==> Setup complete!"
echo "Service status: sudo systemctl status ${SERVICE_NAME} --no-pager"
echo "Live logs:      journalctl -u ${SERVICE_NAME} -f"
