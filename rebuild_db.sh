#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="discord-poker-bot"
DB_INI="db/database.ini"
SCHEMA_FILE="db/schema.sql"
BACKUP_DIR="db/backup"

# --- Extract DB credentials from database.ini ---
DB_USER=$(awk -F= '/^user[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); print $2}' "$DB_INI" | tail -n1)
DB_PASS=$(awk -F= '/^password[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); print $2}' "$DB_INI" | tail -n1)
DB_NAME=$(awk -F= '/^database[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); print $2}' "$DB_INI" | tail -n1)
SAFE_DB_NAME="${DB_NAME// /_}"

# --- Validate required fields ---
: "${DB_USER:?Error: 'user' not found in $DB_INI}"
: "${DB_PASS:?Error: 'password' not found in $DB_INI}"
: "${DB_NAME:?Error: 'database' not found in $DB_INI}"

# --- Confirm destructive action ---
read -p "⚠️  This will completely rebuild the database and erase all data. Continue? (y/N): " CONFIRM
[[ "$CONFIRM" =~ ^[Yy]$ ]] || { echo "Cancelled."; exit 1; }

# --- Stop the bot service ---
echo "Stopping bot service..."
sudo systemctl stop "$SERVICE_NAME"

# --- Ensure PostgreSQL database and user exist safely ---
cd /tmp  # avoid permission errors

# --- Backup existing database (if it exists) ---
if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$SAFE_DB_NAME"; then
    echo "Backing up existing database..."
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    mkdir -p "$BACKUP_DIR"
    sudo -u postgres pg_dump --format=plain --blobs "$SAFE_DB_NAME" -f "$BACKUP_DIR/dump_${TIMESTAMP}.sql"
    echo "Backup saved to $BACKUP_DIR/dump_${TIMESTAMP}.sql"
fi

# --- Drop and recreate the database ---
echo "Recreating database $SAFE_DB_NAME..."
sudo -u postgres dropdb --if-exists "$SAFE_DB_NAME"
sudo -u postgres createdb "$SAFE_DB_NAME"

# --- Ensure user exists and grant privileges ---
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1; then
    echo "Creating database user: $DB_USER"
    sudo -u postgres psql -c "CREATE USER \"${DB_USER}\" WITH PASSWORD '${DB_PASS}';"
fi

sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE \"$SAFE_DB_NAME\" TO \"${DB_USER}\";"

# --- Apply schema.sql if present ---
if [[ -f "$SCHEMA_FILE" ]]; then
    echo "==> Applying schema.sql (non-fatal if objects already exist)..."
    sudo cp "$SCHEMA_FILE" /tmp/schema.sql
    sudo -u postgres psql -d "${SAFE_DB_NAME}" -f /tmp/schema.sql || true
    sudo -u postgres psql -d "$SAFE_DB_NAME" -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$DB_USER\";"
    sudo -u postgres psql -d "$SAFE_DB_NAME" -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"$DB_USER\";"
fi

# --- Restart the bot service ---
echo "Restarting bot service..."
sudo systemctl start "$SERVICE_NAME"

echo "✅ Database rebuild complete and bot restarted!"
