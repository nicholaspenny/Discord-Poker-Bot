#!/bin/bash

echo "Creating virtual environment..."
python3 -m venv venv

echo "Activating virtual environment..."
source venv/bin/activate

echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing dependencies..."
pip install -r requirements.txt

echo "Copying example config files..."
cp -n .env.example .env
cp -n db/database.ini.example db/database.ini

echo "Setup complete!"
echo "Please edit .env and db/database.ini with your real credentials before running the bot."