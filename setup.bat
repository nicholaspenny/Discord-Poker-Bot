@echo off
cd /d "%~dp0"

echo Creating virtual environment...
python -m venv venv

echo Activating virtual environment...
call venv\Scripts\activate.bat

echo Upgrading pip...
python -m pip install --upgrade pip

echo Installing dependencies...
pip install -r requirements.txt

echo Copying example config files...
if not exist ".env" copy .env.example .env
if not exist "db\database.ini" copy db\database.ini.example db\database.ini

echo Setup complete!
echo Please edit .env and db\database.ini with your real credentials before running the bot.
pause
