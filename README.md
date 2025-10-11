# 🃏 Discord Poker Bot

A modular 24/7 Discord bot for online poker games on PokerNow.club, featuring full server management, ledger tracking and data visualization.  
---
Built with **Python**, **PostgreSQL**, and **Google Cloud Platform (GCP)** free-tier Compute Engine.

---

## 🚀 Features
- Discord Bot Application via **Discord API** | https://discord.com/developers/docs/intro
- Automated ledger management:
  - image parsing via **Gemini API** | https://aistudio.google.com/welcome
  - web automation via **Playwright** 
- PostgreSQL database with structured backups  
- Graph generation and visual reporting via **Matplotlib**
---
## ⚙️ Setup & Deployment

**1. 🖥️ Configure a GCP Compute Engine VM (Free Tier Compatible)**

- **e2-micro**: us-west1 | s-central1 | us-east1
  - 2 vCPUs, 1 GB Memory
  - Canonical, Ubuntu LTS, x86/64
  - Up to 30 GB standard persistent disk storage
  - 1 GB of outbound data transfer
---
**2. 📦 Prerequisites & Cloning The Repository**
```bash
sudo apt update -y
sudo apt install -y git curl wget ca-certificates
```
```bash
git clone https://github.com/nicholaspenny/Discord-Poker-Bot.git DiscordPokerBot
cd DiscordPokerBot
```
***
**3. 🛠 Create/Fill environment files**

```bash
cp .env.example .env

cp db/database.ini.example db/database.ini
```
- **.env** - Fill in your Discord bot token and Gemini API key
- **db/database.ini** - Fill in your PostgreSQL host, user, password, and database name
  
***Migrating Database***
- **db/schema.sql** - replace the contents of this file with a previous .sql dump file

***Extra Configuration***
- **src/common.py**: timezone: set to your local timezone. Defaults to UTC.
- **src/bot.py**: channels/roles: customize fallback values for each dictionary.
---
**4. 🪄 Installation**  
Run these lines to install the necessary files for the project and complete the setup to have the bot running.  
```bash
chmod +x vm_setup.sh
./vm_setup.sh
```
This can be executed multiple times to reinstall files without disrupting the database.  

---
***Restart***  
Can be restarted through Discord with !restart; or with the following command line.  
```bash
sudo systemctl restart discord-poker-bot
```
***Database Reconfiguration***  
Backs up and completely rebuilds the PostgreSQL database using the schema or dump in **db/schema.sql**.
```bash
chmod +x rebuild_db.sh
./rebuild_db.sh
```
---
