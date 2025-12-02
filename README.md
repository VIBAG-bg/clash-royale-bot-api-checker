# Clash Royale Bot API Checker

A Python Telegram bot that monitors a Clash Royale clan using the official Clash Royale API and MongoDB Atlas.

## Features

- **River Race Monitoring**: Automatically fetches clan war (River Race) stats at regular intervals
- **Colosseum Week Support**: Tracks Colosseum weeks with a special flag
- **Player Participation Tracking**: Stores per-player participation data including:
  - Season and section (week) index
  - Fame and repair points
  - Decks used (total and daily)
  - Boat attacks
- **Telegram Bot Commands**:
  - `/start` - Welcome message and bot information
  - `/ping` - Check bot responsiveness and API connectivity
  - `/inactive` - Show players with low River Race participation

## Tech Stack

- **aiogram v3** - Modern async Telegram Bot framework
- **httpx** - Async HTTP client for Clash Royale API calls
- **motor** - Async MongoDB driver
- **python-dotenv** - Environment variable management

## Project Structure

```
├── config.py           # Environment variable configuration
├── db.py               # MongoDB connection and operations
├── cr_api.py           # Clash Royale API client
├── bot/
│   ├── __init__.py     # Bot package initialization
│   └── handlers.py     # Telegram command handlers
├── main.py             # Entry point with background tasks
├── requirements.txt    # Python dependencies
├── .env.example        # Environment variable template
└── README.md           # This file
```

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/VIBAG-bg/clash-royale-bot-api-checker.git
cd clash-royale-bot-api-checker
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your values:

- `TELEGRAM_BOT_TOKEN` - Get from [@BotFather](https://t.me/BotFather)
- `CR_API_TOKEN` - Get from [Clash Royale Developer Portal](https://developer.clashroyale.com)
- `CLAN_TAG` - Your clan tag (without the #)
- `MONGODB_URI` - MongoDB Atlas connection string
- `MONGODB_DB_NAME` - Database name (default: `clash_royale_bot`)
- `FETCH_INTERVAL_SECONDS` - Background fetch interval (default: 3600)
- `INACTIVE_DAYS_THRESHOLD` - Days threshold for inactivity (default: 7)

### 5. Run the Bot

```bash
python main.py
```

## Database Schema

### player_participation Collection

Stores player participation data for each River Race week:

```json
{
  "player_tag": "#ABC123",
  "player_name": "Player Name",
  "season_id": 75,
  "section_index": 2,
  "is_colosseum": false,
  "fame": 1500,
  "repair_points": 500,
  "boat_attacks": 2,
  "decks_used": 12,
  "decks_used_today": 4,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T18:00:00Z"
}
```

### river_race_state Collection

Tracks the current River Race state:

```json
{
  "clan_tag": "ABC123",
  "season_id": 75,
  "section_index": 2,
  "is_colosseum": false,
  "period_type": "warDay",
  "clan_score": 45000,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T18:00:00Z"
}
```

## License

MIT