"""Configuration module for loading environment variables."""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_env_var(
    name: str, default: str | None = None, required: bool = True
) -> str | None:
    """Get environment variable with optional default value."""
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


# Telegram Bot configuration
TELEGRAM_BOT_TOKEN: str = get_env_var("TELEGRAM_BOT_TOKEN")

# Clash Royale API configuration
CR_API_TOKEN: str = get_env_var("CR_API_TOKEN")
CLAN_TAG: str = get_env_var("CLAN_TAG")

# PostgreSQL configuration (Heroku provides DATABASE_URL)
DATABASE_URL: str | None = get_env_var("DATABASE_URL", required=False)

# Background task configuration
FETCH_INTERVAL_SECONDS: int = int(
    get_env_var("FETCH_INTERVAL_SECONDS", default="3600", required=False)
)

# Backfill configuration
BACKFILL_WEEKS: int = int(get_env_var("BACKFILL_WEEKS", default="8", required=False))

# Daily snapshot hour (UTC)
SNAPSHOT_UTC_HOUR: int = int(get_env_var("SNAPSHOT_UTC_HOUR", default="0", required=False))

# Inactivity threshold in days
INACTIVE_DAYS_THRESHOLD: int = int(
    get_env_var("INACTIVE_DAYS_THRESHOLD", default="7", required=False)
)

# Clash Royale API base URL
CR_API_BASE_URL: str = os.getenv("CR_API_BASE_URL", "https://api.clashroyale.com/v1")
