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
        return None
    return value


def require_env_value(name: str, value: str | None) -> str:
    """Ensure required env var is present at runtime."""
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


# Telegram Bot configuration
TELEGRAM_BOT_TOKEN: str | None = get_env_var("TELEGRAM_BOT_TOKEN")

# Clash Royale API configuration
CR_API_TOKEN: str | None = get_env_var("CR_API_TOKEN")
CLAN_TAG: str | None = get_env_var("CLAN_TAG")

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

# Protected player tags to exclude from inactive reports
PROTECTED_PLAYER_TAGS: set[str] = {"#LJJUQCJC"}

# Kick shortlist configuration
NEW_MEMBER_WEEKS_PLAYED: int = int(
    get_env_var("NEW_MEMBER_WEEKS_PLAYED", default="2", required=False)
)
REVIVED_DECKS_THRESHOLD: int = int(
    get_env_var("REVIVED_DECKS_THRESHOLD", default="8", required=False)
)
KICK_SHORTLIST_LIMIT: int = int(
    get_env_var("KICK_SHORTLIST_LIMIT", default="3", required=False)
)

# Donation configuration
DONATION_WEEKS_WINDOW: int = int(
    get_env_var("DONATION_WEEKS_WINDOW", default="8", required=False)
)
DONATION_REVIVE_WTD_THRESHOLD: int = int(
    get_env_var("DONATION_REVIVE_WTD_THRESHOLD", default="30", required=False)
)
DONATION_REVIVE_8W_THRESHOLD: int = int(
    get_env_var("DONATION_REVIVE_8W_THRESHOLD", default="120", required=False)
)

# Clash Royale API base URL
CR_API_BASE_URL: str = os.getenv("CR_API_BASE_URL", "https://api.clashroyale.com/v1")

# Telegram bot username for deep-linking (optional)
BOT_USERNAME: str | None = get_env_var("BOT_USERNAME", required=False)

# Admin user ids for /admin_link_name permissions
ADMIN_USER_IDS: set[int] = {
    int(value)
    for value in (get_env_var("ADMIN_USER_IDS", default="", required=False) or "").split(",")
    if value.strip().isdigit()
}
