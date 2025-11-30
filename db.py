"""Database module for MongoDB operations using motor."""

from datetime import datetime, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from config import MONGODB_URI, MONGODB_DB_NAME

# Global database client and database references
_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None


async def connect_db() -> AsyncIOMotorDatabase:
    """Connect to MongoDB and return the database instance."""
    global _client, _db
    if _client is None:
        _client = AsyncIOMotorClient(MONGODB_URI)
        _db = _client[MONGODB_DB_NAME]
    return _db


async def close_db() -> None:
    """Close the MongoDB connection."""
    global _client, _db
    if _client is not None:
        _client.close()
        _client = None
        _db = None


async def get_db() -> AsyncIOMotorDatabase:
    """Get the database instance, connecting if necessary."""
    if _db is None:
        await connect_db()
    return _db


async def save_player_participation(
    player_tag: str,
    player_name: str,
    season_id: int,
    section_index: int,
    is_colosseum: bool,
    fame: int,
    repair_points: int,
    boat_attacks: int,
    decks_used: int,
    decks_used_today: int,
) -> None:
    """
    Save or update player participation data for a River Race week.
    
    Args:
        player_tag: Player's unique tag
        player_name: Player's display name
        season_id: Current season ID
        section_index: Section index within the season (0-based)
        is_colosseum: Whether this week is a Colosseum week
        fame: Player's fame earned
        repair_points: Player's repair points earned
        boat_attacks: Number of boat attacks
        decks_used: Total decks used in the week
        decks_used_today: Decks used today
    """
    db = await get_db()
    collection = db.player_participation
    
    # Create unique identifier for this participation record
    filter_query = {
        "player_tag": player_tag,
        "season_id": season_id,
        "section_index": section_index,
    }
    
    update_data = {
        "$set": {
            "player_name": player_name,
            "is_colosseum": is_colosseum,
            "fame": fame,
            "repair_points": repair_points,
            "boat_attacks": boat_attacks,
            "decks_used": decks_used,
            "decks_used_today": decks_used_today,
            "updated_at": datetime.now(timezone.utc),
        },
        "$setOnInsert": {
            "created_at": datetime.now(timezone.utc),
        },
    }
    
    await collection.update_one(filter_query, update_data, upsert=True)


async def get_inactive_players(
    season_id: int, section_index: int, min_decks: int = 4
) -> list[dict[str, Any]]:
    """
    Get players who haven't used enough decks in the current River Race week.
    
    Args:
        season_id: Current season ID
        section_index: Section index within the season
        min_decks: Minimum decks expected (default: 4 per day)
    
    Returns:
        List of player documents with low participation
    """
    db = await get_db()
    collection = db.player_participation
    
    cursor = collection.find({
        "season_id": season_id,
        "section_index": section_index,
        "decks_used": {"$lt": min_decks},
    }).sort("decks_used", 1)
    
    return await cursor.to_list(length=None)


async def get_all_participation_for_week(
    season_id: int, section_index: int
) -> list[dict[str, Any]]:
    """
    Get all player participation records for a specific week.
    
    Args:
        season_id: Season ID
        section_index: Section index within the season
    
    Returns:
        List of all player participation documents for the week
    """
    db = await get_db()
    collection = db.player_participation
    
    cursor = collection.find({
        "season_id": season_id,
        "section_index": section_index,
    }).sort("fame", -1)
    
    return await cursor.to_list(length=None)


async def get_player_history(player_tag: str, limit: int = 10) -> list[dict[str, Any]]:
    """
    Get participation history for a specific player.
    
    Args:
        player_tag: Player's unique tag
        limit: Maximum number of records to return
    
    Returns:
        List of player's participation records
    """
    db = await get_db()
    collection = db.player_participation
    
    cursor = collection.find({
        "player_tag": player_tag,
    }).sort([("season_id", -1), ("section_index", -1)]).limit(limit)
    
    return await cursor.to_list(length=None)


async def save_river_race_state(
    clan_tag: str,
    season_id: int,
    section_index: int,
    is_colosseum: bool,
    period_type: str,
    clan_score: int,
) -> None:
    """
    Save the current River Race state for tracking.
    
    Args:
        clan_tag: Clan's unique tag
        season_id: Current season ID
        section_index: Section index within the season
        is_colosseum: Whether this is a Colosseum week
        period_type: Current period type (training, war, colosseum)
        clan_score: Current clan score in the race
    """
    db = await get_db()
    collection = db.river_race_state
    
    filter_query = {
        "clan_tag": clan_tag,
        "season_id": season_id,
        "section_index": section_index,
    }
    
    update_data = {
        "$set": {
            "is_colosseum": is_colosseum,
            "period_type": period_type,
            "clan_score": clan_score,
            "updated_at": datetime.now(timezone.utc),
        },
        "$setOnInsert": {
            "created_at": datetime.now(timezone.utc),
        },
    }
    
    await collection.update_one(filter_query, update_data, upsert=True)


async def get_latest_river_race_state(clan_tag: str) -> dict[str, Any] | None:
    """
    Get the latest River Race state for a clan.
    
    Args:
        clan_tag: Clan's unique tag
    
    Returns:
        Latest River Race state document or None
    """
    db = await get_db()
    collection = db.river_race_state
    
    cursor = collection.find({
        "clan_tag": clan_tag,
    }).sort([("season_id", -1), ("section_index", -1)]).limit(1)
    
    results = await cursor.to_list(length=1)
    return results[0] if results else None
