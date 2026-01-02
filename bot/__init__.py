"""Bot package initialization."""

from bot.handlers import router, moderation_router

__all__ = ["router", "moderation_router"]
