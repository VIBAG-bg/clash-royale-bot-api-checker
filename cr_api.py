"""Clash Royale API client module using httpx."""

import asyncio
import logging
from typing import Any
from urllib.parse import quote

import httpx

from config import CR_API_BASE_URL, CR_API_TOKEN

logger = logging.getLogger(__name__)


class ClashRoyaleAPIError(Exception):
    """Custom exception for Clash Royale API errors."""
    
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"CR API Error {status_code}: {message}")


class ClashRoyaleAPI:
    """Async client for Clash Royale API."""
    
    def __init__(self):
        self._client: httpx.AsyncClient | None = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=CR_API_BASE_URL,
                headers={
                    "Authorization": f"Bearer {CR_API_TOKEN}",
                    "Accept": "application/json",
                },
                timeout=30.0,
            )
        return self._client
    
    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
    
    def _encode_tag(self, tag: str) -> str:
        """Encode a player or clan tag for URL usage."""
        # Remove # if present (safely, only remove first character if it's #)
        clean_tag = tag[1:] if tag.startswith("#") else tag
        return quote(f"#{clean_tag}")
    
    async def _request(self, endpoint: str) -> dict[str, Any]:
        """Make an API request and return JSON response."""
        client = await self._get_client()
        retry_statuses = {429, 502, 503, 504}
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(endpoint)
            except httpx.RequestError as e:
                if attempt < max_attempts:
                    delay = 0.5 * (2 ** (attempt - 1))
                    logger.warning(
                        "HTTP request error (attempt %s/%s): %s",
                        attempt,
                        max_attempts,
                        e,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error(f"HTTP request error: {e}")
                raise ClashRoyaleAPIError(0, f"Network error: {str(e)}")

            if response.status_code == 200:
                return response.json()
            if response.status_code == 404:
                raise ClashRoyaleAPIError(404, "Resource not found")
            if response.status_code == 403:
                raise ClashRoyaleAPIError(403, "Access denied - check API token")
            if response.status_code in retry_statuses:
                if attempt < max_attempts:
                    delay = 0.5 * (2 ** (attempt - 1))
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                delay = min(max(float(retry_after), 0.0), 5.0)
                            except ValueError:
                                pass
                    logger.warning(
                        "CR API retry (status %s) attempt %s/%s; sleeping %.1fs",
                        response.status_code,
                        attempt,
                        max_attempts,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                if response.status_code == 429:
                    raise ClashRoyaleAPIError(429, "Rate limit exceeded")
                raise ClashRoyaleAPIError(
                    response.status_code,
                    f"API request failed: {response.text}",
                )
            raise ClashRoyaleAPIError(
                response.status_code,
                f"API request failed: {response.text}",
            )
    
    async def get_clan(self, clan_tag: str) -> dict[str, Any]:
        """
        Get clan information.
        
        Args:
            clan_tag: Clan tag (with or without #)
        
        Returns:
            Clan data dictionary
        """
        encoded_tag = self._encode_tag(clan_tag)
        return await self._request(f"/clans/{encoded_tag}")
    
    async def get_clan_members(self, clan_tag: str) -> list[dict[str, Any]]:
        """
        Get list of clan members.
        
        Args:
            clan_tag: Clan tag (with or without #)
        
        Returns:
            List of clan member dictionaries
        """
        encoded_tag = self._encode_tag(clan_tag)
        response = await self._request(f"/clans/{encoded_tag}/members")
        return response.get("items", [])
    
    async def get_current_river_race(self, clan_tag: str) -> dict[str, Any]:
        """
        Get current River Race (Clan War) information.
        
        This endpoint returns data about the current clan war including:
        - Season ID
        - Section index (week within season)
        - Period type (training, warDay, colosseum)
        - Clan standings and participation
        
        Args:
            clan_tag: Clan tag (with or without #)
        
        Returns:
            Current River Race data dictionary
        """
        encoded_tag = self._encode_tag(clan_tag)
        return await self._request(f"/clans/{encoded_tag}/currentriverrace")
    
    async def get_river_race_log(self, clan_tag: str) -> list[dict[str, Any]]:
        """
        Get River Race log (history of past clan wars).
        
        Args:
            clan_tag: Clan tag (with or without #)
        
        Returns:
            List of past River Race entries
        """
        encoded_tag = self._encode_tag(clan_tag)
        response = await self._request(f"/clans/{encoded_tag}/riverracelog")
        return response.get("items", [])
    
    async def get_player(self, player_tag: str) -> dict[str, Any]:
        """
        Get player information.
        
        Args:
            player_tag: Player tag (with or without #)
        
        Returns:
            Player data dictionary
        """
        encoded_tag = self._encode_tag(player_tag)
        return await self._request(f"/players/{encoded_tag}")


# Global API client instance
_api_client: ClashRoyaleAPI | None = None


async def get_api_client() -> ClashRoyaleAPI:
    """Get or create the global API client instance."""
    global _api_client
    if _api_client is None:
        _api_client = ClashRoyaleAPI()
    return _api_client


async def close_api_client() -> None:
    """Close the global API client."""
    global _api_client
    if _api_client is not None:
        await _api_client.close()
        _api_client = None
