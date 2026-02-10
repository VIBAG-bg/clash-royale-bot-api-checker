"""Strict aiogram-like test doubles."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import AsyncMock


@dataclass(slots=True)
class FakeEntity:
    type: object
    offset: int = 0
    length: int = 0
    url: str | None = None


@dataclass(slots=True)
class FakeUser:
    id: int
    username: str | None = None
    full_name: str = "Test User"
    is_bot: bool = False


@dataclass(slots=True)
class FakeChat:
    id: int
    type: object = "supergroup"


@dataclass(slots=True)
class FakeBot:
    send_message: AsyncMock = field(default_factory=AsyncMock)
    send_photo: AsyncMock = field(default_factory=AsyncMock)
    delete_message: AsyncMock = field(default_factory=AsyncMock)
    restrict_chat_member: AsyncMock = field(default_factory=AsyncMock)
    ban_chat_member: AsyncMock = field(default_factory=AsyncMock)
    get_chat_member: AsyncMock = field(default_factory=AsyncMock)
    get_me: AsyncMock = field(default_factory=AsyncMock)
    promote_chat_member: AsyncMock = field(default_factory=AsyncMock)
    set_chat_administrator_custom_title: AsyncMock = field(default_factory=AsyncMock)


class FakeMessage:
    __slots__ = (
        "bot",
        "chat",
        "from_user",
        "text",
        "caption",
        "entities",
        "caption_entities",
        "message_id",
        "answer",
        "_delete_mock",
    )

    def __init__(
        self,
        *,
        bot: FakeBot,
        chat: FakeChat,
        from_user: FakeUser | None,
        text: str | None = None,
        caption: str | None = None,
        entities: list[FakeEntity] | None = None,
        caption_entities: list[FakeEntity] | None = None,
        message_id: int = 1,
    ):
        self.bot = bot
        self.chat = chat
        self.from_user = from_user
        self.text = text
        self.caption = caption
        self.entities = entities or []
        self.caption_entities = caption_entities or []
        self.message_id = message_id
        self.answer = AsyncMock()
        self._delete_mock = AsyncMock()

    async def delete(self) -> None:
        await self._delete_mock()


class FakeCallbackQuery:
    __slots__ = ("bot", "from_user", "data", "message", "answer")

    def __init__(
        self,
        *,
        bot: FakeBot,
        from_user: FakeUser,
        data: str,
        message: FakeMessage | None = None,
    ):
        self.bot = bot
        self.from_user = from_user
        self.data = data
        self.message = message or SimpleNamespace(chat=FakeChat(id=0))
        self.answer = AsyncMock()

