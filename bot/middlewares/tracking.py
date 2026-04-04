"""Middleware that records every group message into the database."""

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import Message

from bot.database.engine import async_session
from bot.services import message_service


class TrackingMiddleware(BaseMiddleware):
    """Outer middleware on dp.message — tracks group messages."""

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        if event.chat.type in ("group", "supergroup") and event.from_user:
            try:
                async with async_session() as session:
                    await message_service.track_message(
                        session, event.from_user.id, event.chat.id
                    )
            except Exception:
                pass  # Never block message processing
        return await handler(event, data)
