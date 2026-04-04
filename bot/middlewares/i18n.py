"""Middleware that injects the user's language into handler data."""

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Update

from bot.database.engine import async_session
from bot.services import user_service


class I18nMiddleware(BaseMiddleware):
    """Outer middleware registered on dp.update — sets ``data["lang"]``."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        lang = "en"
        user = data.get("event_from_user")
        if user:
            try:
                async with async_session() as session:
                    lang = await user_service.get_language(session, user.id)
            except Exception:
                pass
        data["lang"] = lang
        return await handler(event, data)
