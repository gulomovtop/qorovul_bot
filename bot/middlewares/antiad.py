"""Middleware that detects and removes advertisement messages."""

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware, Bot
from aiogram.types import Message

from bot.database.engine import async_session
from bot.services import settings_service
from bot.utils.filters import contains_ad, is_admin
from bot.utils.i18n import t


class AntiAdMiddleware(BaseMiddleware):
    """Outer middleware on dp.message — deletes ads from non-admins."""

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        # Only check group/supergroup text messages
        if (
            event.chat.type not in ("group", "supergroup")
            or not event.text
            or not event.from_user
        ):
            return await handler(event, data)

        # Skip commands (e.g. /set_ad may contain links)
        if event.text.startswith("/"):
            return await handler(event, data)

        # Check for ad content
        if not contains_ad(event.text):
            return await handler(event, data)

        # Check if anti-ad is enabled for this group
        try:
            async with async_session() as session:
                enabled = await settings_service.is_antiad_enabled(
                    session, event.chat.id
                )
            if not enabled:
                return await handler(event, data)
        except Exception:
            pass  # If DB fails, default to filtering

        # Admins are exempt
        bot: Bot = data["bot"]
        if await is_admin(bot, event.chat.id, event.from_user.id):
            return await handler(event, data)

        # ── Ad detected from a non-admin ─────────────────────────────
        lang = data.get("lang", "en")
        try:
            await event.delete()
        except Exception:
            pass

        # Send group's custom ad-replacement text (or default warning)
        try:
            async with async_session() as session:
                ad_text = await settings_service.get_ad_text(
                    session, event.chat.id, lang
                )
            text = ad_text or t("ad_detected", lang)
            await event.answer(text)
        except Exception:
            pass

        # Do NOT pass to handler — message is deleted
        return None
