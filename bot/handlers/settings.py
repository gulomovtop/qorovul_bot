"""Settings handler: /set_ad — configure ad replacement text."""

from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.types import Message

from bot.database.engine import async_session
from bot.services import settings_service
from bot.utils.filters import is_admin
from bot.utils.i18n import t, LANG_NAMES

router = Router(name="settings")


@router.message(Command("set_ad"))
async def cmd_set_ad(message: Message, bot: Bot, lang: str) -> None:
    """Set ad replacement text: /set_ad uz|ru|en <text>"""
    if not message.from_user:
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(t("not_admin", lang))
        return

    args = (message.text or "").split(maxsplit=2)
    # args[0] = "/set_ad", args[1] = lang code, args[2] = text
    if len(args) < 3 or args[1] not in LANG_NAMES:
        await message.reply(t("ad_set_usage", lang))
        return

    target_lang = args[1]
    ad_text = args[2]

    async with async_session() as session:
        await settings_service.set_ad_text(
            session, message.chat.id, target_lang, ad_text
        )

    await message.reply(
        t("ad_set_success", lang, lang_name=LANG_NAMES[target_lang])
    )


@router.message(Command("antiad"))
async def cmd_antiad(message: Message, bot: Bot, lang: str) -> None:
    """Toggle anti-ad processing for this group."""
    if not message.from_user or message.chat.type not in ("group", "supergroup"):
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(t("not_admin", lang))
        return

    async with async_session() as session:
        new_state = await settings_service.toggle_antiad(session, message.chat.id)

    key = "antiad_on" if new_state else "antiad_off"
    await message.reply(t(key, lang))
