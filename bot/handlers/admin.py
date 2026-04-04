"""Admin commands: /warn, /ban, /unban, /mute, /unmute, /info."""

from datetime import datetime, timedelta, timezone

from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.types import ChatPermissions, Message
from aiogram.exceptions import TelegramBadRequest

from bot.database.engine import async_session
from bot.services import warn_service
from bot.utils.filters import is_admin, parse_duration, format_duration
from bot.utils.i18n import t

router = Router(name="admin")

# ── helpers ───────────────────────────────────────────────────────────

async def _require_admin_and_target(
    message: Message, bot: Bot, lang: str
) -> tuple[bool, int, str]:
    """Validate admin rights and reply target. Returns (ok, target_id, target_name)."""
    if not message.from_user:
        return False, 0, ""
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(t("not_admin", lang))
        return False, 0, ""
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply(t("no_reply", lang))
        return False, 0, ""
    target = message.reply_to_message.from_user
    if await is_admin(bot, message.chat.id, target.id):
        await message.reply(t("cannot_restrict_admin", lang))
        return False, 0, ""
    return True, target.id, target.full_name


# ── /warn ─────────────────────────────────────────────────────────────

@router.message(Command("warn"))
async def cmd_warn(message: Message, bot: Bot, lang: str) -> None:
    ok, uid, name = await _require_admin_and_target(message, bot, lang)
    if not ok:
        return

    async with async_session() as session:
        count = await warn_service.add_warn(session, uid, message.chat.id)

    if count >= 3:
        try:
            until = datetime.now(timezone.utc) + timedelta(hours=24)
            await bot.restrict_chat_member(
                chat_id=message.chat.id,
                user_id=uid,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=until,
            )
            await message.reply(t("warn_auto_mute", lang, name=name))
        except TelegramBadRequest:
            await message.reply(t("bot_no_rights", lang))
    else:
        await message.reply(t("warn_issued", lang, name=name, count=count))


# ── /ban ──────────────────────────────────────────────────────────────

@router.message(Command("ban"))
async def cmd_ban(message: Message, bot: Bot, lang: str) -> None:
    ok, uid, name = await _require_admin_and_target(message, bot, lang)
    if not ok:
        return
    try:
        await bot.ban_chat_member(message.chat.id, uid)
        await message.reply(t("ban_success", lang, name=name))
    except TelegramBadRequest:
        await message.reply(t("bot_no_rights", lang))


# ── /unban ────────────────────────────────────────────────────────────

@router.message(Command("unban"))
async def cmd_unban(message: Message, bot: Bot, lang: str) -> None:
    if not message.from_user:
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(t("not_admin", lang))
        return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply(t("no_reply", lang))
        return
    target = message.reply_to_message.from_user
    try:
        await bot.unban_chat_member(message.chat.id, target.id, only_if_banned=True)
        await message.reply(t("unban_success", lang, name=target.full_name))
    except TelegramBadRequest:
        await message.reply(t("bot_no_rights", lang))


# ── /mute ─────────────────────────────────────────────────────────────

@router.message(Command("mute"))
async def cmd_mute(message: Message, bot: Bot, lang: str) -> None:
    ok, uid, name = await _require_admin_and_target(message, bot, lang)
    if not ok:
        return

    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2:
        await message.reply(t("mute_usage", lang))
        return
    duration = parse_duration(args[1])
    if not duration:
        await message.reply(t("mute_usage", lang))
        return

    try:
        until = datetime.now(timezone.utc) + duration
        await bot.restrict_chat_member(
            chat_id=message.chat.id,
            user_id=uid,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until,
        )
        dur_text = format_duration(duration, lang)
        await message.reply(t("mute_success", lang, name=name, duration=dur_text))
    except TelegramBadRequest:
        await message.reply(t("bot_no_rights", lang))


# ── /unmute ───────────────────────────────────────────────────────────

@router.message(Command("unmute"))
async def cmd_unmute(message: Message, bot: Bot, lang: str) -> None:
    ok, uid, name = await _require_admin_and_target(message, bot, lang)
    if not ok:
        return
    try:
        await bot.restrict_chat_member(
            chat_id=message.chat.id,
            user_id=uid,
            permissions=ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True,
            ),
        )
        await message.reply(t("unmute_success", lang, name=name))
    except TelegramBadRequest:
        await message.reply(t("bot_no_rights", lang))


# ── /info ─────────────────────────────────────────────────────────────

@router.message(Command("info"))
async def cmd_info(message: Message, bot: Bot, lang: str) -> None:
    if not message.from_user:
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(t("not_admin", lang))
        return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply(t("no_reply", lang))
        return
    target = message.reply_to_message.from_user
    await message.reply(
        t(
            "info_result",
            lang,
            name=target.full_name,
            user_id=target.id,
            username=target.username or "N/A",
        )
    )
