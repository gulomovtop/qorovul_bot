"""Handlers: /start, /lang, /mygroups, language callback, welcome, bot membership tracking."""

from aiogram import Router, F, Bot
from aiogram.filters import Command, CommandStart
from aiogram.enums import ChatMemberStatus
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from bot.database.engine import async_session
from bot.services import user_service, group_service
from bot.utils.i18n import t, LANG_NAMES

router = Router(name="start")

LANG_KB = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="🇺🇿 O'zbek", callback_data="lang:uz"),
            InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru"),
            InlineKeyboardButton(text="🇬🇧 English", callback_data="lang:en"),
        ]
    ]
)


# ── /start ────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, lang: str) -> None:
    """Private /start — greet and offer language selection."""
    if message.from_user:
        async with async_session() as session:
            await user_service.get_or_create_user(
                session, message.from_user.id, message.from_user.username
            )
    await message.answer(t("welcome_private", lang), reply_markup=LANG_KB)


# ── /lang ─────────────────────────────────────────────────────────────

@router.message(Command("lang"))
async def cmd_lang(message: Message, lang: str) -> None:
    """Show language selection keyboard."""
    await message.answer(t("lang_prompt", lang), reply_markup=LANG_KB)


# ── language callback ─────────────────────────────────────────────────

@router.callback_query(F.data.startswith("lang:"))
async def cb_lang_select(callback: CallbackQuery, lang: str) -> None:
    """Handle language selection button press."""
    if not callback.data or not callback.from_user:
        return
    new_lang = callback.data.split(":")[1]
    if new_lang not in LANG_NAMES:
        return

    async with async_session() as session:
        await user_service.set_language(session, callback.from_user.id, new_lang)

    await callback.message.edit_text(t("lang_selected", new_lang))  # type: ignore[union-attr]
    await callback.answer()


# ── welcome new members ──────────────────────────────────────────────

@router.message(F.new_chat_members)
async def on_new_members(message: Message, lang: str) -> None:
    """Welcome each new human member with language-selection buttons."""
    if not message.new_chat_members:
        return
    for member in message.new_chat_members:
        if member.is_bot:
            continue
        async with async_session() as session:
            await user_service.get_or_create_user(
                session, member.id, member.username
            )
        await message.answer(
            t("welcome_group", lang, name=member.full_name),
            reply_markup=LANG_KB,
        )


# ── /mygroups — list groups where bot is admin + invite links ─────────

@router.message(Command("mygroups"))
async def cmd_mygroups(message: Message, bot: Bot, lang: str) -> None:
    """Show all groups where the bot is admin, with invite links."""
    if message.chat.type != "private":
        await message.reply(t("mygroups_private_only", lang))
        return

    async with async_session() as session:
        groups = await group_service.get_admin_groups(session)

    if not groups:
        await message.answer(t("mygroups_empty", lang))
        return

    lines = [t("mygroups_header", lang)]
    for idx, grp in enumerate(groups, 1):
        # Try to generate an invite link
        link = await _try_invite_link(bot, grp.group_id)
        link_text = link if link else t("mygroups_no_link", lang)
        lines.append(
            t("mygroups_entry", lang, idx=idx, title=grp.title or str(grp.group_id), link=link_text)
        )

    await message.answer("\n".join(lines))


async def _try_invite_link(bot: Bot, chat_id: int) -> str | None:
    """Try to create an invite link. Returns the URL or None."""
    try:
        link = await bot.create_chat_invite_link(
            chat_id=chat_id,
            name="GroupHelp Bot",
        )
        return link.invite_link
    except Exception:
        return None


# ── my_chat_member — track bot being added/promoted/removed ──────────

@router.my_chat_member()
async def on_bot_membership_change(event: ChatMemberUpdated) -> None:
    """Track when the bot is added, promoted, demoted, or removed."""
    chat = event.chat
    if chat.type not in ("group", "supergroup"):
        return

    new_status = event.new_chat_member.status
    async with async_session() as session:
        if new_status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
            # Bot removed — delete record
            await group_service.remove_group(session, chat.id)
        elif new_status == ChatMemberStatus.ADMINISTRATOR:
            # Bot promoted to admin
            await group_service.upsert_group(session, chat.id, chat.title, is_admin=True)
            try:
                await event.answer(t("bot_promoted", "en"))
            except Exception:
                pass
        elif new_status == ChatMemberStatus.MEMBER:
            # Bot is regular member (added or demoted)
            await group_service.upsert_group(session, chat.id, chat.title, is_admin=False)
            try:
                await event.answer(t("bot_added", "en"))
            except Exception:
                pass
