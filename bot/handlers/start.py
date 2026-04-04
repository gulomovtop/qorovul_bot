"""Handlers: /start, /lang, /panel, /mygroups, language callback, welcome, bot tracking."""

from aiogram import Router, F, Bot
from aiogram.filters import Command, CommandStart
from aiogram.enums import ChatMemberStatus
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from bot.database.engine import async_session
from bot.services import user_service, group_service
from bot.utils.filters import is_owner
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

OWNER_PANEL_KB = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="📋 My Groups", callback_data="owner:groups")],
        [InlineKeyboardButton(text="🔄 Refresh Groups", callback_data="owner:refresh")],
        [InlineKeyboardButton(text="🌐 Change Language", callback_data="owner:lang")],
    ]
)


# ── /start ────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot, lang: str) -> None:
    """Private /start — greet + show owner panel if owner."""
    if message.from_user:
        async with async_session() as session:
            await user_service.get_or_create_user(
                session, message.from_user.id, message.from_user.username
            )
        # Set owner-specific commands menu
        if is_owner(message.from_user.id):
            await _set_owner_commands(bot, message.from_user.id)
            await message.answer(t("owner_panel", lang), reply_markup=OWNER_PANEL_KB)
            return

    await message.answer(t("welcome_private", lang), reply_markup=LANG_KB)


async def _set_owner_commands(bot: Bot, owner_id: int) -> None:
    """Set the bot command menu for the owner with extra tools."""
    try:
        await bot.set_my_commands(
            commands=[
                BotCommand(command="start", description="Start the bot"),
                BotCommand(command="panel", description="👑 Owner panel"),
                BotCommand(command="mygroups", description="📋 My groups"),
                BotCommand(command="lang", description="🌐 Change language"),
                BotCommand(command="top_day", description="📊 Top 10 — 24h"),
                BotCommand(command="top_week", description="📊 Top 10 — 7d"),
            ],
            scope=BotCommandScopeChat(chat_id=owner_id),
        )
    except Exception:
        pass


# ── /panel (owner only) ──────────────────────────────────────────────

@router.message(Command("panel"))
async def cmd_panel(message: Message, lang: str) -> None:
    """Show the owner management panel."""
    if not message.from_user or not is_owner(message.from_user.id):
        await message.reply(t("not_owner", lang))
        return
    await message.answer(t("owner_panel", lang), reply_markup=OWNER_PANEL_KB)


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
    # Send the bot guide after language selection
    await callback.message.answer(t("guide", new_lang))  # type: ignore[union-attr]
    await callback.answer()


# ── owner panel callbacks ─────────────────────────────────────────────

@router.callback_query(F.data == "owner:groups")
async def cb_owner_groups(callback: CallbackQuery, bot: Bot, lang: str) -> None:
    """Show all groups where the bot is admin (owner only)."""
    if not callback.from_user or not is_owner(callback.from_user.id):
        await callback.answer(t("not_owner", lang), show_alert=True)
        return

    async with async_session() as session:
        groups = await group_service.get_admin_groups(session)

    if not groups:
        await callback.message.edit_text(  # type: ignore[union-attr]
            t("mygroups_empty", lang),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Back", callback_data="owner:back")]
            ]),
        )
        await callback.answer()
        return

    lines = [t("mygroups_header", lang)]
    for idx, grp in enumerate(groups, 1):
        link = await _try_invite_link(bot, grp.group_id)
        link_text = link if link else t("mygroups_no_link", lang)
        lines.append(
            t("mygroups_entry", lang, idx=idx, title=grp.title or str(grp.group_id), link=link_text)
        )

    await callback.message.edit_text(  # type: ignore[union-attr]
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Back", callback_data="owner:back")]
        ]),
    )
    await callback.answer()


@router.callback_query(F.data == "owner:refresh")
async def cb_owner_refresh(callback: CallbackQuery, bot: Bot, lang: str) -> None:
    """Scan historical messages to find active groups and refresh bot statuses."""
    if not callback.from_user or not is_owner(callback.from_user.id):
        await callback.answer(t("not_owner", lang), show_alert=True)
        return

    # Notify progress
    await callback.message.edit_text(t("refresh_start", lang))  # type: ignore[union-attr]

    async with async_session() as session:
        group_ids = await group_service.get_all_active_group_ids(session)

        # Check each group against the Telegram API
        for gid in group_ids:
            try:
                member = await bot.get_chat_member(gid, bot.id)
                chat = await bot.get_chat(gid)
                
                is_admin = member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
                await group_service.upsert_group(session, gid, chat.title, is_admin)
            except Exception:
                # If we get an error (bot kicked, chat deleted), just mark as not admin/remove
                await group_service.remove_group(session, gid)

        # Re-fetch admin group count
        groups = await group_service.get_admin_groups(session)
        count = len(groups)

    # Done
    await callback.message.edit_text(  # type: ignore[union-attr]
        t("refresh_done", lang, v=count),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Back", callback_data="owner:back")]
        ])
    )
    await callback.answer()


@router.callback_query(F.data == "owner:lang")
async def cb_owner_lang(callback: CallbackQuery, lang: str) -> None:
    """Show language selection from owner panel."""
    if not callback.from_user or not is_owner(callback.from_user.id):
        await callback.answer(t("not_owner", lang), show_alert=True)
        return
    await callback.message.edit_text(  # type: ignore[union-attr]
        t("lang_prompt", lang), reply_markup=LANG_KB
    )
    await callback.answer()


@router.callback_query(F.data == "owner:back")
async def cb_owner_back(callback: CallbackQuery, lang: str) -> None:
    """Return to owner panel."""
    if not callback.from_user or not is_owner(callback.from_user.id):
        await callback.answer(t("not_owner", lang), show_alert=True)
        return
    await callback.message.edit_text(  # type: ignore[union-attr]
        t("owner_panel", lang), reply_markup=OWNER_PANEL_KB
    )
    await callback.answer()


# ── /mygroups (owner only) ────────────────────────────────────────────

@router.message(Command("mygroups"))
async def cmd_mygroups(message: Message, bot: Bot, lang: str) -> None:
    """List groups where bot is admin — owner only."""
    if not message.from_user or not is_owner(message.from_user.id):
        await message.reply(t("not_owner", lang))
        return

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
        link = await _try_invite_link(bot, grp.group_id)
        link_text = link if link else t("mygroups_no_link", lang)
        lines.append(
            t("mygroups_entry", lang, idx=idx, title=grp.title or str(grp.group_id), link=link_text)
        )

    await message.answer("\n".join(lines))


async def _try_invite_link(bot: Bot, chat_id: int) -> str | None:
    """Try to create an invite link. Returns the URL or None."""
    try:
        link = await bot.create_chat_invite_link(chat_id=chat_id, name="GroupHelp Bot")
        return link.invite_link
    except Exception:
        return None


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
            await user_service.get_or_create_user(session, member.id, member.username)
        await message.answer(
            t("welcome_group", lang, name=member.full_name), reply_markup=LANG_KB,
        )


# ── my_chat_member — track bot membership ────────────────────────────

@router.my_chat_member()
async def on_bot_membership_change(event: ChatMemberUpdated) -> None:
    """Track when the bot is added, promoted, demoted, or removed."""
    chat = event.chat
    if chat.type not in ("group", "supergroup"):
        return

    new_status = event.new_chat_member.status
    async with async_session() as session:
        if new_status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
            await group_service.remove_group(session, chat.id)
        elif new_status == ChatMemberStatus.ADMINISTRATOR:
            await group_service.upsert_group(session, chat.id, chat.title, is_admin=True)
            try:
                await event.answer(t("bot_promoted", "en"))
            except Exception:
                pass
        elif new_status == ChatMemberStatus.MEMBER:
            await group_service.upsert_group(session, chat.id, chat.title, is_admin=False)
            try:
                await event.answer(t("bot_added", "en"))
            except Exception:
                pass
