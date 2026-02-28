"""
GroupHelp Bot — Vercel Serverless Handler
==========================================
A Telegram group-management bot running on Vercel (serverless) with Supabase.

Architecture:
  - FastAPI exposes two endpoints: GET / (health) and POST /webhook (Telegram updates).
  - aiogram v3 Dispatcher processes every update inside the webhook handler.
  - supabase-py talks to a `managed_chats` table in your Supabase project.

Environment variables (set in Vercel dashboard → Settings → Environment Variables):
  BOT_TOKEN        – Telegram bot token from @BotFather
  SUPABASE_URL     – e.g. https://xxxx.supabase.co
  SUPABASE_KEY     – Service-role key (bypasses RLS)
  OWNER_ID         – Your personal Telegram user ID (integer)
  WEBHOOK_SECRET   – Random string; Telegram sends it in X-Telegram-Bot-Api-Secret-Token header
  VERCEL_URL       – Auto-provided by Vercel at deploy time
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.enums import ChatMemberStatus, ChatType
from aiogram.filters import Command, CommandStart
from aiogram.types import ChatPermissions, Update
from fastapi import FastAPI, Request, Response
from supabase import create_client, Client as SupabaseClient

# ──────────────────────────────────────────────
#  Logging
# ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("grouphelp")

# ──────────────────────────────────────────────
#  Configuration (read from environment)
# ──────────────────────────────────────────────
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")
OWNER_ID: int = int(os.environ.get("OWNER_ID", "0"))
WEBHOOK_SECRET: str = os.environ.get("WEBHOOK_SECRET", "")

# ──────────────────────────────────────────────
#  Clients
# ──────────────────────────────────────────────
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Two routers: commands_router is registered FIRST so that command handlers
# always take priority over the antispam catch-all message handler.
router = Router(name="commands")
antispam_router = Router(name="antispam")
dp.include_router(router)          # ← checked first
dp.include_router(antispam_router)  # ← checked second (fallback)

# Supabase client (service-role key → full access, no RLS)
supabase: SupabaseClient = create_client(SUPABASE_URL, SUPABASE_KEY)

# FastAPI application exposed by Vercel
app = FastAPI(title="GroupHelp Bot", docs_url=None, redoc_url=None)

# ──────────────────────────────────────────────
#  Antispam — URL / forward detection patterns
# ──────────────────────────────────────────────
#  Matches http(s) links, t.me links, @username mentions, and
#  common spam patterns like "join", "subscribe", "click".
SPAM_URL_REGEX = re.compile(
    r"(https?://\S+|t\.me/\S+|telegram\.me/\S+)", re.IGNORECASE
)
SPAM_KEYWORDS = re.compile(
    r"\b(join|subscribe|click here|free money|earn now|investment|crypto airdrop)\b",
    re.IGNORECASE,
)

# ──────────────────────────────────────────────
#  i18n — Bilingual message strings (uz / en)
# ──────────────────────────────────────────────
MESSAGES: dict[str, dict[str, str]] = {
    # ── General ──
    "welcome": {
        "uz": (
            "👋 Salom! Men guruh boshqaruv botiman.\n\n"
            "Meni guruhga qo'shing va admin qiling — men guruhingizni boshqarishga yordam beraman.\n\n"
            "📌 Buyruqlar:\n"
            "/lang [uz|en] — Tilni o'zgartirish\n"
            "/antispam [on|off] — Spamga qarshi himoya\n"
            "/ban — Foydalanuvchini bloklash\n"
            "/kick — Foydalanuvchini chiqarish\n"
            "/mute — Foydalanuvchini ovozsiz qilish\n"
            "/unban — Blokni olib tashlash\n"
        ),
        "en": (
            "👋 Hello! I'm a group management bot.\n\n"
            "Add me to your group and make me an admin — I'll help manage your group.\n\n"
            "📌 Commands:\n"
            "/lang [uz|en] — Change language\n"
            "/antispam [on|off] — Toggle spam protection\n"
            "/ban — Ban a user\n"
            "/kick — Kick a user\n"
            "/mute — Mute a user\n"
            "/unban — Unban a user\n"
        ),
    },

    # ── Language ──
    "lang_changed": {
        "uz": "✅ Til o'zgartirildi: {lang}",
        "en": "✅ Language changed to: {lang}",
    },
    "lang_usage": {
        "uz": "ℹ️ Foydalanish: /lang uz yoki /lang en",
        "en": "ℹ️ Usage: /lang uz or /lang en",
    },
    "lang_not_admin": {
        "uz": "⛔ Bu buyruq faqat guruh adminlari uchun.",
        "en": "⛔ This command is for group admins only.",
    },
    "lang_private": {
        "uz": "ℹ️ Bu buyruqni guruh ichida ishlating.",
        "en": "ℹ️ Use this command inside a group.",
    },

    # ── Admin tracking ──
    "added_as_admin": {
        "uz": "✅ Men «{title}» guruhiga admin sifatida qo'shildim!",
        "en": "✅ I've been added as admin in «{title}»!",
    },
    "removed_as_admin": {
        "uz": "❌ Men «{title}» guruhidan chiqarildim yoki admin huquqlarim olib tashlandi.",
        "en": "❌ I've been removed or lost admin rights in «{title}».",
    },

    # ── Stats ──
    "stats_header": {
        "uz": "📊 <b>Bot statistikasi</b>\n\nJami boshqarilayotgan guruhlar: {count}\n",
        "en": "📊 <b>Bot Statistics</b>\n\nTotal managed chats: {count}\n",
    },
    "stats_row": {
        "uz": "• <b>{title}</b> ({chat_type}) — {members} a'zo",
        "en": "• <b>{title}</b> ({chat_type}) — {members} members",
    },
    "stats_empty": {
        "uz": "📭 Hozircha boshqarilayotgan guruhlar yo'q.",
        "en": "📭 No managed chats yet.",
    },
    "stats_error": {
        "uz": "⚠️ «{title}» uchun ma'lumot olib bo'lmadi.",
        "en": "⚠️ Could not fetch data for «{title}».",
    },

    # ── Broadcast ──
    "broadcast_done": {
        "uz": "✅ Xabar {success}/{total} guruhga yuborildi.",
        "en": "✅ Message sent to {success}/{total} chats.",
    },
    "broadcast_no_reply": {
        "uz": "ℹ️ Iltimos, xabarga javob (reply) qilib /broadcast yozing.",
        "en": "ℹ️ Please reply to a message with /broadcast.",
    },
    "broadcast_empty": {
        "uz": "📭 Boshqarilayotgan guruhlar yo'q.",
        "en": "📭 No managed chats to broadcast to.",
    },

    # ── Invite link ──
    "link_success": {
        "uz": "🔗 «{title}» uchun havola:\n{link}",
        "en": "🔗 Invite link for «{title}»:\n{link}",
    },
    "link_error": {
        "uz": "⚠️ Havola yaratib bo'lmadi. Bot admin ekanligini tekshiring.\nXato: {error}",
        "en": "⚠️ Could not create invite link. Make sure I'm an admin.\nError: {error}",
    },
    "link_usage": {
        "uz": "ℹ️ Foydalanish: /get_link <chat_id>",
        "en": "ℹ️ Usage: /get_link <chat_id>",
    },

    # ── Owner guard ──
    "not_owner": {
        "uz": "⛔ Bu buyruq faqat bot egasi uchun.",
        "en": "⛔ This command is for the bot owner only.",
    },

    # ── Antispam ──
    "antispam_on": {
        "uz": "🛡 Spam himoyasi yoqildi. Reklama va havolalar avtomatik o'chiriladi.",
        "en": "🛡 Spam protection enabled. Ads and links will be auto-deleted.",
    },
    "antispam_off": {
        "uz": "🔓 Spam himoyasi o'chirildi.",
        "en": "🔓 Spam protection disabled.",
    },
    "antispam_usage": {
        "uz": "ℹ️ Foydalanish: /antispam on yoki /antispam off",
        "en": "ℹ️ Usage: /antispam on or /antispam off",
    },
    "antispam_deleted": {
        "uz": "🗑 Spam xabar o'chirildi. Foydalanuvchi: {user}",
        "en": "🗑 Spam message deleted. User: {user}",
    },

    # ── Ban ──
    "ban_done": {
        "uz": "🚫 <b>{user}</b> guruhdan bloklandi.",
        "en": "🚫 <b>{user}</b> has been banned from the group.",
    },
    "ban_reply": {
        "uz": "ℹ️ Bloklash uchun foydalanuvchi xabariga javob bering.",
        "en": "ℹ️ Reply to a user's message to ban them.",
    },
    "ban_failed": {
        "uz": "⚠️ Bloklash amalga oshmadi: {error}",
        "en": "⚠️ Ban failed: {error}",
    },
    "ban_cannot_admin": {
        "uz": "⛔ Adminlarni bloklash mumkin emas.",
        "en": "⛔ Cannot ban administrators.",
    },

    # ── Kick ──
    "kick_done": {
        "uz": "👢 <b>{user}</b> guruhdan chiqarildi.",
        "en": "👢 <b>{user}</b> has been kicked from the group.",
    },
    "kick_reply": {
        "uz": "ℹ️ Chiqarish uchun foydalanuvchi xabariga javob bering.",
        "en": "ℹ️ Reply to a user's message to kick them.",
    },
    "kick_failed": {
        "uz": "⚠️ Chiqarish amalga oshmadi: {error}",
        "en": "⚠️ Kick failed: {error}",
    },

    # ── Mute ──
    "mute_done": {
        "uz": "🔇 <b>{user}</b> {duration} ga ovozsiz qilindi.",
        "en": "🔇 <b>{user}</b> has been muted for {duration}.",
    },
    "mute_reply": {
        "uz": "ℹ️ Ovozsiz qilish uchun foydalanuvchi xabariga javob bering.\nFoydalanish: /mute yoki /mute 1h",
        "en": "ℹ️ Reply to a user's message to mute them.\nUsage: /mute or /mute 1h",
    },
    "mute_failed": {
        "uz": "⚠️ Ovozsiz qilish amalga oshmadi: {error}",
        "en": "⚠️ Mute failed: {error}",
    },

    # ── Unban ──
    "unban_done": {
        "uz": "✅ <b>{user}</b> blokdan chiqarildi.",
        "en": "✅ <b>{user}</b> has been unbanned.",
    },
    "unban_usage": {
        "uz": "ℹ️ Foydalanish: /unban <user_id> yoki xabarga javob bering.",
        "en": "ℹ️ Usage: /unban <user_id> or reply to a user's message.",
    },
    "unban_failed": {
        "uz": "⚠️ Blokdan chiqarish amalga oshmadi: {error}",
        "en": "⚠️ Unban failed: {error}",
    },

    # ── Group-only ──
    "group_only": {
        "uz": "ℹ️ Bu buyruqni guruh ichida ishlating.",
        "en": "ℹ️ Use this command inside a group.",
    },
}


def t(key: str, lang: str = "uz", **kwargs: Any) -> str:
    """Get a translated message string by *key* in the given *lang*."""
    template = MESSAGES.get(key, {}).get(lang, MESSAGES.get(key, {}).get("uz", key))
    return template.format(**kwargs) if kwargs else template


# ──────────────────────────────────────────────
#  Supabase helpers
# ──────────────────────────────────────────────

async def get_chat_lang(chat_id: int) -> str:
    """Return the language setting for *chat_id*, defaulting to 'uz'."""
    try:
        result = (
            supabase.table("managed_chats")
            .select("language")
            .eq("chat_id", chat_id)
            .maybe_single()
            .execute()
        )
        if result.data:
            return result.data.get("language", "uz")
    except Exception as exc:
        logger.warning("get_chat_lang error for %s: %s", chat_id, exc)
    return "uz"


async def get_chat_settings(chat_id: int) -> dict:
    """Return the full row for *chat_id*, or an empty dict."""
    try:
        result = (
            supabase.table("managed_chats")
            .select("*")
            .eq("chat_id", chat_id)
            .maybe_single()
            .execute()
        )
        return result.data or {}
    except Exception as exc:
        logger.warning("get_chat_settings error for %s: %s", chat_id, exc)
    return {}


async def upsert_chat(
    chat_id: int,
    chat_title: str,
    chat_type: str,
    is_admin: bool,
    language: str = "uz",
    antispam: bool | None = None,
) -> None:
    """Insert or update a row in `managed_chats`."""
    try:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "chat_title": chat_title,
            "chat_type": chat_type,
            "is_admin": is_admin,
            "language": language,
        }
        # Only include antispam if explicitly set (avoids overwriting existing value)
        if antispam is not None:
            payload["antispam"] = antispam

        supabase.table("managed_chats").upsert(
            payload,
            on_conflict="chat_id",
        ).execute()
        logger.info("Upserted chat %s (%s) is_admin=%s", chat_id, chat_title, is_admin)
    except Exception as exc:
        logger.error("upsert_chat error: %s", exc)


async def set_antispam(chat_id: int, enabled: bool) -> None:
    """Toggle the antispam column for a specific chat."""
    try:
        supabase.table("managed_chats").update(
            {"antispam": enabled}
        ).eq("chat_id", chat_id).execute()
    except Exception as exc:
        logger.error("set_antispam error: %s", exc)


async def get_admin_chats() -> list[dict]:
    """Return all rows where is_admin == True."""
    try:
        result = (
            supabase.table("managed_chats")
            .select("*")
            .eq("is_admin", True)
            .execute()
        )
        return result.data or []
    except Exception as exc:
        logger.error("get_admin_chats error: %s", exc)
        return []


# ──────────────────────────────────────────────
#  Permission helpers
# ──────────────────────────────────────────────

def is_owner(message: types.Message) -> bool:
    """Return True if the sender is the bot owner."""
    return message.from_user is not None and message.from_user.id == OWNER_ID


async def is_chat_admin(chat_id: int, user_id: int) -> bool:
    """
    Return True if *user_id* is an admin or creator in *chat_id*.
    Also returns True if *user_id* is the bot owner (global override).
    """
    if user_id == OWNER_ID:
        return True
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in (
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        )
    except Exception:
        return False


async def is_target_admin(chat_id: int, user_id: int) -> bool:
    """Return True if the *target* user is an admin/creator (to prevent banning admins)."""
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in (
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        )
    except Exception:
        return False


def parse_duration(text: str) -> timedelta | None:
    """
    Parse a human-readable duration like '1h', '30m', '2d'.
    Returns None if parsing fails, which means permanent/default.
    Supported suffixes: m (minutes), h (hours), d (days).
    """
    match = re.match(r"^(\d+)\s*([mhd])$", text.strip().lower())
    if not match:
        return None
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "m":
        return timedelta(minutes=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    return None


def user_display_name(user: types.User) -> str:
    """Return a readable display name for a user."""
    if user.username:
        return f"@{user.username}"
    name = user.first_name or ""
    if user.last_name:
        name += f" {user.last_name}"
    return name.strip() or str(user.id)


# ──────────────────────────────────────────────
#  /start — Private chat welcome
# ──────────────────────────────────────────────

@router.message(CommandStart(), F.chat.type == ChatType.PRIVATE)
async def cmd_start(message: types.Message) -> None:
    """Reply with a welcome message in the owner's language (default uz)."""
    lang = await get_chat_lang(message.chat.id)
    await message.answer(t("welcome", lang))


# ──────────────────────────────────────────────
#  /lang — Toggle group language (admin-only)
# ──────────────────────────────────────────────

@router.message(Command("lang"))
async def cmd_lang(message: types.Message) -> None:
    """
    Usage: /lang uz  or  /lang en
    Only group/supergroup admins can change the language.
    """
    # Must be used inside a group
    if message.chat.type == ChatType.PRIVATE:
        await message.answer(t("lang_private", "uz"))
        return

    lang = await get_chat_lang(message.chat.id)

    # Check that the sender is an admin of this chat
    if not await is_chat_admin(message.chat.id, message.from_user.id):
        await message.answer(t("lang_not_admin", lang))
        return

    # Parse the desired language from the command arguments
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or args[1].strip().lower() not in ("uz", "en"):
        await message.answer(t("lang_usage", lang))
        return

    new_lang = args[1].strip().lower()

    # Update Supabase
    await upsert_chat(
        chat_id=message.chat.id,
        chat_title=message.chat.title or "",
        chat_type=message.chat.type,
        is_admin=True,
        language=new_lang,
    )

    await message.answer(t("lang_changed", new_lang, lang=new_lang))


# ──────────────────────────────────────────────
#  /antispam — Toggle spam protection (admin-only)
# ──────────────────────────────────────────────

@router.message(Command("antispam"))
async def cmd_antispam(message: types.Message) -> None:
    """
    Usage: /antispam on  or  /antispam off
    Toggles auto-deletion of messages containing links, forwards, or spam keywords.
    """
    if message.chat.type == ChatType.PRIVATE:
        await message.answer(t("group_only", "uz"))
        return

    lang = await get_chat_lang(message.chat.id)

    if not await is_chat_admin(message.chat.id, message.from_user.id):
        await message.answer(t("lang_not_admin", lang))
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2 or args[1].strip().lower() not in ("on", "off"):
        await message.answer(t("antispam_usage", lang))
        return

    enabled = args[1].strip().lower() == "on"
    await set_antispam(message.chat.id, enabled)

    if enabled:
        await message.answer(t("antispam_on", lang))
    else:
        await message.answer(t("antispam_off", lang))


# ──────────────────────────────────────────────
#  Antispam auto-delete handler
#  (runs on every group message when antispam is ON)
# ──────────────────────────────────────────────

@antispam_router.message(
    F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
)
async def antispam_filter(message: types.Message) -> None:
    """
    Checks every group message for spam indicators:
      1. Forwarded from a channel / another user
      2. Contains URLs (http, t.me, telegram.me)
      3. Contains known spam keywords

    If antispam is enabled AND the sender is NOT an admin, the message
    is deleted and a notification is posted.
    """
    # Skip if no sender (channel posts, etc.)
    if not message.from_user:
        return

    # Fetch chat settings to check if antispam is enabled
    settings = await get_chat_settings(message.chat.id)
    if not settings.get("antispam", False):
        return  # antispam is off for this chat

    # Admins and the bot owner are always exempt from spam filtering
    if await is_chat_admin(message.chat.id, message.from_user.id):
        return

    lang = settings.get("language", "uz")
    is_spam = False

    # Check 1: Forwarded messages from channels / other users
    if message.forward_from or message.forward_from_chat:
        is_spam = True

    # Check 2: URLs in text
    text_to_check = message.text or message.caption or ""
    if not is_spam and SPAM_URL_REGEX.search(text_to_check):
        is_spam = True

    # Check 3: Spam keywords
    if not is_spam and SPAM_KEYWORDS.search(text_to_check):
        is_spam = True

    # Check 4: Inline URL entities (hidden links)
    if not is_spam and message.entities:
        for entity in message.entities:
            if entity.type in ("url", "text_link"):
                is_spam = True
                break

    if not is_spam and message.caption_entities:
        for entity in message.caption_entities:
            if entity.type in ("url", "text_link"):
                is_spam = True
                break

    if is_spam:
        try:
            await message.delete()
            user_name = user_display_name(message.from_user)
            # Send a temporary notification (auto-deletes are informational)
            await message.answer(t("antispam_deleted", lang, user=user_name))
            logger.info(
                "Antispam: deleted message from %s in chat %s",
                message.from_user.id,
                message.chat.id,
            )
        except Exception as exc:
            logger.warning("Antispam delete failed: %s", exc)


# ──────────────────────────────────────────────
#  /ban — Ban a user (admin-only, reply-based)
# ──────────────────────────────────────────────

@router.message(Command("ban"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_ban(message: types.Message) -> None:
    """Reply to a user's message with /ban to permanently ban them."""
    lang = await get_chat_lang(message.chat.id)

    # Only admins / owner can ban
    if not await is_chat_admin(message.chat.id, message.from_user.id):
        await message.answer(t("lang_not_admin", lang))
        return

    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.answer(t("ban_reply", lang))
        return

    target = message.reply_to_message.from_user

    # Cannot ban other admins
    if await is_target_admin(message.chat.id, target.id):
        await message.answer(t("ban_cannot_admin", lang))
        return

    try:
        await bot.ban_chat_member(
            chat_id=message.chat.id,
            user_id=target.id,
        )
        await message.answer(
            t("ban_done", lang, user=user_display_name(target)),
            parse_mode="HTML",
        )
    except Exception as exc:
        logger.error("Ban failed: %s", exc)
        await message.answer(t("ban_failed", lang, error=str(exc)))


# ──────────────────────────────────────────────
#  /kick — Kick (remove) a user (admin-only)
# ──────────────────────────────────────────────

@router.message(Command("kick"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_kick(message: types.Message) -> None:
    """Reply to a user's message with /kick to remove them (they can rejoin)."""
    lang = await get_chat_lang(message.chat.id)

    if not await is_chat_admin(message.chat.id, message.from_user.id):
        await message.answer(t("lang_not_admin", lang))
        return

    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.answer(t("kick_reply", lang))
        return

    target = message.reply_to_message.from_user

    if await is_target_admin(message.chat.id, target.id):
        await message.answer(t("ban_cannot_admin", lang))
        return

    try:
        # Ban then immediately unban = kick (user can rejoin via link)
        await bot.ban_chat_member(chat_id=message.chat.id, user_id=target.id)
        await bot.unban_chat_member(
            chat_id=message.chat.id, user_id=target.id, only_if_banned=True
        )
        await message.answer(
            t("kick_done", lang, user=user_display_name(target)),
            parse_mode="HTML",
        )
    except Exception as exc:
        logger.error("Kick failed: %s", exc)
        await message.answer(t("kick_failed", lang, error=str(exc)))


# ──────────────────────────────────────────────
#  /mute — Restrict a user (admin-only)
# ──────────────────────────────────────────────

@router.message(Command("mute"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_mute(message: types.Message) -> None:
    """
    Reply to a user's message with /mute [duration] to restrict them.
    Duration examples: 30m, 1h, 2d. Default: 1 hour.
    """
    lang = await get_chat_lang(message.chat.id)

    if not await is_chat_admin(message.chat.id, message.from_user.id):
        await message.answer(t("lang_not_admin", lang))
        return

    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.answer(t("mute_reply", lang))
        return

    target = message.reply_to_message.from_user

    if await is_target_admin(message.chat.id, target.id):
        await message.answer(t("ban_cannot_admin", lang))
        return

    # Parse optional duration argument
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        delta = parse_duration(args[1])
    else:
        delta = None

    # Default to 1 hour if no valid duration provided
    if delta is None:
        delta = timedelta(hours=1)

    until_date = datetime.now(timezone.utc) + delta

    # Human-readable duration string
    total_seconds = int(delta.total_seconds())
    if total_seconds >= 86400:
        duration_str = f"{total_seconds // 86400}d"
    elif total_seconds >= 3600:
        duration_str = f"{total_seconds // 3600}h"
    else:
        duration_str = f"{total_seconds // 60}m"

    try:
        await bot.restrict_chat_member(
            chat_id=message.chat.id,
            user_id=target.id,
            permissions=ChatPermissions(
                can_send_messages=False,
                can_send_audios=False,
                can_send_documents=False,
                can_send_photos=False,
                can_send_videos=False,
                can_send_video_notes=False,
                can_send_voice_notes=False,
                can_send_polls=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False,
                can_change_info=False,
                can_invite_users=False,
                can_pin_messages=False,
                can_manage_topics=False,
            ),
            until_date=until_date,
        )
        await message.answer(
            t("mute_done", lang, user=user_display_name(target), duration=duration_str),
            parse_mode="HTML",
        )
    except Exception as exc:
        logger.error("Mute failed: %s", exc)
        await message.answer(t("mute_failed", lang, error=str(exc)))


# ──────────────────────────────────────────────
#  /unban — Unban a user (admin-only)
# ──────────────────────────────────────────────

@router.message(Command("unban"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_unban(message: types.Message) -> None:
    """
    Unban a user by replying to their message or passing a user ID:
      /unban          (reply to a message)
      /unban 123456   (by user ID)
    """
    lang = await get_chat_lang(message.chat.id)

    if not await is_chat_admin(message.chat.id, message.from_user.id):
        await message.answer(t("lang_not_admin", lang))
        return

    target_id: int | None = None
    display_name: str = ""

    # Option 1: Reply to a user's message
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        display_name = user_display_name(message.reply_to_message.from_user)

    # Option 2: Pass user_id as argument
    if target_id is None:
        args = message.text.split(maxsplit=1)
        if len(args) >= 2:
            try:
                target_id = int(args[1].strip())
                display_name = str(target_id)
            except ValueError:
                pass

    if target_id is None:
        await message.answer(t("unban_usage", lang))
        return

    try:
        await bot.unban_chat_member(
            chat_id=message.chat.id,
            user_id=target_id,
            only_if_banned=True,
        )
        await message.answer(
            t("unban_done", lang, user=display_name),
            parse_mode="HTML",
        )
    except Exception as exc:
        logger.error("Unban failed: %s", exc)
        await message.answer(t("unban_failed", lang, error=str(exc)))


# ──────────────────────────────────────────────
#  my_chat_member — Track bot admin status
# ──────────────────────────────────────────────

@router.my_chat_member()
async def on_my_chat_member(update: types.ChatMemberUpdated) -> None:
    """
    Fires when the bot's own membership/role changes in any chat.
    - Promoted to admin → upsert with is_admin=True
    - Demoted / kicked   → upsert with is_admin=False
    """
    new_status = update.new_chat_member.status
    chat = update.chat

    # Determine if the bot is now an admin
    is_now_admin = new_status in (
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.CREATOR,
    )

    # Get existing language or default
    lang = await get_chat_lang(chat.id)

    await upsert_chat(
        chat_id=chat.id,
        chat_title=chat.title or str(chat.id),
        chat_type=chat.type,
        is_admin=is_now_admin,
        language=lang,
    )

    # Notify the owner about the status change
    try:
        if is_now_admin:
            await bot.send_message(
                OWNER_ID,
                t("added_as_admin", lang, title=chat.title or str(chat.id)),
            )
        else:
            await bot.send_message(
                OWNER_ID,
                t("removed_as_admin", lang, title=chat.title or str(chat.id)),
            )
    except Exception as exc:
        logger.warning("Could not notify owner: %s", exc)


# ──────────────────────────────────────────────
#  /stats — Owner-only: Show managed chats stats
# ──────────────────────────────────────────────

@router.message(Command("stats"), F.chat.type == ChatType.PRIVATE)
async def cmd_stats(message: types.Message) -> None:
    """
    Query all chats where is_admin == True, fetch member counts via
    Telegram API, and reply with a formatted list.
    """
    if not is_owner(message):
        await message.answer(t("not_owner", "en"))
        return

    chats = await get_admin_chats()

    if not chats:
        await message.answer(t("stats_empty", "en"))
        return

    lang = "en"  # owner always sees English
    lines: list[str] = [t("stats_header", lang, count=len(chats))]

    for chat_row in chats:
        try:
            member_count = await bot.get_chat_member_count(chat_row["chat_id"])
            lines.append(
                t(
                    "stats_row",
                    lang,
                    title=chat_row.get("chat_title", "Unknown"),
                    chat_type=chat_row.get("chat_type", "?"),
                    members=member_count,
                )
            )
        except Exception as exc:
            logger.warning("Stats error for chat %s: %s", chat_row["chat_id"], exc)
            lines.append(
                t("stats_error", lang, title=chat_row.get("chat_title", "Unknown"))
            )

    await message.answer("\n".join(lines), parse_mode="HTML")


# ──────────────────────────────────────────────
#  /broadcast — Owner-only: Forward a message to all managed chats
# ──────────────────────────────────────────────

@router.message(Command("broadcast"), F.chat.type == ChatType.PRIVATE)
async def cmd_broadcast(message: types.Message) -> None:
    """
    Reply to any message with /broadcast to forward it to every chat
    where the bot is an admin.
    """
    if not is_owner(message):
        await message.answer(t("not_owner", "en"))
        return

    # The message being broadcast is the one the owner replied to
    if not message.reply_to_message:
        await message.answer(t("broadcast_no_reply", "en"))
        return

    chats = await get_admin_chats()
    if not chats:
        await message.answer(t("broadcast_empty", "en"))
        return

    source = message.reply_to_message
    success = 0
    total = len(chats)

    for chat_row in chats:
        try:
            # Forward the original replied-to message
            await source.forward(chat_id=chat_row["chat_id"])
            success += 1
        except Exception as exc:
            logger.warning(
                "Broadcast failed for chat %s: %s", chat_row["chat_id"], exc
            )

    await message.answer(
        t("broadcast_done", "en", success=success, total=total)
    )


# ──────────────────────────────────────────────
#  /get_link — Owner-only: Generate invite link for a chat
# ──────────────────────────────────────────────

@router.message(Command("get_link"), F.chat.type == ChatType.PRIVATE)
async def cmd_get_link(message: types.Message) -> None:
    """
    Usage: /get_link <chat_id>
    Creates a one-time invite link for the specified chat (bot must be admin).
    """
    if not is_owner(message):
        await message.answer(t("not_owner", "en"))
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(t("link_usage", "en"))
        return

    try:
        target_chat_id = int(args[1].strip())
    except ValueError:
        await message.answer(t("link_usage", "en"))
        return

    try:
        # Create a primary invite link (non-expiring, unlimited uses)
        link = await bot.create_chat_invite_link(
            chat_id=target_chat_id,
            name="Generated by GroupHelp Bot",
            creates_join_request=False,
        )

        # Attempt to get the chat title for a nicer message
        try:
            chat_info = await bot.get_chat(target_chat_id)
            title = chat_info.title or str(target_chat_id)
        except Exception:
            title = str(target_chat_id)

        await message.answer(
            t("link_success", "en", title=title, link=link.invite_link)
        )

    except Exception as exc:
        logger.error("get_link error for %s: %s", target_chat_id, exc)
        await message.answer(t("link_error", "en", error=str(exc)))


# ══════════════════════════════════════════════
#  FastAPI Endpoints
# ══════════════════════════════════════════════

@app.get("/")
async def health() -> dict[str, str]:
    """Simple health-check endpoint."""
    return {"status": "ok", "bot": "GroupHelp Bot"}


@app.post("/webhook")
async def webhook(request: Request) -> Response:
    """
    Telegram sends every update to this endpoint as a POST with JSON body.

    Flow:
      1. Validate the secret token header (optional but recommended).
      2. Parse the raw JSON into an aiogram Update object.
      3. Feed it to the Dispatcher for processing.
      4. Return 200 OK immediately so Telegram doesn't retry.
    """
    # ── Step 1: Validate secret token ──
    secret = request.headers.get("x-telegram-bot-api-secret-token", "")
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        logger.warning("Webhook request with invalid secret token")
        return Response(status_code=403, content="Forbidden")

    # ── Step 2: Parse the Telegram update ──
    try:
        data = await request.json()
        update = Update.model_validate(data, context={"bot": bot})
    except Exception as exc:
        logger.error("Failed to parse update: %s", exc)
        return Response(status_code=200, content="ok")

    # ── Step 3: Feed to Dispatcher ──
    try:
        await dp.feed_update(bot=bot, update=update)
    except Exception as exc:
        logger.error("Error processing update: %s", exc)

    # ── Step 4: Always return 200 ──
    return Response(status_code=200, content="ok")
