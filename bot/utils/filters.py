"""Utility helpers: anti-ad regex, admin check, duration parser."""

import re
from datetime import timedelta
from typing import Optional

from aiogram import Bot
from aiogram.enums import ChatMemberStatus

# ── Anti-advertisement pattern ────────────────────────────────────────
AD_PATTERN = re.compile(
    r"("
    r"https?://\S+"                                           # http(s) links
    r"|t\.me/\S+"                                             # t.me links
    r"|@[a-zA-Z]\w{3,}"                                      # @username (4+ chars)
    r"|[\w-]+\.(com|ru|uz|net|org|io|me|info|xyz|dev|app|pro|site|online|store|shop)\b"
    r")",
    re.IGNORECASE,
)


def contains_ad(text: str | None) -> bool:
    """Return True if the text contains a link / mention / domain."""
    if not text:
        return False
    return bool(AD_PATTERN.search(text))


async def is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    """Check whether *user_id* is an admin or creator in *chat_id*."""
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in (
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        )
    except Exception:
        return False


_DURATION_RE = re.compile(r"^(\d+)([mhd])$", re.IGNORECASE)


def parse_duration(text: str) -> Optional[timedelta]:
    """Parse '30m', '2h', '1d' into a timedelta."""
    m = _DURATION_RE.match(text.strip())
    if not m:
        return None
    value, unit = int(m.group(1)), m.group(2).lower()
    if unit == "m":
        return timedelta(minutes=value)
    if unit == "h":
        return timedelta(hours=value)
    if unit == "d":
        return timedelta(days=value)
    return None


def format_duration(delta: timedelta, lang: str = "en") -> str:
    """Human-readable duration label."""
    from bot.utils.i18n import t

    total_seconds = int(delta.total_seconds())
    if total_seconds >= 86400:
        return t("dur_day", lang, v=total_seconds // 86400)
    if total_seconds >= 3600:
        return t("dur_hour", lang, v=total_seconds // 3600)
    return t("dur_min", lang, v=max(1, total_seconds // 60))
