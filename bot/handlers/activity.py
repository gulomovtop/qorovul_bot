"""Activity commands: /top_day, /top_week."""

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.database.engine import async_session
from bot.services import message_service
from bot.utils.i18n import t

router = Router(name="activity")

MEDALS = ["🥇", "🥈", "🥉"]


def _build_leaderboard(rows, header_key: str, lang: str) -> str:
    """Format a top-10 leaderboard."""
    if not rows:
        return t("top_empty", lang)

    lines = [t(header_key, lang)]
    for idx, (user_id, username, count) in enumerate(rows):
        medal = MEDALS[idx] if idx < 3 else f"{idx + 1}."
        name = f"@{username}" if username else str(user_id)
        lines.append(t("top_entry", lang, medal=medal, name=name, count=count))
    return "\n".join(lines)


@router.message(Command("top_day"))
async def cmd_top_day(message: Message, lang: str) -> None:
    async with async_session() as session:
        rows = await message_service.get_top_users(session, message.chat.id, hours=24)
    await message.reply(_build_leaderboard(rows, "top_header_day", lang))


@router.message(Command("top_week"))
async def cmd_top_week(message: Message, lang: str) -> None:
    async with async_session() as session:
        rows = await message_service.get_top_users(session, message.chat.id, hours=168)
    await message.reply(_build_leaderboard(rows, "top_header_week", lang))
