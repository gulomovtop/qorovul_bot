"""Message tracking and top-user queries."""

from datetime import datetime, timedelta, timezone
from typing import Sequence

from sqlalchemy import select, func, BigInteger, text
from sqlalchemy.ext.asyncio import AsyncSession

from bot.database.models import Message, User


async def track_message(
    session: AsyncSession, user_id: int, group_id: int
) -> None:
    """Insert a new message record."""
    session.add(Message(user_id=user_id, group_id=group_id))
    await session.commit()


async def get_top_users(
    session: AsyncSession, group_id: int, hours: int
) -> Sequence[tuple[int, str | None, int]]:
    """Return top-10 (user_id, username, count) for the given time window."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    stmt = (
        select(
            Message.user_id,
            User.username,
            func.count().label("cnt"),
        )
        .outerjoin(User, Message.user_id == User.user_id)
        .where(Message.group_id == group_id, Message.timestamp >= since)
        .group_by(Message.user_id, User.username)
        .order_by(func.count().desc())
        .limit(10)
    )
    result = await session.execute(stmt)
    return result.all()  # type: ignore[return-value]
