"""Warning count management."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from bot.database.models import Warn


async def add_warn(session: AsyncSession, user_id: int, group_id: int) -> int:
    """Increment warn count (upsert). Return new count."""
    stmt = (
        pg_insert(Warn)
        .values(user_id=user_id, group_id=group_id, count=1)
        .on_conflict_do_update(
            constraint="uq_warns_user_group",
            set_={"count": Warn.count + 1},
        )
        .returning(Warn.count)
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.scalar_one()


async def get_warns(session: AsyncSession, user_id: int, group_id: int) -> int:
    """Return current warning count (0 if none)."""
    result = await session.execute(
        select(Warn.count).where(
            Warn.user_id == user_id, Warn.group_id == group_id
        )
    )
    return result.scalar_one_or_none() or 0
