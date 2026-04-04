"""Group tracking service — bot membership & invite links."""

from typing import Sequence

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from bot.database.models import BotGroup


async def upsert_group(
    session: AsyncSession, group_id: int, title: str | None, is_admin: bool
) -> None:
    """Insert or update a group record."""
    stmt = (
        pg_insert(BotGroup)
        .values(group_id=group_id, title=title, is_admin=is_admin)
        .on_conflict_do_update(
            index_elements=["group_id"],
            set_={"title": title, "is_admin": is_admin},
        )
    )
    await session.execute(stmt)
    await session.commit()


async def remove_group(session: AsyncSession, group_id: int) -> None:
    """Remove a group (bot was kicked/left)."""
    await session.execute(
        delete(BotGroup).where(BotGroup.group_id == group_id)
    )
    await session.commit()


async def get_admin_groups(session: AsyncSession) -> Sequence[BotGroup]:
    """Return all groups where the bot is an admin."""
    result = await session.execute(
        select(BotGroup).where(BotGroup.is_admin == True)  # noqa: E712
    )
    return result.scalars().all()
