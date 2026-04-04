"""User CRUD and language management."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from bot.database.models import User


async def get_or_create_user(
    session: AsyncSession, user_id: int, username: str | None = None
) -> User:
    """Return existing user or create a new one (upsert username)."""
    stmt = (
        pg_insert(User)
        .values(user_id=user_id, username=username, language="en")
        .on_conflict_do_update(
            index_elements=["user_id"],
            set_={"username": username},
        )
        .returning(User)
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.scalar_one()


async def set_language(session: AsyncSession, user_id: int, lang: str) -> None:
    """Update (or create) user language preference."""
    stmt = (
        pg_insert(User)
        .values(user_id=user_id, language=lang)
        .on_conflict_do_update(
            index_elements=["user_id"],
            set_={"language": lang},
        )
    )
    await session.execute(stmt)
    await session.commit()


async def get_language(session: AsyncSession, user_id: int) -> str:
    """Return the user's language code, defaulting to 'en'."""
    result = await session.execute(
        select(User.language).where(User.user_id == user_id)
    )
    row = result.scalar_one_or_none()
    return row or "en"
