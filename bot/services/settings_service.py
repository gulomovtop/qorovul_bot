"""Group ad-text settings management."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from bot.database.models import Settings


_LANG_COL = {"uz": "ad_text_uz", "ru": "ad_text_ru", "en": "ad_text_en"}


async def set_ad_text(
    session: AsyncSession, group_id: int, lang: str, text: str
) -> None:
    """Upsert ad replacement text for a specific language."""
    col = _LANG_COL.get(lang, "ad_text_en")
    stmt = (
        pg_insert(Settings)
        .values(group_id=group_id, **{col: text})
        .on_conflict_do_update(
            index_elements=["group_id"],
            set_={col: text},
        )
    )
    await session.execute(stmt)
    await session.commit()


async def get_ad_text(
    session: AsyncSession, group_id: int, lang: str
) -> str | None:
    """Return the ad replacement text for the given language, or None."""
    col_name = _LANG_COL.get(lang, "ad_text_en")
    col = getattr(Settings, col_name)
    result = await session.execute(
        select(col).where(Settings.group_id == group_id)
    )
    return result.scalar_one_or_none()
