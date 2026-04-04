"""SQLAlchemy ORM models for the bot database."""

from datetime import datetime

from sqlalchemy import BigInteger, Integer, Text, DateTime, Index, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(Text, nullable=True)
    language: Mapped[str] = mapped_column(Text, default="en", server_default="en")


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    group_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("ix_messages_group_ts", "group_id", "timestamp"),
        Index("ix_messages_group_user", "group_id", "user_id"),
    )


class Warn(Base):
    __tablename__ = "warns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    group_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")

    __table_args__ = (
        UniqueConstraint("user_id", "group_id", name="uq_warns_user_group"),
    )


class Settings(Base):
    __tablename__ = "settings"

    group_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ad_text_uz: Mapped[str | None] = mapped_column(Text, nullable=True)
    ad_text_ru: Mapped[str | None] = mapped_column(Text, nullable=True)
    ad_text_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    antiad_enabled: Mapped[bool] = mapped_column(default=True, server_default="true")


class BotGroup(Base):
    """Tracks groups where the bot is present / admin."""
    __tablename__ = "bot_groups"

    group_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_admin: Mapped[bool] = mapped_column(default=False, server_default="false")
