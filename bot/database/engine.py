"""Async SQLAlchemy engine and session factory for Supabase PostgreSQL."""

import os
import ssl as ssl_mod

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import NullPool

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Ensure asyncpg driver prefix
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Permissive SSL context for Supabase managed connections
_ssl_ctx = ssl_mod.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl_mod.CERT_NONE

engine = create_async_engine(
    DATABASE_URL,
    poolclass=NullPool,  # Best for serverless — no persistent pool
    connect_args={"ssl": _ssl_ctx},
)

async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
