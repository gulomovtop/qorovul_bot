"""Bot and Dispatcher initialization — single source of truth."""

import os

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from bot.handlers import start, admin, activity, settings
from bot.middlewares.i18n import I18nMiddleware
from bot.middlewares.tracking import TrackingMiddleware
from bot.middlewares.antiad import AntiAdMiddleware

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)

dp = Dispatcher()

# ── Register routers (order matters — first match wins) ───────────────
dp.include_router(start.router)
dp.include_router(admin.router)
dp.include_router(settings.router)
dp.include_router(activity.router)  # keep last (no catch-all, but logical)

# ── Register middleware ───────────────────────────────────────────────
# Update-level: runs on every update type
dp.update.outer_middleware(I18nMiddleware())

# Message-level: anti-ad runs first, then tracking
dp.message.outer_middleware(AntiAdMiddleware())
dp.message.outer_middleware(TrackingMiddleware())
