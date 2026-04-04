"""Vercel serverless entry point — FastAPI + aiogram 3 webhook."""

import os
import sys
from contextlib import asynccontextmanager

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from aiogram.types import Update

from bot.loader import bot, dp

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Set webhook on first cold-start, clean up on shutdown."""
    if WEBHOOK_URL:
        await bot.set_webhook(
            url=WEBHOOK_URL,
            allowed_updates=["message", "callback_query", "my_chat_member"],
            drop_pending_updates=False,
        )
    yield
    await bot.session.close()


app = FastAPI(lifespan=lifespan)


@app.post("/webhook")
async def webhook_handler(request: Request):
    """Receive a Telegram update and feed it to aiogram."""
    try:
        data = await request.json()
        update = Update.model_validate(data, context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception as exc:
        print(f"[webhook] error: {exc}")
    return JSONResponse(content={"ok": True})


@app.get("/")
async def health():
    return {"status": "alive", "bot": "GroupHelpBot"}


@app.get("/set-webhook")
async def manual_set_webhook():
    """Call once after deploy to register the webhook with Telegram."""
    if not WEBHOOK_URL:
        return {"error": "WEBHOOK_URL env var not set"}
    info = await bot.set_webhook(
        url=WEBHOOK_URL,
        allowed_updates=["message", "callback_query", "my_chat_member"],
        drop_pending_updates=True,
    )
    return {"ok": info, "webhook": WEBHOOK_URL}
