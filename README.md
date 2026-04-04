# GroupHelp Bot — Telegram Group Management

Production-ready Telegram group management bot built with **aiogram 3.x**, **FastAPI**, **Supabase PostgreSQL**, deployed on **Vercel**.

## Features

| Feature | Description |
|---------|-------------|
| 📊 Activity Tracking | `/top_day`, `/top_week` with medal leaderboards |
| ⚠️ Admin Commands | `/warn`, `/ban`, `/unban`, `/mute`, `/unmute`, `/info` |
| 🚫 Anti-Advertisement | Auto-detects and removes links/ads from non-admins |
| 📝 Custom Ad Text | `/set_ad` — per-language ad replacement messages |
| 🌐 Multilingual | Uzbek, Russian, English with inline language selection |
| 👋 Welcome System | Greets new members with language choice buttons |

---

## 1. Supabase Database Setup

1. Create a project at [supabase.com](https://supabase.com)
2. Go to **SQL Editor** and run this SQL:

```sql
-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id   BIGINT PRIMARY KEY,
    username  TEXT,
    language  TEXT NOT NULL DEFAULT 'en'
);

-- Messages table (activity tracking)
CREATE TABLE IF NOT EXISTS messages (
    id        SERIAL PRIMARY KEY,
    user_id   BIGINT NOT NULL,
    group_id  BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_messages_group_ts
    ON messages (group_id, timestamp);
CREATE INDEX IF NOT EXISTS ix_messages_group_user
    ON messages (group_id, user_id);

-- Warnings table
CREATE TABLE IF NOT EXISTS warns (
    id        SERIAL PRIMARY KEY,
    user_id   BIGINT NOT NULL,
    group_id  BIGINT NOT NULL,
    count     INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT uq_warns_user_group UNIQUE (user_id, group_id)
);

-- Settings table (ad replacement text)
CREATE TABLE IF NOT EXISTS settings (
    group_id   BIGINT PRIMARY KEY,
    ad_text_uz TEXT,
    ad_text_ru TEXT,
    ad_text_en TEXT
);

-- Bot groups table (tracks where the bot is admin)
CREATE TABLE IF NOT EXISTS bot_groups (
    group_id   BIGINT PRIMARY KEY,
    title      TEXT,
    is_admin   BOOLEAN NOT NULL DEFAULT FALSE
);
```

3. Copy your **connection string** from **Settings → Database → Connection string → URI** (use the **Transaction pooler** for serverless).

---

## 2. Environment Variables

| Variable | Example |
|----------|---------|
| `BOT_TOKEN` | `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11` |
| `DATABASE_URL` | `postgresql://postgres.xxxx:password@aws-0-region.pooler.supabase.com:6543/postgres` |
| `WEBHOOK_URL` | `https://your-project.vercel.app/webhook` |

---

## 3. Deploy to Vercel

### Option A: Vercel CLI

```bash
# Install Vercel CLI
npm i -g vercel

# From the project root
vercel

# Set environment variables
vercel env add BOT_TOKEN
vercel env add DATABASE_URL
vercel env add WEBHOOK_URL

# Deploy to production
vercel --prod
```

### Option B: Vercel Dashboard

1. Push the code to a GitHub repo
2. Import the repo at [vercel.com/new](https://vercel.com/new)
3. Add the three environment variables in **Settings → Environment Variables**
4. Deploy

---

## 4. Set Webhook

After deployment, visit this URL **once** in your browser:

```
https://your-project.vercel.app/set-webhook
```

You should see `{"ok": true, "webhook": "https://..."}`.

Alternatively, the webhook is automatically set on the first cold start.

---

## 5. Bot Commands Reference

### All Users
| Command | Description |
|---------|-------------|
| `/start` | Start bot (private chat) |
| `/lang` | Change language |
| `/top_day` | Top 10 users — last 24 hours |
| `/top_week` | Top 10 users — last 7 days |
| `/mygroups` | List groups where bot is admin + invite links (private chat) |

### Admins Only (reply to a message)
| Command | Description |
|---------|-------------|
| `/warn` | Warn user (auto-mute at 3) |
| `/ban` | Ban user |
| `/unban` | Unban user |
| `/mute 30m\|2h\|1d` | Mute with duration |
| `/unmute` | Unmute user |
| `/info` | Show user ID & username |
| `/set_ad uz\|ru\|en <text>` | Set ad replacement text |

---

## Project Structure

```
├── api/
│   └── index.py          # Vercel entry point (FastAPI)
├── bot/
│   ├── loader.py          # Bot + Dispatcher init
│   ├── handlers/          # Command handlers
│   ├── middlewares/        # i18n, tracking, anti-ad
│   ├── services/          # Business logic (DB queries)
│   ├── database/          # SQLAlchemy engine + models
│   └── utils/             # Translations, filters
├── requirements.txt
└── vercel.json
```

## License

MIT
