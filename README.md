# Group Management Bot

A production-ready Telegram group management bot — **Node.js + Telegraf + Supabase**, deployed on **Vercel**.

## Tech Stack
- **Runtime**: Node.js
- **Framework**: Telegraf v4
- **Database**: Supabase (PostgreSQL)
- **Hosting**: Vercel (serverless webhook)

---

## Setup Guide

### Step 1 — Create accounts (all free)
- **Supabase**: https://supabase.com → create a new project
- **Vercel**: https://vercel.com → create a new project
- **GitHub**: create a new repository for this code

### Step 2 — Set up the Supabase database
1. Open your Supabase project → **SQL Editor** → **New query**
2. Paste the entire contents of `supabase_setup.sql`
3. Click **Run** — all 8 tables will be created

### Step 3 — Get your credentials

| Variable | Where to find it |
|---|---|
| `BOT_TOKEN` | @BotFather on Telegram |
| `OWNER_ID` | Message @userinfobot on Telegram |
| `SUPABASE_URL` | Supabase → Project Settings → API → Project URL |
| `SUPABASE_KEY` | Supabase → Project Settings → API → `anon` public key |
| `WEBHOOK_URL` | Your Vercel deployment URL (set after deploy) |

### Step 4 — Deploy to Vercel
1. Push this code to your new GitHub repo
2. Go to Vercel → **Add New Project** → import your GitHub repo
3. In **Environment Variables**, add all 5 variables from Step 3
   - Leave `WEBHOOK_URL` as your Vercel project URL: `https://your-project.vercel.app`
4. Click **Deploy**

### Step 5 — Register the webhook (ONE TIME ONLY)
After Vercel deploys, run this locally:
```bash
npm install
node index.js
```
You should see:
```
✅ Webhook successfully registered:
   https://your-project.vercel.app/api/bot
```

### Step 6 — Add bot to your group
Add the bot as **admin** with these permissions:
- ✅ Delete messages
- ✅ Ban users
- ✅ Restrict members

---

## Commands

| Command | Description | Who |
|---|---|---|
| `/admin` | Promote user to bot-admin (reply) | Owner only |
| `/warn [reason]` | Warn user — 3 warnings = 6h auto-mute (reply) | Admins |
| `/unwarn` | Remove most recent warning (reply) | Admins |
| `/warnings` | Check warning count (reply) | Admins |
| `/mute [Xh\|Xd]` | Mute user, default 1h (reply) | Admins |
| `/unmute` | Unmute user (reply) | Admins |
| `/ban [reason]` | Ban user (reply) | Admins |
| `/unban` | Unban user (reply) | Admins |
| `/stats` | Group statistics | Everyone |
| `/userinfo` | User profile — self or reply target | Everyone |
| `/antiflood on\|off` | Toggle flood protection | Admins |
| `/antispam on\|off` | Toggle spam protection | Admins |

---

## Project Structure

```
├── api/
│   └── bot.js            # Vercel serverless function (webhook)
├── config/
│   └── config.js         # Env loader + validation
├── src/
│   ├── database/
│   │   └── db.js         # Supabase client
│   ├── utils/
│   │   └── permissions.js
│   ├── commands/
│   │   ├── admin.js
│   │   ├── ban.js
│   │   ├── mute.js
│   │   ├── stats.js
│   │   ├── userinfo.js
│   │   └── warn.js
│   └── handlers/
│       ├── antiflood.js
│       ├── antispam.js
│       └── newMember.js
├── index.js              # Webhook registration script (run once)
├── supabase_setup.sql    # Run in Supabase SQL Editor
├── vercel.json
├── package.json
└── .env                  # Local only — never commit this
```
