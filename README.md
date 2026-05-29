# Group Management Bot

A production-ready Telegram group management bot built with **Node.js + Telegraf + SQLite**.

## Tech Stack

- **Runtime**: Node.js
- **Framework**: Telegraf v4
- **Database**: SQLite via better-sqlite3
- **Environment**: dotenv

## Setup

1. **Clone the repo & install dependencies**
   ```bash
   npm install
   ```

2. **Configure environment**

   Edit the `.env` file:
   ```
   BOT_TOKEN=your_bot_token_here
   OWNER_ID=your_telegram_user_id_here
   ```
   Get your `OWNER_ID` by messaging [@userinfobot](https://t.me/userinfobot) on Telegram.

3. **Start the bot**
   ```bash
   node index.js
   ```
   Or with auto-restart on file changes (Node.js 18+):
   ```bash
   npm run dev
   ```

4. **Add the bot to your group as admin** with these permissions:
   - ✅ Delete messages
   - ✅ Ban users
   - ✅ Restrict members

The database (`data/bot.db`) is created automatically on first run.

---

## Commands

| Command | Description | Who |
|---|---|---|
| `/admin` | Promote user to bot-admin (reply) | Owner only |
| `/warn [reason]` | Warn a user — 3 warnings = auto-mute 6h (reply) | Admins |
| `/unwarn` | Remove the most recent warning (reply) | Admins |
| `/warnings` | Check user warning count (reply) | Admins |
| `/mute [Xh\|Xd]` | Mute user for duration, default 1h (reply) | Admins |
| `/unmute` | Unmute user (reply) | Admins |
| `/ban [reason]` | Ban user from group (reply) | Admins |
| `/unban` | Unban user (reply) | Admins |
| `/stats` | Group statistics | Everyone |
| `/userinfo` | User profile — self or reply target | Everyone |
| `/antiflood on\|off` | Toggle flood protection | Admins |
| `/antispam on\|off` | Toggle spam protection | Admins |

---

## Auto-Protection

### Antiflood
Detects users sending too many messages too fast.
- **Trigger**: more than `antiflood_limit` messages in `antiflood_window` seconds (default: 5 msgs / 3s)
- **Action 1**: Warning message
- **Action 2**: Mute for 10 minutes

Enable per group: `/antiflood on`

### Antispam
Detects repeated identical messages.
- **Trigger**: same message sent within `antispam_window` seconds (default: 10s window, 3 repeats)
- **Action 1**: Delete message + warning
- **Action 2**: Mute for 30 minutes

Enable per group: `/antispam on`

---

## Permissions

- **Owner** (`OWNER_ID` in `.env`): Can use `/admin` to promote users
- **Admins**: Telegram admins + owner + bot-promoted users (via `/admin`)
- **Everyone**: `/stats`, `/userinfo`

---

## Project Structure

```
├── index.js              # Entry point
├── config/
│   └── config.js         # Env config loader
├── src/
│   ├── database/
│   │   └── db.js         # SQLite init & schema
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
├── data/                 # Auto-created — SQLite DB lives here
├── .env                  # Your secrets (not in git)
└── package.json
```
