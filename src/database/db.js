const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

// Ensure data directory exists
const dataDir = path.join(__dirname, '..', '..', 'data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new Database(path.join(dataDir, 'bot.db'));

// Enable WAL mode for better concurrent performance
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// ─── Create Tables ────────────────────────────────────────────────────────────

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    user_id      INTEGER NOT NULL,
    group_id     INTEGER NOT NULL,
    username     TEXT,
    first_name   TEXT,
    join_date    TEXT NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, group_id)
  );

  CREATE TABLE IF NOT EXISTS warnings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    group_id    INTEGER NOT NULL,
    reason      TEXT,
    date        TEXT NOT NULL,
    warned_by   INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS mutes (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id   INTEGER NOT NULL,
    group_id  INTEGER NOT NULL,
    until     TEXT,
    muted_by  INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS bans (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    group_id    INTEGER NOT NULL,
    reason      TEXT,
    date        TEXT NOT NULL,
    banned_by   INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS admins (
    user_id      INTEGER NOT NULL,
    group_id     INTEGER NOT NULL,
    promoted_by  INTEGER NOT NULL,
    date         TEXT NOT NULL,
    PRIMARY KEY (user_id, group_id)
  );

  CREATE TABLE IF NOT EXISTS settings (
    group_id            INTEGER PRIMARY KEY,
    antiflood_enabled   INTEGER NOT NULL DEFAULT 0,
    antiflood_limit     INTEGER NOT NULL DEFAULT 5,
    antiflood_window    INTEGER NOT NULL DEFAULT 3,
    antispam_enabled    INTEGER NOT NULL DEFAULT 0,
    antispam_window     INTEGER NOT NULL DEFAULT 10,
    antispam_max        INTEGER NOT NULL DEFAULT 3
  );
`);

console.log('[DB] Database initialized successfully');

module.exports = db;
