-- Run this entire script in your Supabase SQL Editor
-- Project: https://app.supabase.com → SQL Editor → New query

CREATE TABLE IF NOT EXISTS users (
  user_id       BIGINT NOT NULL,
  group_id      BIGINT NOT NULL,
  username      TEXT,
  first_name    TEXT,
  join_date     TEXT NOT NULL,
  message_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, group_id)
);

CREATE TABLE IF NOT EXISTS warnings (
  id          BIGSERIAL PRIMARY KEY,
  user_id     BIGINT NOT NULL,
  group_id    BIGINT NOT NULL,
  reason      TEXT,
  date        TEXT NOT NULL,
  warned_by   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS mutes (
  id        BIGSERIAL PRIMARY KEY,
  user_id   BIGINT NOT NULL,
  group_id  BIGINT NOT NULL,
  until     TEXT,
  muted_by  BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS bans (
  id          BIGSERIAL PRIMARY KEY,
  user_id     BIGINT NOT NULL,
  group_id    BIGINT NOT NULL,
  reason      TEXT,
  date        TEXT NOT NULL,
  banned_by   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS admins (
  user_id      BIGINT NOT NULL,
  group_id     BIGINT NOT NULL,
  promoted_by  BIGINT NOT NULL,
  date         TEXT NOT NULL,
  PRIMARY KEY (user_id, group_id)
);

CREATE TABLE IF NOT EXISTS settings (
  group_id            BIGINT PRIMARY KEY,
  antiflood_enabled   INTEGER NOT NULL DEFAULT 0,
  antiflood_limit     INTEGER NOT NULL DEFAULT 5,
  antiflood_window    INTEGER NOT NULL DEFAULT 3,
  antispam_enabled    INTEGER NOT NULL DEFAULT 0,
  antispam_window     INTEGER NOT NULL DEFAULT 10,
  antispam_max        INTEGER NOT NULL DEFAULT 3
);

CREATE TABLE IF NOT EXISTS flood_tracker (
  user_id    BIGINT NOT NULL,
  group_id   BIGINT NOT NULL,
  timestamps JSONB NOT NULL DEFAULT '[]',
  warned     BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (user_id, group_id)
);

CREATE TABLE IF NOT EXISTS spam_tracker (
  user_id       BIGINT NOT NULL,
  group_id      BIGINT NOT NULL,
  last_message  TEXT NOT NULL DEFAULT '',
  last_time     BIGINT NOT NULL DEFAULT 0,
  count         INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, group_id)
);
