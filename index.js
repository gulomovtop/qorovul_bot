require('dotenv').config();

const { Telegraf } = require('telegraf');
const { BOT_TOKEN } = require('./config/config');
const db = require('./src/database/db');

// ─── Handlers & Commands ──────────────────────────────────────────────────────
const antifloodHandler = require('./src/handlers/antiflood');
const antispamHandler = require('./src/handlers/antispam');
const registerNewMember = require('./src/handlers/newMember');

const registerAdmin = require('./src/commands/admin');
const registerWarn = require('./src/commands/warn');
const registerMute = require('./src/commands/mute');
const registerBan = require('./src/commands/ban');
const registerStats = require('./src/commands/stats');
const registerUserinfo = require('./src/commands/userinfo');

// ─── Bot Init ─────────────────────────────────────────────────────────────────
const bot = new Telegraf(BOT_TOKEN);

// ─── Message Tracking Middleware ─────────────────────────────────────────────
// Upsert user record and increment message_count on every group text message
bot.on('text', (ctx, next) => {
  if (!['group', 'supergroup'].includes(ctx.chat?.type)) return next();
  if (!ctx.from || ctx.from.is_bot) return next();

  const { id: userId, username, first_name } = ctx.from;
  const groupId = ctx.chat.id;

  try {
    db.prepare(`
      INSERT INTO users (user_id, group_id, username, first_name, join_date, message_count)
      VALUES (?, ?, ?, ?, ?, 1)
      ON CONFLICT(user_id, group_id)
      DO UPDATE SET
        username = excluded.username,
        first_name = excluded.first_name,
        message_count = message_count + 1
    `).run(userId, groupId, username || null, first_name, new Date().toISOString());
  } catch (err) {
    console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
  }

  return next();
});

// ─── Protection Middleware ────────────────────────────────────────────────────
bot.on('text', antifloodHandler);
bot.on('text', antispamHandler);

// ─── Register Commands ────────────────────────────────────────────────────────
registerAdmin(bot);
registerWarn(bot);
registerMute(bot);
registerBan(bot);
registerStats(bot);
registerUserinfo(bot);

// ─── Register Handlers ────────────────────────────────────────────────────────
registerNewMember(bot);

// ─── Antiflood/Antispam toggle commands ──────────────────────────────────────
const { requireAdmin } = require('./src/utils/permissions');

bot.command('antiflood', requireAdmin, (ctx) => {
  if (!['group', 'supergroup'].includes(ctx.chat.type)) {
    return ctx.reply('⚠️ This command only works in groups');
  }

  const args = ctx.message.text.split(' ').slice(1);
  const toggle = args[0]?.toLowerCase();

  if (toggle !== 'on' && toggle !== 'off') {
    return ctx.reply('⚠️ Usage: /antiflood on|off');
  }

  const enabled = toggle === 'on' ? 1 : 0;
  db.prepare(`
    INSERT INTO settings (group_id, antiflood_enabled) VALUES (?, ?)
    ON CONFLICT(group_id) DO UPDATE SET antiflood_enabled = excluded.antiflood_enabled
  `).run(ctx.chat.id, enabled);

  return ctx.reply(`⚡ Antiflood protection has been turned ${toggle}`);
});

bot.command('antispam', requireAdmin, (ctx) => {
  if (!['group', 'supergroup'].includes(ctx.chat.type)) {
    return ctx.reply('⚠️ This command only works in groups');
  }

  const args = ctx.message.text.split(' ').slice(1);
  const toggle = args[0]?.toLowerCase();

  if (toggle !== 'on' && toggle !== 'off') {
    return ctx.reply('⚠️ Usage: /antispam on|off');
  }

  const enabled = toggle === 'on' ? 1 : 0;
  db.prepare(`
    INSERT INTO settings (group_id, antispam_enabled) VALUES (?, ?)
    ON CONFLICT(group_id) DO UPDATE SET antispam_enabled = excluded.antispam_enabled
  `).run(ctx.chat.id, enabled);

  return ctx.reply(`🔁 Antispam protection has been turned ${toggle}`);
});

// ─── Launch ───────────────────────────────────────────────────────────────────
bot.launch()
  .then(() => console.log('🤖 Bot is running...'))
  .catch((err) => {
    console.error(`[ERROR ${new Date().toISOString()}] Failed to launch bot:`, err.message);
    process.exit(1);
  });

// ─── Graceful Shutdown ────────────────────────────────────────────────────────
process.once('SIGINT', () => {
  console.log('\n🛑 Stopping bot (SIGINT)...');
  bot.stop('SIGINT');
});

process.once('SIGTERM', () => {
  console.log('\n🛑 Stopping bot (SIGTERM)...');
  bot.stop('SIGTERM');
});
