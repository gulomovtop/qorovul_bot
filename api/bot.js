const { Telegraf } = require('telegraf');
const { BOT_TOKEN } = require('../config/config');

// ─── Create bot instance (reused across warm Vercel invocations) ──────────────
const bot = new Telegraf(BOT_TOKEN);

// ─── Message Tracking Middleware ──────────────────────────────────────────────
const supabase = require('../src/database/db');

bot.on('text', async (ctx, next) => {
  if (!['group', 'supergroup'].includes(ctx.chat?.type)) return next();
  if (!ctx.from || ctx.from.is_bot) return next();

  const { id: userId, username, first_name } = ctx.from;
  const groupId = ctx.chat.id;

  try {
    // Check if user exists
    const { data: existing } = await supabase
      .from('users')
      .select('message_count')
      .eq('user_id', userId)
      .eq('group_id', groupId)
      .single();

    if (existing) {
      await supabase
        .from('users')
        .update({
          username: username || null,
          first_name,
          message_count: existing.message_count + 1,
        })
        .eq('user_id', userId)
        .eq('group_id', groupId);
    } else {
      await supabase.from('users').insert({
        user_id: userId,
        group_id: groupId,
        username: username || null,
        first_name,
        join_date: new Date().toISOString(),
        message_count: 1,
      });
    }
  } catch (err) {
    console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
  }

  return next();
});

// ─── Protection Middleware ────────────────────────────────────────────────────
const antifloodHandler = require('../src/handlers/antiflood');
const antispamHandler = require('../src/handlers/antispam');

bot.on('text', antifloodHandler);
bot.on('text', antispamHandler);

// ─── Commands ─────────────────────────────────────────────────────────────────
require('../src/commands/admin')(bot);
require('../src/commands/warn')(bot);
require('../src/commands/mute')(bot);
require('../src/commands/ban')(bot);
require('../src/commands/stats')(bot);
require('../src/commands/userinfo')(bot);

// ─── Handlers ─────────────────────────────────────────────────────────────────
require('../src/handlers/newMember')(bot);

// ─── Toggle Commands ──────────────────────────────────────────────────────────
const { requireAdmin } = require('../src/utils/permissions');

bot.command('antiflood', requireAdmin, async (ctx) => {
  if (!['group', 'supergroup'].includes(ctx.chat.type)) {
    return ctx.reply('⚠️ This command only works in groups');
  }
  const args = ctx.message.text.split(' ').slice(1);
  const toggle = args[0]?.toLowerCase();
  if (toggle !== 'on' && toggle !== 'off') {
    return ctx.reply('⚠️ Usage: /antiflood on|off');
  }
  const enabled = toggle === 'on' ? 1 : 0;
  await supabase
    .from('settings')
    .upsert({ group_id: ctx.chat.id, antiflood_enabled: enabled }, { onConflict: 'group_id' });
  return ctx.reply(`⚡ Antiflood protection has been turned ${toggle}`);
});

bot.command('antispam', requireAdmin, async (ctx) => {
  if (!['group', 'supergroup'].includes(ctx.chat.type)) {
    return ctx.reply('⚠️ This command only works in groups');
  }
  const args = ctx.message.text.split(' ').slice(1);
  const toggle = args[0]?.toLowerCase();
  if (toggle !== 'on' && toggle !== 'off') {
    return ctx.reply('⚠️ Usage: /antispam on|off');
  }
  const enabled = toggle === 'on' ? 1 : 0;
  await supabase
    .from('settings')
    .upsert({ group_id: ctx.chat.id, antispam_enabled: enabled }, { onConflict: 'group_id' });
  return ctx.reply(`🔁 Antispam protection has been turned ${toggle}`);
});

// ─── Vercel Serverless Handler ────────────────────────────────────────────────
module.exports = async (req, res) => {
  if (req.method === 'POST') {
    try {
      await bot.handleUpdate(req.body);
      res.status(200).json({ ok: true });
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      res.status(500).json({ error: err.message });
    }
  } else {
    res.status(200).json({ status: '🤖 Bot is running', ok: true });
  }
};
