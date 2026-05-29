const db = require('../database/db');
const { requireAdmin } = require('../utils/permissions');

// Helper to get display name for a user
function getUsername(user) {
  return user.username ? `@${user.username}` : user.first_name;
}

/**
 * /warn [reason] — Warn a user (3 warnings = 6h auto-mute)
 */
function registerWarn(bot) {
  bot.command('warn', requireAdmin, async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }
    if (!ctx.message.reply_to_message) {
      return ctx.reply('⚠️ Please reply to a user to use this command');
    }

    const target = ctx.message.reply_to_message.from;
    const groupId = ctx.chat.id;
    const args = ctx.message.text.split(' ').slice(1);
    const reason = args.join(' ') || 'No reason provided';

    try {
      // Insert warning
      db.prepare(
        'INSERT INTO warnings (user_id, group_id, reason, date, warned_by) VALUES (?, ?, ?, ?, ?)'
      ).run(target.id, groupId, reason, new Date().toISOString(), ctx.from.id);

      // Count warnings
      const { count } = db
        .prepare('SELECT COUNT(*) as count FROM warnings WHERE user_id = ? AND group_id = ?')
        .get(target.id, groupId);

      if (count < 3) {
        return ctx.reply(
          `⚠️ ${getUsername(target)} has been warned (${count}/3)\nReason: ${reason}`
        );
      }

      // 3 warnings reached — mute for 6 hours
      const sixHoursSeconds = 6 * 60 * 60;
      const untilDate = Math.floor(Date.now() / 1000) + sixHoursSeconds;
      const untilISO = new Date(untilDate * 1000).toISOString();

      await ctx.telegram.restrictChatMember(groupId, target.id, {
        permissions: {
          can_send_messages: false,
          can_send_audios: false,
          can_send_documents: false,
          can_send_photos: false,
          can_send_videos: false,
          can_send_other_messages: false,
        },
        until_date: untilDate,
      });

      db.prepare(
        'INSERT INTO mutes (user_id, group_id, until, muted_by) VALUES (?, ?, ?, ?)'
      ).run(target.id, groupId, untilISO, ctx.from.id);

      // Reset warnings
      db.prepare(
        'DELETE FROM warnings WHERE user_id = ? AND group_id = ?'
      ).run(target.id, groupId);

      return ctx.reply(
        `🔇 ${getUsername(target)} has been muted for 6 hours — 3 warnings reached. Warnings have been reset.`
      );
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      if (err.description && err.description.includes('not enough rights')) {
        return ctx.reply(
          "🚫 I don't have enough permissions. Please make me an admin with all permissions"
        );
      }
      return ctx.reply('❌ Failed to warn user');
    }
  });
}

/**
 * /unwarn — Remove the most recent warning from a user
 */
function registerUnwarn(bot) {
  bot.command('unwarn', requireAdmin, async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }
    if (!ctx.message.reply_to_message) {
      return ctx.reply('⚠️ Please reply to a user to use this command');
    }

    const target = ctx.message.reply_to_message.from;
    const groupId = ctx.chat.id;

    const latest = db
      .prepare(
        'SELECT id FROM warnings WHERE user_id = ? AND group_id = ? ORDER BY id DESC LIMIT 1'
      )
      .get(target.id, groupId);

    if (!latest) {
      return ctx.reply(`✅ ${getUsername(target)} has no warnings to remove`);
    }

    db.prepare('DELETE FROM warnings WHERE id = ?').run(latest.id);

    const { count } = db
      .prepare('SELECT COUNT(*) as count FROM warnings WHERE user_id = ? AND group_id = ?')
      .get(target.id, groupId);

    return ctx.reply(
      `✅ 1 warning removed from ${getUsername(target)} (now ${count}/3)`
    );
  });
}

/**
 * /warnings — Check current warning count for a user
 */
function registerWarnings(bot) {
  bot.command('warnings', requireAdmin, async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }
    if (!ctx.message.reply_to_message) {
      return ctx.reply('⚠️ Please reply to a user to use this command');
    }

    const target = ctx.message.reply_to_message.from;
    const groupId = ctx.chat.id;

    const { count } = db
      .prepare('SELECT COUNT(*) as count FROM warnings WHERE user_id = ? AND group_id = ?')
      .get(target.id, groupId);

    return ctx.reply(`⚠️ ${getUsername(target)} has ${count}/3 warnings`);
  });
}

module.exports = (bot) => {
  registerWarn(bot);
  registerUnwarn(bot);
  registerWarnings(bot);
};
