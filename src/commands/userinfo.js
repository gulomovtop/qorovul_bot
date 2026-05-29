const db = require('../database/db');

/**
 * /userinfo — Full user profile (self or reply target)
 */
module.exports = (bot) => {
  bot.command('userinfo', async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }

    const target = ctx.message.reply_to_message
      ? ctx.message.reply_to_message.from
      : ctx.from;

    const groupId = ctx.chat.id;

    // DB queries
    const { count: warnCount } = db
      .prepare('SELECT COUNT(*) as count FROM warnings WHERE user_id = ? AND group_id = ?')
      .get(target.id, groupId);

    const mute = db
      .prepare('SELECT until FROM mutes WHERE user_id = ? AND group_id = ? ORDER BY id DESC LIMIT 1')
      .get(target.id, groupId);

    const ban = db
      .prepare('SELECT 1 FROM bans WHERE user_id = ? AND group_id = ?')
      .get(target.id, groupId);

    const userRow = db
      .prepare('SELECT message_count FROM users WHERE user_id = ? AND group_id = ?')
      .get(target.id, groupId);

    const messageCount = userRow ? userRow.message_count : 0;

    // Determine status
    let status = '✅ Active';
    if (ban) {
      status = '🚫 Banned';
    } else if (mute) {
      const untilDate = mute.until ? new Date(mute.until) : null;
      if (!untilDate || untilDate > new Date()) {
        status = '🔇 Muted';
      }
    }

    // Get live Telegram status
    try {
      const member = await ctx.telegram.getChatMember(groupId, target.id);
      if (member.status === 'kicked') status = '🚫 Banned';
      else if (member.status === 'restricted') status = '🔇 Muted';
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
    }

    const fullName = [target.first_name, target.last_name].filter(Boolean).join(' ');
    const username = target.username ? `@${target.username}` : 'N/A';

    return ctx.reply(
      `👤 User Info:\n` +
      `├ Name: ${fullName}\n` +
      `├ Username: ${username}\n` +
      `├ ID: ${target.id}\n` +
      `├ Warnings: ${warnCount}/3\n` +
      `├ Status: ${status}\n` +
      `└ Messages sent: ${messageCount}`
    );
  });
};
