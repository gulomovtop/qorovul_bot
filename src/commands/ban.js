const db = require('../database/db');
const { requireAdmin } = require('../utils/permissions');

function getUsername(user) {
  return user.username ? `@${user.username}` : user.first_name;
}

/**
 * /ban [reason] — Ban a user from the group
 */
function registerBan(bot) {
  bot.command('ban', requireAdmin, async (ctx) => {
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
      await ctx.telegram.banChatMember(groupId, target.id);

      db.prepare(
        'INSERT INTO bans (user_id, group_id, reason, date, banned_by) VALUES (?, ?, ?, ?, ?)'
      ).run(target.id, groupId, reason, new Date().toISOString(), ctx.from.id);

      return ctx.reply(
        `🚫 ${getUsername(target)} has been banned\nReason: ${reason}`
      );
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      if (err.description && err.description.includes('not enough rights')) {
        return ctx.reply(
          "🚫 I don't have enough permissions. Please make me an admin with all permissions"
        );
      }
      return ctx.reply('❌ Failed to ban user');
    }
  });
}

/**
 * /unban — Unban a user from the group
 */
function registerUnban(bot) {
  bot.command('unban', requireAdmin, async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }
    if (!ctx.message.reply_to_message) {
      return ctx.reply('⚠️ Please reply to a user to use this command');
    }

    const target = ctx.message.reply_to_message.from;
    const groupId = ctx.chat.id;

    try {
      await ctx.telegram.unbanChatMember(groupId, target.id);

      db.prepare(
        'DELETE FROM bans WHERE user_id = ? AND group_id = ?'
      ).run(target.id, groupId);

      return ctx.reply(`✅ ${getUsername(target)} has been unbanned`);
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      if (err.description && err.description.includes('not enough rights')) {
        return ctx.reply(
          "🚫 I don't have enough permissions. Please make me an admin with all permissions"
        );
      }
      return ctx.reply('❌ Failed to unban user');
    }
  });
}

module.exports = (bot) => {
  registerBan(bot);
  registerUnban(bot);
};
