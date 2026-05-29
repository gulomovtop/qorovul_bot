const db = require('../database/db');
const { requireOwner } = require('../utils/permissions');

/**
 * /admin — Promote a user to bot admin (owner only)
 */
module.exports = (bot) => {
  bot.command('admin', requireOwner, async (ctx) => {
    // Group-only check
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }

    // Reply-to check
    if (!ctx.message.reply_to_message) {
      return ctx.reply('⚠️ Please reply to a user to use this command');
    }

    const target = ctx.message.reply_to_message.from;
    const groupId = ctx.chat.id;

    // Check if already admin
    const existing = db
      .prepare('SELECT 1 FROM admins WHERE user_id = ? AND group_id = ?')
      .get(target.id, groupId);

    if (existing) {
      return ctx.reply(`⚠️ This user is already an admin`);
    }

    try {
      db.prepare(
        'INSERT INTO admins (user_id, group_id, promoted_by, date) VALUES (?, ?, ?, ?)'
      ).run(target.id, groupId, ctx.from.id, new Date().toISOString());

      return ctx.reply(
        `✅ ${target.first_name} has been promoted to admin by the owner`
      );
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      return ctx.reply('❌ Failed to promote user');
    }
  });
};
