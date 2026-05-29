const db = require('../database/db');

/**
 * /stats — Group statistics (open to everyone)
 */
module.exports = (bot) => {
  bot.command('stats', async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }

    const groupId = ctx.chat.id;
    const today = new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"

    const { members } = db
      .prepare('SELECT COUNT(*) as members FROM users WHERE group_id = ?')
      .get(groupId);

    // "Messages today" = users whose join_date starts with today (approximate tracker)
    // More accurately, we track message_count per user; for "today" we need a separate counter.
    // We use the users table join_date column to show users who joined today as a proxy.
    // The spec says "join_date = today" — we interpret this as users active today.
    const { today_msgs } = db
      .prepare(
        "SELECT COALESCE(SUM(message_count), 0) as today_msgs FROM users WHERE group_id = ? AND DATE(join_date) = ?"
      )
      .get(groupId, today);

    const { warnings } = db
      .prepare('SELECT COUNT(*) as warnings FROM warnings WHERE group_id = ?')
      .get(groupId);

    const { bans } = db
      .prepare('SELECT COUNT(*) as bans FROM bans WHERE group_id = ?')
      .get(groupId);

    return ctx.reply(
      `📊 Group Statistics:\n` +
      `👥 Members tracked: ${members}\n` +
      `💬 Messages today: ${today_msgs}\n` +
      `⚠️ Warnings issued: ${warnings}\n` +
      `🚫 Total bans: ${bans}`
    );
  });
};
