const db = require('../database/db');

/**
 * Handles bot being added to or removed from a group
 */
function registerNewMember(bot) {
  bot.on('my_chat_member', (ctx) => {
    const update = ctx.myChatMember;
    if (!update) return;

    const { chat, new_chat_member } = update;
    const status = new_chat_member?.status;

    if (status === 'member' || status === 'administrator') {
      // Bot was added to a group
      const existing = db
        .prepare('SELECT 1 FROM settings WHERE group_id = ?')
        .get(chat.id);

      if (!existing) {
        db.prepare(
          `INSERT INTO settings
            (group_id, antiflood_enabled, antiflood_limit, antiflood_window,
             antispam_enabled, antispam_window, antispam_max)
           VALUES (?, 0, 5, 3, 0, 10, 3)`
        ).run(chat.id);
      }

      console.log(`✅ Bot added to group: ${chat.title} (ID: ${chat.id})`);
    } else if (status === 'kicked' || status === 'left') {
      console.log(`❌ Bot removed from group: ${chat.title} (ID: ${chat.id})`);
    }
  });
}

module.exports = registerNewMember;
