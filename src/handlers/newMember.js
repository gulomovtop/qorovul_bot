const supabase = require('../database/db');

function registerNewMember(bot) {
  bot.on('my_chat_member', async (ctx) => {
    const update = ctx.myChatMember;
    if (!update) return;

    const { chat, new_chat_member } = update;
    const status = new_chat_member?.status;

    if (status === 'member' || status === 'administrator') {
      // Bot was added — insert default settings if not exists
      await supabase.from('settings').upsert(
        {
          group_id: chat.id,
          antiflood_enabled: 0,
          antiflood_limit: 5,
          antiflood_window: 3,
          antispam_enabled: 0,
          antispam_window: 10,
          antispam_max: 3,
        },
        { onConflict: 'group_id', ignoreDuplicates: true }
      );

      console.log(`✅ Bot added to group: ${chat.title} (ID: ${chat.id})`);
    } else if (status === 'kicked' || status === 'left') {
      console.log(`❌ Bot removed from group: ${chat.title} (ID: ${chat.id})`);
    }
  });
}

module.exports = registerNewMember;
