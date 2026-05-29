const supabase = require('../database/db');
const { requireOwner } = require('../utils/permissions');

module.exports = (bot) => {
  bot.command('admin', requireOwner, async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }
    if (!ctx.message.reply_to_message) {
      return ctx.reply('⚠️ Please reply to a user to use this command');
    }

    const target = ctx.message.reply_to_message.from;
    const groupId = ctx.chat.id;

    try {
      const { data: existing } = await supabase
        .from('admins')
        .select('user_id')
        .eq('user_id', target.id)
        .eq('group_id', groupId)
        .single();

      if (existing) {
        return ctx.reply(`⚠️ This user is already an admin`);
      }

      await supabase.from('admins').insert({
        user_id: target.id,
        group_id: groupId,
        promoted_by: ctx.from.id,
        date: new Date().toISOString(),
      });

      return ctx.reply(`✅ ${target.first_name} has been promoted to admin by the owner`);
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      return ctx.reply('❌ Failed to promote user');
    }
  });
};
