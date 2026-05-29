const supabase = require('../database/db');
const { requireAdmin } = require('../utils/permissions');

function getUsername(user) {
  return user.username ? `@${user.username}` : user.first_name;
}

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

      await supabase.from('bans').insert({
        user_id: target.id,
        group_id: groupId,
        reason,
        date: new Date().toISOString(),
        banned_by: ctx.from.id,
      });

      return ctx.reply(`🚫 ${getUsername(target)} has been banned\nReason: ${reason}`);
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      if (err.description?.includes('not enough rights')) {
        return ctx.reply("🚫 I don't have enough permissions. Please make me an admin with all permissions");
      }
      return ctx.reply('❌ Failed to ban user');
    }
  });
}

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

      await supabase
        .from('bans')
        .delete()
        .eq('user_id', target.id)
        .eq('group_id', groupId);

      return ctx.reply(`✅ ${getUsername(target)} has been unbanned`);
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      if (err.description?.includes('not enough rights')) {
        return ctx.reply("🚫 I don't have enough permissions. Please make me an admin with all permissions");
      }
      return ctx.reply('❌ Failed to unban user');
    }
  });
}

module.exports = (bot) => {
  registerBan(bot);
  registerUnban(bot);
};
