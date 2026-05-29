const supabase = require('../database/db');
const { requireAdmin } = require('../utils/permissions');

function getUsername(user) {
  return user.username ? `@${user.username}` : user.first_name;
}

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
      await supabase.from('warnings').insert({
        user_id: target.id,
        group_id: groupId,
        reason,
        date: new Date().toISOString(),
        warned_by: ctx.from.id,
      });

      // Count warnings
      const { count } = await supabase
        .from('warnings')
        .select('*', { count: 'exact', head: true })
        .eq('user_id', target.id)
        .eq('group_id', groupId);

      if (count < 3) {
        return ctx.reply(
          `⚠️ ${getUsername(target)} has been warned (${count}/3)\nReason: ${reason}`
        );
      }

      // 3 warnings — mute 6 hours
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

      await supabase.from('mutes').insert({
        user_id: target.id,
        group_id: groupId,
        until: untilISO,
        muted_by: ctx.from.id,
      });

      // Reset warnings
      await supabase
        .from('warnings')
        .delete()
        .eq('user_id', target.id)
        .eq('group_id', groupId);

      return ctx.reply(
        `🔇 ${getUsername(target)} has been muted for 6 hours — 3 warnings reached. Warnings have been reset.`
      );
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      if (err.description?.includes('not enough rights')) {
        return ctx.reply("🚫 I don't have enough permissions. Please make me an admin with all permissions");
      }
      return ctx.reply('❌ Failed to warn user');
    }
  });
}

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

    try {
      const { data: latest } = await supabase
        .from('warnings')
        .select('id')
        .eq('user_id', target.id)
        .eq('group_id', groupId)
        .order('id', { ascending: false })
        .limit(1)
        .single();

      if (!latest) {
        return ctx.reply(`✅ ${getUsername(target)} has no warnings to remove`);
      }

      await supabase.from('warnings').delete().eq('id', latest.id);

      const { count } = await supabase
        .from('warnings')
        .select('*', { count: 'exact', head: true })
        .eq('user_id', target.id)
        .eq('group_id', groupId);

      return ctx.reply(`✅ 1 warning removed from ${getUsername(target)} (now ${count}/3)`);
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      return ctx.reply('❌ Failed to remove warning');
    }
  });
}

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

    const { count } = await supabase
      .from('warnings')
      .select('*', { count: 'exact', head: true })
      .eq('user_id', target.id)
      .eq('group_id', groupId);

    return ctx.reply(`⚠️ ${getUsername(target)} has ${count}/3 warnings`);
  });
}

module.exports = (bot) => {
  registerWarn(bot);
  registerUnwarn(bot);
  registerWarnings(bot);
};
