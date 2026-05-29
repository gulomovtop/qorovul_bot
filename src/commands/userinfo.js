const supabase = require('../database/db');

module.exports = (bot) => {
  bot.command('userinfo', async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }

    const target = ctx.message.reply_to_message
      ? ctx.message.reply_to_message.from
      : ctx.from;
    const groupId = ctx.chat.id;

    try {
      const { count: warnCount } = await supabase
        .from('warnings')
        .select('*', { count: 'exact', head: true })
        .eq('user_id', target.id)
        .eq('group_id', groupId);

      const { data: mute } = await supabase
        .from('mutes')
        .select('until')
        .eq('user_id', target.id)
        .eq('group_id', groupId)
        .order('id', { ascending: false })
        .limit(1)
        .single();

      const { data: ban } = await supabase
        .from('bans')
        .select('user_id')
        .eq('user_id', target.id)
        .eq('group_id', groupId)
        .single();

      const { data: userRow } = await supabase
        .from('users')
        .select('message_count')
        .eq('user_id', target.id)
        .eq('group_id', groupId)
        .single();

      const messageCount = userRow?.message_count || 0;

      // Determine status
      let status = '✅ Active';
      if (ban) {
        status = '🚫 Banned';
      } else if (mute?.until) {
        const untilDate = new Date(mute.until);
        if (untilDate > new Date()) status = '🔇 Muted';
      }

      // Cross-check with live Telegram status
      try {
        const member = await ctx.telegram.getChatMember(groupId, target.id);
        if (member.status === 'kicked') status = '🚫 Banned';
        else if (member.status === 'restricted') status = '🔇 Muted';
      } catch (_) {}

      const fullName = [target.first_name, target.last_name].filter(Boolean).join(' ');
      const username = target.username ? `@${target.username}` : 'N/A';

      return ctx.reply(
        `👤 User Info:\n` +
        `├ Name: ${fullName}\n` +
        `├ Username: ${username}\n` +
        `├ ID: ${target.id}\n` +
        `├ Warnings: ${warnCount || 0}/3\n` +
        `├ Status: ${status}\n` +
        `└ Messages sent: ${messageCount}`
      );
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      return ctx.reply('❌ Failed to fetch user info');
    }
  });
};
