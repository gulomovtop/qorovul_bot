const supabase = require('../database/db');

module.exports = (bot) => {
  bot.command('stats', async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }

    const groupId = ctx.chat.id;
    const today = new Date().toISOString().slice(0, 10);

    try {
      const { count: members } = await supabase
        .from('users')
        .select('*', { count: 'exact', head: true })
        .eq('group_id', groupId);

      // Messages today: sum of message_count for users who joined today
      const { data: todayUsers } = await supabase
        .from('users')
        .select('message_count')
        .eq('group_id', groupId)
        .gte('join_date', `${today}T00:00:00.000Z`)
        .lt('join_date', `${today}T23:59:59.999Z`);

      const todayMsgs = (todayUsers || []).reduce((sum, u) => sum + (u.message_count || 0), 0);

      const { count: warnings } = await supabase
        .from('warnings')
        .select('*', { count: 'exact', head: true })
        .eq('group_id', groupId);

      const { count: bans } = await supabase
        .from('bans')
        .select('*', { count: 'exact', head: true })
        .eq('group_id', groupId);

      return ctx.reply(
        `📊 Group Statistics:\n` +
        `👥 Members tracked: ${members || 0}\n` +
        `💬 Messages today: ${todayMsgs}\n` +
        `⚠️ Warnings issued: ${warnings || 0}\n` +
        `🚫 Total bans: ${bans || 0}`
      );
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      return ctx.reply('❌ Failed to fetch stats');
    }
  });
};
