const supabase = require('../database/db');

async function antispamHandler(ctx, next) {
  if (!['group', 'supergroup'].includes(ctx.chat?.type)) return next();
  if (!ctx.from || ctx.from.is_bot) return next();
  if (!ctx.message?.text) return next();

  const groupId = ctx.chat.id;
  const userId = ctx.from.id;
  const text = ctx.message.text;
  const now = Date.now();

  // Load settings
  const { data: settings } = await supabase
    .from('settings')
    .select('antispam_enabled, antispam_window, antispam_max')
    .eq('group_id', groupId)
    .single();

  if (!settings?.antispam_enabled) return next();

  const { antispam_window, antispam_max } = settings;
  const windowMs = antispam_window * 1000;

  // Get spam tracker entry
  const { data: entry } = await supabase
    .from('spam_tracker')
    .select('last_message, last_time, count')
    .eq('user_id', userId)
    .eq('group_id', groupId)
    .single();

  const lastMessage = entry?.last_message || '';
  const lastTime = entry?.last_time || 0;
  const count = entry?.count || 0;

  const isDuplicate = text === lastMessage && now - lastTime < windowMs;

  if (isDuplicate) {
    const newCount = count + 1;

    // Delete duplicate message
    try { await ctx.deleteMessage(); } catch (_) {}

    if (newCount === 1) {
      await ctx.reply(`🔁 @${ctx.from.username || ctx.from.first_name} please don't repeat messages`);
    } else if (newCount >= antispam_max) {
      // Mute 30 minutes
      const untilDate = Math.floor(now / 1000) + 1800;
      const untilISO = new Date(untilDate * 1000).toISOString();

      try {
        await ctx.telegram.restrictChatMember(groupId, userId, {
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

        await supabase.from('mutes').insert({ user_id: userId, group_id: groupId, until: untilISO, muted_by: 0 });
        await ctx.reply(`🔇 @${ctx.from.username || ctx.from.first_name} muted 30 min for spamming`);
      } catch (err) {
        console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      }

      // Reset tracker
      await supabase
        .from('spam_tracker')
        .upsert({ user_id: userId, group_id: groupId, last_message: '', last_time: 0, count: 0 }, { onConflict: 'user_id,group_id' });

      return;
    }

    await supabase
      .from('spam_tracker')
      .upsert({ user_id: userId, group_id: groupId, last_message: text, last_time: now, count: newCount }, { onConflict: 'user_id,group_id' });

    return;
  }

  // Not a duplicate — reset
  await supabase
    .from('spam_tracker')
    .upsert({ user_id: userId, group_id: groupId, last_message: text, last_time: now, count: 0 }, { onConflict: 'user_id,group_id' });

  return next();
}

module.exports = antispamHandler;
