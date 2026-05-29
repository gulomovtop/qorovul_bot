const supabase = require('../database/db');

async function antifloodHandler(ctx, next) {
  if (!['group', 'supergroup'].includes(ctx.chat?.type)) return next();
  if (!ctx.from || ctx.from.is_bot) return next();

  const groupId = ctx.chat.id;
  const userId = ctx.from.id;

  // Load settings
  const { data: settings } = await supabase
    .from('settings')
    .select('antiflood_enabled, antiflood_limit, antiflood_window')
    .eq('group_id', groupId)
    .single();

  if (!settings?.antiflood_enabled) return next();

  const { antiflood_limit, antiflood_window } = settings;
  const now = Date.now();
  const windowMs = antiflood_window * 1000;

  // Get or create flood tracker entry
  const { data: entry } = await supabase
    .from('flood_tracker')
    .select('timestamps, warned')
    .eq('user_id', userId)
    .eq('group_id', groupId)
    .single();

  const timestamps = ((entry?.timestamps) || []).filter((t) => now - t < windowMs);
  timestamps.push(now);

  if (timestamps.length > antiflood_limit) {
    if (!entry?.warned) {
      // First offense — warn
      await supabase
        .from('flood_tracker')
        .upsert({ user_id: userId, group_id: groupId, timestamps: [], warned: true }, { onConflict: 'user_id,group_id' });

      await ctx.reply(`⚡ @${ctx.from.username || ctx.from.first_name} slow down!`);
    } else {
      // Second offense — mute 10 min
      const untilDate = Math.floor(now / 1000) + 600;
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
        await ctx.reply(`🔇 @${ctx.from.username || ctx.from.first_name} has been muted for 10 minutes for flooding`);
      } catch (err) {
        console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      }

      // Reset tracker
      await supabase
        .from('flood_tracker')
        .upsert({ user_id: userId, group_id: groupId, timestamps: [], warned: false }, { onConflict: 'user_id,group_id' });
    }
    return;
  }

  // Update timestamps
  await supabase
    .from('flood_tracker')
    .upsert({ user_id: userId, group_id: groupId, timestamps, warned: entry?.warned || false }, { onConflict: 'user_id,group_id' });

  return next();
}

module.exports = antifloodHandler;
