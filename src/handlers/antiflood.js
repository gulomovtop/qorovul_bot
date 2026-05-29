const db = require('../database/db');

// In-memory flood tracker: "userId:groupId" → { timestamps: [], warned: false }
const floodMap = new Map();

/**
 * Antiflood middleware — call this on every group text message
 */
async function antifloodHandler(ctx, next) {
  // Only handle group messages
  if (!['group', 'supergroup'].includes(ctx.chat?.type)) return next();
  if (!ctx.from || ctx.from.is_bot) return next();

  const groupId = ctx.chat.id;

  // Load settings for this group
  const settings = db
    .prepare('SELECT antiflood_enabled, antiflood_limit, antiflood_window FROM settings WHERE group_id = ?')
    .get(groupId);

  if (!settings || !settings.antiflood_enabled) return next();

  const { antiflood_limit, antiflood_window } = settings;
  const key = `${ctx.from.id}:${groupId}`;
  const now = Date.now();

  if (!floodMap.has(key)) {
    floodMap.set(key, { timestamps: [], warned: false });
  }

  const entry = floodMap.get(key);

  // Add current timestamp
  entry.timestamps.push(now);

  // Filter to only keep timestamps within the window
  const windowMs = antiflood_window * 1000;
  entry.timestamps = entry.timestamps.filter((t) => now - t < windowMs);

  if (entry.timestamps.length > antiflood_limit) {
    if (!entry.warned) {
      // First offense: warn
      entry.warned = true;
      await ctx.reply(
        `⚡ @${ctx.from.username || ctx.from.first_name} slow down!`
      );
    } else {
      // Second offense: mute for 10 minutes
      const tenMinSeconds = 10 * 60;
      const untilDate = Math.floor(now / 1000) + tenMinSeconds;
      const untilISO = new Date(untilDate * 1000).toISOString();

      try {
        await ctx.telegram.restrictChatMember(groupId, ctx.from.id, {
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

        db.prepare(
          'INSERT INTO mutes (user_id, group_id, until, muted_by) VALUES (?, ?, ?, ?)'
        ).run(ctx.from.id, groupId, untilISO, ctx.botInfo.id);

        await ctx.reply(
          `🔇 @${ctx.from.username || ctx.from.first_name} has been muted for 10 minutes for flooding`
        );
      } catch (err) {
        console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      }
    }

    // Reset timestamps after action
    entry.timestamps = [];
    entry.warned = false;
    return; // Don't call next — message is considered handled
  }

  return next();
}

module.exports = antifloodHandler;
