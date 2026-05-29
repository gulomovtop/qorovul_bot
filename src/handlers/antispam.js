const db = require('../database/db');

// In-memory spam tracker: "userId:groupId" → { lastMessage: "", lastTime: 0, count: 0 }
const spamMap = new Map();

/**
 * Antispam middleware — call this on every group text message
 */
async function antispamHandler(ctx, next) {
  if (!['group', 'supergroup'].includes(ctx.chat?.type)) return next();
  if (!ctx.from || ctx.from.is_bot) return next();
  if (!ctx.message?.text) return next();

  const groupId = ctx.chat.id;

  // Load settings
  const settings = db
    .prepare('SELECT antispam_enabled, antispam_window, antispam_max FROM settings WHERE group_id = ?')
    .get(groupId);

  if (!settings || !settings.antispam_enabled) return next();

  const { antispam_window, antispam_max } = settings;
  const key = `${ctx.from.id}:${groupId}`;
  const now = Date.now();
  const text = ctx.message.text;

  if (!spamMap.has(key)) {
    spamMap.set(key, { lastMessage: '', lastTime: 0, count: 0 });
  }

  const entry = spamMap.get(key);
  const windowMs = antispam_window * 1000;
  const isDuplicate = text === entry.lastMessage && now - entry.lastTime < windowMs;

  if (isDuplicate) {
    entry.count += 1;

    // Delete duplicate message
    try {
      await ctx.deleteMessage();
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
    }

    if (entry.count === 1) {
      await ctx.reply(
        `🔁 @${ctx.from.username || ctx.from.first_name} please don't repeat messages`
      );
    } else if (entry.count >= antispam_max) {
      // Mute for 30 minutes
      const thirtyMinSeconds = 30 * 60;
      const untilDate = Math.floor(now / 1000) + thirtyMinSeconds;
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
          `🔇 @${ctx.from.username || ctx.from.first_name} muted 30 min for spamming`
        );
      } catch (err) {
        console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      }

      // Reset after mute
      entry.count = 0;
      entry.lastMessage = '';
      entry.lastTime = 0;
    }
    return; // Don't propagate duplicate
  }

  // Not a duplicate — reset tracker
  entry.count = 0;
  entry.lastMessage = text;
  entry.lastTime = now;

  return next();
}

module.exports = antispamHandler;
