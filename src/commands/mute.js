const db = require('../database/db');
const { requireAdmin } = require('../utils/permissions');

function getUsername(user) {
  return user.username ? `@${user.username}` : user.first_name;
}

/**
 * Parse duration string: "2h" → seconds, "1d" → seconds, default 1h
 */
function parseDuration(arg) {
  if (!arg) return { seconds: 3600, label: '1 hour' };

  const match = arg.match(/^(\d+)(h|d)$/i);
  if (!match) return { seconds: 3600, label: '1 hour' };

  const value = parseInt(match[1], 10);
  const unit = match[2].toLowerCase();

  if (unit === 'h') {
    return {
      seconds: value * 3600,
      label: `${value} hour${value !== 1 ? 's' : ''}`,
    };
  } else {
    return {
      seconds: value * 86400,
      label: `${value} day${value !== 1 ? 's' : ''}`,
    };
  }
}

/**
 * /mute [Xh|Xd] — Mute a user
 */
function registerMute(bot) {
  bot.command('mute', requireAdmin, async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }
    if (!ctx.message.reply_to_message) {
      return ctx.reply('⚠️ Please reply to a user to use this command');
    }

    const target = ctx.message.reply_to_message.from;
    const groupId = ctx.chat.id;
    const args = ctx.message.text.split(' ').slice(1);
    const { seconds, label } = parseDuration(args[0]);

    const untilDate = Math.floor(Date.now() / 1000) + seconds;
    const untilISO = new Date(untilDate * 1000).toISOString();

    try {
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

      db.prepare(
        'INSERT INTO mutes (user_id, group_id, until, muted_by) VALUES (?, ?, ?, ?)'
      ).run(target.id, groupId, untilISO, ctx.from.id);

      return ctx.reply(`🔇 ${getUsername(target)} has been muted for ${label}`);
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      if (err.description && err.description.includes('not enough rights')) {
        return ctx.reply(
          "🚫 I don't have enough permissions. Please make me an admin with all permissions"
        );
      }
      return ctx.reply('❌ Failed to mute user');
    }
  });
}

/**
 * /unmute — Unmute a user
 */
function registerUnmute(bot) {
  bot.command('unmute', requireAdmin, async (ctx) => {
    if (!['group', 'supergroup'].includes(ctx.chat.type)) {
      return ctx.reply('⚠️ This command only works in groups');
    }
    if (!ctx.message.reply_to_message) {
      return ctx.reply('⚠️ Please reply to a user to use this command');
    }

    const target = ctx.message.reply_to_message.from;
    const groupId = ctx.chat.id;

    try {
      await ctx.telegram.restrictChatMember(groupId, target.id, {
        permissions: {
          can_send_messages: true,
          can_send_audios: true,
          can_send_documents: true,
          can_send_photos: true,
          can_send_videos: true,
          can_send_other_messages: true,
          can_add_web_page_previews: true,
          can_send_polls: true,
          can_change_info: false,
          can_invite_users: true,
          can_pin_messages: false,
        },
      });

      db.prepare(
        'DELETE FROM mutes WHERE user_id = ? AND group_id = ?'
      ).run(target.id, groupId);

      return ctx.reply(`🔊 ${getUsername(target)} has been unmuted`);
    } catch (err) {
      console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
      if (err.description && err.description.includes('not enough rights')) {
        return ctx.reply(
          "🚫 I don't have enough permissions. Please make me an admin with all permissions"
        );
      }
      return ctx.reply('❌ Failed to unmute user');
    }
  });
}

module.exports = (bot) => {
  registerMute(bot);
  registerUnmute(bot);
};
