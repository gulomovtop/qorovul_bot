const { OWNER_ID } = require('../../config/config');
const db = require('../database/db');

/**
 * Check if the sender is the bot owner
 */
function isOwner(ctx) {
  return ctx.from && ctx.from.id === OWNER_ID;
}

/**
 * Check if the sender is an admin (owner, Telegram admin, or bot-promoted admin)
 */
async function isAdmin(ctx) {
  if (!ctx.from || !ctx.chat) return false;

  // 1. Owner is always admin
  if (isOwner(ctx)) return true;

  // 2. Check Telegram API chat member status
  try {
    const member = await ctx.telegram.getChatMember(ctx.chat.id, ctx.from.id);
    if (member.status === 'administrator' || member.status === 'creator') {
      return true;
    }
  } catch (err) {
    console.error(`[ERROR ${new Date().toISOString()}]`, err.message);
  }

  // 3. Check bot-promoted admins table
  const row = db
    .prepare('SELECT 1 FROM admins WHERE user_id = ? AND group_id = ?')
    .get(ctx.from.id, ctx.chat.id);

  return !!row;
}

/**
 * Middleware: only allows the owner to proceed
 */
async function requireOwner(ctx, next) {
  if (isOwner(ctx)) return next();
  return ctx.reply('🚫 Only the group owner can use this command');
}

/**
 * Middleware: only allows admins (owner + Telegram admins + bot-promoted) to proceed
 */
async function requireAdmin(ctx, next) {
  const admin = await isAdmin(ctx);
  if (admin) return next();
  return ctx.reply('🚫 This command is for admins only');
}

module.exports = { isOwner, isAdmin, requireOwner, requireAdmin };
