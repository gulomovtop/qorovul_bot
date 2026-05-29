require('dotenv').config();
const { Telegraf } = require('telegraf');
const { BOT_TOKEN, WEBHOOK_URL } = require('./config/config');

// ─── Register all commands & handlers ────────────────────────────────────────
const bot = new Telegraf(BOT_TOKEN);

require('./src/handlers/newMember')(bot);
require('./src/commands/admin')(bot);
require('./src/commands/warn')(bot);
require('./src/commands/mute')(bot);
require('./src/commands/ban')(bot);
require('./src/commands/stats')(bot);
require('./src/commands/userinfo')(bot);

// ─── Set Webhook ──────────────────────────────────────────────────────────────
// Run this script ONCE after your first Vercel deploy:
//   node index.js
const webhookUrl = `${WEBHOOK_URL}/api/bot`;

bot.telegram
  .setWebhook(webhookUrl)
  .then(() => {
    console.log(`✅ Webhook successfully registered:`);
    console.log(`   ${webhookUrl}`);
    console.log(`\nYour bot is ready! Telegram will now send updates to Vercel.`);
    process.exit(0);
  })
  .catch((err) => {
    console.error(`❌ Failed to set webhook:`, err.message);
    process.exit(1);
  });
