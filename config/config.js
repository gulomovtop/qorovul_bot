require('dotenv').config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const OWNER_ID = parseInt(process.env.OWNER_ID, 10);

if (!BOT_TOKEN) {
  console.error('[CONFIG ERROR] BOT_TOKEN is missing in .env file');
  process.exit(1);
}

if (!OWNER_ID || isNaN(OWNER_ID)) {
  console.error('[CONFIG ERROR] OWNER_ID is missing or invalid in .env file');
  process.exit(1);
}

module.exports = { BOT_TOKEN, OWNER_ID };
