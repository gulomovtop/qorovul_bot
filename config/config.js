require('dotenv').config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const OWNER_ID = parseInt(process.env.OWNER_ID, 10);
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const WEBHOOK_URL = process.env.WEBHOOK_URL;

if (!BOT_TOKEN) {
  console.error('[CONFIG ERROR] BOT_TOKEN is missing in .env');
  process.exit(1);
}
if (!OWNER_ID || isNaN(OWNER_ID)) {
  console.error('[CONFIG ERROR] OWNER_ID is missing or invalid in .env');
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('[CONFIG ERROR] SUPABASE_URL or SUPABASE_KEY is missing in .env');
  process.exit(1);
}

module.exports = { BOT_TOKEN, OWNER_ID, SUPABASE_URL, SUPABASE_KEY, WEBHOOK_URL };
