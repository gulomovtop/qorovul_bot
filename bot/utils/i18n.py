"""Dictionary-based i18n translation system."""

from typing import Any

TRANSLATIONS: dict[str, dict[str, str]] = {
    # ── Private /start ───────────────────────────────────────────────
    "welcome_private": {
        "en": "👋 <b>Welcome!</b>\n\nI'm <b>GroupHelp</b> — a powerful group management bot.\n\n🌐 First, please select your language:",
        "ru": "👋 <b>Добро пожаловать!</b>\n\nЯ <b>GroupHelp</b> — мощный бот для управления группами.\n\n🌐 Сначала выберите язык:",
        "uz": "👋 <b>Xush kelibsiz!</b>\n\nMen <b>GroupHelp</b> — kuchli guruh boshqaruv boti.\n\n🌐 Avval tilni tanlang:",
    },
    # ── Group welcome ────────────────────────────────────────────────
    "welcome_group": {
        "en": "👋 Welcome to the group, <b>{name}</b>!\nPlease select your language:",
        "ru": "👋 Добро пожаловать в группу, <b>{name}</b>!\nПожалуйста, выберите язык:",
        "uz": "👋 Guruhga xush kelibsiz, <b>{name}</b>!\nIltimos, tilni tanlang:",
    },
    # ── Language ─────────────────────────────────────────────────────
    "lang_prompt": {
        "en": "🌐 Select your language:",
        "ru": "🌐 Выберите язык:",
        "uz": "🌐 Tilni tanlang:",
    },
    "lang_selected": {
        "en": "✅ Language set to English.",
        "ru": "✅ Язык установлен: Русский.",
        "uz": "✅ Til tanlandi: O'zbek.",
    },
    "guide": {
        "en": (
            "📖 <b>How I Work</b>\n\n"
            "<b>1. Add me to a group</b>\n"
            "Add me and make me an admin so I can manage the group.\n\n"
            "<b>2. Activity Tracking</b> 📊\n"
            "/top_day — Top 10 most active users (24h)\n"
            "/top_week — Top 10 most active users (7 days)\n\n"
            "<b>3. Admin Commands</b> 🛡\n"
            "Reply to a message and use:\n"
            "  /warn — Warn a user (auto-mute after 3)\n"
            "  /ban — Ban a user\n"
            "  /unban — Unban a user\n"
            "  /mute 30m|2h|1d — Mute with duration\n"
            "  /unmute — Unmute a user\n"
            "  /info — Show user info\n\n"
            "<b>4. Anti-Advertisement</b> 🚫\n"
            "Links and ads from non-admins are automatically deleted.\n"
            "/antiad — Toggle this feature on/off in your group.\n\n"
            "<b>5. Custom Ad Text</b> 📝\n"
            "/set_ad uz|ru|en Your text — Set replacement message\n\n"
            "<b>6. Language</b> 🌐\n"
            "/lang — Change your language anytime"
        ),
        "ru": (
            "📖 <b>Как я работаю</b>\n\n"
            "<b>1. Добавьте меня в группу</b>\n"
            "Добавьте меня и сделайте админом для управления группой.\n\n"
            "<b>2. Отслеживание активности</b> 📊\n"
            "/top_day — Топ 10 за 24 часа\n"
            "/top_week — Топ 10 за 7 дней\n\n"
            "<b>3. Админ-команды</b> 🛡\n"
            "Ответьте на сообщение и используйте:\n"
            "  /warn — Предупредить (мут после 3)\n"
            "  /ban — Забанить\n"
            "  /unban — Разбанить\n"
            "  /mute 30m|2h|1d — Замутить\n"
            "  /unmute — Размутить\n"
            "  /info — Инфо о пользователе\n\n"
            "<b>4. Антиреклама</b> 🚫\n"
            "Ссылки от обычных пользователей удаляются автоматически.\n"
            "/antiad — Включить/выключить эту функцию в группе.\n\n"
            "<b>5. Текст замены рекламы</b> 📝\n"
            "/set_ad uz|ru|en Текст — Установить сообщение\n\n"
            "<b>6. Язык</b> 🌐\n"
            "/lang — Сменить язык"
        ),
        "uz": (
            "📖 <b>Men qanday ishlayman</b>\n\n"
            "<b>1. Meni guruhga qo'shing</b>\n"
            "Meni qo'shing va admin qiling — guruhni boshqaraman.\n\n"
            "<b>2. Faollik kuzatuvi</b> 📊\n"
            "/top_day — 24 soat ichida eng faollar\n"
            "/top_week — 7 kun ichida eng faollar\n\n"
            "<b>3. Admin buyruqlari</b> 🛡\n"
            "Xabarga javob berib ishlating:\n"
            "  /warn — Ogohlantirish (3 tadan keyin mute)\n"
            "  /ban — Bloklash\n"
            "  /unban — Blokdan chiqarish\n"
            "  /mute 30m|2h|1d — Ovozini o'chirish\n"
            "  /unmute — Ovozini qaytarish\n"
            "  /info — Foydalanuvchi haqida\n\n"
            "<b>4. Reklama taqiqi</b> 🚫\n"
            "Admin bo'lmaganlarning havolalari avtomatik o'chiriladi.\n"
            "/antiad — Bu xususiyatni guruhda yoqish yoki o'chirish.\n\n"
            "<b>5. Reklama matni</b> 📝\n"
            "/set_ad uz|ru|en Matn — Xabar o'rnatish\n\n"
            "<b>6. Til</b> 🌐\n"
            "/lang — Tilni o'zgartirish"
        ),
    },
    # ── Top ───────────────────────────────────────────────────────────
    "top_header_day": {
        "en": "📊 <b>Top 10 — Last 24 hours</b>\n",
        "ru": "📊 <b>Топ 10 — За последние 24 часа</b>\n",
        "uz": "📊 <b>Top 10 — So'nggi 24 soat</b>\n",
    },
    "top_header_week": {
        "en": "📊 <b>Top 10 — Last 7 days</b>\n",
        "ru": "📊 <b>Топ 10 — За последние 7 дней</b>\n",
        "uz": "📊 <b>Top 10 — So'nggi 7 kun</b>\n",
    },
    "top_empty": {
        "en": "😔 No activity recorded yet.",
        "ru": "😔 Активность ещё не зафиксирована.",
        "uz": "😔 Hozircha faollik qayd etilmagan.",
    },
    "top_entry": {
        "en": "{medal} <b>{name}</b> — {count} messages",
        "ru": "{medal} <b>{name}</b> — {count} сообщений",
        "uz": "{medal} <b>{name}</b> — {count} ta xabar",
    },
    # ── Warn ──────────────────────────────────────────────────────────
    "warn_issued": {
        "en": "⚠️ <b>{name}</b> warned! ({count}/3)",
        "ru": "⚠️ <b>{name}</b> предупреждён! ({count}/3)",
        "uz": "⚠️ <b>{name}</b> ogohlantirildi! ({count}/3)",
    },
    "warn_auto_mute": {
        "en": "🔇 <b>{name}</b> auto-muted for 24 h after 3 warnings.",
        "ru": "🔇 <b>{name}</b> замьючен на 24 ч после 3 предупреждений.",
        "uz": "🔇 <b>{name}</b> 3 ta ogohlantirishdan keyin 24 soatga ovozi o'chirildi.",
    },
    # ── Ban / Unban ───────────────────────────────────────────────────
    "ban_success": {
        "en": "🚫 <b>{name}</b> has been banned.",
        "ru": "🚫 <b>{name}</b> заблокирован.",
        "uz": "🚫 <b>{name}</b> bloklandi.",
    },
    "unban_success": {
        "en": "✅ <b>{name}</b> has been unbanned.",
        "ru": "✅ <b>{name}</b> разблокирован.",
        "uz": "✅ <b>{name}</b> blokdan chiqarildi.",
    },
    # ── Mute / Unmute ─────────────────────────────────────────────────
    "mute_success": {
        "en": "🔇 <b>{name}</b> muted for {duration}.",
        "ru": "🔇 <b>{name}</b> замьючен на {duration}.",
        "uz": "🔇 <b>{name}</b> {duration} ga ovozi o'chirildi.",
    },
    "unmute_success": {
        "en": "🔊 <b>{name}</b> has been unmuted.",
        "ru": "🔊 <b>{name}</b> размьючен.",
        "uz": "🔊 <b>{name}</b> ovozi qaytarildi.",
    },
    "mute_usage": {
        "en": "Usage: reply to a message with <code>/mute 30m|2h|1d</code>",
        "ru": "Формат: ответьте на сообщение — <code>/mute 30m|2h|1d</code>",
        "uz": "Format: xabarga javob bering — <code>/mute 30m|2h|1d</code>",
    },
    # ── Info ──────────────────────────────────────────────────────────
    "info_result": {
        "en": (
            "ℹ️ <b>User Info</b>\n"
            "👤 Name: {name}\n"
            "🆔 ID: <code>{user_id}</code>\n"
            "📛 Username: @{username}"
        ),
        "ru": (
            "ℹ️ <b>Информация</b>\n"
            "👤 Имя: {name}\n"
            "🆔 ID: <code>{user_id}</code>\n"
            "📛 Юзернейм: @{username}"
        ),
        "uz": (
            "ℹ️ <b>Foydalanuvchi</b>\n"
            "👤 Ism: {name}\n"
            "🆔 ID: <code>{user_id}</code>\n"
            "📛 Username: @{username}"
        ),
    },
    # ── Errors ────────────────────────────────────────────────────────
    "not_admin": {
        "en": "❌ You are not an admin.",
        "ru": "❌ Вы не администратор.",
        "uz": "❌ Siz admin emassiz.",
    },
    "no_reply": {
        "en": "❌ Reply to a user's message to use this command.",
        "ru": "❌ Ответьте на сообщение пользователя.",
        "uz": "❌ Bu buyruqni ishlatish uchun xabarga javob bering.",
    },
    "cannot_restrict_admin": {
        "en": "❌ Cannot restrict an admin.",
        "ru": "❌ Невозможно ограничить администратора.",
        "uz": "❌ Adminni cheklash mumkin emas.",
    },
    "bot_no_rights": {
        "en": "❌ I don't have enough permissions.",
        "ru": "❌ У меня недостаточно прав.",
        "uz": "❌ Menda yetarli huquqlar yo'q.",
    },
    # ── Anti-ad ───────────────────────────────────────────────────────
    "ad_detected": {
        "en": "🚫 Advertising is not allowed in this group.",
        "ru": "🚫 Реклама в этой группе запрещена.",
        "uz": "🚫 Bu guruhda reklama taqiqlangan.",
    },
    "antiad_on": {
        "en": "✅ Anti-advertisement is now <b>ENABLED</b>.",
        "ru": "✅ Антиреклама теперь <b>ВКЛЮЧЕНА</b>.",
        "uz": "✅ Reklama taqiqi endi <b>YONIQ</b>.",
    },
    "antiad_off": {
        "en": "❌ Anti-advertisement is now <b>DISABLED</b>.",
        "ru": "❌ Антиреклама теперь <b>ВЫКЛЮЧЕНА</b>.",
        "uz": "❌ Reklama taqiqi endi <b>O'CHIQ</b>.",
    },
    # ── /set_ad ───────────────────────────────────────────────────────
    "ad_set_success": {
        "en": "✅ Ad text for <b>{lang_name}</b> saved.",
        "ru": "✅ Текст для <b>{lang_name}</b> сохранён.",
        "uz": "✅ <b>{lang_name}</b> uchun matn saqlandi.",
    },
    "ad_set_usage": {
        "en": "Usage: <code>/set_ad uz|ru|en Your text</code>",
        "ru": "Формат: <code>/set_ad uz|ru|en Ваш текст</code>",
        "uz": "Format: <code>/set_ad uz|ru|en Matn</code>",
    },
    # ── /mygroups ─────────────────────────────────────────────────────
    "mygroups_header": {
        "en": "📋 <b>Groups where I'm admin:</b>\n",
        "ru": "📋 <b>Группы, где я администратор:</b>\n",
        "uz": "📋 <b>Men admin bo'lgan guruhlar:</b>\n",
    },
    "mygroups_empty": {
        "en": "😔 I'm not an admin in any group yet.",
        "ru": "😔 Я ещё не являюсь администратором ни в одной группе.",
        "uz": "😔 Men hali hech qaysi guruhda admin emasman.",
    },
    "mygroups_entry": {
        "en": "{idx}. 👥 <b>{title}</b>\n   🔗 {link}\n",
        "ru": "{idx}. 👥 <b>{title}</b>\n   🔗 {link}\n",
        "uz": "{idx}. 👥 <b>{title}</b>\n   🔗 {link}\n",
    },
    "mygroups_no_link": {
        "en": "No invite permission",
        "ru": "Нет прав на приглашение",
        "uz": "Taklif huquqi yo'q",
    },
    "mygroups_private_only": {
        "en": "❌ Use this command in private chat with me.",
        "ru": "❌ Используйте эту команду в личном чате со мной.",
        "uz": "❌ Bu buyruqni men bilan shaxsiy chatda ishlating.",
    },
    "refresh_start": {
        "en": "🔄 <b>Refreshing groups...</b>\n\nI'm scanning previously active groups to check my admin status. This might take a moment.",
        "ru": "🔄 <b>Обновление групп...</b>\n\nПроверяю свой статус администратора в ранее активных группах. Это может занять некоторое время.",
        "uz": "🔄 <b>Guruhlar yangilanmoqda...</b>\n\nOldin faol bo'lgan guruhlarda adminligimni tekshiryapman. Bu biroz vaqt olishi mumkin.",
    },
    "refresh_done": {
        "en": "✅ <b>Refresh complete!</b>\n\nFound {v} groups where I'm currently admin.",
        "ru": "✅ <b>Обновление завершено!</b>\n\nНайдено {v} групп, где я администратор.",
        "uz": "✅ <b>Yangilash tugadi!</b>\n\nMen admin bo'lgan {v} guruh topildi.",
    },
    "bot_added": {
        "en": "👋 Thanks for adding me! Make me an admin so I can manage the group.",
        "ru": "👋 Спасибо, что добавили! Сделайте меня админом для управления группой.",
        "uz": "👋 Qo'shganingiz uchun rahmat! Guruhni boshqarish uchun meni admin qiling.",
    },
    "bot_promoted": {
        "en": "✅ I'm now an admin! I'll help manage this group.",
        "ru": "✅ Теперь я админ! Помогу управлять группой.",
        "uz": "✅ Endi men adminman! Guruhni boshqarishga yordam beraman.",
    },
    # ── Duration labels ───────────────────────────────────────────────
    "dur_min": {"en": "{v} min", "ru": "{v} мин", "uz": "{v} daq"},
    "dur_hour": {"en": "{v} h", "ru": "{v} ч", "uz": "{v} soat"},
    "dur_day": {"en": "{v} d", "ru": "{v} д", "uz": "{v} kun"},
    # ── Owner panel ───────────────────────────────────────────────────
    "owner_panel": {
        "en": "👑 <b>Owner Panel</b>\n\nSelect a tool below:",
        "ru": "👑 <b>Панель владельца</b>\n\nВыберите инструмент:",
        "uz": "👑 <b>Egasi paneli</b>\n\nQuyidagi vositani tanlang:",
    },
    "not_owner": {
        "en": "❌ This command is only for the bot owner.",
        "ru": "❌ Эта команда доступна только владельцу бота.",
        "uz": "❌ Bu buyruq faqat bot egasi uchun.",
    },
}

LANG_NAMES = {"en": "English", "ru": "Русский", "uz": "O'zbek"}


def t(key: str, lang: str = "en", **kwargs: Any) -> str:
    """Return translated string, falling back to English."""
    entry = TRANSLATIONS.get(key, {})
    text = entry.get(lang) or entry.get("en") or key
    if kwargs:
        try:
            text = text.format(**kwargs)
        except (KeyError, IndexError):
            pass
    return text
