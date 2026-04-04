"""Dictionary-based i18n translation system."""

from typing import Any

TRANSLATIONS: dict[str, dict[str, str]] = {
    # ── Private /start ───────────────────────────────────────────────
    "welcome_private": {
        "en": "👋 Hello! I'm a group management bot.\nPlease select your language:",
        "ru": "👋 Привет! Я бот для управления группами.\nПожалуйста, выберите язык:",
        "uz": "👋 Salom! Men guruhlarni boshqarish botiman.\nIltimos, tilni tanlang:",
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
