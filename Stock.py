import json
import asyncio
import re
import time
import logging
import hashlib
from typing import Dict, List
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from telegram.error import TelegramError
import cloudscraper
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import os
from requests.exceptions import Timeout, HTTPError, ConnectionError

# Настройка логирования
logging.basicConfig(
    filename='sticker_log.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы
TELEGRAM_BOT_TOKEN = "7566077832:AAF7oS5iOWfSGA14NM5AjrO2u8kNUM-djws"
STOCK_CHANNEL_ID = "@sbtdrasik"
FULL_STOCK_CHANNEL_ID = "@autostockgag"
STOCK_URL = "https://www.vulcanvalues.com/grow-a-garden/stock"
STOCK_CHECK_INTERVAL = 60
LOG_CLEAN_INTERVAL = 1800
MSG_ID_FILE = "last_messages.json"
PREV_STOCK_FILE = "prev_stock.json"
LAST_SENT_PERIODS_FILE = "last_sent_periods.json"
USERS_FILE = "users.json"
ADMIN_IDS = ["5194736461", "5485387724"]
ALLOWED_GROUP_ID = "-1002672611835"
BOT_USERNAME = "@Stockrasik_bot"

SECTION_LIST = ["GEAR STOCK", "EGG STOCK", "SEEDS STOCK"]
SELECTABLE_SECTIONS = ["GEAR STOCK", "EGG STOCK", "SEEDS STOCK"]
SECTION_EMOJI = {
    "GEAR STOCK": "⚙️",
    "EGG STOCK": "🥚",
    "SEEDS STOCK": "🌱"
}
SECTION_TRANSLATE = {
    "GEAR STOCK": "Предметы",
    "EGG STOCK": "Яйца",
    "SEEDS STOCK": "Семена"
}
SECTION_PERIOD = {
    "GEAR STOCK": 5,
    "SEEDS STOCK": 5,
    "EGG STOCK": 30
}

ITEM_TRANSLATE = {
    "Watering Can": "Лейка",
    "Favorite Tool": "Инструмент фаворита",
    "Recall Wrench": "Ключ возврата",
    "Trowel": "Совок",
    "Basic Sprinkler": "Обычный базовый",
    "Advanced Sprinkler": "Улучшенный разбрызгиватель",
    "Godly Sprinkler": "Годли разбрызгиватель",
    "Master Sprinkler": "Мастер разбрызгиватель",
    "Shovel": "Лопата",
    "Lightning Rod": "Громоотвод",
    "Harvest Tool": "Инструмент урожая",
    "Common Egg": "Обычное яйцо",
    "Uncommon Egg": "Необычное яйцо",
    "Rare Egg": "Редкое яйцо",
    "Bug Egg": "Баг яйцо",
    "Legendary Egg": "Легендарное яйцо",
    "Mythical Egg": "Мифическое яйцо",
    "Carrot": "Морковь",
    "Strawberry": "Клубника",
    "Coconut": "Кокос",
    "Tomato": "Томат",
    "Blueberry": "Черника",
    "Apple": "Яблоко",
    "Banana": "Банан",
    "Pineapple": "Ананас",
    "Grape": "Виноград",
    "Watermelon": "Арбуз",
    "Peach": "Персик",
    "Mango": "Манго",
    "Cherry": "Вишня",
    "Raspberry": "Малина",
    "Blackberry": "Ежевика",
    "Pumpkin": "Тыква",
    "Eggplant": "Баклажан",
    "Corn": "Кукуруза",
    "Pepper": "Перец",
    "Bell Pepper": "Болгарский перец",
    "Bamboo": "Бамбук",
    "Cactus": "Кактус",
    "Dragon Fruit": "Драконий фрукт",
    "Mushroom": "Гриб",
    "Cacao": "Какао",
    "Beanstalk": "Бобовый стебель",
    "Orange Tulip": "Оранжевый тюльпан",
    "Daffodil": "Нарцисс",
    "Sugar Apple": "Сахарное яблоко",
    "Kiwi": "Киви",
    "Green Apple": "Зелёное яблоко",
    "Avocado": "Авокадо",
    "Prickly Pear": "Кактусовый инжир",
    "Loquat": "Мушмула",
    "Feijoa": "Фейхоа",
    "Tanning Mirror": "Зеркало для загара",
    "Paradise Egg": "Райское яйцо",
    "Cauliflower": "Капуста"
}

ITEM_EMOJI = {
    "Лейка": "💧",
    "Инструмент фаворита": "💖",
    "Ключ возврата": "🔧",
    "Совок": "🪣",
    "Обычный базовый": "🚿",
    "Улучшенный разбрызгиватель": "🚿",
    "Годли разбрызгиватель": "⭐",
    "Мастер разбрызгиватель": "🏆",
    "Лопата": "🛠️",
    "Громоотвод": "⚡️",
    "Инструмент урожая": "🌾",
    "Обычное яйцо": "🥚",
    "Необычное яйцо": "🥚",
    "Редкое яйцо": "🥚",
    "Баг яйцо": "🐞",
    "Легендарное яйцо": "🌟",
    "Мифическое яйцо": "🔴",
    "Морковь": "🥕",
    "Клубника": "🍓",
    "Кокос": "🥥",
    "Томат": "🍅",
    "Черника": "🫐",
    "Яблоко": "🍏",
    "Сахарное яблоко": "🍎",
    "Банан": "🍌",
    "Ананас": "🍍",
    "Виноград": "🍇",
    "Арбуз": "🍉",
    "Персик": "🍑",
    "Манго": "🥭",
    "Вишня": "🍒",
    "Малина": "🍇",
    "Ежевика": "🍇",
    "Тыква": "🎃",
    "Баклажан": "🍆",
    "Кукуруза": "🌽",
    "Перец": "🌶️",
    "Болгарский перец": "🫑",
    "Бамбук": "🎋",
    "Кактус": "🌵",
    "Драконий фрукт": "🐉",
    "Гриб": "🍄",
    "Какао": "🍫",
    "Бобовый стебель": "🌱",
    "Оранжевый тюльпан": "🌷",
    "Нарцисс": "🌼",
    "Киви": "🥝",
    "Зелёное яблоко": "🍏",
    "Авокадо": "🥑",
    "Кактусовый инжир": "🌵",
    "Мушмула": "🍑",
    "Фейхоа": "🥭",
    "Зеркало для загара": "🪞",
    "Райское яйцо": "☀",
    "Капуста": "🥬"
}

egg_colors = {
    "Обычное яйцо": "⚪️",
    "Необычное яйцо": "🟤",
    "Редкое яйцо": "💙",
    "Баг яйцо": "🟢",
    "Легендарное яйцо": "🧡",
    "Райское яйцо": "☀",
    "Мифическое яйцо": "🟥"
}

ITEM_PRICES = {
    "Сахарное яблоко": "$25М шекелей",
    "Какао": "2.5 шекелей",
    "Перец": "1М шекелей",
    "Болгарский перец": "55 000 шекелей",
    "Бобовый стебель": "10М шекелей",
    "Манго": "100 000 шекелей",
    "Драконий фрукт": "50 000 шекелей",
    "Киви": "10 000",
    "Кактусовый инжир": "555 000 шекелей",
    "Мушмула": "900 000 шекелей",
    "Фейхоа": "2.75М шекелей",
    "Гриб": "900 000 шекелей",
    "Мифическое яйцо": "8М шекелей",
    "Баг яйцо": "50М шекелей",
    "Райское яйцо": "50М шекелей",
    "Мастер разбрызгиватель": "10М шекелей"
}

ALLOWED_ITEMS_CHANNEL = [
    "Сахарное яблоко", "Какао", "Перец", "Болгарский перец", "Бобовый стебель",
    "Манго", "Драконий фрукт", "Киви", "Кактусовый инжир", "Мушмула",
    "Фейхоа", "Гриб", "Мифическое яйцо", "Мастер разбрызгиватель", "Баг яйцо", "Райское яйцо"
]
ALLOWED_ITEMS_DM = list(ITEM_TRANSLATE.values())

last_update_time = 0
update_lock = asyncio.Lock()
pending_broadcast = {}

def normalize_item_name(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r'\s*x\d+\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name.strip())
    name_lower = name.lower()
    for eng_name in ITEM_TRANSLATE:
        if eng_name.lower() == name_lower:
            return eng_name
    return name.title()

def normalize_stock(stocks: Dict) -> Dict:
    normalized = {}
    for section, items in stocks.items():
        normalized[section] = sorted(
            [
                {
                    "name": item["name"].strip().lower(),
                    "emoji": ''.join(sorted(item["emoji"].strip())),
                    "qty": item["qty"].strip().lower()
                }
                for item in items
            ],
            key=lambda x: x["name"]
        )
    return normalized

def get_stock_hash(stocks: Dict) -> str:
    norm_stocks = normalize_stock(stocks)
    stock_str = json.dumps(norm_stocks, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(stock_str.encode('utf-8')).hexdigest()

def get_period_block(section: str) -> str:
    now = datetime.now(timezone(timedelta(hours=3)))  # EEST
    period_min = SECTION_PERIOD.get(section, 30)
    block_start = now.replace(second=0, microsecond=0)
    minute = (block_start.minute // period_min) * period_min
    block_start = block_start.replace(minute=minute)
    block_end = block_start + timedelta(minutes=period_min)
    period_str = block_end.strftime("%H:%M:%S")
    logger.debug(f"Сгенерирован период для {section}: {period_str} (текущее время: {now}, period_min: {period_min}, block_start: {block_start})")
    return period_str

def get_stock(bot: Bot) -> Dict:
    max_retries = 5
    base_timeout = 15
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Cache-Control": "no-cache",
    }
    cookies = {"cookieConsent": "accepted"}
    
    ignored_items = []
    for attempt in range(1, max_retries + 1):
        try:
            scraper = cloudscraper.create_scraper()
            logger.info(f"Попытка {attempt}: Запрос к {STOCK_URL}")
            response = scraper.get(STOCK_URL, timeout=base_timeout + attempt * 5, headers=headers, cookies=cookies)
            response.raise_for_status()
            logger.info(f"HTTP статус: {response.status_code}")
            with open("response.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.debug(f"HTML сохранён в response.html (первые 500 символов): {response.text[:500]}...")
            
            soup = BeautifulSoup(response.text, "html.parser")
            if not soup.find_all("h2"):
                logger.warning(f"HTML не содержит секций h2 на попытке {attempt}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                logger.error("HTML пустой после всех попыток")
                return {section: [] for section in SECTION_LIST}
            
            stocks = {section: [] for section in SECTION_LIST}
            ignored_items = []
            for h2 in soup.find_all("h2"):
                section_name = h2.get_text(strip=True).upper()
                if section_name not in SECTION_LIST:
                    logger.debug(f"Пропущена секция: {section_name}")
                    continue
                ul = h2.find_next(lambda tag: tag.name == "ul")
                if not ul:
                    logger.debug(f"Список не найден для секции: {section_name}")
                    continue
                formatted_items = []
                for li in ul.find_all("li"):
                    text = li.get_text(strip=True, separator=" ")
                    logger.debug(f"Обрабатываем элемент списка: {text}")
                    qty_match = re.search(r'\s*x(\d+)\s*$', text, re.IGNORECASE)
                    qty = qty_match.group(1) if qty_match else ""
                    name = re.sub(r'\s*x\d+\s*$', '', text, flags=re.IGNORECASE).strip()
                    if not name:
                        logger.debug("Имя предмета пустое, пропуск")
                        continue
                    spans = li.find_all("span")
                    if len(spans) >= 1:
                        name = spans[0].get_text(strip=True)
                        logger.debug(f"Имя из span: {name}")
                        if len(spans) >= 2:
                            qty = spans[1].get_text(strip=True).replace("x", "").strip()
                            logger.debug(f"Количество из span: {qty}")
                    normalized_name = normalize_item_name(name)
                    logger.debug(f"Нормализованное имя: {normalized_name}")
                    translated_name = ITEM_TRANSLATE.get(normalized_name, None)
                    if translated_name is None or translated_name not in ALLOWED_ITEMS_DM:
                        logger.debug(f"Предмет {normalized_name} не в ALLOWED_ITEMS_DM, пропуск")
                        ignored_items.append(normalized_name)
                        continue
                    logger.debug(f"Переведённое имя: {translated_name}")
                    emoji = ITEM_EMOJI.get(translated_name, SECTION_EMOJI.get(section_name, ""))
                    color_emoji = egg_colors.get(translated_name, "")
                    item_data = {
                        "name": translated_name,
                        "emoji": f"{emoji}{color_emoji}",
                        "qty": f"x{qty}" if qty else ""
                    }
                    formatted_items.append(item_data)
                    logger.debug(f"Добавлен предмет: {translated_name}, эмодзи: {emoji}{color_emoji}, количество: {qty}")
                stocks[section_name] = formatted_items
            
            logger.info(f"Извлечённый сток: {json.dumps(stocks, ensure_ascii=False, indent=2)}")
            if ignored_items:
                logger.warning(f"Игнорируемые предметы: {', '.join(set(ignored_items))}")
            if not any(stocks[section] for section in SELECTABLE_SECTIONS):
                logger.warning(f"Пустой сток на попытке {attempt}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                logger.error("Все попытки дали пустой сток")
                prev_stock = load_json_file(PREV_STOCK_FILE, {section: [] for section in SECTION_LIST})
                if any(prev_stock[section] for section in SELECTABLE_SECTIONS):
                    logger.info("Используется предыдущий сток")
                    return prev_stock
                logger.error("Предыдущий сток также пуст, возвращается пустой сток")
                return {section: [] for section in SECTION_LIST}
            logger.info(f"Успешно получен сток за попытку {attempt}")
            return stocks
        except Timeout as e:
            logger.warning(f"Таймаут на попытке {attempt}: {str(e)}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)
                continue
            logger.error(f"Не удалось получить сток после {max_retries} попыток: {str(e)}")
            prev_stock = load_json_file(PREV_STOCK_FILE, {section: [] for section in SECTION_LIST})
            if any(prev_stock[section] for section in SELECTABLE_SECTIONS):
                logger.info("Используется предыдущий сток")
                return prev_stock
            return {section: [] for section in SECTION_LIST}
        except HTTPError as e:
            logger.warning(f"HTTP ошибка на попытке {attempt}: {str(e)}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)
                continue
            logger.error(f"Не удалось получить сток после {max_retries} попыток: {str(e)}")
            return {section: [] for section in SECTION_LIST}
        except ConnectionError as e:
            logger.warning(f"Ошибка соединения на попытке {attempt}: {str(e)}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)
                continue
            logger.error(f"Не удалось получить сток после {max_retries} попыток: {str(e)}")
            return {section: [] for section in SECTION_LIST}
        except Exception as e:
            logger.warning(f"Неожиданная ошибка на попытке {attempt}: {str(e)}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)
                continue
            logger.error(f"Не удалось получить сток после {max_retries} попыток: {str(e)}")
            return {section: [] for section in SECTION_LIST}

def load_json_file(path: str, default: Dict) -> Dict:
    try:
        if not os.path.exists(path):
            logger.info(f"Файл {path} не существует, создаём новый с значениями по умолчанию")
            save_json_file(path, default)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            logger.debug(f"Загружен файл {path}: {data}")
            if path == MSG_ID_FILE:
                for key in default:
                    if key not in data or not isinstance(data[key], list):
                        data[key] = []
            elif path == USERS_FILE:
                if "users" not in data or not isinstance(data["users"], list):
                    data = {"users": default.get("users", [])}
            elif path == LAST_SENT_PERIODS_FILE:
                if not isinstance(data, dict):
                    data = default
            return data
    except PermissionError as e:
        logger.error(f"Ошибка прав доступа при чтении {path}: {str(e)}")
        return default
    except Exception as e:
        logger.warning(f"Ошибка загрузки {path}: {str(e)}, используется значение по умолчанию")
        return default

def save_json_file(path: str, data: Dict):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.debug(f"Сохранён файл {path}: {data}")
    except PermissionError as e:
        logger.error(f"Ошибка прав доступа при сохранении {path}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Ошибка сохранения {path}: {str(e)}")
        raise

async def clean_log_file():
    while True:
        try:
            logger.info("Очистка файла логов")
            with open('sticker_log.log', 'w', encoding='utf-8') as f:
                f.write("")
            logger.info("Файл логов очищен")
        except Exception as e:
            logger.error(f"Ошибка при очистке файла логов: {str(e)}")
        await asyncio.sleep(LOG_CLEAN_INTERVAL)

async def check_bot_permissions(bot: Bot, chat_id: str) -> bool:
    try:
        bot_member = await bot.get_chat_member(chat_id=chat_id, user_id=bot.id)
        logger.debug(f"Права бота в {chat_id}: {bot_member}")
        if bot_member.status not in ['administrator', 'creator']:
            logger.error(f"Бот не админ в {chat_id}")
            return False
        if not bot_member.can_post_messages:
            logger.error(f"Недостаточно прав в {chat_id}")
            return False
        logger.info(f"Бот имеет права в {chat_id}")
        return True
    except TelegramError as e:
        logger.error(f"Ошибка проверки прав в {chat_id}: {str(e)}")
        return False

async def send_sticker_stock(bot: Bot, chat_id: str, stocks: Dict, last_msgs: Dict, last_sent_periods: Dict, is_full_stock: bool = False) -> Dict:
    logger.debug(f"Начало отправки текстового сообщения для стока в {chat_id}, is_full_stock={is_full_stock}")
    
    message_lines = []
    if is_full_stock:
        message_lines.append("📋 **Полный сток**")
    
    for section in SELECTABLE_SECTIONS:
        items = stocks.get(section, [])
        logger.debug(f"Обрабатываем секцию {section}: {items}")
        if not items:
            logger.debug(f"Нет предметов в секции {section}, пропуск")
            continue
        allowed_items = ALLOWED_ITEMS_DM if is_full_stock else ALLOWED_ITEMS_CHANNEL
        filtered_items = [item for item in items if item['name'] in allowed_items]
        if not filtered_items:
            logger.debug(f"Нет отфильтрованных предметов для канала {chat_id} в секции {section}: {items}")
            continue
        
        # Проверка периода для EGG STOCK в @sbtdrasik
        if chat_id == STOCK_CHANNEL_ID and section == "EGG STOCK":
            current_period = get_period_block(section)
            last_period = last_sent_periods.get(section, "")
            logger.debug(f"Проверка периода для EGG STOCK: текущий={current_period}, последний={last_period}")
            if last_period == current_period:
                logger.debug(f"Пропуск отправки EGG STOCK в {chat_id}: период не изменился ({last_period})")
                continue
        
        section_key = section.lower().replace(" ", "_")
        translated_section = SECTION_TRANSLATE.get(section, section)
        
        try:
            section_lines = [f"**{translated_section}**"]
            section_lines.append(f"⌛ **Пропадёт в:** {get_period_block(section)}")
            for item in filtered_items:
                qty = item['qty'].replace("x", "").strip() or "1"
                qty_text = "один" if qty == "1" else qty
                emoji = item['emoji']
                item_lines = [f"{emoji} **{item['name']}**", f"✨ **В наличии:** {qty_text}"]
                price = ITEM_PRICES.get(item['name'])
                if price:
                    item_lines.append(f"💰 **Цена:** {price}")
                section_lines.extend(item_lines)
                section_lines.append("")  # Пустая строка для разделения предметов
            if is_full_stock:
                message_lines.extend(section_lines)
            else:
                message_lines = section_lines
                message_lines.append("**❤ Захватил – радость!**")
                message_lines.append("**👍 Упустил – чёрт...**")
                
                keyboard = [
                    [
                        InlineKeyboardButton("🤖 Зайти в бота", url="https://t.me/Stockrasik_bot"),
                        InlineKeyboardButton("🚀 Бустнуть канал", url="https://t.me/boost/sbtdrasik")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                message_text = "\n".join(message_lines)
                msg = await bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    parse_mode="Markdown",
                    reply_markup=reply_markup
                )
                last_msgs[section_key] = last_msgs.get(section_key, []) + [msg.message_id]
                last_sent_periods[section] = get_period_block(section)
                logger.info(f"Отправлено сообщение для {section} в {chat_id}, ID: {msg.message_id}, период: {last_sent_periods[section]}")
                
        except TelegramError as e:
            logger.error(f"Ошибка отправки сообщения для {section} в {chat_id}: {str(e)}")
    
    if is_full_stock and message_lines:
        message_lines.append("**❤ Захватил – радость!**")
        message_lines.append("**👍 Упустил – чёрт...**")
        keyboard = [
            [
                InlineKeyboardButton("🤖 Зайти в бота", url="https://t.me/Stockrasik_bot"),
                InlineKeyboardButton("🚀 Бустнуть канал", url="https://t.me/boost/sbtdrasik")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "\n".join(message_lines)
        try:
            msg = await bot.send_message(
                chat_id=chat_id,
                text=message_text,
                parse_mode="Markdown",
                reply_markup=reply_markup
            )
            last_msgs['full_stock'] = last_msgs.get('full_stock', []) + [msg.message_id]
            logger.info(f"Отправлен полный сток в {chat_id}, ID: {msg.message_id}")
        except TelegramError as e:
            logger.error(f"Ошибка отправки полного стока в {chat_id}: {str(e)}")
    
    try:
        save_json_file(LAST_SENT_PERIODS_FILE, last_sent_periods)
        logger.debug(f"Файл {LAST_SENT_PERIODS_FILE} успешно сохранён: {last_sent_periods}")
    except Exception as e:
        logger.error(f"Не удалось сохранить {LAST_SENT_PERIODS_FILE}: {str(e)}")
    save_json_file(MSG_ID_FILE, last_msgs)
    return last_msgs

async def send_stock_to_user(bot: Bot, user_id: str, section: str = None):
    try:
        stocks = load_json_file(PREV_STOCK_FILE, {section: [] for section in SECTION_LIST})
        message_lines = []
        
        if section:
            items = stocks.get(section, [])
            if not items:
                await bot.send_message(user_id, f"📭 Секция {SECTION_TRANSLATE.get(section, section)} пуста.")
                return
            message_lines.append(f"📋 **{SECTION_TRANSLATE.get(section, section)}**")
            message_lines.append(f"⌛ **Пропадёт в:** {get_period_block(section)}")
            for item in items:
                if item['name'] in ALLOWED_ITEMS_DM:
                    qty = item['qty'].replace("x", "").strip() or "1"
                    qty_text = "один" if qty == "1" else qty
                    emoji = item['emoji']
                    item_lines = [f"{emoji} **{item['name']}**", f"✨ В наличии: {qty_text}"]
                    price = ITEM_PRICES.get(item['name'])
                    if price:
                        item_lines.append(f"💰 Цена: {price}")
                    message_lines.extend(item_lines)
                    message_lines.append("")
        else:
            message_lines.append("📋 **Полный сток**")
            for section in SELECTABLE_SECTIONS:
                items = stocks.get(section, [])
                if items:
                    message_lines.append(f"\n**{SECTION_TRANSLATE.get(section, section)}**")
                    message_lines.append(f"⌛ **Пропадёт в:** {get_period_block(section)}")
                    for item in items:
                        if item['name'] in ALLOWED_ITEMS_DM:
                            qty = item['qty'].replace("x", "").strip() or "1"
                            qty_text = "один" if qty == "1" else qty
                            emoji = item['emoji']
                            item_lines = [f"{emoji} **{item['name']}**", f"✨ В наличии: {qty_text}"]
                            price = ITEM_PRICES.get(item['name'])
                            if price:
                                item_lines.append(f"💰 Цена: {price}")
                            message_lines.extend(item_lines)
                            message_lines.append("")
            if len(message_lines) == 1:
                message_lines.append("📭 Сток пуст.")
        
        keyboard = [
            [
                InlineKeyboardButton("🌱 Проверить семена", callback_data="check_seeds"),
                InlineKeyboardButton("⚙️ Проверить предметы", callback_data="check_gear"),
                InlineKeyboardButton("🥚 Проверить яйца", callback_data="check_eggs")
            ],
            [InlineKeyboardButton("🔄 Обновить сток", callback_data="update_stock")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = "\n".join(message_lines)
        await bot.send_message(user_id, message_text, parse_mode="Markdown", reply_markup=reply_markup)
        logger.info(f"Отправлен сток ({section or 'полный'}) пользователю {user_id}")
    except TelegramError as e:
        logger.error(f"Ошибка отправки стока пользователю {user_id}: {str(e)}")
        if "blocked by user" in str(e).lower():
            logger.warning(f"Пользователь {user_id} заблокировал бота")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке стока пользователю {user_id}: {str(e)}")

async def start(update: Update, context):
    user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)
    logger.debug(f"Получена команда /start от {user_id} в чате {chat_id}")
    try:
        users = load_json_file(USERS_FILE, {"users": ADMIN_IDS})
        if user_id not in users["users"]:
            users["users"].append(user_id)
            save_json_file(USERS_FILE, users)
            logger.info(f"Добавлен пользователь {user_id} в {USERS_FILE}")
        await update.message.reply_text(
            f"👋 Привет! Бот отслеживает сток в {STOCK_CHANNEL_ID}. Проверяй сток с помощью кнопок ниже."
        )
        await send_stock_to_user(context.bot, user_id)
    except Exception as e:
        logger.error(f"Ошибка обработки команды /start для {user_id}: {str(e)}")
        await update.message.reply_text("⚠️ Произошла ошибка. Попробуйте позже.")

async def users(update: Update, context):
    user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)
    logger.debug(f"Получена команда /users от {user_id} в чате {chat_id}")
    try:
        if chat_id != ALLOWED_GROUP_ID:
            await update.message.reply_text(f"⛔ Команда /users работает только в группе с ID {ALLOWED_GROUP_ID}.")
            return
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("⛔ Доступ запрещён. Только администраторы могут использовать эту команду.")
            return
        users = load_json_file(USERS_FILE, {"users": ADMIN_IDS})
        await update.message.reply_text(f"📋 Пользователи: {len(users['users'])}")
    except Exception as e:
        logger.error(f"Ошибка обработки команды /users для {user_id}: {str(e)}")
        await update.message.reply_text("⚠️ Произошла ошибка. Попробуйте позже.")

async def rasik(update: Update, context):
    user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)
    logger.debug(f"Получена команда /rasik от {user_id} в чате {chat_id}")
    try:
        if chat_id != ALLOWED_GROUP_ID:
            await update.message.reply_text(f"⛔ Команда /rasik работает только в группе с ID {ALLOWED_GROUP_ID}.")
            return
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("⛔ Доступ запрещён. Только администраторы могут использовать эту команду.")
            return
        pending_broadcast[user_id] = {"timestamp": time.time()}
        await update.message.reply_text("📤 Отправь сообщение для рассылки.")
    except Exception as e:
        logger.error(f"Ошибка обработки команды /rasik для {user_id}: {str(e)}")
        await update.message.reply_text("⚠️ Произошла ошибка. Попробуйте позже.")

async def handle_broadcast_message(update: Update, context):
    user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)
    logger.debug(f"Получено сообщение для рассылки от {user_id} в чате {chat_id}")
    try:
        if user_id not in pending_broadcast:
            logger.debug(f"Нет активной рассылки для {user_id}")
            return
        if chat_id != ALLOWED_GROUP_ID:
            await update.message.reply_text(f"⛔ Рассылка работает только в группе с ID {ALLOWED_GROUP_ID}.")
            return
        if time.time() - pending_broadcast[user_id]["timestamp"] > 300:
            await update.message.reply_text("⏰ Время ожидания истекло. Используй /rasik заново.")
            del pending_broadcast[user_id]
            return
        pending_broadcast[user_id]["message"] = update.message
        keyboard = [[InlineKeyboardButton("✅ Подтвердить рассылку", callback_data=f"confirm_broadcast_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("📤 Подтвердите рассылку этого сообщения.", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения для рассылки от {user_id}: {str(e)}")
        await update.message.reply_text("⚠️ Произошла ошибка. Попробуйте позже.")

async def broadcast_callback(update: Update, context):
    user_id = str(update.effective_user.id)
    query = update.callback_query
    logger.debug(f"Callback от {user_id}: {query.data}")
    try:
        if user_id not in ADMIN_IDS:
            await query.answer("⛔ Доступ запрещён.")
            return
        if not query.data.startswith("confirm_broadcast_"):
            await query.answer("❌ Неверный callback.")
            return
        if user_id not in pending_broadcast or "message" not in pending_broadcast[user_id]:
            await query.answer("❌ Нет сообщения для рассылки.")
            return
        users = load_json_file(USERS_FILE, {"users": ADMIN_IDS})
        message = pending_broadcast[user_id]["message"]
        success_count = 0
        for target_id in users["users"]:
            try:
                await message.forward(chat_id=target_id)
                success_count += 1
                logger.info(f"Сообщение переслано пользователю {target_id}")
                await asyncio.sleep(0.1)
            except TelegramError as e:
                logger.error(f"Ошибка пересылки пользователю {target_id}: {str(e)}")
                if "blocked by user" not in str(e).lower():
                    logger.warning(f"Ошибка пересылки пользователю {target_id}: {str(e)}")
        await query.message.reply_text(f"📬 Рассылка завершена: отправлено {success_count} из {len(users['users'])} пользователям.")
        await query.answer()
        del pending_broadcast[user_id]
    except Exception as e:
        logger.error(f"Ошибка обработки callback рассылки для {user_id}: {str(e)}")
        await query.message.reply_text("⚠️ Произошла ошибка при рассылке. Попробуйте позже.")
        await query.answer()

async def stock_callback(update: Update, context):
    user_id = str(update.effective_user.id)
    query = update.callback_query
    logger.debug(f"Callback от {user_id}: {query.data}")
    try:
        if query.data == "check_seeds":
            await send_stock_to_user(context.bot, user_id, "SEEDS STOCK")
        elif query.data == "check_gear":
            await send_stock_to_user(context.bot, user_id, "GEAR STOCK")
        elif query.data == "check_eggs":
            await send_stock_to_user(context.bot, user_id, "EGG STOCK")
        elif query.data == "update_stock":
            await send_stock_to_user(context.bot, user_id)
        await query.answer()
    except Exception as e:
        logger.error(f"Ошибка обработки callback для {user_id}: {str(e)}")
        await query.answer("⚠️ Произошла ошибка. Попробуйте снова.")

async def update_stock(app: Application):
    global last_update_time
    async with update_lock:
        current_time = time.time()
        logger.debug(f"Начало update_stock, текущее время: {current_time}, последнее обновление: {last_update_time}")
        if current_time - last_update_time < STOCK_CHECK_INTERVAL - 5:
            logger.debug(f"Пропущена проверка: слишком рано (прошло {current_time - last_update_time:.1f} сек)")
            return
        last_update_time = current_time
        bot = app.bot
        start_time = time.time()
        logger.info("Проверка стока начата")
        try:
            stocks = get_stock(bot)
            logger.debug(f"Получен сток: {json.dumps(stocks, ensure_ascii=False)}")
            if not any(stocks[section] for section in SELECTABLE_SECTIONS):
                logger.warning("Сток пустой, пропуск публикации")
                return
            current_hash = get_stock_hash(stocks)
            prev_stock = load_json_file(PREV_STOCK_FILE, {section: [] for section in SECTION_LIST})
            prev_hash = get_stock_hash(prev_stock)
            logger.debug(f"Текущий сток (хэш: {current_hash}): {json.dumps(normalize_stock(stocks), ensure_ascii=False)}")
            logger.debug(f"Предыдущий сток (хэш: {prev_hash}): {json.dumps(normalize_stock(prev_stock), ensure_ascii=False)}")
            if current_hash == prev_hash:
                logger.info("Сток не изменился, пропуск публикации")
                return
            # Отправка в @sbtdrasik (по секциям, отфильтрованный сток, EGG STOCK только при смене периода)
            if await check_bot_permissions(bot, STOCK_CHANNEL_ID):
                last_msgs = load_json_file(MSG_ID_FILE, {section.lower().replace(" ", "_"): [] for section in SELECTABLE_SECTIONS})
                last_sent_periods = load_json_file(LAST_SENT_PERIODS_FILE, {section: "" for section in SELECTABLE_SECTIONS})
                logger.debug(f"Загружены last_sent_periods: {last_sent_periods}")
                last_msgs = await send_sticker_stock(bot, STOCK_CHANNEL_ID, stocks, last_msgs, last_sent_periods, is_full_stock=False)
                save_json_file(MSG_ID_FILE, last_msgs)
                try:
                    save_json_file(LAST_SENT_PERIODS_FILE, last_sent_periods)
                    logger.debug(f"Файл {LAST_SENT_PERIODS_FILE} успешно сохранён после отправки в @sbtdrasik: {last_sent_periods}")
                except Exception as e:
                    logger.error(f"Не удалось сохранить {LAST_SENT_PERIODS_FILE} после отправки в @sbtdrasik: {str(e)}")
            else:
                logger.error("Публикация невозможна в @sbtdrasik: бот не имеет прав")
            # Отправка в @autostockgag (полный сток)
            if await check_bot_permissions(bot, FULL_STOCK_CHANNEL_ID):
                last_msgs = load_json_file(MSG_ID_FILE, {section.lower().replace(" ", "_"): [] for section in SELECTABLE_SECTIONS})
                last_sent_periods = load_json_file(LAST_SENT_PERIODS_FILE, {section: "" for section in SELECTABLE_SECTIONS})
                last_msgs = await send_sticker_stock(bot, FULL_STOCK_CHANNEL_ID, stocks, last_msgs, last_sent_periods, is_full_stock=True)
                save_json_file(MSG_ID_FILE, last_msgs)
                try:
                    save_json_file(LAST_SENT_PERIODS_FILE, last_sent_periods)
                    logger.debug(f"Файл {LAST_SENT_PERIODS_FILE} успешно сохранён после отправки в @autostockgag: {last_sent_periods}")
                except Exception as e:
                    logger.error(f"Не удалось сохранить {LAST_SENT_PERIODS_FILE} после отправки в @autostockgag: {str(e)}")
            else:
                logger.error("Публикация невозможна в @autostockgag: бот не имеет прав")
            save_json_file(PREV_STOCK_FILE, stocks)
            logger.info(f"Сток обновлён и отправлен за {time.time() - start_time:.2f} сек")
        except Exception as e:
            logger.error(f"Ошибка обновления стока: {str(e)}")
            return

async def main():
    logger.info("Запуск бота")
    if not TELEGRAM_BOT_TOKEN or "YOUR_STICKER_BOT_TOKEN_HERE" in TELEGRAM_BOT_TOKEN:
        logger.error("Токен не указан")
        return
    try:
        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("users", users))
        app.add_handler(CommandHandler("rasik", rasik))
        app.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^confirm_broadcast_"))
        app.add_handler(CallbackQueryHandler(stock_callback, pattern="^(check_seeds|check_gear|check_eggs|update_stock)$"))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.User(user_id=ADMIN_IDS) & filters.Chat(chat_id=int(ALLOWED_GROUP_ID)), handle_broadcast_message))
        logger.info("Обработчики добавлены")
        
        asyncio.create_task(clean_log_file())
        
        async with app:
            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            logger.info("Бот успешно запущен")
            await update_stock(app)
            while True:
                try:
                    await update_stock(app)
                    await asyncio.sleep(STOCK_CHECK_INTERVAL)
                except Exception as e:
                    logger.error(f"Ошибка в цикле update_stock: {str(e)}")
                    await asyncio.sleep(60)
    except Exception as e:
        logger.error(f"Ошибка запуска бота: {str(e)}")
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
