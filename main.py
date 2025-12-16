# m5.py
import os
import asyncio
import aiosqlite
import logging
from asyncio import create_task, sleep
import random
import uuid
import time
from aiogram.types import ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime, timedelta
from aiogram.types import FSInputFile
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.filters import Command
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message
from aiogram.filters import CommandStart, CommandObject  # ← Важно!
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import StateFilter

import csv
import time
import aiosqlite  # если ещё не импортирован

import csv
from datetime import datetime as _dt

async def export_to_csv(data, drop_id: int) -> str:
    """
    data["orders"] — это список строк из generate_drop_statistics,
    каждая строка = 14 полей:

    0: order_id
    1: admin_id
    2: admin_username
    3: drop_id
    4: drop_username
    5: amount
    6: status
    7: created_at
    8: expires_at
    9: check_file
    10: card_id
    11: card_number
    12: bank
    13: fio
    """

    orders = data["orders"]
    path = f"drop_{drop_id}.csv"

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=';')

        # Заголовки
        writer.writerow([
            "order_id",
            "admin_id",
            "admin_username",
            "drop_id",
            "drop_username",
            "amount",
            "status",
            "created_at_ts",
            "created_at",
            "expires_at_ts",
            "expires_at",
            "check_file",
            "card_id",
            "card_number",
            "bank",
            "fio",
        ])

        for row in orders:
            (
                order_id,
                admin_id, admin_username,
                d_id, d_username,
                amount, status,
                created_at, expires_at,
                check_file,
                card_id, card_number, bank, fio,
            ) = row

            created_ts = created_at or 0
            expires_ts = expires_at or 0

            created_str = _dt.fromtimestamp(created_ts).strftime("%Y-%m-%d %H:%M:%S") if created_at else ""
            expires_str = _dt.fromtimestamp(expires_ts).strftime("%Y-%m-%d %H:%M:%S") if expires_at else ""

            writer.writerow([
                order_id,
                admin_id,
                admin_username or "",
                d_id,
                d_username or "",
                amount,
                status,
                created_ts or "",
                created_str,
                expires_ts or "",
                expires_str,
                check_file or "",
                card_id or "",
                card_number or "",
                bank or "",
                fio or "",
            ])

    return path



from dotenv import load_dotenv

# загружаем .env
load_dotenv()

TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("❌ TOKEN не задан в .env")
DB_NAME = "cicada.db"
LOG_FILE = "logs/bot.log"

# ---------- Logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- Bot / Dispatcher ----------
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# ---------- FSM ----------
class AddCard(StatesGroup):
    number = State()
    bank = State()
    fio = State()
    min_payment = State() 

class AddReceipt(StatesGroup):
    wait_file = State()

class SearchCheck(StatesGroup):
    waiting_for_order_id = State()

class EditLimit(StatesGroup):
    waiting_new_limit = State()

class AdminCheckState(StatesGroup):
    waiting_for_check_photo = State()

class CreateOrder(StatesGroup):
    amount = State()
class EditLimitState(StatesGroup):
    waiting_for_new_limit = State()
# ---------- Keyboards ----------
CANCEL_KB = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True)


async def safe_edit(message, text=None, reply_markup=None):
    """
    Безопасное редактирование сообщений:
    - Не падает при 'message is not modified'
    - Автоматически подбирает edit_text или edit_reply_markup
    """
    try:
        if text is not None:
            await message.edit_text(text, reply_markup=reply_markup)
        else:
            await message.edit_reply_markup(reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return  # тихо игнорируем
        raise

def menu_for(user_type: int):
    if user_type == 1:
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Добавить карту"), KeyboardButton(text="Мои карты")],
                [KeyboardButton(text="Мои заявки"), KeyboardButton(text="Статистика")]
            ],
            resize_keyboard=True
        )
    else:  # админ
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Создать заявку"), KeyboardButton(text="Активные заявки")],
                [KeyboardButton(text="Дропы"), KeyboardButton(text="Пригласить")],
                [KeyboardButton(text="Поиск по чеку")]  # ← новые кнопки
            ],
            resize_keyboard=True
        )

async def build_drops_list():
    async with aiosqlite.connect(DB_NAME) as db:
        # Берём всех дропов
        async with db.execute("""
            SELECT id, username, approved, monthly_limit, monthly_used
            FROM users
            WHERE user_type = 1
            ORDER BY id
        """) as cur:
            drops = await cur.fetchall()

        # Карты по дропам
        cards_count_map = {}
        async with db.execute("""
            SELECT user_id, COUNT(*)
            FROM cards
            GROUP BY user_id
        """) as cur:
            for user_id, cnt in await cur.fetchall():
                cards_count_map[user_id] = cnt

    if not drops:
        text = "Список дропов пуст."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_menu")]
        ])
        return text, kb

    text = "<b>Список дропов</b>\n\n"
    kb_lines = []

    for drop_id, username, approved, monthly_limit, monthly_used in drops:
        username_view = (
            f"@{username}" if username and not username.startswith("@")
            else (username or "Без имени")
        )

        monthly_limit = monthly_limit or 0
        monthly_used = monthly_used or 0
        remaining = max(monthly_limit - monthly_used, 0)
        cards_count = cards_count_map.get(drop_id, 0)

        if monthly_limit > 0:
            used_percent = (monthly_used / monthly_limit) * 100
        else:
            used_percent = 0.0

        status_txt = "Утверждён" if approved else "Не утверждён"

        text += (
            f"{username_view} (ID: {drop_id})\n"
            f"   Карт: {cards_count} | Лимит: {monthly_limit:,}₽\n"
            f"   Доступно: {remaining:,}₽ ({used_percent:.1f}%)\n\n"
        )

        kb_lines.append([
            InlineKeyboardButton(
                text=f"{username_view} • детали",
                callback_data=f"dropdetail_{drop_id}"
            )
        ])

    kb_lines.append([
        InlineKeyboardButton(text="Назад", callback_data="admin_menu")
    ])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_lines)
    return text, kb

@dp.message(Command("cicada3301"))
async def download_db(message: Message):
    user_type = await get_user_type(message.from_user.id)

    # доступ только админам
    if user_type != 2:
        return await message.answer("⛔ Команда доступна только администраторам.")

    # путь к файлу БД
    db_path = DB_NAME  # если у тебя переменная DB_NAME = "cicada.db"

    try:
        await message.answer_document(
            FSInputFile(db_path),
            caption="🔐 Файл базы данных `cicada.db`"
        )
    except Exception as e:
        logger.error(f"Ошибка отправки базы: {e}")
        await message.answer("⚠ Не удалось отправить файл базы данных.")


@dp.callback_query(F.data == "admin_menu")
async def back_to_admin_menu(call: CallbackQuery):
    await call.message.delete()
    await call.message.answer(
        "Выберите действие:",
        reply_markup=menu_for(2)
    )
    await call.answer()


@dp.message(F.text == "Пригласить")
async def invite_handler(message: Message):
    if await get_user_type(message.from_user.id) != 2:
        return
    token = str(uuid.uuid4())
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO invite_tokens(token, inviter_id) VALUES(?,?)", (token, message.from_user.id))
        await db.commit()
    link = f"https://t.me/{(await bot.get_me()).username}?start={token}"
    await message.answer(f"Одноразовая ссылка:\n{link}")


@dp.message(CommandStart())
async def start_handler(message: Message, command: CommandObject):
    user_id = message.from_user.id

    # 1️⃣ Если пользователь уже есть в базе — сразу показываем его меню
    # (используем твой helper, который везде уже есть)
    user_type = await get_user_type(user_id)  # вернёт 1, 2, ... или None/0

    if user_type:
        # можно один раз написать разный текст для типов
        text = "👋 С возвращением!" if user_type == 1 else "👋 Админ-меню:"
        return await message.answer(
            text,
            reply_markup=menu_for(user_type)
        )

    # 2️⃣ Пользователя ещё нет — работаем по инвайт-ссылке
    token = command.args  # то, что идёт после /start

    # Если человек пришёл без ссылки — не пускаем
    if not token:
        return await message.answer("❌ У вас нет приглашения. Доступ запрещён.")

    # Проверяем токен
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT inviter_id, used FROM invite_tokens WHERE token = ?",
            (token,)
        )
        row = await cur.fetchone()

        if not row:
            return await message.answer("❌ Неверная или использованная ссылка.")

        inviter_id, used = row

        if used == 1:
            return await message.answer("❌ Эта ссылка уже активирована.")

        # Помечаем как использованную
        await db.execute(
            "UPDATE invite_tokens SET used = 1 WHERE token = ?",
            (token,)
        )
        await db.commit()

    # 3️⃣ Регистрируем нового пользователя (как обычного юзера, user_type = 1)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO users(id, username, user_type) VALUES(?, ?, ?)",
            (user_id, message.from_user.username or "", 1)
        )
        await db.commit()

    logger.info(f"User registered as USER: {user_id}")

    # Главное меню новому пользователю
    await message.answer(
        "✅ Вы зарегистрированы как Пользователь.",
        reply_markup=menu_for(1)
    )

    # Уведомляем пригласившего
    try:
        await message.bot.send_message(
            inviter_id,
            f"🎉 Новый пользователь вошёл по вашей ссылке: "
            f"@{message.from_user.username or user_id}"
        )
    except:
        pass


# ---------- DB init ----------
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                user_type INTEGER,
                approved INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                number TEXT UNIQUE,
                bank TEXT,
                fio TEXT,
                daily_limit INTEGER DEFAULT 0,
                daily_used INTEGER DEFAULT 0,
                monthly_limit INTEGER DEFAULT 0,
                monthly_used INTEGER DEFAULT 0,
                last_reset TEXT,
                month_reset TEXT,
                active INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                admin_id INTEGER,
                admin_chat_id INTEGER,
                admin_message_id INTEGER,
                drop_id INTEGER,
                card_id INTEGER,
                amount INTEGER,
                created_at INTEGER,
                expires_at INTEGER,
                status TEXT DEFAULT 'pending',
                check_file TEXT,          -- file_id чека
                check_file_type TEXT      -- 'photo' / 'document'
            );

            CREATE TABLE IF NOT EXISTS payments (
                id TEXT PRIMARY KEY,
                order_id TEXT,
                card_id INTEGER,
                amount INTEGER,
                admin_id INTEGER,
                timestamp INTEGER
            );

            CREATE TABLE IF NOT EXISTS invite_tokens (
                token TEXT PRIMARY KEY,
                inviter_id INTEGER,
                used INTEGER DEFAULT 0,
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS order_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                old_status TEXT,
                new_status TEXT,
                changed_by INTEGER,
                timestamp INTEGER
            );
        """)

        async def add_column_if_not_exists(table: str, column: str, definition: str):
            async with db.execute(f"PRAGMA table_info({table})") as cur:
                existing = [row[1] for row in await cur.fetchall()]
            if column not in existing:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                await db.commit()
                logger.info(f"Добавлена колонка {column} в таблицу {table}")

        # существующие
        await add_column_if_not_exists("users", "approved", "INTEGER DEFAULT 0")
        await add_column_if_not_exists("invite_tokens", "used", "INTEGER DEFAULT 0")
        await add_column_if_not_exists("invite_tokens", "inviter_id", "INTEGER")
        await add_column_if_not_exists("invite_tokens", "created", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        await add_column_if_not_exists("orders", "check_file", "TEXT")
        await add_column_if_not_exists("orders", "check_file_type", "TEXT")  # 👈 добавили
        await add_column_if_not_exists("cards", "min_payment", "INTEGER DEFAULT 0")
        await add_column_if_not_exists("orders", "drop_chat_id", "INTEGER")
        await add_column_if_not_exists("orders", "drop_message_id", "INTEGER")
        await add_column_if_not_exists("users", "monthly_limit", "INTEGER DEFAULT 100000")
        await add_column_if_not_exists("users", "monthly_used", "INTEGER DEFAULT 0")
        await add_column_if_not_exists("orders", "check_message_id", "INTEGER")

        await db.execute("""
            UPDATE users
            SET monthly_limit = 100000
            WHERE monthly_limit IS NULL OR monthly_limit = 0
        """)
        await db.execute("""
            UPDATE users
            SET monthly_used = 0
            WHERE monthly_used IS NULL
        """)

        await db.commit()
        logger.info("База данных инициализирована и обновлена")


def status_name(db_status: str) -> str:
    return {
        "pending": "Ожидает отправки",
        "active": "Активная",
        "done": "Завершена",
        "canceled": "Отменена",
        "expired": "Истекла",
        "rejected": "Отклоненный",
        "completed": "Выполнена",
        "timeout": "Просрочена"
    }.get(db_status, db_status)



# ---------- Helpers ----------
async def get_user_type(user_id: int) -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_type FROM users WHERE id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

def mask_card(number: str) -> str:
    # Защита на случай, если number меньше 16 символов
    n = ''.join(ch for ch in str(number) if ch.isdigit())
    if len(n) >= 16:
        return " ".join(n[i:i+4] for i in range(0, 16, 4))
    # fallback: группируем по 4
    return " ".join(n[i:i+4] for i in range(0, len(n), 4))

async def auto_cancel_worker():
    while True:
        try:
            await asyncio.sleep(30)  # проверка каждые 30 секунд
            now = int(time.time())
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute(
                    "SELECT id, admin_chat_id, admin_message_id, drop_id, amount, status FROM orders WHERE status IN ('active','pending') AND expires_at <= ?",
                    (now,)
                ) as cur:
                    expired = await cur.fetchall()

                for order in expired:
                    order_id = order[0]
                    admin_chat_id = order[1]
                    admin_message_id = order[2]
                    drop_id = order[3]
                    amount = order[4]
                    status_cur = order[5]
                    try:
                        await db.execute("UPDATE orders SET status=? WHERE id=?", ("timeout", order_id))
                        await db.commit()
                        logger.info(f"Order {order_id} expired -> timeout")
                    except Exception:
                        logger.exception("DB update error on expire")

                    # Уведомляем дропа (если был отправлен)
                    if drop_id:
                        try:
                            await bot.send_message(
                                drop_id,
                                f"⌛ <b>Заявка просрочена</b>\nID: <code>{order_id}</code>\nСумма: <b>{amount:,}₽</b>\nСрок действия 30 минут истёк."
                            )
                        except Exception as e:
                            logger.warning(f"Can't notify drop {drop_id}: {e}")

                    # Обновляем сообщение админа, если есть
                    if admin_chat_id and admin_message_id:
                        try:
                            await bot.edit_message_text(chat_id=admin_chat_id, message_id=admin_message_id,
                                                        text=f"🖤 <b>Заявка {order_id}</b>\n\n<b>Статус:</b> Время истекло ⌛\nСумма: {amount:,}₽")
                        except Exception:
                            try:
                                await bot.send_message(admin_chat_id, f"⌛ Заявка {order_id} истекла. Сумма: {amount:,}₽")
                            except Exception:
                                logger.warning(f"Can't notify admin {admin_chat_id} about timeout")
        except Exception:
            logger.exception("Error in auto_cancel_worker")




@dp.message(F.text == "/777")
async def cmd_reg_admin(message: Message):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO users(id, username, user_type) VALUES(?, ?, ?)",
                         (message.from_user.id, message.from_user.username or "", 2))
        await db.commit()
    logger.info(f"User registered as ADMIN: {message.from_user.id}")
    await message.answer("🛡️ Вы зарегистрированы как Админ.", reply_markup=menu_for(2))

@dp.message(lambda m: m.text and m.text.strip().lower() == "отмена")
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("⛔ Действие отменено.", reply_markup=menu_for(await get_user_type(message.from_user.id)))


async def get_drop_stats_for_period(drop_id: int, ts_from: int, ts_to: int):
    """
    Возвращает (count, sum) по всем платежам дропа за период [ts_from; ts_to)
    """
    async with aiosqlite.connect(DB_NAME) as db:
        # берём все карты дропа
        async with db.execute(
            "SELECT id FROM cards WHERE user_id = ?",
            (drop_id,)
        ) as cur:
            card_rows = await cur.fetchall()

        if not card_rows:
            return 0, 0

        card_ids = [row[0] for row in card_rows]

        # формируем IN (...) динамически
        placeholders = ",".join("?" for _ in card_ids)
        sql = f"""
            SELECT COUNT(*), COALESCE(SUM(amount), 0)
            FROM payments
            WHERE card_id IN ({placeholders})
              AND timestamp >= ?
              AND timestamp < ?
        """
        params = [*card_ids, ts_from, ts_to]

        async with db.execute(sql, params) as cur:
            row = await cur.fetchone()

    count = row[0] or 0
    total = row[1] or 0
    return count, total


# Универсальная функция — работает и с Message, и с CallbackQuery.message
# Универсальная функция показа списка дропов
async def show_drops_list(source):
    if hasattr(source, "message"):  # CallbackQuery
        message = source.message
        edit_mode = True
        user_id = source.from_user.id
    else:  # Message
        message = source
        edit_mode = False
        user_id = source.from_user.id

    if await get_user_type(user_id) != 2:
        if edit_mode:
            await source.answer("Доступ запрещён", show_alert=True)
        return

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT u.id, u.username, u.approved,
                   COUNT(c.id) as card_count,
                   COALESCE(SUM(c.daily_limit), 0) as total_limit,
                   COALESCE(SUM(c.daily_used), 0) as total_used
            FROM users u
            LEFT JOIN cards c ON c.user_id = u.id AND c.active = 1
            WHERE u.user_type = 1
            GROUP BY u.id
            ORDER BY total_limit DESC
        """) as cur:
            drops = await cur.fetchall()

    if not drops:
        text = "Дропов пока нет."
        kb = None
    else:
        text_lines = ["<b>Список дропов</b>\n"]
        kb_lines = []

        for user_id, username, approved, card_count, total_limit, total_used in drops:
            name = username or "Без имени" if username else "Без имени"
            if name != "Без имени" and not name.startswith("@"):
                name = f"@{name}"

            available = total_limit - total_used
            percent = (total_used / total_limit * 100) if total_limit > 0 else 0
            status = "Утверждён" if approved else "Не утверждён"

            text_lines.append(
                f"<b>{name}</b> (ID: {user_id})\n"
                f"   Карт: {card_count} | Лимит: {total_limit:,}₽\n"
                f"   Доступно: <b>{available:,}₽</b> ({percent:.1f}%)\n"
            )

            kb_lines.append([
                InlineKeyboardButton(
                    text=f"{name} — {card_count} карт",
                    callback_data=f"dropdetail_{user_id}"
                )
            ])

        #kb_lines.append([InlineKeyboardButton(text="Обновить", callback_data="refresh_drops")])
        text = "\n".join(text_lines)
        kb = InlineKeyboardMarkup(inline_keyboard=kb_lines)

    try:
        if edit_mode:
            await message.edit_text(text=text, reply_markup=kb)  # ← ВАЖНО: передаём настоящий text!
        else:
            await message.answer(text=text, reply_markup=kb)
    except Exception as e:
        logger.error(f"Ошибка списка дропов: {e}")

async def update_order_status(order_id: str, new_status: str, changed_by: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT status FROM orders WHERE id = ?", (order_id,))
        row = await cur.fetchone()
        if not row:
            return False
        old_status = row[0]

        await db.execute("UPDATE orders SET status = ? WHERE id = ?", (new_status, order_id))
        await db.execute(
            "INSERT INTO order_history(order_id, old_status, new_status, changed_by, timestamp) VALUES(?,?,?,?,?)",
            (order_id, old_status, new_status, changed_by, int(time.time()))
        )
        await db.commit()
        return True
    


async def get_full_statistics():
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT COUNT(*), SUM(amount) FROM orders")
        total_orders, total_amount = await cur.fetchone()

        cur = await db.execute("SELECT COUNT(*) FROM orders WHERE status='pending'")
        pending = (await cur.fetchone())[0]

        cur = await db.execute("SELECT COUNT(*) FROM orders WHERE status='completed'")
        completed = (await cur.fetchone())[0]

        cur = await db.execute("SELECT COUNT(*) FROM orders WHERE status='rejected'")
        rejected = (await cur.fetchone())[0]

        cur = await db.execute(
            "SELECT order_id, old_status, new_status, changed_by, timestamp FROM order_history ORDER BY timestamp"
        )
        history = await cur.fetchall()

    return {
        "total_orders": total_orders,
        "total_amount": total_amount or 0,
        "pending": pending,
        "completed": completed,
        "rejected": rejected,
        "history": history
    }

@dp.message(F.text == "Статистика")
async def statistics_handler(message: Message):
    stats = await get_full_statistics()

    text = (
        f"📊 <b>Общая статистика</b>\n"
        f"Всего заявок: {stats['total_orders']}\n"
        f"Сумма: {stats['total_amount']} ₽\n"
        f"В ожидании: {stats['pending']}\n"
        f"Выполнено: {stats['completed']}\n"
        f"Отклонено: {stats['rejected']}\n\n"
        f"<b>История изменений:</b>\n"
    )

    for h in stats['history'][-20:]:  # последние 20 изменений
        order_id, old_status, new_status, changed_by, timestamp = h
        t_str = time.strftime("%d.%m %H:%M", time.localtime(timestamp))
        text += f"#{order_id}: {old_status} → {new_status} (админ {changed_by}) {t_str}\n"

    await message.answer(text, parse_mode="HTML")


@dp.message(F.text == "Поиск по чеку")
async def search_check_start(message: Message, state: FSMContext):
    # Если нужно ограничить только админам:
    if await get_user_type(message.from_user.id) != 2:
        return await message.answer("⛔ Эта функция только для администраторов.")

    await state.set_state(SearchCheck.waiting_for_order_id)

    await message.answer(
        "🔍 Введи <b>ID заявки</b> (16 символов) или его начало (например, первые 6–8 символов).\n"
        "Для отмены — напиши <code>Отмена</code>.",
        parse_mode="HTML"
    )


@dp.message(StateFilter(SearchCheck.waiting_for_order_id))
async def search_check_process(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    # Отмена
    if text.lower() == "отмена":
        await state.clear()
        return await message.answer("❌ Поиск по чеку отменён.")

    query = text.replace(" ", "")

    if not query:
        return await message.answer("⚠ Введи ID заявки или его часть.")

    async with aiosqlite.connect(DB_NAME) as db:
        # Полный ID (16 символов)
        if len(query) == 16:
            sql = """
                SELECT id, drop_id, amount, status, check_file, created_at
                FROM orders
                WHERE id = ?
            """
            params = (query,)
        else:
            # По префиксу ID
            sql = """
                SELECT id, drop_id, amount, status, check_file, created_at
                FROM orders
                WHERE id LIKE ?
                ORDER BY created_at DESC
                LIMIT 1
            """
            params = (query + "%",)

        async with db.execute(sql, params) as cur:
            row = await cur.fetchone()

    if not row:
        await message.answer("❌ Чек / 🧾 Заявка с таким ID не найдены.")
        await state.clear()
        return

    order_id, drop_id, amount, status, check_file, created_at = row
    created_str = time.strftime("%d.%m %H:%M", time.localtime(created_at)) if created_at else "—"

    text_resp = (
        f"🧾 <b>Результат поиска по ID</b>: <code>{query}</code>\n\n"
        f"🆔 Заявка: <code>{order_id}</code>\n"
        f"👤 Дроп ID: <code>{drop_id}</code>\n"
        f"💰 Сумма: <b>{amount:,}₽</b>\n"
        f"📌 Статус: <b>{status_name(status)}</b>\n"
        f"🕒 Создана: {created_str}\n"
    )

    if check_file:
        text_resp += "📎 Чек: <b>прикреплён</b>\n"
    else:
        text_resp += "📎 Чек: <b>не прикреплён</b>\n"

    await message.answer(text_resp, parse_mode="HTML")

    # Если чек есть — показать его сразу
    if check_file:
        try:
            # формат "photo:<file_id>" или "doc:<file_id>"
            if check_file.startswith("photo:"):
                file_id = check_file.split(":", 1)[1]
                await message.answer_photo(
                    photo=file_id,
                    caption=f"📎 Чек по заявке <code>{order_id}</code>",
                    parse_mode="HTML"
                )
            elif check_file.startswith("doc:"):
                file_id = check_file.split(":", 1)[1]
                await message.answer_document(
                    document=file_id,
                    caption=f"📎 Чек по заявке <code>{order_id}</code>",
                    parse_mode="HTML"
                )
            else:
                # старый формат без префикса — пробуем как фото
                await message.answer_photo(
                    photo=check_file,
                    caption=f"📎 Чек по заявке <code>{order_id}</code>",
                    parse_mode="HTML"
                )
        except Exception as e:
            # если не получилось загрузить чек — можно вывести диагностическое сообщение
            await message.answer("⚠ Не удалось загрузить файл чека (возможно, file_id устарел).")

    await state.clear()




# Кнопки меню
@dp.message(F.text == "Дропы")
async def cmd_drops(message: Message):
    # Если нужно ограничить только админам – оставь/добавь проверку
    # if await get_user_type(message.from_user.id) != 2:
    #     return

    text, kb = await build_drops_list()
    await message.answer(text, reply_markup=kb)



@dp.callback_query(F.data == "refresh_drops")
async def refresh_drops(call: CallbackQuery):
    text, kb = await build_drops_list()
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()


# Детали дропа
@dp.callback_query(F.data.startswith("dropdetail_"))
async def show_drop_detail(call: CallbackQuery):
    drop_id = int(call.data.split("_")[1])

    async with aiosqlite.connect(DB_NAME) as db:
        # Берём дропа + его общий лимит
        async with db.execute(
            "SELECT username, approved, monthly_limit, monthly_used FROM users WHERE id=?",
            (drop_id,)
        ) as cur:
            row = await cur.fetchone()

        if not row:
            return await call.answer("Дроп не найден", show_alert=True)

        username, approved, monthly_limit, monthly_used = row
        username = f"@{username}" if username and not username.startswith("@") else (username or "Без имени")

        # Берём карты дропа (лимит ДОПОЛЬШЕ уже не нужен на карте)
        async with db.execute("""
            SELECT id, number, bank, fio, active, min_payment
            FROM cards
            WHERE user_id = ?
        """, (drop_id,)) as cur:
            cards = await cur.fetchall()

    # Считаем остаток лимита дропа
    monthly_limit = monthly_limit or 0
    monthly_used = monthly_used or 0
    remaining = monthly_limit - monthly_used

    text = (
        f"<b>Дроп: {username}</b> (ID: {drop_id})\n\n"
        f"💰 <b>Лимит дропа:</b> {monthly_limit:,}₽\n"
        f"📉 <b>Использовано:</b> {monthly_used:,}₽\n"
        f"✅ <b>Остаток:</b> {max(0, remaining):,}₽\n\n"
    )

    kb_lines = []

    if not cards:
        text += "— Нет карт —\n"
    else:
        text += "🖤 Карты дропа:\n\n"
        for card_id, number, bank, fio, active, min_payment in cards:
            last4 = number[-4:]
            status = "🟢 Активна" if active else "⏸ Пауза"

            text += (
                f"• **** {last4} | {bank}\n"
                f"  {fio}\n"
                f"  Мин. сумма: <b>{min_payment:,}₽</b>\n"
                f"  Статус: {status_name(status)}\n\n"
            )

        # 🔧 Вместо "Изменить лимит карты" логичнее сделать "Изменить лимит дропа"
        if approved:
            kb_lines.append([
                InlineKeyboardButton(
                    text="Изменить лимит дропа",
                    callback_data=f"editdroplimit_{drop_id}"
                )
            ])

    # Остальные кнопки, как и были
    kb_lines.extend([
        [InlineKeyboardButton(text="Статистика", callback_data=f"stats_{drop_id}")],
        [InlineKeyboardButton(text="📊 Полная статистика", callback_data=f"statsfilter_{drop_id}_all")],
        [InlineKeyboardButton(text="📆 Неделя (Пн 22:00)-(Пн 22:00)", callback_data=f"statsweek_{drop_id}")],
        [InlineKeyboardButton(text="♻️ Сбросить лимит", callback_data=f"resetlimit_{drop_id}")],
        [InlineKeyboardButton(text="✏️ Изменить лимит", callback_data=f"editlimit_{drop_id}")],
        [InlineKeyboardButton(text="Удалить дропа", callback_data=f"delete_{drop_id}")],
        [InlineKeyboardButton(text="Назад", callback_data="refresh_drops")]
    ])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_lines)

    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data.startswith("statsweek_"))
async def stats_last_week_monday_22(call: CallbackQuery):
    if await get_user_type(call.from_user.id) != 2:
        return await call.answer("⛔ Только для администраторов.", show_alert=True)

    drop_id = int(call.data.split("_", 1)[1])

    now = datetime.now()

    # Определяем "этот понедельник 22:00"
    # weekday(): Пн=0, Вт=1, ..., Вс=6
    weekday = now.weekday()
    today_2200 = now.replace(hour=22, minute=0, second=0, microsecond=0)
    this_monday_2200 = today_2200 - timedelta(days=weekday)

    # Если сейчас < этого понедельника 22:00 — считаем, что отчёт делаем за прошлую неделю,
    # но "верхняя граница" = прошлый понедельник 22:00
    if now < this_monday_2200:
        this_monday_2200 -= timedelta(days=7)

    last_monday_2200 = this_monday_2200 - timedelta(days=7)

    ts_from = int(last_monday_2200.timestamp())
    ts_to = int(this_monday_2200.timestamp())

    count, total = await get_drop_stats_for_period(drop_id, ts_from, ts_to)

    # Красиво покажем даты
    period_from_str = last_monday_2200.strftime("%d.%m %H:%M")
    period_to_str = this_monday_2200.strftime("%d.%m %H:%M")

    text = (
        f"📆 Статистика за неделю\n"
        f"с <b>{period_from_str}</b> по <b>{period_to_str}</b>\n\n"
        f"👤 Дроп ID: <code>{drop_id}</code>\n"
        f"🧾 Кол-во оплат: <b>{count}</b>\n"
        f"💰 Сумма: <b>{total:,}₽</b>"
    )

    await call.message.answer(text, parse_mode="HTML")
    await call.answer()



@dp.callback_query(F.data.startswith("stats_"))
async def drop_stats(call: CallbackQuery):
    drop_id = int(call.data.split("_")[1])

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Все", callback_data=f"statsfilter_{drop_id}_all"),
            InlineKeyboardButton(text="⏳ В ожидании", callback_data=f"statsfilter_{drop_id}_pending"),
        ],
        [
            InlineKeyboardButton(text="✔ Завершенные", callback_data=f"statsfilter_{drop_id}_completed"),
            InlineKeyboardButton(text="❌ Отклоненные", callback_data=f"statsfilter_{drop_id}_rejected"),
        ],
        [
            InlineKeyboardButton(text="Последние 7 дней", callback_data=f"statsfilter_{drop_id}_7"),
            InlineKeyboardButton(text="Последние 30 дней", callback_data=f"statsfilter_{drop_id}_30"),
        ],
        [
            InlineKeyboardButton(text="📤 Экспорт CSV", callback_data=f"export_{drop_id}_csv")
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data=f"dropdetail_{drop_id}")
        ]
    ])

    await call.message.edit_text(
        f"📊 <b>Статистика дропа ID {drop_id}</b>\n\nВыберите фильтр:",
        reply_markup=kb
    )
    await call.answer()



@dp.callback_query(F.data.startswith("statsfilter_"))
async def stats_filter(call: CallbackQuery):
    _, drop_id, flt = call.data.split("_")
    drop_id = int(drop_id)

    # убрали updated_at
    query = "SELECT id, amount, status, created_at FROM orders WHERE drop_id=?"
    params = [drop_id]

    now = int(time.time())

    if flt == "pending":
        query += " AND status='pending'"
    elif flt == "completed":
        query += " AND status='completed'"
    elif flt == "rejected":
        query += " AND status='rejected'"
    elif flt == "7":
        query += " AND created_at > ?"
        params.append(now - 7 * 86400)
    elif flt == "30":
        query += " AND created_at > ?"
        params.append(now - 30 * 86400)

    query += " ORDER BY id DESC"

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(query, params) as cur:
            orders = await cur.fetchall()

    text = f"📊 <b>Статистика дропа ID {drop_id}</b>\n\n"
    kb = []

    # тоже без updated
    for oid, amount, status, created in orders:
        t_created = datetime.fromtimestamp(created).strftime("%d.%m %H:%M")

        text += f"• <b>ID {oid}</b> | {amount}₽ | {status_name(status)}\n"
        text += f"   🕒 {t_created}\n\n"

        kb.append([InlineKeyboardButton(text=f"Заявка {oid}", callback_data=f"orderinfo_{oid}")])

    kb.append([InlineKeyboardButton(text="Назад", callback_data=f"stats_{drop_id}")])

    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await call.answer()


@dp.callback_query(F.data.startswith("orderinfo_"))
async def order_info(call: CallbackQuery):
    oid = call.data.split("_")[1]

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT drop_id, amount, status, created_at, expires_at
            FROM orders WHERE id=?
        """, (oid,)) as cur:
            row = await cur.fetchone()

        async with db.execute("""
            SELECT old_status, new_status, changed_by, timestamp
            FROM order_history
            WHERE order_id=?
            ORDER BY id ASC
        """, (oid,)) as cur:
            history = await cur.fetchall()

    if not row:
        return await call.answer("🧾 Заявкане найдена", show_alert=True)

    drop_id, amount, status, created, expires = row

    text = (
        f"<b>🧾 ЗаявкаID {oid}</b>\n"
        f"Сумма: {amount}₽\n"
        f"Статус: <b>{status_name(status)}</b>\n"
        f"Создана: {datetime.fromtimestamp(created)}\n"
        f"Истекает: {datetime.fromtimestamp(expires)}\n\n"
        f"<b>История статусов:</b>\n"
    )

    if history:
        for old, new, by, ts in history:
            t = datetime.fromtimestamp(ts).strftime("%d.%m %H:%M:%S")
            text += f"• {t}: {old} → <b>{new}</b> (админ {by})\n"
    else:
        text += "— Пусто —\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data=f"stats_{drop_id}")]
        ]
    )

    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()



async def generate_drop_statistics(drop_id: int, status_filter=None, days_filter=None, full_data: bool = False):
    import aiosqlite, time
    from datetime import datetime, timedelta

    async with aiosqlite.connect(DB_NAME) as db:
        # ============================
        # 1) Все заявки по дропу + JOIN'ы
        # ============================
        sql = """
            SELECT
                o.id,                         -- 0
                o.admin_id,                   -- 1
                COALESCE(ua.username, '') AS admin_username,  -- 2
                o.drop_id,                    -- 3
                COALESCE(ud.username, '') AS drop_username,   -- 4
                o.amount,                     -- 5
                o.status,                     -- 6
                o.created_at,                 -- 7
                o.expires_at,                 -- 8
                o.check_file,                 -- 9
                o.card_id,                    -- 10
                c.number,                     -- 11
                c.bank,                       -- 12
                c.fio                         -- 13
            FROM orders o
            LEFT JOIN users ua ON ua.id = o.admin_id      -- админ, создавший заявку
            LEFT JOIN users ud ON ud.id = o.drop_id       -- дроп
            LEFT JOIN cards c ON c.id = o.card_id         -- карта
            WHERE o.drop_id = ?
        """
        params = [drop_id]

        if status_filter:
            sql += " AND o.status = ?"
            params.append(status_filter)

        if days_filter:
            cutoff = int((datetime.now() - timedelta(days=days_filter)).timestamp())
            sql += " AND o.created_at >= ?"
            params.append(cutoff)

        sql += " ORDER BY o.created_at DESC"

        async with db.execute(sql, params) as cur:
            orders = await cur.fetchall()

        # 2) История статусов
        history = []
        if orders:
            sql_hist = """
                SELECT order_id, old_status, new_status, changed_by, timestamp
                FROM order_history
                WHERE order_id IN (SELECT id FROM orders WHERE drop_id = ?)
            """
            params_hist = [drop_id]

            if days_filter:
                cutoff = int((datetime.now() - timedelta(days=days_filter)).timestamp())
                sql_hist += " AND timestamp >= ?"
                params_hist.append(cutoff)

            sql_hist += " ORDER BY timestamp ASC"

            async with db.execute(sql_hist, params_hist) as cur:
                history = await cur.fetchall()

    # ======= Шапка =======
    total_orders = len(orders)
    total_amount = sum(o[5] for o in orders) if orders else 0

    status_count = {}
    for o in orders:
        st = o[6]
        status_count[st] = status_count.get(st, 0) + 1

    if orders:
        _, _, _, d_id, d_username, *_ = orders[0]
        drop_title = f"{('@' + d_username) if d_username else d_id} (ID: {d_id})"
    else:
        drop_title = f"ID {drop_id}"

    text = f"<b>📊 Статистика дропа</b>\n"
    text += f"👤 Дроп: <b>{drop_title}</b>\n\n"
    text += f"Всего заявок: <b>{total_orders}</b>\n"
    text += f"Сумма всех заявок: <b>{total_amount:,}₽</b>\n\n"

    for st, c in status_count.items():
        emoji = {
            "pending": "🟡",
            "active": "🟣",
            "completed": "🟢",
            "rejected": "🔴",
            "timeout": "⚪",
            "expired": "⚪",
        }.get(st, "▪️")
        text += f"{emoji} <b>{st}</b>: {c}\n"

    # ======= Подробный список заявок =======
    if orders:
        text += "\n<b>📦 Список заявок:</b>\n"

    from datetime import datetime as _dt

    for row in orders:
        (order_id,
         admin_id, admin_username,
         d_id, d_username,
         amount, status,
         created_at, expires_at,
         check_file,
         card_id, card_number, bank, fio) = row

        created_dt = _dt.fromtimestamp(created_at).strftime("%d.%m %H:%M") if created_at else "—"
        expires_dt = _dt.fromtimestamp(expires_at).strftime("%d.%m %H:%M") if expires_at else "—"

        admin_label = ("@" + admin_username) if admin_username else str(admin_id)
        drop_label = ("@" + d_username) if d_username else str(d_id)

        if card_number:
            masked_card = f"**** {card_number[-4:]}"
        else:
            masked_card = "—"

        text += (
            f"\n🧾 <b>Заявка {order_id}</b>\n"
            f"• Создал админ: <code>{admin_label}</code> (ID: {admin_id})\n"
            f"• Дроп: <code>{drop_label}</code> (ID: {d_id})\n"
            f"• Статус: <b>{status_name(status)}</b>\n"
            f"• Сумма: <b>{amount:,}₽</b>\n"
            f"• Карта: {masked_card} ({bank or '—'})\n"
            f"• ФИО: {fio or '—'}\n"
            f"• Создано: {created_dt}\n"
            f"• Истекает: {expires_dt}\n"
        )

        if check_file:
            text += f"• Чек: 📎 <code>{check_file}</code>\n"
        else:
            text += "• Чек: отсутствует\n"

    # ======= История =======
    if history:
        text += "\n<b>📜 История изменений (последние 20):</b>\n"
        for order_id, old, new, changer, ts in history[-20:]:
            t = _dt.fromtimestamp(ts).strftime("%d.%m %H:%M")
            text += f"• #{order_id}: {old} → <b>{new}</b> (админ {changer}) {t}\n"

    if full_data:
        return text, {"orders": orders, "history": history}

    return text



from aiogram.types import InputFile

@dp.callback_query(F.data.startswith("export_"))
async def export_stats_handler(call: CallbackQuery):
    parts = call.data.split("_")
    drop_id = int(parts[1])
    file_type = parts[2]  # сейчас используем только csv

    # Получаем все данные для дропа
    stats_text, stats_data = await generate_drop_statistics(drop_id, full_data=True)

    if file_type == "csv":
        path = await export_to_csv(stats_data, drop_id)
    else:
        await call.answer("Неизвестный формат экспорта", show_alert=True)
        return

    await call.message.answer_document(
        FSInputFile(path),
        caption=f"Экспорт {file_type.upper()} дропа ID {drop_id}"
    )


    await call.answer(f"Файл {file_type.upper()} сгенерирован")



async def log_status(db, order_id, old, new, admin_id):
    ts = int(time.time())
    await db.execute("""
        INSERT INTO order_history (order_id, old_status, new_status, changed_by, timestamp)
        VALUES (?, ?, ?, ?, ?)
    """, (order_id, old, new, admin_id, ts))
    await db.commit()



# Заглушка, если уже утверждён
@dp.callback_query(F.data == "already_approved")
async def already_approved(call: CallbackQuery):
    await call.answer("Дроп уже утверждён", show_alert=False)



# ===== Callback: Удалить дропа =====
@dp.callback_query(F.data.startswith("delete_"))
async def delete_drop(call: CallbackQuery):
    drop_id = int(call.data.split("_")[1])
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM users WHERE id=?", (drop_id,))
        await db.execute("DELETE FROM cards WHERE user_id=?", (drop_id,))
        await db.execute("DELETE FROM orders WHERE drop_id=?", (drop_id,))
        await db.commit()
    await call.message.edit_text("✅ Дроп удалён.")
    await call.answer()






# ---------- Add card (user) ----------
from aiogram import F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

class AddCard(StatesGroup):
    number = State()
    bank = State()
    fio = State()
    min_payment = State()


@dp.message(F.text == "Добавить карту")
async def add_card_start(message: Message, state: FSMContext):
    if await get_user_type(message.from_user.id) != 1:
        return

    # фикс: запоминаем, кто начал добавление карты
    await state.set_state(AddCard.number)
    await state.update_data(initiator_id=message.from_user.id)

    await message.answer(
        "Введите номер карты (16 цифр) или номер телефона по СБП (11 цифр).\n"
        "Можно с пробелами, дефисами и т.п.",
        reply_markup=CANCEL_KB
    )


async def _check_initiator(message: Message, state: FSMContext) -> bool:
    """
    Возвращает True, если это тот же пользователь, который начал процесс.
    Иначе — False (хендлер просто игнорирует сообщение).
    """
    data = await state.get_data()
    initiator_id = data.get("initiator_id")
    return initiator_id == message.from_user.id


@dp.message(StateFilter(AddCard.number))
async def add_card_number(message: Message, state: FSMContext):
    if not await _check_initiator(message, state):
        return  # чужое сообщение — игнорируем

    num = ''.join(ch for ch in message.text if ch.isdigit())

    if len(num) == 16:
        number_type = "card"      # банковская карта
    elif len(num) == 11:
        number_type = "sbp_phone" # номер телефона / СБП
    else:
        return await message.answer(
            "Нужны 16 цифр (карта) или 11 цифр (телефон по СБП)."
        )

    if number_type == "sbp_phone":
        # 👉 Автоматически ставим банк СПБ и сразу идём на ввод ФИО
        await state.update_data(
            number=num,
            number_type=number_type,
            bank="СПБ"
        )
        await state.set_state(AddCard.fio)
        return await message.answer("ФИО владельца:", reply_markup=CANCEL_KB)

    # обычная карта — спрашиваем банк
    await state.update_data(number=num, number_type=number_type)
    await state.set_state(AddCard.bank)
    await message.answer("Название банка:", reply_markup=CANCEL_KB)


@dp.message(StateFilter(AddCard.bank))
async def add_card_bank(message: Message, state: FSMContext):
    if not await _check_initiator(message, state):
        return

    await state.update_data(bank=message.text.strip())
    await state.set_state(AddCard.fio)
    await message.answer("ФИО владельца:", reply_markup=CANCEL_KB)


@dp.message(StateFilter(AddCard.fio))
async def add_card_fio_and_finish(message: Message, state: FSMContext):
    if not await _check_initiator(message, state):
        return

    await state.update_data(fio=message.text.strip())
    await state.set_state(AddCard.min_payment)
    await message.answer("Введите минимальный платеж для этой карты (₽):", reply_markup=CANCEL_KB)


@dp.message(StateFilter(AddCard.min_payment))
async def add_card_min_payment(message: Message, state: FSMContext):
    if not await _check_initiator(message, state):
        return

    try:
        min_payment = int(message.text.replace(" ", ""))
        if min_payment <= 0:
            raise ValueError
    except:
        return await message.answer("⚠ Введите корректное число больше 0.")
    
    await state.update_data(min_payment=min_payment)
    data = await state.get_data()
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("""
                INSERT INTO cards(
                    user_id, number, bank, fio, min_payment, active
                ) VALUES(?, ?, ?, ?, ?, 1)
            """, (
                message.from_user.id,
                data["number"],
                data["bank"],   # здесь уже либо введённый банк, либо "СПБ" для 11 цифр
                data["fio"],
                min_payment
            ))
            await db.commit()
    except aiosqlite.IntegrityError:
        await state.clear()
        return await message.answer("Эта карта уже добавлена в системе.", reply_markup=menu_for(1))

    await state.clear()
    last4 = data["number"][-4:]
    await message.answer(
        f"✅ Карта ****{last4} добавлена. Минимальный платеж: {min_payment:,}₽\n",
        reply_markup=menu_for(1)
    )
    logger.info(f"Карта добавлена: user={message.from_user.id}, ****{last4}, min_payment {min_payment}")


@dp.callback_query(F.data.startswith("resetlimit_"))
async def reset_limit_handler(call: CallbackQuery):
    # только админ
    if await get_user_type(call.from_user.id) != 2:
        return await call.answer("⛔ Только для администраторов.", show_alert=True)

    drop_id = int(call.data.split("_", 1)[1])

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET monthly_used = 0 WHERE id = ?",
            (drop_id,)
        )
        await db.commit()

    await call.answer("♻️ Лимит дропа сброшен (monthly_used = 0).", show_alert=True)

@dp.callback_query(F.data.startswith("editlimit_"))
async def edit_limit_start(call: CallbackQuery, state: FSMContext):
    # только админ
    if await get_user_type(call.from_user.id) != 2:
        return await call.answer("⛔ Только для администраторов.", show_alert=True)

    drop_id = int(call.data.split("_", 1)[1])

    await state.set_state(EditLimit.waiting_new_limit)
    await state.update_data(drop_id=drop_id)

    await call.message.answer(
        f"✏️ Введи новый лимит для дропа ID <code>{drop_id}</code> (в ₽, только цифры).",
        parse_mode="HTML"
    )
    await call.answer()

@dp.message(StateFilter(EditLimit.waiting_new_limit))
async def edit_limit_set(message: Message, state: FSMContext):
    # только админ
    if await get_user_type(message.from_user.id) != 2:
        await state.clear()
        return await message.answer("⛔ Только для администраторов.")

    data = await state.get_data()
    drop_id = data.get("drop_id")

    text = (message.text or "").replace(" ", "")
    try:
        new_limit = int(text)
        if new_limit <= 0:
            raise ValueError
    except ValueError:
        return await message.answer("⚠ Введи корректное число больше 0 (лимит в ₽).")

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET monthly_limit = ? WHERE id = ?",
            (new_limit, drop_id)
        )
        await db.commit()

    await state.clear()

    await message.answer(
        f"✅ Лимит для дропа ID <code>{drop_id}</code> установлен на <b>{new_limit:,}₽</b>.",
        parse_mode="HTML"
    )


@dp.message(F.text == "Мои карты")
async def my_cards(message: Message):
    if await get_user_type(message.from_user.id) != 1:
        return

    user_id = message.from_user.id

    async with aiosqlite.connect(DB_NAME) as db:
        # Берём лимит дропа
        async with db.execute(
            "SELECT monthly_limit, monthly_used FROM users WHERE id=?",
            (user_id,)
        ) as cur:
            urow = await cur.fetchone()

        if not urow:
            monthly_limit, monthly_used = 0, 0
        else:
            monthly_limit, monthly_used = urow

        remaining = max((monthly_limit or 0) - (monthly_used or 0), 0)

        # Берём карты дропа
        async with db.execute(
            """
            SELECT id, number, bank, active, min_payment
            FROM cards
            WHERE user_id=?
            """,
            (user_id,)
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        return await message.answer("— У тебя нет карт —", reply_markup=menu_for(1))

    text = "🖤 <b>Твои карты</b> 🖤\n\n"
    text += (
        f"💰 <b>Лимит:</b> {monthly_limit:,}₽\n"
        f"📉 <b>Использовано:</b> {monthly_used:,}₽\n"
        f"✅ <b>Доступно:</b> {remaining:,}₽\n\n"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[])

    for cid, number, bank, active, min_payment in rows:
        last4 = number[-4:]
        status = "🟢 Активна" if active else "🔴 Пауза"

        text += (
            f"• <b>**** {last4}</b> — {bank} — {status_name(status)}\n"
            f"  Мин. сумма: <b>{min_payment:,}₽</b>\n\n"
        )

        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"**** {last4} • {bank}",
                callback_data=f"card_{cid}"
            )
        ])

    await message.answer(text, reply_markup=kb)


# ---------- My cards ----------
@dp.callback_query(F.data.startswith("card_"))
async def card_menu(call: CallbackQuery):
    cid = int(call.data.split("_")[1])
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Берём карту без лимита (лимит теперь на дропа)
        async with db.execute("""
            SELECT number, bank, fio, active, user_id, min_payment
            FROM cards WHERE id=?
        """, (cid,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("Карта не найдена")

    number, bank, fio, active, user_id, min_payment = row
    last4 = number[-4:]

    # Берём данные дропа + его лимит
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT user_type, approved, monthly_limit, monthly_used FROM users WHERE id=?",
            (user_id,)
        ) as cur:
            user_row = await cur.fetchone()

    if not user_row:
        return await call.answer("Пользователь не найден")

    user_type, approved, monthly_limit, monthly_used = user_row
    monthly_limit = monthly_limit or 0
    monthly_used = monthly_used or 0
    remaining = max(monthly_limit - monthly_used, 0)

    text = (
        f"🖤 <b>Карта **** {last4}</b>\n\n"
        f"🏦 {bank}\n"
        f"👤 {fio}\n"
        f"💳 Мин. сумма: <b>{min_payment:,}₽</b>\n"
        f"⚙️ Статус: {'🟢 Активна' if active else '🔴 Пауза'}\n\n"
        f"💰 <b>Лимит дропа:</b> {monthly_limit:,}₽\n"
        f"📉 <b>Использовано:</b> {monthly_used:,}₽\n"
        f"✅ <b>Остаток:</b> {remaining:,}₽"
    )

    kb_lines = [
        [InlineKeyboardButton(
            text=("Пауза" if active else "Активировать"),
            callback_data=f"tog_{cid}"
        )],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del_{cid}")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back_cards")],
    ]

    # Если потом захочешь редактировать именно лимит дропа — добавим сюда кнопку editdroplimit_{user_id}
    # if user_type == 1 and approved == 1:
    #     kb_lines.insert(1, [InlineKeyboardButton(
    #         text="Изменить лимит дропа",
    #         callback_data=f"editdroplimit_{user_id}"
    #     )])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_lines)
    await call.message.edit_text(text, reply_markup=kb)




@dp.callback_query(F.data == "back_cards")
async def back_cards(call: CallbackQuery):
    # Получаем user_id
    user_id = call.from_user.id
    # Формируем список карт заново
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, number, bank, daily_used, daily_limit, active FROM cards WHERE user_id=?", (user_id,)) as cur:
            rows = await cur.fetchall()
    if not rows:
        return await call.message.edit_text("— У тебя нет карт —", reply_markup=menu_for(1))
    text = "🖤 <b>Твои карты</b> 🖤\n\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for cid, number, bank, used, limit, active in rows:
        last4 = number[-4:]
        status = "🟢 Активна" if active else "🔴 Пауза"
        avail = max(limit - used, 0)
        text += f"• <b>**** {last4}</b> — {bank} — {status_name(status)}\n  Доступно: <b>{avail:,}₽</b> / {limit:,}₽\n\n"
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"**** {last4} • {bank}", callback_data=f"card_{cid}")])
    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("editlimit_"))
async def edit_limit_start(call: CallbackQuery, state: FSMContext):
    cid = int(call.data.split("_")[1])
    
    await state.set_state(EditLimitState.waiting_for_new_limit)
    await state.update_data(card_id=cid)

    await call.message.answer(
        "Введите новый дневной лимит (в рублях, только цифры):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )
    await call.answer()


@dp.message(F.text == "Отмена", EditLimitState.waiting_for_new_limit)
async def cancel_edit(message: Message, state: FSMContext):
    await message.answer("Изменение лимита отменено.", reply_markup=ReplyKeyboardRemove())
    await state.clear()


@dp.message(F.text.regexp(r"^\d+$"), EditLimitState.waiting_for_new_limit)
async def process_new_limit(message: Message, state: FSMContext):
    new_limit = int(message.text.replace(" ", ""))

    data = await state.get_data()
    cid = data["card_id"]

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE cards SET daily_limit = ? WHERE id = ?", (new_limit, cid))
        await db.commit()

    await message.answer(f"Лимит успешно изменён на <b>{new_limit:,} ₽</b>", reply_markup=ReplyKeyboardRemove())
    await state.clear()

    # Обновляем карточку
    fake_call = CallbackQuery(
        id="tmp", from_user=message.from_user, message=message,
        chat_instance="", data=f"card_{cid}"
    )
    await card_menu(fake_call)


@dp.callback_query(F.data == "back")
async def back(call: CallbackQuery):
    # Получаем user_id
    user_id = call.from_user.id
    # Формируем список карт заново
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, number, bank, daily_used, daily_limit, active FROM cards WHERE user_id=?", (user_id,)) as cur:
            rows = await cur.fetchall()
    if not rows:
        return await call.message.edit_text("— У тебя нет карт —", reply_markup=menu_for(1))
    text = "🖤 <b>Твои карты</b> 🖤\n\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for cid, number, bank, used, limit, active in rows:
        last4 = number[-4:]
        status = "🟢 Активна" if active else "🔴 Пауза"
        avail = max(limit - used, 0)
        text += f"• <b>**** {last4}</b> — {bank} — {status_name(status)}\n  Доступно: <b>{avail:,}₽</b> / {limit:,}₽\n\n"
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"**** {last4} • {bank}", callback_data=f"card_{cid}")])
    await call.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("tog_"))
async def toggle_card(call: CallbackQuery):
    cid = int(call.data.split("_")[1])
    async with aiosqlite.connect(DB_NAME) as db:
        # переключаем статус
        await db.execute("UPDATE cards SET active = 1 - active WHERE id=?", (cid,))
        await db.commit()
        # получаем обновленные данные
        async with db.execute("SELECT number, bank, fio, daily_used, daily_limit, active FROM cards WHERE id=?", (cid,)) as cur:
            row = await cur.fetchone()
    if not row:
        return await call.answer("Карта не найдена", show_alert=True)
    number, bank, fio, used, limit, active = row
    last4 = number[-4:]
    avail = max(limit - used, 0)
    status = "🟢 Активна" if active else "🔴 Пауза"
    text = (f"🖤 <b>Карта **** {last4}</b>\n\n"
            f"🏦 {bank}\n"
            f"👤 {fio}\n"
            f"Доступно: <b>{avail:,}₽</b> / {limit:,}₽\n"
            f"Статус: {status_name(status)}")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("Пауза" if active else "Активировать"), callback_data=f"tog_{cid}")],
        [InlineKeyboardButton(text="Удалить", callback_data=f"del_{cid}")],
        [InlineKeyboardButton(text="Назад", callback_data="back")]
    ])
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer("Статус карты изменён", show_alert=False)

@dp.callback_query(F.data.startswith("del_"))
async def delete_card(call: CallbackQuery):
    cid = int(call.data.split("_")[1])
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM cards WHERE id=?", (cid,))
        await db.commit()
    await call.answer("Карта удалена")
    # Обновляем список карт пользователю
    await my_cards(call.message)

@dp.callback_query(F.data == "back_cards")
async def back_cards(call: CallbackQuery):
    await my_cards(call.message)

# ------------------- Мои заявки -------------------
@dp.message(F.text == "Мои заявки")
async def my_orders(message: Message):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(""" 
            SELECT o.id, o.amount, o.status, o.created_at, o.expires_at, c.number 
            FROM orders o 
            JOIN cards c ON o.card_id = c.id 
            WHERE c.user_id = ? 
            ORDER BY o.created_at DESC
        """, (message.from_user.id,)) as cur:
            rows = await cur.fetchall()

    if not rows:
        return await message.answer("У вас нет активных заявок.")

    text_lines = ["<b>Ваши заявки:</b>\n"]
    kb_lines = []

    for i, (order_id, amount, status, created_at, expires_at, card_number) in enumerate(rows):
        if status != "active":
            continue  # показываем только активные заявки
        short_id = order_id
        timestamp = time.strftime("%d.%m %H:%M", time.localtime(created_at))
        
        # Рассчитываем оставшееся время
        remaining = expires_at - int(time.time())
        if remaining > 0:
            hours, rem = divmod(remaining, 3600)
            minutes, seconds = divmod(rem, 60)
            remaining_str = f"{hours}ч {minutes}м {seconds}с"
        else:
            remaining_str = "Время истекло"
        
        text_lines.append(
            f"<b>{i+1}.</b> 🆔 <code>{short_id}</code>\n"
            f"    💰 Сумма: <b>{amount:,}₽</b> | {status_name(status)}\n"
            f"    ⏳ Создано: {timestamp}\n"
            f"    ⏲️ Осталось времени: {remaining_str}\n"
        )
        kb_lines.append([ 
            InlineKeyboardButton( 
                text=f"💳 {short_id} — {amount:,}₽ [{status_name(status)}]",
                callback_data=f"vieworder_{order_id}"
            )
        ])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_lines)
    await message.answer("\n".join(text_lines), reply_markup=kb)

# ------------------- Просмотр заявки -------------------
# ------------------- Просмотр заявки (обновлённая версия) -------------------
@dp.callback_query(F.data.startswith("vieworder_"))
async def view_order(call: CallbackQuery):
    order_id = call.data.split("_", 1)[1]

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT o.amount, o.status, o.created_at, o.expires_at, 
                   c.number, c.bank, c.fio, o.check_file
            FROM orders o 
            JOIN cards c ON o.card_id = c.id 
            WHERE o.id = ?
        """, (order_id,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("🧾 Заявкане найдена", show_alert=True)

    amount, status, created_at, expires_at, number, bank, fio, check_file = row
    masked = f"{number[:4]} {number[4:8]} **** {number[-4:]}"
    tm = time.strftime("%d.%m.%Y %H:%M", time.localtime(created_at))

    # === Формируем клавиатуру ===
    kb_lines = []

    # Кнопка "Посмотреть чек" — если чек уже есть
    if check_file:
        kb_lines.append([InlineKeyboardButton(
            text="Посмотреть чек",
            callback_data=f"viewchk_{order_id}"
        )])

    # Кнопка "Запросить чек" — если чека нет и заявка активна
    if not check_file and status == "active":
        kb_lines.append([InlineKeyboardButton(
            text="Запросить чек",
            callback_data=f"reqchk_{order_id}"
        )])

    # Основные кнопки
    if status == "active":
        kb_lines.extend([
            [InlineKeyboardButton(text="Подтвердить оплату", callback_data=f"done_{order_id}")],
            [InlineKeyboardButton(text="Отменить", callback_data=f"cancel_{order_id}")]
        ])

    kb_lines.append([InlineKeyboardButton(text="Назад", callback_data="back_to_my_orders")])

    kb_owner = InlineKeyboardMarkup(inline_keyboard=kb_lines)

    # === Текст заявки ===
    base_text = (
        f"🖤 <b>Заявка</b> 🖤\n\n"
        f"🆔: <code>{order_id}</code>\n"
        f"💳 Карта: <code>{masked}</code>\n"
        f"🏦 Банк: {bank}\n"
        f"👤 ФИО: {fio}\n"
        f"💰 Сумма: <b>{amount:,}₽</b>\n"
        f"⏳ Создана: {tm}\n"
        f"❇️ Статус: {status_name(status)}"
    )

    message = await call.message.edit_text(
        base_text + "\n⏲️ Осталось времени: ",
        reply_markup=kb_owner
    )

    # === Таймер обратного отсчёта ===
    while status == "active":
        now = int(time.time())
        remaining = max(0, expires_at - now)

        if remaining == 0:
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute("UPDATE orders SET status='expired' WHERE id=?", (order_id,))
                await db.commit()
            status = "expired"
            remaining_str = "Время истекло"
            # Обновляем клавиатуру — убираем кнопки действий
            final_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_my_orders")]
            ])
            try:
                await message.edit_text(base_text + f"\n⏲️ Осталось времени: {remaining_str}", reply_markup=final_kb)
            except:
                pass
            break
        else:
            h, rem = divmod(remaining, 3600)
            m, s = divmod(rem, 60)
            remaining_str = f"{h}ч {m}м {s}с"

        try:
            await message.edit_text(
                base_text + f"\n⏲️ Осталось времени: {remaining_str}",
                reply_markup=kb_owner
            )
        except:
            break

        await asyncio.sleep(10)

@dp.callback_query(F.data == "back_to_my_orders")
async def back_to_orders(call: CallbackQuery):
    await call.message.delete()


@dp.callback_query(F.data.startswith("viewchk_"))
async def view_check(call: CallbackQuery):
    order_id = call.data.split("_", 1)[1]

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT check_file FROM orders WHERE id = ?",
            (order_id,)
        ) as cur:
            row = await cur.fetchone()

    if not row or not row[0]:
        return await call.answer(
            "Чек не прикреплён к этой заявке.",
            show_alert=True
        )

    check_file = row[0]

    msg_chk = None
    try:
        if check_file.startswith("photo:"):
            file_id = check_file.split(":", 1)[1]
            msg_chk = await bot.send_photo(
                call.message.chat.id,
                file_id,
                caption=f"📎 Чек по заявке <code>{order_id}</code>",
                parse_mode="HTML"
            )
        elif check_file.startswith("doc:"):
            file_id = check_file.split(":", 1)[1]
            msg_chk = await bot.send_document(
                call.message.chat.id,
                file_id,
                caption=f"📎 Чек по заявке <code>{order_id}</code>",
                parse_mode="HTML"
            )
        else:
            # старый формат без префикса — пробуем как фото
            msg_chk = await bot.send_photo(
                call.message.chat.id,
                check_file,
                caption=f"📎 Чек по заявке <code>{order_id}</code>",
                parse_mode="HTML"
            )
    except Exception:
        return await call.answer(
            "⚠ Не удалось загрузить чек (возможно, file_id устарел).",
            show_alert=True
        )

    # 👉 если чек успешно отправили дропу — запомним его message_id,
    # чтобы потом удалить при done_/cancel_
    if msg_chk:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE orders SET check_message_id = ?, drop_chat_id = ? WHERE id = ?",
                (msg_chk.message_id, call.message.chat.id, order_id)
            )
            await db.commit()

    await call.answer()


# ------------------- Статистика пользователя -------------------
@dp.message(F.text == "Статистика")
async def user_stats(message: Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        # ✅ Всего выполненных платежей
        async with db.execute("""
            SELECT COUNT(*), SUM(amount)
            FROM payments p
            JOIN cards c ON p.card_id = c.id
            WHERE c.user_id = ?
        """, (user_id,)) as cur:
            row = await cur.fetchone()
            total_orders, total_amount = (row if row else (0, 0))
        
        total_orders = total_orders or 0
        total_amount = total_amount or 0

        # ✅ Активные заявки (ещё не оплачены)
        async with db.execute("""
            SELECT COUNT(*), SUM(amount)
            FROM orders o
            JOIN cards c ON o.card_id = c.id
            WHERE c.user_id = ? AND o.status = 'active'
        """, (user_id,)) as cur:
            row2 = await cur.fetchone()
            active_orders, active_amount = (row2 if row2 else (0, 0))
        
        active_orders = active_orders or 0
        active_amount = active_amount or 0

    text = (
        f"<b>📊 Ваша статистика</b>\n\n"
        f"💰 Выполнено платежей: {total_orders}\n"
        f"💵 Общая сумма выполненных: {total_amount:,}₽\n\n"
        f"🕒 Активные заявки: {active_orders}\n"
        f"💸 Сумма активных: {active_amount:,}₽"
    )
    
    await message.answer(text)

# ---------- Create order (admin) ----------
@dp.message(F.text == "Создать заявку")
async def create_order_start(message: Message, state: FSMContext):
    if await get_user_type(message.from_user.id) != 2:
        return
    await state.set_state(CreateOrder.amount)
    await message.answer("💸 Введи сумму заявки (цифрами):")

from asyncio import create_task, sleep

@dp.message(StateFilter(CreateOrder.amount))
async def create_order_amount(message: Message, state: FSMContext):
    # 1. Парсим сумму
    try:
        amount = int(message.text.replace(" ", ""))
        if amount <= 0:
            raise ValueError
    except:
        return await message.answer("⚠ Введи корректное число.")

    # 2. Выбираем подходящие карты c учётом ЛИМИТА ДРОПА
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            """
            SELECT c.id, c.number, c.bank, c.fio, c.user_id, c.min_payment
            FROM cards c
            JOIN users u ON u.id = c.user_id
            WHERE c.active = 1
              AND ? >= c.min_payment
              AND (COALESCE(u.monthly_limit, 0) - COALESCE(u.monthly_used, 0)) >= ?
            """,
            (amount, amount)
        ) as cur:
            cards = await cur.fetchall()

    if not cards:
        await state.clear()
        return await message.answer(f"🔴 Нет доступных карт под сумму {amount}₽.")

    # 3. Случайная карта
    card = random.choice(cards)
    card_id, number, bank, fio, drop_id, min_payment = card

    # 4. (доп. проверка уже не обязательна, но если хочешь — можно оставить)
    #    либо вообще убрать этот блок:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT monthly_limit, monthly_used FROM users WHERE id = ?",
            (drop_id,)
        ) as cur:
            row = await cur.fetchone()

    if not row:
        await state.clear()
        return await message.answer("❌ Ошибка: не найден дроп для карты.")

    monthly_limit, monthly_used = row
    monthly_limit = monthly_limit or 0
    monthly_used = monthly_used or 0
    remaining = monthly_limit - monthly_used
    if remaining < amount:
        await state.clear()
        return await message.answer(
            f"🚫 Лимит дропа превышен.\n"
            f"💰 Лимит: <b>{monthly_limit:,}₽</b>\n"
            f"📉 Использовано: <b>{monthly_used:,}₽</b>\n"
            f"✅ Доступно: <b>{max(0, remaining):,}₽</b>\n\n"
            f"Попробуй меньшую сумму."
        )

    # Дальше оставляешь твой код как был:
    # 5. вытаскиваем username дропа
    # 6. создаём заявку
    # 7+ — текст, таймер и т.д.


    # 5. Узнаём username дропа
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT username FROM users WHERE id = ?",
            (drop_id,)
        ) as cur:
            row = await cur.fetchone()
    drop_username = row[0] if row and row[0] else None

    # 6. Создаём заявку
    order_id = str(uuid.uuid4()).replace("-", "")[:16]
    created = int(time.time())
    expires = created + 1800  # 30 минут

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            INSERT INTO orders(
                id,
                admin_id,
                admin_chat_id,
                admin_message_id,
                drop_id,
                card_id,
                amount,
                created_at,
                expires_at,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                message.from_user.id,  # кто создал (админ)
                message.chat.id,
                None,                   # message_id допишем после отправки
                drop_id,
                card_id,
                amount,
                created,
                expires,
                "pending",
            ),
        )
        await db.commit()

    # 7. Маскируем номер
    masked = mask_card(number)

    # 8. Шаблон текста для АДМИНА (без f-строк, чистый .format)
    # 9. Текст заявки для АДМИНА
    admin_text_template = (
        f"🆔 Заявка: <code>{order_id}</code>\n"
        f"👤 Дроп: ID <code>{drop_id}</code>\n"
        f"💳 Реквизит: <code>{masked}</code>\n"
        f"🏦 Банк: {bank}\n"
        f"💰 Сумма: <b>{amount:,}₽</b>\n"
        f"💳 Карта: <code>{number}</code>\n"
        f"🏦 Банк: <code>{bank}</code>\n"
        f"👤 ФИО: <code>{fio}</code>\n\n"
        f"⏱ Заявка действует 30 минут\n"
        f"📌 Статус: <b>Ожидает отправки</b>\n"
        f"⏱ Осталось: {{remaining}}"  # двойные { } оставляют плейсхолдер для .format()
    )

    kb_admin = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📎 Прикрепить чек", callback_data=f"attachchk_{order_id}")],
        [InlineKeyboardButton(text="📤 Отправить дропу", callback_data=f"send_drop_{order_id}")],
        [InlineKeyboardButton(text="⛔ Отменить", callback_data=f"cancel_pending_{order_id}")]
    ])


    # создаём сообщение админу с начальным значением таймера
    msg = await message.answer(
        admin_text_template.format(remaining="30:00"),
        reply_markup=kb_admin
    )

    admin_chat_id = msg.chat.id
    admin_message_id = msg.message_id

    # 10. Сохраняем admin_message_id в БД
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE orders SET admin_chat_id = ?, admin_message_id = ? WHERE id = ?",
            (admin_chat_id, admin_message_id, order_id)
        )
        await db.commit()

    # 11. Таймер для админа
    async def update_admin_timer():
        last_text = ""
        while True:
            remaining_sec = expires - int(time.time())
            if remaining_sec <= 0:
                remaining_str = "00:00"
            else:
                m, s = divmod(remaining_sec, 60)
                remaining_str = f"{m:02d}:{s:02d}"

            new_text = admin_text_template.format(remaining=remaining_str)

            if new_text != last_text:
                try:
                    await bot.edit_message_text(
                        chat_id=admin_chat_id,
                        message_id=admin_message_id,
                        text=new_text,
                        reply_markup=kb_admin,
                        parse_mode="HTML"
                    )
                    last_text = new_text
                except:
                    # если сообщение удалили/изменили — просто выходим
                    break

            if remaining_sec <= 0:
                break

            await asyncio.sleep(5)

    asyncio.create_task(update_admin_timer())

    await state.clear()




from aiogram import F
from aiogram.types import CallbackQuery

@dp.callback_query(F.data.startswith("attachchk_"))
async def attach_check_start(call: CallbackQuery, state: FSMContext):
    order_id = call.data.split("_", 1)[1]

    await state.set_state(AddReceipt.wait_file)
    await state.update_data(
        order_id=order_id,
        initiator_id=call.from_user.id,
    )

    # 👉 сохраняем message_id подсказки, чтобы потом удалить
    msg = await call.message.answer(
        "📎 Отправь чек одним фото или документом.",
        reply_markup=CANCEL_KB,
    )
    await state.update_data(prompt_msg_id=msg.message_id)

    await call.answer()


@dp.message(StateFilter(AddReceipt.wait_file))
async def attach_check_file(message: Message, state: FSMContext):
    data = await state.get_data()

    # чтобы чужие сообщения не цеплялись
    if data.get("initiator_id") != message.from_user.id:
        return

    stored = None

    if message.photo:
        file_id = message.photo[-1].file_id
        stored = f"photo:{file_id}"
    elif message.document:
        file_id = message.document.file_id
        stored = f"doc:{file_id}"
    else:
        return await message.answer("⚠ Пришли фото или документ с чеком.")

    order_id = data["order_id"]

    # сохраняем чек в orders.check_file
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "UPDATE orders SET check_file = ? WHERE id = ?",
            (stored, order_id),
        )
        await db.commit()

        if cur.rowcount == 0:
            await state.clear()
            return await message.answer(
                f"⚠ Не нашёл заявку с ID {order_id} при сохранении чека."
            )

    # удаляем сообщение с чеком
    try:
        await message.delete()
    except:
        pass

    # 👉 удаляем подсказку "Отправь чек..."
    prompt_msg_id = data.get("prompt_msg_id")
    if prompt_msg_id:
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=prompt_msg_id
            )
        except:
            pass

    await state.clear()

    # возвращаем главное меню
    kb = menu_for(await get_user_type(message.from_user.id))

    await message.answer(
        f"✅ Чек прикреплён к заявке {order_id}.",
        reply_markup=kb,
    )



# ---------- Send to drop (admin action) ----------
@dp.callback_query(F.data.startswith("send_drop_"))
async def send_drop_handler(call: CallbackQuery):
    order_id = call.data.split("_", 2)[2]

    # Получаем данные заявки + check_file
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT drop_id,
                   card_id,
                   amount,
                   admin_chat_id,
                   admin_message_id,
                   status,
                   expires_at,
                   check_file
            FROM orders
            WHERE id=?
        """, (order_id,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("🧾 Заявка не найдена", show_alert=True)

    (drop_id,
     card_id,
     amount,
     admin_chat_id,
     admin_message_id,
     status,
     expires_at,
     check_file) = row

    if status != "pending":
        return await call.answer("Уже отправлено.", show_alert=True)

    # Получаем карту
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT number, bank, fio FROM cards WHERE id=?",
            (card_id,),
        ) as cur:
            c = await cur.fetchone()

    if not c:
        return await call.answer("Карта не найдена", show_alert=True)

    number, bank, fio = c
    masked = mask_card(number)

    # 👉 Клавиатура дропа зависит от того, есть ли чек
    if check_file:
        kb_owner = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💸 Подтвердить оплату", callback_data=f"done_{order_id}")],
            [InlineKeyboardButton(text="⛔ Отменить", callback_data=f"cancel_{order_id}")],
        ])
    else:
        kb_owner = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💸 Подтвердить оплату", callback_data=f"done_{order_id}")],
            [InlineKeyboardButton(text="📤 Запросить чек", callback_data=f"reqchk_{order_id}")],
            [InlineKeyboardButton(text="⛔ Отменить", callback_data=f"cancel_{order_id}")],
        ])

    # Сообщение дропу
    text_template = (
        f"🔴 <b>НОВАЯ ЗАЯВКА</b> 🔴\n\n"
        f"ID: <code>{order_id}</code>\n"
        f"💳 Реквизит: {masked}\n"
        f"🏦 Банк: {bank}\n"
        f"👤 ФИО: {fio}\n"
        f"💰 Сумма: <b>{amount:,}₽</b>\n\n"
        f"⏱ Осталось: {{remaining}}"
    )

    try:
        owner_msg = await bot.send_message(
            drop_id,
            text_template.format(remaining="30:00"),
            reply_markup=kb_owner,
        )
    except Exception:
        return await call.answer(
            "Ошибка отправки дропу. Возможно, он не запустил бота.",
            show_alert=True,
        )

    # 👉 Пытаемся отправить чек, если есть, и запоминаем message_id
    check_message_id = None

    if check_file:
        try:
            if check_file.startswith("photo:"):
                file_id = check_file.split(":", 1)[1]
                msg_chk = await bot.send_photo(
                    drop_id,
                    file_id,
                    caption=f"📎 Чек по заявке {order_id}",
                )
                check_message_id = msg_chk.message_id
            elif check_file.startswith("doc:"):
                file_id = check_file.split(":", 1)[1]
                msg_chk = await bot.send_document(
                    drop_id,
                    file_id,
                    caption=f"📎 Чек по заявке {order_id}",
                )
                check_message_id = msg_chk.message_id
        except Exception as e:
            logger.error(f"Ошибка отправки чека дропу: {e}")
    else:
        # debug: если чек не найден — кидаем админу инфу
        try:
            await bot.send_message(
                admin_chat_id,
                f"⚠ Отправленна Без Чека.",
            )
        except:
            pass

    old_status = status
    new_status = "active"

    # Сохраняем сообщение дропу в orders + check_message_id
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            UPDATE orders
            SET status = ?,
                drop_chat_id = ?,
                drop_message_id = ?,
                check_message_id = ?
            WHERE id = ?
            """,
            (new_status, owner_msg.chat.id, owner_msg.message_id, check_message_id, order_id),
        )
        await log_status(db, order_id, old_status, new_status, call.from_user.id)
        await db.commit()

    # Таймер для дропа
    async def update_drop_timer():
        last_text = ""
        while True:
            remaining_sec = expires_at - int(time.time())
            if remaining_sec <= 0:
                remaining_str = "00:00"
            else:
                m, s = divmod(remaining_sec, 60)
                remaining_str = f"{m:02d}:{s:02d}"

            new_text = text_template.format(remaining=remaining_str)

            if new_text != last_text:
                try:
                    await bot.edit_message_text(
                        chat_id=owner_msg.chat.id,
                        message_id=owner_msg.message_id,
                        text=new_text,
                        reply_markup=kb_owner,
                        parse_mode="HTML",
                    )
                    last_text = new_text
                except:
                    pass

            if remaining_sec <= 0:
                break

            await asyncio.sleep(5)

    asyncio.create_task(update_drop_timer())

    # Обновляем сообщение админа
    try:
        await bot.edit_message_text(
            chat_id=admin_chat_id,
            message_id=admin_message_id,
            text=(
                f"🖤 <b>Заявка отправлена дропу</b> 🖤\n\n"
                f"ID: <code>{order_id}</code>\n"
                f"💰 Сумма: {amount:,}₽\n"
                f"<b>Статус:</b> 🟢 Активна"
            ),
            parse_mode="HTML",
        )
    except:
        pass

    await call.answer("📤 Отправлено дропу!")





@dp.callback_query(F.data.startswith("reqchk_"))
async def request_check_by_owner(call: CallbackQuery):
    order_id = call.data.split("_", 1)[1]

    # получить заявку
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT admin_id, amount, card_id, drop_id
            FROM orders WHERE id=?
        """, (order_id,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("🧾 Заявкане найдена.", show_alert=True)

    admin_id, amount, card_id, drop_id = row

    # владельцу заявки
    if call.from_user.id != drop_id:
        return await call.answer("Ты не владелец этой заявки.", show_alert=True)

    # уведомить владельца
    await call.message.answer("✅ Запрос чека отправлен админу!")

    # отправляем админу запрос на чек
    # Формируем текст для администратора
    text_admin = (
        f"📤 <b>Запрос чека по заявке</b>\n\n"
        f"🆔 ID: <code>{order_id}</code>\n"
        f"💰 Сумма: {amount:,}₽\n\n"
        f"Пожалуйста, пришлите чек."
    )

    # Создаём inline-кнопку для прикрепления чека
    kb_admin = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📎 Прикрепить чек", callback_data=f"attachchk_{order_id}")]
        ]
    )

    # Отправляем сообщение администратору
    try:
        await bot.send_message(admin_id, text_admin, reply_markup=kb_admin)
    except Exception as e:
        logger.error(f"Ошибка отправки администратору: {e}")

    logger.info(f"Owner requested check for order {order_id}")


def admin_kb(order_id: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📎 Прикрепить чек", callback_data=f"attachchk_{order_id}")]
        ]
    )

# ------------------------
# Callback для кнопки "Прикрепить чек"
# ------------------------


@dp.callback_query(F.data.startswith("attachchk_"))
async def admin_attach_check(call: CallbackQuery, state: FSMContext):
    order_id = call.data.split("_", 1)[1]

    # Получаем данные заявки из БД
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT drop_id, amount, status FROM orders WHERE id = ?",
            (order_id,)
        ) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("🧾 Заявкане найдена.", show_alert=True)

    drop_id, amount, status = row

    if status != "active":
        return await call.answer("🧾 Заявкауже завершена или отменена.", show_alert=True)

    # ВАЖНО: используем правильное состояние из StatesGroup!
    await state.set_state(AdminCheckState.waiting_for_check_photo)

    # Сохраняем всё нужное: order_id, drop_id, сумму
    await state.update_data(
        order_id=order_id,
        drop_id=drop_id,
        amount=amount
    )

    # Клавиатура с кнопкой "Отмена"
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await call.message.answer(
        f"Прикрепите фото чека\n\n"
        f"ID заявки: <code>{order_id}</code>\n"
        f"Сумма: <b>{amount:,} ₽</b>",
        reply_markup=cancel_kb
    )

    await call.answer("Ожидаю фото чека...")



@dp.message(StateFilter(AdminCheckState.waiting_for_check_photo))
async def receive_check(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("order_id")
    drop_id = data.get("drop_id")        # ← ВОТ ТАК ПОЛУЧАЕШЬ drop_id!
    amount = data.get("amount", 0)       # ← если ты его тоже сохранял

    if not order_id or not drop_id:
        await message.answer("Ошибка: не найден ID заявки или дропа.")
        await state.clear()
        return

    # === Отмена ===
    if message.text and message.text.strip() == "Отмена":
        await message.answer("Загрузка чека отменена.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    # === Если не фото ===
    if not message.photo:
        await message.answer("Пожалуйста, отправьте фото чека или нажмите «Отмена».")
        return

    photo = message.photo[-1]
    file_id = photo.file_id  # ← Это и есть идентификатор файла в Telegram

    # === Сохраняем file_id в БД (а не путь на диске!) ===
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE orders SET check_file = ? WHERE id = ?",
            (file_id, order_id)
        )
        await db.commit()

    # === Отправляем дропу по file_id (самый надёжный способ) ===
    try:
        kb_chek = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💸 Подтвердить оплату", callback_data=f"done_{order_id}")],
                [InlineKeyboardButton(text="⛔ Отменить", callback_data=f"cancel_{order_id}")]
            ])
        await bot.send_photo(
            chat_id=drop_id,
            photo=file_id,  # ← Просто file_id! Ничего не скачиваем!
            caption=f"📎 Чек по вашей заявке\n"
                    f"🆔 <code>{order_id}</code>\n"
                    f"💰 Сумма: <b>{amount:,}₽</b>\n"
                    f"✅ Прикреплён администратором"
        , reply_markup=kb_chek)

    except Exception as e:
        logger.error(f"Не удалось отправить чек дропу {drop_id}: {e}")
        await message.answer("Чек сохранён, но не удалось отправить дропу (возможно, он заблокировал бота).")
    else:
        await message.answer("✅ Чек успешно прикреплён и отправлен дропу!", reply_markup=menu_for(2))

    await state.clear()


@dp.message(F.photo, StateFilter("waiting_for_check_photo"))
async def admin_send_check_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data["order_id"]
    admin_id = data["admin_id"]

    file_id = message.photo[-1].file_id

    # сохраняем чек в БД
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE orders SET check_file=? WHERE id=?",
            (file_id, order_id)
        )
        await db.commit()

    # получаем drop_id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT drop_id FROM orders WHERE id=?", (order_id,)) as cur:
            row = await cur.fetchone()

    drop_id = row[0]

    # отправляем дропу чек
    try:
        await bot.send_photo(
            drop_id,
            file_id,
            caption=f"📎 Чек по заявке <code>{order_id}</code>\nПередано администратором."
        )
    except Exception as e:
        logger.error(f"Не смог отправить чек дропу: {e}")

    # подтверждение админу
    await message.answer("✅ Чек прикреплён и отправлен дропу.")

    await state.clear()



# ---------- Cancel pending (admin) ----------
from aiogram.exceptions import TelegramBadRequest

@dp.callback_query(F.data.startswith("cancel_pending_"))
async def cancel_pending_handler(call: CallbackQuery):
    order_id = call.data.split("_", 2)[2]

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            """
            SELECT drop_chat_id, drop_message_id, status, amount
            FROM orders
            WHERE id = ?
            """,
            (order_id,)
        ) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("🧾 Заявкане найдена", show_alert=True)

    drop_chat_id, drop_message_id, status, amount = row

    # Если заявка уже не активна/ожидает – можно запретить отмену или просто отметить
    if status not in ("pending", "active"):
        return await call.answer("🧾 Заявкауже обработана.", show_alert=True)

    # Пытаемся удалить сообщение у дропа
    if drop_chat_id and drop_message_id:
        try:
            await bot.delete_message(drop_chat_id, drop_message_id)
        except TelegramBadRequest:
            # сообщение уже удалено / слишком старое / нет прав – просто игнорим
            pass

    # Обновляем статус заявки
    async with aiosqlite.connect(DB_NAME) as db:
        old_status = status
        new_status = "rejected"
        await db.execute(
            "UPDATE orders SET status=? WHERE id=?",
            (new_status, order_id)
        )
        await log_status(db, order_id, old_status, new_status, call.from_user.id)
        await db.commit()

    # Обновляем сообщение у админа (текущее, где он нажал кнопку)
    try:
        await call.message.edit_text(
            f"⛔ <b>Заявка отменена администратором</b>\n\n"
            f"ID: <code>{order_id}</code>\n"
            f"💰 Сумма: {amount:,}₽\n"
            f"<b>Статус:</b> 🔴 Отклонена",
            parse_mode="HTML"
        )
    except:
        pass

    await call.answer("🧾 Заявкаотменена, сообщение дропу удалено.")


# ---------- Owner button handlers ----------
@dp.callback_query(F.data.startswith(("done_", "cancel_")))
async def owner_action(call: CallbackQuery):
    action, order_id = call.data.split("_", 1)

    # Загружаем заявку
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT admin_id,
                   admin_chat_id,
                   admin_message_id,
                   drop_id,
                   drop_chat_id,
                   drop_message_id,
                   check_message_id,
                   card_id,
                   amount,
                   status
            FROM orders
            WHERE id = ?
        """, (order_id,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("🧾 Заявка не найдена.", show_alert=True)

    (admin_id,
     admin_chat_id,
     admin_message_id,
     drop_id,
     drop_chat_id,
     drop_message_id,
     check_message_id,
     card_id,
     amount,
     status) = row

    if call.from_user.id != drop_id:
        return await call.answer("⛔ Ты не владелец этой заявки.", show_alert=True)

    if status != "active":
        return await call.answer("⚠ Заявка уже обработана.", show_alert=True)

    # ====== Подтверждение ======
    if action == "done":
        ts = int(time.time())
        try:
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute(
                    "UPDATE users SET monthly_used = COALESCE(monthly_used,0) + ? WHERE id=?",
                    (amount, drop_id)
                )
                await db.execute(
                    "UPDATE orders SET status='completed' WHERE id=?",
                    (order_id,)
                )
                pay_id = str(uuid.uuid4())
                await db.execute("""
                    INSERT INTO payments(id, order_id, card_id, amount, admin_id, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (pay_id, order_id, card_id, amount, admin_id, ts))
                await db.commit()
        except Exception:
            logger.exception("Error during confirming payment")
            return await call.answer("❌ Ошибка при подтверждении.", show_alert=True)

        status_text = "🟢 Выполнена"

    # ====== Отмена ======
    else:  # action == "cancel"
        try:
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute(
                    "UPDATE orders SET status='rejected' WHERE id=?",
                    (order_id,)
                )
                await db.commit()
        except Exception:
            logger.exception("Error during cancel")
            return await call.answer("❌ Ошибка при отмене.", show_alert=True)

        status_text = "🔴 Отменена"

    # ==== Обновление/удаление сообщения у админа ====
    if admin_chat_id and admin_message_id:
        deleted = False
        try:
            await bot.delete_message(admin_chat_id, admin_message_id)
            deleted = True
        except Exception:
            deleted = False

        if not deleted:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=admin_chat_id,
                    message_id=admin_message_id,
                    reply_markup=None,
                )
            except:
                pass

            try:
                await bot.edit_message_text(
                    chat_id=admin_chat_id,
                    message_id=admin_message_id,
                    text=(
                        f"🧾 Заявка <code>{order_id}</code>\n"
                        f"💰 Сумма: <b>{amount:,}₽</b>\n"
                        f"📌 Статус: {status_text}"
                    ),
                    parse_mode="HTML",
                )
            except:
                pass

    # Доп. уведомление админу
    try:
        await bot.send_message(
            admin_id,
            (
                f"🔔 Обновление по заявке <code>{order_id}</code>\n"
                f"💰 Сумма: <b>{amount:,}₽</b>\n"
                f"👤 Дроп ID: <code>{drop_id}</code>\n"
                f"📌 Статус: {status_text}"
            ),
            parse_mode="HTML",
        )
    except:
        pass

    # ==== Удаляем у дропа ====

    # 1) сообщение с заявкой (там, где нажата кнопка)
    try:
        await call.message.delete()
    except:
        pass

    # 2) последнее сообщение с чеком, если мы его сохраняли
    if check_message_id:
        try:
            chat_id = drop_chat_id or drop_id
            await bot.delete_message(chat_id, check_message_id)
        except:
            pass

    await call.answer(f"Статус заявки: {status_text}", show_alert=True)



# ---------- Admin: Active orders ----------
@dp.message(F.text == "Активные заявки")
async def active_orders_admin(message: Message):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT o.id, o.amount, u.username, o.created_at, o.expires_at
            FROM orders o
            LEFT JOIN users u ON u.id = o.drop_id
            WHERE o.status = 'active'
            ORDER BY o.created_at DESC
        """) as cur:
            rows = await cur.fetchall()

    if not rows:
        return await message.answer("Нет активных заявок.")

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    text_lines = ["<b>Активные заявки:</b>\n"]

    now = datetime.now()

    for order_id, amount, username, created_at, expires_at in rows:
        created_dt = datetime.fromtimestamp(created_at)
        expire_dt = datetime.fromtimestamp(expires_at)
        remaining = expire_dt - now
        if remaining.total_seconds() < 0:
            remaining_text = "⏰ Время истекло"
        else:
            minutes, seconds = divmod(int(remaining.total_seconds()), 60)
            remaining_text = f"{minutes} мин"

        short_id = order_id
        created_str = created_dt.strftime("%d.%m %H:%M")
        username_display = f"@{username}" if username and not username.startswith("@") else (username or "Без имени")

        text_lines.append(f"• {short_id} — {amount:,}₽ — дроп {username_display} — {created_str} — {remaining_text}")

        # каждая заявка — кнопка для просмотра/действия
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{short_id} — {amount:,}₽ — {remaining_text}",
                callback_data=f"vieworder22_{order_id}"
            )
        ])

    await message.answer("\n".join(text_lines), reply_markup=kb)


# Пример просмотра заявки через админскую кнопку
@dp.callback_query(F.data.startswith("vieworder22_"))
async def view_order_admin(call: CallbackQuery):
    order_id = call.data.split("_")[1]

    async with aiosqlite.connect(DB_NAME) as db:
        # Получаем информацию о заявке и дропе
        async with db.execute("""
            SELECT o.id, o.amount, o.card_id, o.drop_id, o.created_at, o.expires_at, u.username, c.number, c.bank, c.fio
            FROM orders o
            LEFT JOIN users u ON u.id = o.drop_id
            LEFT JOIN cards c ON c.id = o.card_id
            WHERE o.id = ?
        """, (order_id,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("🧾 Заявкане найдена", show_alert=True)

    order_id, amount, card_id, drop_id, created_at, expires_at, username, number, bank, fio = row

    masked = mask_card(number)
    username_display = f"@{username}" if username and not username.startswith("@") else (username or "Без имени")

    # Время и оставшееся время
    created_dt = datetime.fromtimestamp(created_at)
    expire_dt = datetime.fromtimestamp(expires_at)
    remaining = expire_dt - datetime.now()
    remaining_text = f"{max(int(remaining.total_seconds() // 60),0)} мин" if remaining.total_seconds() > 0 else "⏰ Время истекло"

    text = (
        f"🖤 <b>Заявка</b> 🖤\n\n"
        f"🆔: {order_id}\n"
        f"💳 Реквизит: {masked}\n"
        f"🏦 Банк: {bank}\n"
        f"💰 Сумма: {amount:,}₽\n"
        f"👤 Дроп: {username_display}\n"
        f"🕒 Создано: {created_dt.strftime('%d.%m %H:%M')} — {remaining_text}"
    )

        # Кнопки только для админа
    kb_admin = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить дропу", callback_data=f"send_drop_{order_id}")],
        [InlineKeyboardButton(text="⛔ Отменить (не отправлять)", callback_data=f"cancel_pending_{order_id}")]
    ])

    await call.message.edit_text(text, reply_markup=kb_admin, parse_mode="HTML")
    await call.answer()




@dp.callback_query(F.data == "back_to_active_orders")
async def back_to_active_orders(call: CallbackQuery):
    await call.message.delete()

# ---------- Admin statistics (existing handlers kept) ----------
@dp.message(F.text == "Админ: Статистика")
async def admin_stats_menu(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Общая статистика", callback_data="stats_main")],
        [InlineKeyboardButton(text="Статистика по дропам", callback_data="stats_drops")],
        [InlineKeyboardButton(text="Назад", callback_data="admin_menu_back")]
    ])
    await message.answer("Админ: Статистика\nВыберите раздел:", reply_markup=kb)

@dp.callback_query(F.data == "stats_main")
async def stats_main(call: CallbackQuery):
    await show_general_stats(call, period="all")

@dp.callback_query(F.data.startswith("gen_"))
async def stats_general_period(call: CallbackQuery):
    period = call.data.split("_")[1]  # all / week / month
    await show_general_stats(call, period=period)

async def show_general_stats(call: CallbackQuery, period: str = "all"):
    now = datetime.now()

    if period == "week":
        start = now - timedelta(days=7)
        title = "за неделю"
    elif period == "month":
        start = now - timedelta(days=30)
        title = "за месяц"
    else:
        start = None
        title = "за всё время"

    async with aiosqlite.connect(DB_NAME) as db:
        if start:
            cursor = await db.execute("""
                SELECT COUNT(*), COALESCE(SUM(amount), 0)
                FROM orders
                WHERE status = 'completed' AND created_at >= ?
            """, (int(start.timestamp()),))
        else:
            cursor = await db.execute("""
                SELECT COUNT(*), COALESCE(SUM(amount), 0)
                FROM orders
                WHERE status = 'completed'
            """)

        row = await cursor.fetchone()
        await cursor.close()

    count, total = row or (0, 0)

    text = (
        f"<b>Общая статистика {title}</b>\n\n"
        f"Выполнено заявок: <b>{count}</b>\n"
        f"Выплачено всего: <b>{total:,} ₽</b>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Всё время", callback_data="gen_all"),
            InlineKeyboardButton(text="Неделя", callback_data="gen_week"),
            InlineKeyboardButton(text="Месяц", callback_data="gen_month")
        ],
        [InlineKeyboardButton(text="Назад", callback_data="stats_main_back2")]
    ])

    # ✔ безопасное редактирование
    await safe_edit(call.message, text, kb)

@dp.callback_query(F.data == "stats_drops")
async def stats_drops(call: CallbackQuery):
    await show_drops_stats(call, period="all")

@dp.callback_query(F.data.startswith("drop_"))
async def stats_drops_period(call: CallbackQuery):
    period = call.data.split("_")[1]
    await show_drops_stats(call, period=period)

async def show_drops_stats(call: CallbackQuery, period: str = "all"):
    now = datetime.now()

    if period == "week":
        start = now - timedelta(days=7)
        title = "за неделю"
    elif period == "month":
        start = now - timedelta(days=30)
        title = "за месяц"
    else:
        start = None
        title = "за всё время"

    async with aiosqlite.connect(DB_NAME) as db:
        if start:
            async with db.execute("""
                SELECT c.fio, COUNT(o.id), COALESCE(SUM(o.amount), 0)
                FROM orders o
                JOIN cards c ON o.card_id = c.id
                WHERE o.status = 'completed' AND o.created_at >= ?
                GROUP BY c.fio
                ORDER BY SUM(o.amount) DESC
                LIMIT 20
            """, (int(start.timestamp()),)) as cur:
                rows = await cur.fetchall()
        else:
            async with db.execute("""
                SELECT c.fio, COUNT(o.id), COALESCE(SUM(o.amount), 0)
                FROM orders o
                JOIN cards c ON o.card_id = c.id
                WHERE o.status = 'completed'
                GROUP BY c.fio
                ORDER BY SUM(o.amount) DESC
                LIMIT 20
            """) as cur:
                rows = await cur.fetchall()

    if not rows:
        text = f"Статистика по дропам {title}\nНет выполненных выплат."
    else:
        lines = [f"Статистика по дропам {title}\n"]
        for fio, cnt, amt in rows:
            lines.append(f"• <b>{fio}</b> — {cnt} выплат — <b>{amt:,} ₽</b>")
        text = "\n".join(lines)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Всё", callback_data="drop_all"),
            InlineKeyboardButton(text="Неделя", callback_data="drop_week"),
            InlineKeyboardButton(text="Месяц", callback_data="drop_month")
        ],
        [InlineKeyboardButton(text="Назад", callback_data="stats_main_back3")]
    ])

    # ✔ безопасное редактирование
    await safe_edit(call.message, text, kb)

@dp.callback_query(F.data == "stats_main_back3")
async def stats_back(call: CallbackQuery):
    await call.message.delete()

@dp.callback_query(F.data == "stats_main_back2")
async def stats_back2(call: CallbackQuery):
    await call.message.delete()

@dp.callback_query(F.data == "stats_main_back5")
async def stats_main_back5(call: CallbackQuery):
    await call.message.delete()

@dp.callback_query(F.data == "admin_menu_back")
async def admin_menu_back(call: CallbackQuery):
    await call.message.delete()


# ---------- Run ----------
async def main():
    await init_db()
    # start background workers
    asyncio.create_task(auto_cancel_worker())
    logger.info("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
