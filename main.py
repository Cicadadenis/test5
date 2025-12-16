# m4.py
import os
import asyncio
import aiosqlite
import logging
import random
import uuid
import time
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import StateFilter

# ---------- CONFIG ----------
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
    limit = State()

class CreateOrder(StatesGroup):
    amount = State()

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
    else:
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Создать заявку"), KeyboardButton(text="Активные заявки")],
                [KeyboardButton(text="Админ: Статистика")]
            ],
            resize_keyboard=True
        )

# ---------- DB init ----------
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            user_type INTEGER
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
            status TEXT DEFAULT 'pending'
        );

        CREATE TABLE IF NOT EXISTS payments (
            id TEXT PRIMARY KEY,
            order_id TEXT,
            card_id INTEGER,
            amount INTEGER,
            admin_id INTEGER,
            timestamp INTEGER
        );
        """)
        await db.commit()
    logger.info("DB initialized")

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
                                f"⌛ <b>Заявка просрочена</b>\nID: <code>{order_id[:8]}</code>\nСумма: <b>{amount:,}₽</b>\nСрок действия 30 минут истёк."
                            )
                        except Exception as e:
                            logger.warning(f"Can't notify drop {drop_id}: {e}")

                    # Обновляем сообщение админа, если есть
                    if admin_chat_id and admin_message_id:
                        try:
                            await bot.edit_message_text(chat_id=admin_chat_id, message_id=admin_message_id,
                                                        text=f"🖤 <b>Заявка {order_id[:8]}</b>\n\n<b>Статус:</b> Время истекло ⌛\nСумма: {amount:,}₽")
                        except Exception:
                            try:
                                await bot.send_message(admin_chat_id, f"⌛ Заявка {order_id[:8]} истекла. Сумма: {amount:,}₽")
                            except Exception:
                                logger.warning(f"Can't notify admin {admin_chat_id} about timeout")
        except Exception:
            logger.exception("Error in auto_cancel_worker")

# ---------- Commands: register /start ----------
@dp.message(F.text == "/start")
async def cmd_start(message: Message):
    user_type = await get_user_type(message.from_user.id)
    if user_type == 0:
        return await message.answer("🖤 Добро пожаловать. Вы не зарегистрированы.\nДля регистрации отправьте /888 (пользователь) или /777 (админ).")
    await message.answer("🖤 Главное меню:", reply_markup=menu_for(user_type))

@dp.message(F.text == "/888")
async def cmd_reg_user(message: Message):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO users(id, username, user_type) VALUES(?, ?, ?)",
                         (message.from_user.id, message.from_user.username or "", 1))
        await db.commit()
    logger.info(f"User registered as USER: {message.from_user.id}")
    await message.answer("✅ Вы зарегистрированы как Пользователь.", reply_markup=menu_for(1))

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

# ---------- Add card (user) ----------
@dp.message(F.text == "Добавить карту")
async def add_card_start(message: Message, state: FSMContext):
    if await get_user_type(message.from_user.id) != 1:
        return
    await state.set_state(AddCard.number)
    await message.answer("📥 Введи номер карты (16 цифр, можно с пробелами):", reply_markup=CANCEL_KB)

@dp.message(StateFilter(AddCard.number))
async def add_card_number(message: Message, state: FSMContext):
    num = ''.join(ch for ch in message.text if ch.isdigit())
    if len(num) != 16:
        return await message.answer("⚠ Нужны ровно 16 цифр.")
    await state.update_data(number=num)
    await state.set_state(AddCard.bank)
    await message.answer("🏦 Название банка:", reply_markup=CANCEL_KB)

@dp.message(StateFilter(AddCard.bank))
async def add_card_bank(message: Message, state: FSMContext):
    await state.update_data(bank=message.text.strip())
    await state.set_state(AddCard.fio)
    await message.answer("👤 ФИО владельца:", reply_markup=CANCEL_KB)

@dp.message(StateFilter(AddCard.fio))
async def add_card_fio(message: Message, state: FSMContext):
    await state.update_data(fio=message.text.strip())
    await state.set_state(AddCard.limit)
    await message.answer("💳 Месячный лимит (число):", reply_markup=CANCEL_KB)

@dp.message(StateFilter(AddCard.limit))
async def add_card_limit(message: Message, state: FSMContext):
    try:
        limit = int(message.text.replace(" ", ""))
        if limit <= 0:
            raise ValueError
    except:
        return await message.answer("⚠ Введи корректное число.")
    data = await state.get_data()
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO cards(user_id, number, bank, fio, daily_limit, last_reset) VALUES(?,?,?,?,?,?)",
                (message.from_user.id, data["number"], data["bank"], data["fio"], limit, today)
            )
            await db.commit()
    except aiosqlite.IntegrityError:
        return await message.answer("⚠ Эта карта уже добавлена.")
    await state.clear()
    logger.info(f"Card added user={message.from_user.id} number=****{data['number'][-4:]}")
    await message.answer(f"✅ Карта **** {data['number'][-4:]} добавлена. Лимит: {limit:,}₽", reply_markup=menu_for(1))

# ---------- My cards ----------
@dp.message(F.text == "Мои карты")
async def my_cards(message: Message):
    if await get_user_type(message.from_user.id) != 1:
        return
    # reset daily if needed (simple approach)
    today = datetime.now().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE cards SET daily_used = 0, last_reset = ? WHERE last_reset != ? OR last_reset IS NULL", (today, today))
        await db.commit()
        async with db.execute("SELECT id, number, bank, daily_used, daily_limit, active FROM cards WHERE user_id=?", (message.from_user.id,)) as cur:
            rows = await cur.fetchall()
    if not rows:
        return await message.answer("— У тебя нет карт —", reply_markup=menu_for(1))
    text = "🖤 <b>Твои карты</b> 🖤\n\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for cid, number, bank, used, limit, active in rows:
        last4 = number[-4:]
        status = "🟢 Активна" if active else "🔴 Пауза"
        avail = max(limit - used, 0)
        text += f"• <b>**** {last4}</b> — {bank} — {status}\n  Доступно: <b>{avail:,}₽</b> / {limit:,}₽\n\n"
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"**** {last4} • {bank}", callback_data=f"card_{cid}")])
    await message.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("card_"))
async def card_menu(call: CallbackQuery):
    cid = int(call.data.split("_")[1])
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT number, bank, fio, daily_used, daily_limit, active FROM cards WHERE id=?", (cid,)) as cur:
            row = await cur.fetchone()
    if not row:
        return await call.answer("Карта не найдена")
    number, bank, fio, used, limit, active = row
    last4 = number[-4:]
    avail = max(limit - used, 0)
    text = (f"🖤 <b>Карта **** {last4}</b>\n\n"
            f"🏦 {bank}\n"
            f"👤 {fio}\n"
            f"Доступно: <b>{avail:,}₽</b> / {limit:,}₽\n"
            f"Статус: {'🟢 Активна' if active else '🔴 Пауза'}")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("Пауза" if active else "Активировать"), callback_data=f"tog_{cid}")],
        [InlineKeyboardButton(text="Удалить", callback_data=f"del_{cid}")],
        [InlineKeyboardButton(text="Назад", callback_data="back")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

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
        text += f"• <b>**** {last4}</b> — {bank} — {status}\n  Доступно: <b>{avail:,}₽</b> / {limit:,}₽\n\n"
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
            f"Статус: {status}")
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
        short_id = order_id[:8]
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
            f"<b>{i+1}.</b> ID: <code>{short_id}</code>\n"
            f"    Сумма: <b>{amount:,}₽</b> | {status}\n"
            f"    Создано: {timestamp}\n"
            f"    Осталось времени: {remaining_str}\n"
        )
        kb_lines.append([ 
            InlineKeyboardButton( 
                text=f"💳 {short_id} — {amount:,}₽ [{status}]",
                callback_data=f"vieworder_{order_id}"
            )
        ])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_lines)
    await message.answer("\n".join(text_lines), reply_markup=kb)

# ------------------- Просмотр заявки -------------------
@dp.callback_query(F.data.startswith("vieworder_"))
async def view_order(call: CallbackQuery):
    order_id = call.data.split("_", 1)[1]

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(""" 
            SELECT o.amount, o.status, o.created_at, o.expires_at, 
                   c.number, c.bank, c.fio 
            FROM orders o 
            JOIN cards c ON o.card_id = c.id 
            WHERE o.id = ? 
        """, (order_id,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("Заявка не найдена", show_alert=True)

    amount, status, created_at, expires_at, number, bank, fio = row
    masked = f"{number[:4]} {number[4:8]} **** {number[-4:]}"
    tm = time.strftime("%d.%m.%Y %H:%M", time.localtime(created_at))

    # Инлайн-кнопки для действий с заявкой
    kb_owner = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Подтвердить оплату", callback_data=f"done_{order_id}")],
        [InlineKeyboardButton(text="⛔ Отменить", callback_data=f"cancel_{order_id}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_my_orders")]
    ])

    message = await call.message.edit_text(
        f"<b>Заявка</b>\n\n"
        f"ID: <code>{order_id[:8]}</code>\n"
        f"Карта: <code>{masked}</code>\n"
        f"Банк: {bank}\n"
        f"ФИО: {fio}\n"
        f"Сумма: <b>{amount:,}₽</b>\n"
        f"Создана: {tm}\n"
        f"Статус: {status}\n"
        f"Осталось времени: ",
        reply_markup=kb_owner
    )

    # Обновление оставшегося времени каждые 10 секунд (пока пользователь смотрит)
    while status == "active":
        now = int(time.time())
        remaining = max(0, expires_at - now)
        if remaining == 0:
            # Время истекло, помечаем заявку как expired
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute("UPDATE orders SET status='expired' WHERE id=?", (order_id,))
                await db.commit()
            status = "expired"
            remaining_str = "Время истекло"
        else:
            hours, rem = divmod(remaining, 3600)
            minutes, seconds = divmod(rem, 60)
            remaining_str = f"{hours}ч {minutes}м {seconds}с"

        try:
            await message.edit_text(
                f"<b>Заявка</b>\n\n"
                f"ID: <code>{order_id[:8]}</code>\n"
                f"Карта: <code>{masked}</code>\n"
                f"Банк: {bank}\n"
                f"ФИО: {fio}\n"
                f"Сумма: <b>{amount:,}₽</b>\n"
                f"Создана: {tm}\n"
                f"Статус: {status}\n"
                f"Осталось времени: {remaining_str}",
                reply_markup=kb_owner
            )
        except Exception:
            break  # сообщение удалено или недоступно

        if status != "active":
            break
        await asyncio.sleep(10)  # обновлять каждые 10 секунд

@dp.callback_query(F.data == "back_to_my_orders")
async def back_to_orders(call: CallbackQuery):
    await call.message.delete()

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
    await message.answer("💸 Введи сумму заявки (цифрами):", reply_markup=CANCEL_KB)

@dp.message(StateFilter(CreateOrder.amount))
async def create_order_amount(message: Message, state: FSMContext):
    try:
        amount = int(message.text.replace(" ", ""))
        if amount <= 0:
            raise ValueError
    except:
        return await message.answer("⚠ Введи корректное число.")
    # choose random card meeting daily_limit - daily_used >= amount and active
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT id, number, bank, fio, user_id, daily_limit, daily_used FROM cards WHERE active=1 AND (daily_limit - daily_used) >= ?",
            (amount,)
        ) as cur:
            cards = await cur.fetchall()
    if not cards:
        await state.clear()
        return await message.answer("🔴 Нет доступных карт с таким лимитом.")
    card = random.choice(cards)
    card_id, number, bank, fio, owner_id, dlimit, dused = card
    order_id = str(uuid.uuid4()).replace("-", "")[:16]
    created = int(time.time())
    expires = created + 1800  # 30 minutes

    # insert order with status = pending (not sent to drop yet)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO orders(id, admin_id, admin_chat_id, admin_message_id, drop_id, card_id, amount, created_at, expires_at, status) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (order_id, message.from_user.id, None, None, owner_id, card_id, amount, created, expires, "pending")
        )
        await db.commit()
    masked = mask_card(number)

    # Admin preview with SEND button
    text_admin = (
        f"🖤 <b>Заявка (предпросмотр)</b> 🖤\n\n"
        f"🆔: {order_id}\n"
        f"💳 Реквизит: {masked}\n"
        f"🏦 Банк: {bank}\n"
        f"💰 Сумма: {amount}₽\n"
        f"👤 Дроп (владелец карты): <code>{owner_id}</code>\n\n"
        f"Заявка действует 30 минут после отправки дропу.\n"
        f"<b>Статус:</b> Ожидает отправки (pending)"
    )
    kb_admin = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить дропу", callback_data=f"send_drop_{order_id}")],
        [InlineKeyboardButton(text="⛔ Отменить (не отправлять)", callback_data=f"cancel_pending_{order_id}")]
    ])
    admin_sent = await message.answer(text_admin, reply_markup=kb_admin)
    admin_chat_id = admin_sent.chat.id
    admin_message_id = admin_sent.message_id

    # update order row with admin message identifiers
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE orders SET admin_chat_id=?, admin_message_id=? WHERE id=?",
            (admin_chat_id, admin_message_id, order_id)
        )
        await db.commit()

    await message.answer("✅ Заявка создана в режиме ожидания. Нажмите «Отправить дропу», чтобы отправить её владельцу карты.", reply_markup=menu_for(2))
    await state.clear()
    logger.info(f"Order {order_id} created by admin {message.from_user.id} (pending) for drop {owner_id} amount {amount}")

# ---------- Send to drop (admin action) ----------
@dp.callback_query(F.data.startswith("send_drop_"))
async def send_drop_handler(call: CallbackQuery):
    order_id = call.data.split("_", 2)[2]

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT drop_id, card_id, amount, admin_chat_id, admin_message_id, status
            FROM orders WHERE id=?
        """, (order_id,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("Заявка не найдена", show_alert=True)

    drop_id, card_id, amount, admin_chat_id, admin_message_id, status = row

    if status != "pending":
        return await call.answer("Эта заявка уже отправлена или обработана.", show_alert=True)

    # Get card details to show to drop
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT number, bank, fio FROM cards WHERE id=?", (card_id,)) as cur:
            c = await cur.fetchone()
    if not c:
        return await call.answer("Карта не найдена", show_alert=True)
    number, bank, fio = c
    masked = mask_card(number)

    # send owner (drop) message with confirm/cancel buttons
    try:
        text_owner = (
            f"🔴 <b>НОВАЯ ЗАЯВКА</b> 🔴\n\n"
            f"ID: <code>{order_id}</code>\n"
            f"💳 Реквизит: {masked}\n"
            f"🏦 Банк: {bank}\n"
            f"👤 ФИО: {fio}\n"
            f"💰 Сумма: <b>{amount:,}₽</b>\n\n"
            f"Заявка действует 30 минут ⏱️"
        )

        kb_owner = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💸 Подтвердить оплату", callback_data=f"done_{order_id}")],
            [InlineKeyboardButton(text="📤 Запросить чек", callback_data=f"reqchk_{order_id}")],
            [InlineKeyboardButton(text="⛔ Отменить", callback_data=f"cancel_{order_id}")]
        ])

        owner_msg = await bot.send_message(owner_id, text_owner, reply_markup=kb_owner)
        owner_msg_id = owner_msg.message_id

    except Exception as e:
        owner_msg_id = None
        logger.warning(f"Could not send owner message for order {order_id}: {e}")


        logger.info(f"Order {order_id} sent to drop {drop_id}")
    except Exception as e:
        logger.error(f"Ошибка отправки дропу {drop_id} для order {order_id}: {e}")
        return await call.answer("Ошибка отправки дропу (возможно, дроп не запустил бота).", show_alert=True)

    # Update order: set status active and save drop message id if needed
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE orders SET status='active' WHERE id=?", (order_id,))
        await db.commit()

    # Update admin message text to reflect sent status
    try:
        await bot.edit_message_text(chat_id=admin_chat_id, message_id=admin_message_id,
                                    text=f"🖤 <b>Заявка отправлена дропу</b> 🖤\n\nID: <code>{order_id}</code>\n💰 Сумма: {amount:,}₽\n<b>Статус:</b> 🟢 Активна")
    except Exception:
        # если редактирование не удалось — просто уведомим админа
        try:
            await bot.send_message(call.from_user.id, f"✅ Заявка {order_id[:8]} отправлена дропу.")
        except Exception:
            logger.warning(f"Не удалось уведомить админа об отправке {order_id}")

    await call.answer("📤 Заявка отправлена дропу!")

# ---------- Cancel pending (admin) ----------
@dp.callback_query(F.data.startswith("cancel_pending_"))
async def cancel_pending(call: CallbackQuery):
    order_id = call.data.split("_", 2)[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE orders SET status='canceled' WHERE id=?", (order_id,))
        await db.commit()
    try:
        await call.message.edit_text(f"❌ Заявка <code>{order_id}</code> отменена админом.")
    except Exception:
        pass
    await call.answer("Отменено.")

# ---------- Owner button handlers ----------
@dp.callback_query(F.data.startswith(("done_", "cancel_")))
async def owner_action(call: CallbackQuery):
    action, order_id = call.data.split("_", 1)

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT admin_id, admin_chat_id, admin_message_id,
                   drop_id, card_id, amount, status
            FROM orders WHERE id=?
        """, (order_id,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("Заявка не найдена.", show_alert=True)

    admin_id, admin_chat_id, admin_message_id, drop_id, card_id, amount, status = row

    # Владелец заявки
    if call.from_user.id != drop_id:
        return await call.answer("Ты не владелец этой карты.", show_alert=True)

    # Защита: нельзя подтвердить/отменить НЕ active заявку
    if status != "active":
        return await call.answer("Эта заявка уже обработана или не была отправлена.", show_alert=True)

    # --- Подтверждение ---
    if action == "done":
        ts = int(time.time())
        try:
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute(
                    "UPDATE cards SET daily_used = daily_used + ? WHERE id=?",
                    (amount, card_id),
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
            return await call.answer("Ошибка при обработке.", show_alert=True)

        status_text = "✅ Выполнена"

    # --- Отмена ---
    else:
        try:
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute(
                    "UPDATE orders SET status='canceled' WHERE id=?",
                    (order_id,)
                )
                await db.commit()
        except Exception:
            logger.exception("Error during canceling order")
            return await call.answer("Ошибка при отмене.", show_alert=True)

        status_text = "❌ Отменена"
        logger.info(f"Order {order_id} cancelled by drop {drop_id}")

    # Edit owner's message (call.message) — remove buttons and update status
    try:
        new_text = call.message.text + f"\n\n<b>Статус:</b> {status_text}"
        await call.message.edit_text(new_text)
    except Exception:
        # maybe message was removed or not editable
        pass

    # Edit admin message if exists
    if admin_chat_id and admin_message_id:
        try:
            admin_update = (
                f"🖤 <b>Заявка {order_id[:8]}</b> 🖤\n\n"
                f"<b>Сумма:</b> <b>{amount:,}₽</b>\n"
                f"<b>Статус:</b> {status_text}\n"
                f"<b>Дроп:</b> <code>{drop_id}</code>"
            )
            await bot.edit_message_text(chat_id=admin_chat_id, message_id=admin_message_id, text=admin_update)
        except Exception:
            # if edit fails, try sending plain notify
            try:
                await bot.send_message(admin_id, f"Обновление: заявка {order_id[:8]} — {status_text} — {amount:,}₽")
            except Exception:
                logger.warning(f"Can't notify admin {admin_id} about order {order_id}")

    # Notify admin directly
    try:
        await bot.send_message(admin_id, f"🔔 Заявка <code>{order_id[:8]}</code> — {status_text}\nСумма: {amount:,}₽")
    except Exception:
        logger.warning(f"Failed sending admin notification for {order_id}")

    await call.answer("Статус изменён", show_alert=False)

# ---------- Admin: Active orders ----------
@dp.message(F.text == "Активные заявки")
async def active_orders_admin(message: Message):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT id, amount, drop_id, created_at, expires_at
            FROM orders
            WHERE status = 'active'
            ORDER BY created_at DESC
        """) as cur:
            rows = await cur.fetchall()

    if not rows:
        return await message.answer("Нет активных заявок.")

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    text_lines = ["<b>Активные заявки:</b>\n"]

    for order_id, amount, drop_id, created_at, expires_at in rows:
        created_dt = datetime.fromtimestamp(created_at)
        expire_dt = datetime.fromtimestamp(expires_at)
        remaining = expire_dt - datetime.now()
        if remaining.total_seconds() < 0:
            remaining_text = "⏰ Время истекло"
        else:
            minutes, seconds = divmod(int(remaining.total_seconds()), 60)
            remaining_text = f"{minutes} мин"

        short_id = order_id[:8]
        created_str = created_dt.strftime("%d.%m %H:%M")
        text_lines.append(f"• {short_id} — {amount:,}₽ — дроп {drop_id} — {created_str} — {remaining_text}")

        # каждая заявка — кнопка для просмотра/действия
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{short_id} — {amount:,}₽ — {remaining_text}",
                callback_data=f"vieworder_{order_id}"
            )
        ])

    await message.answer("\n".join(text_lines), reply_markup=kb)

# Пример просмотра заявки через админскую кнопку
@dp.callback_query(F.data.startswith("vieworder_"))
async def view_order_admin(call: CallbackQuery):
    order_id = call.data.split("_", 1)[1]

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT o.amount, o.status, o.created_at,
                   o.drop_id, c.number, c.bank, c.fio
            FROM orders o
            JOIN cards c ON o.card_id = c.id
            WHERE o.id = ?
        """, (order_id,)) as cur:
            row = await cur.fetchone()

    if not row:
        return await call.answer("Заявка не найдена", show_alert=True)

    amount, status, created_at, drop_id, number, bank, fio = row
    masked = f"{number[:4]} {number[4:8]} **** {number[-4:]}"
    tm = datetime.fromtimestamp(created_at).strftime("%d.%m.%Y %H:%M")

    text = (
        f"<b>Заявка</b>\n\n"
        f"ID: <code>{order_id[:8]}</code>\n"
        f"Карта: <code>{masked}</code>\n"
        f"Банк: {bank}\n"
        f"ФИО: {fio}\n"
        f"Сумма: <b>{amount:,}₽</b>\n"
        f"Дроп: {drop_id}\n"
        f"Создана: {tm}\n"
        f"Статус: {status}"
    )

    kb_admin = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_active_orders")]
    ])

    await call.message.edit_text(text, reply_markup=kb_admin)

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
            rows = await db.execute_fetchall("""
                SELECT c.fio, COUNT(o.id), COALESCE(SUM(o.amount), 0)
                FROM orders o
                JOIN cards c ON o.card_id = c.id
                WHERE o.status = 'completed' AND o.created_at >= ?
                GROUP BY c.fio
                ORDER BY SUM(o.amount) DESC
                LIMIT 20
            """, (int(start.timestamp()),))
        else:
            rows = await db.execute_fetchall("""
                SELECT c.fio, COUNT(o.id), COALESCE(SUM(o.amount), 0)
                FROM orders o
                JOIN cards c ON o.card_id = c.id
                WHERE o.status = 'completed'
                GROUP BY c.fio
                ORDER BY SUM(o.amount) DESC
                LIMIT 20
            """)

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

@dp.callback_query(F.data == "admin_menu_back")
async def admin_menu_back(call: CallbackQuery):
    await call.message.delete()

async def monthly_reset_worker():
    while True:
        try:
            await asyncio.sleep(3600)  # проверка каждый час
            today = datetime.now()
            month_str = today.strftime("%Y-%m")
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute(
                    "UPDATE cards SET monthly_used = 0, month_reset = ? WHERE month_reset != ? OR month_reset IS NULL",
                    (month_str, month_str)
                )
                await db.commit()
        except Exception:
            logger.exception("Error in monthly_reset_worker")

# ---------- Run ----------
async def main():
    await init_db()
    # start background workers
    asyncio.create_task(auto_cancel_worker())
    asyncio.create_task(monthly_reset_worker())
    logger.info("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
