"""
Бизнес-бот для гаранта сделок (aiogram 3 + Telegram Business).

Что умеет:
  • перехват удалённых и изменённых сообщений клиентов (с медиа), хранится в SQLite;
  • команды прямо в бизнес-чате (префикс / или .) — сообщение владельца заменяется ответом;
  • учёт сделок: создание, оплата, завершение, спор, отмена, комиссия, заметки, экспорт;
  • шаблоны ответов (.имя), настройки (комиссия, реквизиты, правила);
  • доступ: полный — только владелец, остальные — только если он разрешил (/allow).
"""
import asyncio
import csv
import html
import io
import logging
import math
import os
import re
import time
from datetime import datetime
from typing import Awaitable, Callable

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.filters import BaseFilter, Command, CommandObject
from aiogram.methods import TelegramMethod
from aiogram.types import (
    BufferedInputFile,
    BusinessConnection,
    BusinessMessagesDeleted,
    Message,
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# ======================= НАСТРОЙКИ =======================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
DB_PATH = os.getenv("DB_PATH", "garant.db")
MSG_TTL = 48 * 3600  # сколько хранить сообщения для перехвата удалённых

if not BOT_TOKEN or not OWNER_ID:
    raise SystemExit("Укажите переменные окружения BOT_TOKEN и OWNER_ID")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("garant")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

DEFAULTS = {
    "fee_percent": "5",
    "min_fee": "0",
    "currency": "RUB",
    "requisites": "Реквизиты не заданы. Задайте: /set requisites текст",
    "rules": (
        "📜 <b>Правила гаранта</b>\n"
        "1. Деньги принимаются только на реквизиты гаранта.\n"
        "2. Передача товара/услуги — только после подтверждения оплаты.\n"
        "3. Средства выдаются после подтверждения обеими сторонами.\n"
        "4. Спорные ситуации разбираются по переписке и доказательствам."
    ),
    "greeting": "Здравствуйте! Я гарант сделок. Опишите условия — оформим сделку.",
    "notify_delete": "1",
    "notify_edit": "1",
}

STATUS = {
    "new": "🆕 Ожидает оплаты",
    "paid": "💰 Оплачено, средства удержаны",
    "done": "✅ Завершена",
    "cancelled": "❌ Отменена",
    "dispute": "⚠️ Спор",
}
ACTIVE = ("new", "paid", "dispute")
TRANSITIONS = {
    "paid": {"new", "dispute"},
    "done": {"paid", "dispute"},
    "cancelled": {"new", "paid", "dispute"},
    "dispute": {"new", "paid"},
}

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS allowed(user_id INTEGER PRIMARY KEY, added_at INTEGER);
CREATE TABLE IF NOT EXISTS templates(name TEXT PRIMARY KEY, text TEXT);
CREATE TABLE IF NOT EXISTS messages(
    chat_id INTEGER, msg_id INTEGER, sender_id INTEGER, sender_name TEXT,
    text TEXT, kind TEXT, file_id TEXT, ts INTEGER,
    PRIMARY KEY(chat_id, msg_id)
);
CREATE TABLE IF NOT EXISTS deals(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER, client_name TEXT, amount REAL, fee REAL, currency TEXT,
    description TEXT, status TEXT, note TEXT, created_at INTEGER, updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_deals_chat ON deals(chat_id);
"""


# ======================= БАЗА ДАННЫХ =======================
class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn: aiosqlite.Connection | None = None

    async def init(self):
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()

    async def one(self, q, args=()):
        async with self.conn.execute(q, args) as cur:
            return await cur.fetchone()

    async def all(self, q, args=()):
        async with self.conn.execute(q, args) as cur:
            return await cur.fetchall()

    async def run(self, q, args=()):
        cur = await self.conn.execute(q, args)
        await self.conn.commit()
        return cur


db = DB(DB_PATH)
allowed_ids: set[int] = set()
_conn_owner: dict[str, int] = {}


# ======================= УТИЛИТЫ =======================
class CmdError(Exception):
    """Ошибка команды — уходит владельцу в ЛС, а не клиенту."""


class DeleteBusinessMessages(TelegramMethod[bool]):
    __returning__ = bool
    __api_method__ = "deleteBusinessMessages"
    business_connection_id: str
    message_ids: list[int]


def esc(s) -> str:
    return html.escape(str(s if s is not None else ""), quote=False)


def money(x: float) -> str:
    s = f"{x:,.2f}".replace(",", " ")
    return s[:-3] if s.endswith(".00") else s


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")


def parse_amount(s: str) -> float:
    if not s:
        raise CmdError("Укажите сумму, например: 5000")
    try:
        v = float(s.replace(",", ".").replace("_", ""))
    except ValueError:
        raise CmdError(f"Некорректная сумма: {esc(s)}")
    if not math.isfinite(v) or v <= 0 or v > 1e12:
        raise CmdError("Сумма должна быть положительным числом")
    return round(v, 2)


async def get_setting(key: str) -> str:
    row = await db.one("SELECT value FROM settings WHERE key=?", (key,))
    return row["value"] if row else DEFAULTS[key]


async def calc_fee(amount: float) -> float:
    pct = float(await get_setting("fee_percent"))
    mn = float(await get_setting("min_fee"))
    return round(max(amount * pct / 100, mn), 2)


async def dm(text: str):
    """Сообщение владельцу в ЛС бота (владелец должен нажать /start у бота)."""
    try:
        await bot.send_message(OWNER_ID, text[:4000])
    except TelegramAPIError as e:
        log.warning("Не удалось написать владельцу: %s", e)


async def is_my_connection(conn_id: str | None) -> bool:
    """Обслуживаем только бизнес-подключения владельца."""
    if not conn_id:
        return False
    if conn_id not in _conn_owner:
        try:
            c = await bot.get_business_connection(conn_id)
            _conn_owner[conn_id] = c.user.id
        except TelegramAPIError:
            return False
    return _conn_owner[conn_id] == OWNER_ID


def extract_media(m: Message):
    if m.photo:
        return "photo", m.photo[-1].file_id
    for kind in ("video", "document", "audio", "voice", "video_note", "animation", "sticker"):
        obj = getattr(m, kind, None)
        if obj:
            return kind, obj.file_id
    return None, None


SEND_METHOD = {
    "photo": "send_photo", "video": "send_video", "document": "send_document",
    "audio": "send_audio", "voice": "send_voice", "video_note": "send_video_note",
    "animation": "send_animation", "sticker": "send_sticker",
}
KIND_RU = {
    "photo": "фото", "video": "видео", "document": "файл", "audio": "аудио",
    "voice": "голосовое", "video_note": "кружок", "animation": "гифка", "sticker": "стикер",
}


async def store_message(m: Message):
    kind, fid = extract_media(m)
    u = m.from_user
    name = u.full_name + (f" (@{u.username})" if u.username else "") if u else None
    await db.run(
        "INSERT OR REPLACE INTO messages VALUES (?,?,?,?,?,?,?,?)",
        (m.chat.id, m.message_id, u.id if u else None, name,
         m.text or m.caption, kind, fid, int(m.date.timestamp())),
    )


async def drop_message(m: Message) -> bool:
    """Удалить сообщение владельца в бизнес-чате (нужно право на удаление у бота)."""
    try:
        await bot(DeleteBusinessMessages(
            business_connection_id=m.business_connection_id, message_ids=[m.message_id]))
        return True
    except TelegramAPIError as e:
        log.warning("Не удалось удалить сообщение: %s", e)
        return False


async def replace_message(m: Message, text: str):
    """Заменить сообщение владельца текстом ответа (редактированием)."""
    kw = dict(chat_id=m.chat.id, message_id=m.message_id,
              business_connection_id=m.business_connection_id)
    try:
        await bot.edit_message_text(text=text, **kw)
        return
    except TelegramBadRequest as e:
        if "parse" in str(e).lower():  # кривой HTML в шаблоне — шлём как обычный текст
            try:
                await bot.edit_message_text(text=text, parse_mode=None, **kw)
                return
            except TelegramAPIError:
                pass
        log.warning("edit не удался (%s), отправляю новым сообщением", e)
    except TelegramAPIError as e:
        log.warning("edit не удался (%s), отправляю новым сообщением", e)
    try:
        await bot.send_message(m.chat.id, text, business_connection_id=m.business_connection_id)
        await drop_message(m)
    except TelegramAPIError as e:
        await dm(f"⚠️ Не удалось отправить ответ в чат: {esc(e)}")


def chat_label(m: Message) -> str:
    name = m.chat.full_name or m.chat.title or str(m.chat.id)
    un = f" (@{m.chat.username})" if m.chat.username else ""
    return f"💬 {esc(name)}{esc(un)}"


# ======================= СДЕЛКИ =======================
async def get_deal(deal_id: int):
    return await db.one("SELECT * FROM deals WHERE id=?", (deal_id,))


async def active_deal(chat_id: int):
    return await db.one(
        f"SELECT * FROM deals WHERE chat_id=? AND status IN ({','.join('?' * len(ACTIVE))}) "
        "ORDER BY id DESC LIMIT 1", (chat_id, *ACTIVE))


async def last_deal(chat_id: int):
    return await db.one("SELECT * FROM deals WHERE chat_id=? ORDER BY id DESC LIMIT 1", (chat_id,))


async def set_status(deal_id: int, status: str):
    await db.run("UPDATE deals SET status=?, updated_at=? WHERE id=?",
                 (status, int(time.time()), deal_id))


async def add_note(deal_id: int, text: str):
    d = await get_deal(deal_id)
    line = f"[{fmt_ts(int(time.time()))}] {text}"
    note = f"{d['note']}\n{line}" if d["note"] else line
    await db.run("UPDATE deals SET note=?, updated_at=? WHERE id=?",
                 (note, int(time.time()), deal_id))


def deal_card(d, private: bool = False) -> str:
    cur = esc(d["currency"])
    lines = [
        f"🤝 <b>Сделка #{d['id']}</b>",
        f"👤 Клиент: {esc(d['client_name'])}",
        f"💵 Сумма: <b>{money(d['amount'])} {cur}</b>",
        f"💼 Комиссия гаранта: {money(d['fee'])} {cur}",
        f"📝 Условия: {esc(d['description'])}",
        f"📊 Статус: {STATUS[d['status']]}",
    ]
    if private:
        lines.append(f"🕒 Создана: {fmt_ts(d['created_at'])}")
        if d["note"]:
            lines.append(f"🔒 Заметки:\n{esc(d['note'])}")
    return "\n".join(lines)


def deal_line(d) -> str:
    emoji = STATUS[d["status"]].split()[0]
    return (f"{emoji} <b>#{d['id']}</b> · {money(d['amount'])} {esc(d['currency'])} · "
            f"{esc(d['client_name'])}")


# ======================= КОМАНДЫ В БИЗНЕС-ЧАТЕ =======================
CmdFunc = Callable[[Message, str], Awaitable[str]]
PUBLIC: dict[str, CmdFunc] = {}   # ответ виден клиенту (сообщение заменяется)
PRIVATE: dict[str, CmdFunc] = {}  # команда удаляется, ответ приходит владельцу в ЛС


def reg(table: dict, *names: str):
    def deco(fn):
        for n in names:
            table[n] = fn
        return fn
    return deco


BUSINESS_HELP = (
    "<b>Команды в бизнес-чате</b> (префикс <code>/</code> или <code>.</code>)\n\n"
    "<b>Видны клиенту</b> (ваше сообщение заменяется ответом):\n"
    ".hi — приветствие\n"
    ".rules — правила гаранта\n"
    ".req — реквизиты\n"
    ".fee 5000 — расчёт комиссии\n"
    ".new 5000 описание — создать сделку\n"
    ".deal — карточка текущей сделки\n"
    ".paid — оплата получена\n"
    ".done — сделка завершена\n"
    ".dispute — открыть спор\n"
    ".cancel — отменить сделку\n"
    ".имя — ваш шаблон (/tadd в ЛС бота)\n\n"
    "<b>Приватные</b> (команда удаляется, ответ — вам в ЛС):\n"
    ".info — сделка и история клиента\n"
    ".note текст — заметка к сделке\n"
    ".help — эта справка"
)


@reg(PUBLIC, "hi", "привет")
async def c_hi(m, a):
    return await get_setting("greeting")


@reg(PUBLIC, "rules", "правила")
async def c_rules(m, a):
    return await get_setting("rules")


@reg(PUBLIC, "req", "реквизиты")
async def c_req(m, a):
    return "💳 <b>Реквизиты для оплаты</b>\n" + await get_setting("requisites")


@reg(PUBLIC, "fee", "комиссия")
async def c_fee(m, a):
    amount = parse_amount(a.split()[0] if a else "")
    fee = await calc_fee(amount)
    cur = esc(await get_setting("currency"))
    return (f"💼 Комиссия гаранта с суммы {money(amount)} {cur}: <b>{money(fee)} {cur}</b>\n"
            f"Итого к оплате: <b>{money(amount + fee)} {cur}</b>")


@reg(PUBLIC, "new", "сделка")
async def c_new(m, a):
    if await active_deal(m.chat.id):
        raise CmdError("В этом чате уже есть активная сделка. Закройте её: .done или .cancel")
    parts = a.split(maxsplit=1)
    amount = parse_amount(parts[0] if parts else "")
    desc = parts[1].strip() if len(parts) > 1 else "—"
    fee = await calc_fee(amount)
    now = int(time.time())
    cur = await db.run(
        "INSERT INTO deals(chat_id, client_name, amount, fee, currency, description, status, "
        "note, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (m.chat.id, m.chat.full_name or str(m.chat.id), amount, fee,
         await get_setting("currency"), desc, "new", "", now, now))
    d = await get_deal(cur.lastrowid)
    return deal_card(d) + "\n\nОжидаем оплату. Реквизиты: .req"


@reg(PUBLIC, "deal", "статус")
async def c_deal(m, a):
    d = await active_deal(m.chat.id) or await last_deal(m.chat.id)
    if not d:
        raise CmdError("В этом чате ещё нет сделок.")
    return deal_card(d)


async def change_status(m: Message, new: str, headline: str) -> str:
    d = await active_deal(m.chat.id)
    if not d:
        raise CmdError("Нет активной сделки в этом чате.")
    if d["status"] not in TRANSITIONS[new]:
        raise CmdError(f"Нельзя перейти из «{STATUS[d['status']]}» в «{STATUS[new]}».")
    await set_status(d["id"], new)
    return f"{headline}\n\n" + deal_card(await get_deal(d["id"]))


@reg(PUBLIC, "paid", "оплачено")
async def c_paid(m, a):
    return await change_status(
        m, "paid", "✅ <b>Оплата получена.</b> Средства удерживаются гарантом до выполнения условий.")


@reg(PUBLIC, "done", "готово")
async def c_done(m, a):
    return await change_status(
        m, "done", "🎉 <b>Сделка завершена.</b> Обязательства выполнены, средства переданы.")


@reg(PUBLIC, "dispute", "спор")
async def c_dispute(m, a):
    return await change_status(
        m, "dispute", "⚠️ <b>Открыт спор.</b> Средства заморожены до разбирательства.")


@reg(PUBLIC, "cancel", "отмена")
async def c_cancel(m, a):
    return await change_status(m, "cancelled", "❌ <b>Сделка отменена.</b>")


@reg(PRIVATE, "help", "помощь")
async def c_help(m, a):
    return BUSINESS_HELP


@reg(PRIVATE, "info", "инфо")
async def c_info(m, a):
    d = await active_deal(m.chat.id) or await last_deal(m.chat.id)
    st = await db.one(
        "SELECT COUNT(*) c, COALESCE(SUM(amount),0) s FROM deals WHERE chat_id=? AND status='done'",
        (m.chat.id,))
    total = await db.one("SELECT COUNT(*) c FROM deals WHERE chat_id=?", (m.chat.id,))
    head = deal_card(d, private=True) if d else "Сделок с этим клиентом пока нет."
    return (f"{head}\n\n📈 История клиента: всего сделок {total['c']}, "
            f"завершено {st['c']} на {money(st['s'])}")


@reg(PRIVATE, "note", "заметка")
async def c_note(m, a):
    if not a:
        raise CmdError("Укажите текст: .note текст")
    d = await active_deal(m.chat.id) or await last_deal(m.chat.id)
    if not d:
        raise CmdError("Нет сделки, к которой можно добавить заметку.")
    await add_note(d["id"], a)
    return f"📝 Заметка добавлена к сделке #{d['id']}"


CMD_RE = re.compile(r"^[/.](\w+)(?:@\w+)?(?:\s+(.*))?$", re.S)


async def handle_owner_command(m: Message):
    mt = CMD_RE.match(m.text.strip())
    if not mt:
        return
    name, args = mt.group(1).lower(), (mt.group(2) or "").strip()

    mode, fn = None, None
    if name in PUBLIC:
        mode, fn = "public", PUBLIC[name]
    elif name in PRIVATE:
        mode, fn = "private", PRIVATE[name]
    else:
        row = await db.one("SELECT text FROM templates WHERE name=?", (name,))
        if not row:
            return  # не наша команда — просто обычное сообщение
        tpl = row["text"]

        async def fn(_m, _a, _t=tpl):
            return _t
        mode = "public"

    try:
        result = await fn(m, args)
    except CmdError as e:
        await drop_message(m)
        await dm(f"{chat_label(m)}\n⚠️ {e}")
        return
    except Exception:
        log.exception("Ошибка команды %s", name)
        await drop_message(m)
        await dm(f"{chat_label(m)}\n⚠️ Внутренняя ошибка команды .{esc(name)}")
        return

    if mode == "public":
        await replace_message(m, result)
    else:
        await drop_message(m)
        await dm(f"{chat_label(m)}\n{result}")


# ======================= БИЗНЕС-СОБЫТИЯ =======================
biz = Router(name="business")


@biz.business_connection()
async def on_connection(c: BusinessConnection):
    _conn_owner[c.id] = c.user.id
    if c.user.id == OWNER_ID:
        state = "подключён ✅" if c.is_enabled else "отключён ❌"
        await dm(f"Бизнес-аккаунт {state}")
    else:
        await dm(f"⛔ Чужой аккаунт {esc(c.user.full_name)} (<code>{c.user.id}</code>) "
                 "подключил бота. Он игнорируется.")


@biz.business_message()
async def on_business_message(m: Message):
    if not await is_my_connection(m.business_connection_id):
        return
    await store_message(m)
    from_owner = bool(m.from_user and m.from_user.id == OWNER_ID)
    if from_owner and m.text and m.text[:1] in "/.":
        await handle_owner_command(m)


@biz.edited_business_message()
async def on_business_edit(m: Message):
    if not await is_my_connection(m.business_connection_id):
        return
    old = await db.one("SELECT * FROM messages WHERE chat_id=? AND msg_id=?",
                       (m.chat.id, m.message_id))
    await store_message(m)
    if not old or (m.from_user and m.from_user.id == OWNER_ID):
        return
    new_text = m.text or m.caption
    if old["text"] == new_text or await get_setting("notify_edit") != "1":
        return
    await dm(
        f"✏️ <b>Сообщение изменено</b>\n👤 {esc(old['sender_name'])}\n{chat_label(m)}\n\n"
        f"<b>Было:</b> {esc(old['text'] or '—')}\n<b>Стало:</b> {esc(new_text or '—')}")


@biz.deleted_business_messages()
async def on_business_deleted(ev: BusinessMessagesDeleted):
    if not await is_my_connection(ev.business_connection_id):
        return
    notify = await get_setting("notify_delete") == "1"
    unknown = 0
    for mid in ev.message_ids:
        row = await db.one("SELECT * FROM messages WHERE chat_id=? AND msg_id=?", (ev.chat.id, mid))
        if not row:
            unknown += 1
            continue
        await db.run("DELETE FROM messages WHERE chat_id=? AND msg_id=?", (ev.chat.id, mid))
        if not notify or row["sender_id"] == OWNER_ID:
            continue
        title = ev.chat.full_name or ev.chat.title or str(ev.chat.id)
        report = (f"🗑 <b>Удалено сообщение</b>\n👤 {esc(row['sender_name'])}\n"
                  f"💬 Чат: {esc(title)}\n🕒 Отправлено: {fmt_ts(row['ts'])}\n")
        if row["text"]:
            report += f"\n📝 {esc(row['text'])}"
        if row["kind"]:
            report += f"\n📎 Вложение: {KIND_RU.get(row['kind'], row['kind'])}"
        await dm(report)
        if row["kind"] and row["file_id"]:
            try:
                await getattr(bot, SEND_METHOD[row["kind"]])(OWNER_ID, row["file_id"])
            except TelegramAPIError as e:
                log.warning("Не удалось переслать вложение: %s", e)
    if notify and unknown:
        await dm(f"ℹ️ Удалено сообщений, которых нет в кэше: {unknown}")


# ======================= ЛС С БОТОМ: ДОСТУП =======================
class IsOwner(BaseFilter):
    async def __call__(self, m: Message) -> bool:
        return bool(m.from_user and m.from_user.id == OWNER_ID)


class IsAllowed(BaseFilter):
    async def __call__(self, m: Message) -> bool:
        return bool(m.from_user and (m.from_user.id == OWNER_ID or m.from_user.id in allowed_ids))


priv = Router(name="private")
priv.message.filter(F.chat.type == "private")
owner_r = Router(name="owner")
owner_r.message.filter(IsOwner())
user_r = Router(name="user")
user_r.message.filter(IsAllowed())
fallback_r = Router(name="fallback")

OWNER_HELP = (
    "<b>🛠 Команды владельца</b>\n\n"
    "<b>Доступ</b>\n"
    "/allow ID — дать доступ\n/deny ID — забрать доступ\n/users — список допущенных\n\n"
    "<b>Сделки</b>\n"
    "/deals [статус] — список (new, paid, done, cancelled, dispute)\n"
    "/deal ID — карточка\n/find текст — поиск\n"
    "/setstatus ID статус — сменить статус\n/dnote ID текст — заметка\n"
    "/stats — статистика\n/export — выгрузка CSV\n/fee сумма — расчёт комиссии\n\n"
    "<b>Настройки и шаблоны</b>\n"
    "/settings — все настройки\n/set ключ значение\n"
    "/tadd имя текст — шаблон (в чате: .имя)\n/tlist — шаблоны\n/tdel имя\n\n"
    "/bhelp — команды для бизнес-чата\n/id — ваш ID"
)
USER_HELP = (
    "<b>Доступные вам команды</b>\n"
    "/deals [статус] — список сделок\n/deal ID — карточка\n"
    "/find текст — поиск\n/fee сумма — расчёт комиссии\n/id — ваш ID"
)


# ---------- команды, доступные допущенным (и владельцу) ----------
@user_r.message(Command("start", "help"))
async def u_help(m: Message):
    await m.answer(OWNER_HELP if m.from_user.id == OWNER_ID else USER_HELP)


@user_r.message(Command("id"))
async def u_id(m: Message):
    await m.answer(f"Ваш ID: <code>{m.from_user.id}</code>")


@user_r.message(Command("fee"))
async def u_fee(m: Message, command: CommandObject):
    try:
        amount = parse_amount((command.args or "").split()[0] if command.args else "")
    except CmdError as e:
        return await m.answer(f"⚠️ {e}")
    fee = await calc_fee(amount)
    cur = esc(await get_setting("currency"))
    await m.answer(f"💼 Комиссия: <b>{money(fee)} {cur}</b>\n"
                   f"Итого: <b>{money(amount + fee)} {cur}</b>")


@user_r.message(Command("deals"))
async def u_deals(m: Message, command: CommandObject):
    st = (command.args or "").strip().lower()
    if st and st not in STATUS:
        return await m.answer("Статусы: " + ", ".join(STATUS))
    if st:
        rows = await db.all("SELECT * FROM deals WHERE status=? ORDER BY id DESC LIMIT 20", (st,))
    else:
        rows = await db.all("SELECT * FROM deals ORDER BY id DESC LIMIT 20")
    await m.answer("\n".join(deal_line(d) for d in rows) if rows else "Сделок нет.")


@user_r.message(Command("deal"))
async def u_deal(m: Message, command: CommandObject):
    if not (command.args or "").strip().isdigit():
        return await m.answer("Использование: /deal ID")
    d = await get_deal(int(command.args))
    if not d:
        return await m.answer("Сделка не найдена.")
    await m.answer(deal_card(d, private=m.from_user.id == OWNER_ID))


@user_r.message(Command("find"))
async def u_find(m: Message, command: CommandObject):
    q = (command.args or "").strip()
    if len(q) < 2:
        return await m.answer("Использование: /find текст (минимум 2 символа)")
    like = f"%{q}%"
    rows = await db.all(
        "SELECT * FROM deals WHERE client_name LIKE ? OR description LIKE ? "
        "ORDER BY id DESC LIMIT 20", (like, like))
    await m.answer("\n".join(deal_line(d) for d in rows) if rows else "Ничего не найдено.")


# ---------- команды только владельца ----------
@owner_r.message(Command("bhelp"))
async def o_bhelp(m: Message):
    await m.answer(BUSINESS_HELP)


@owner_r.message(Command("allow"))
async def o_allow(m: Message, command: CommandObject):
    arg = (command.args or "").strip()
    if not arg.lstrip("-").isdigit():
        return await m.answer("Использование: /allow ID (пользователь может узнать ID командой /id)")
    uid = int(arg)
    if uid == OWNER_ID:
        return await m.answer("Вы и так владелец 🙂")
    await db.run("INSERT OR REPLACE INTO allowed VALUES (?,?)", (uid, int(time.time())))
    allowed_ids.add(uid)
    await m.answer(f"✅ Доступ выдан: <code>{uid}</code>")


@owner_r.message(Command("deny"))
async def o_deny(m: Message, command: CommandObject):
    arg = (command.args or "").strip()
    if not arg.lstrip("-").isdigit():
        return await m.answer("Использование: /deny ID")
    uid = int(arg)
    await db.run("DELETE FROM allowed WHERE user_id=?", (uid,))
    allowed_ids.discard(uid)
    await m.answer(f"🚫 Доступ отозван: <code>{uid}</code>")


@owner_r.message(Command("users"))
async def o_users(m: Message):
    rows = await db.all("SELECT * FROM allowed ORDER BY added_at")
    if not rows:
        return await m.answer("Допущенных пользователей нет.")
    await m.answer("<b>Допущены:</b>\n" + "\n".join(
        f'<a href="tg://user?id={r["user_id"]}">{r["user_id"]}</a> · с {fmt_ts(r["added_at"])}'
        for r in rows))


@owner_r.message(Command("settings"))
async def o_settings(m: Message):
    lines = []
    for k in DEFAULTS:
        v = await get_setting(k)
        v = v if len(v) <= 80 else v[:80] + "…"
        lines.append(f"<code>{k}</code> = {esc(v)}")
    await m.answer("<b>Настройки</b>\n" + "\n".join(lines) + "\n\nИзменить: /set ключ значение")


@owner_r.message(Command("set"))
async def o_set(m: Message, command: CommandObject):
    parts = (command.args or "").split(maxsplit=1)
    if len(parts) < 2 or parts[0] not in DEFAULTS:
        return await m.answer("Использование: /set ключ значение\nКлючи: " + ", ".join(DEFAULTS))
    key, val = parts[0], parts[1].strip()
    try:
        if key == "fee_percent":
            if not 0 <= float(val.replace(",", ".")) <= 100:
                raise ValueError
            val = val.replace(",", ".")
        elif key == "min_fee":
            if float(val.replace(",", ".")) < 0:
                raise ValueError
            val = val.replace(",", ".")
        elif key == "currency":
            val = val.upper()[:8]
        elif key.startswith("notify_") and val not in ("0", "1"):
            raise ValueError
    except ValueError:
        return await m.answer("⚠️ Некорректное значение для этого параметра.")
    await db.run("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, val))
    await m.answer(f"✅ <code>{key}</code> обновлено")


@owner_r.message(Command("tadd"))
async def o_tadd(m: Message, command: CommandObject):
    parts = (command.args or "").split(maxsplit=1)
    if len(parts) < 2 or not re.fullmatch(r"\w{1,32}", parts[0]):
        return await m.answer("Использование: /tadd имя текст\nИмя — буквы/цифры, до 32 символов.")
    name = parts[0].lower()
    if name in PUBLIC or name in PRIVATE:
        return await m.answer("Это имя занято встроенной командой.")
    await db.run("INSERT OR REPLACE INTO templates VALUES (?,?)", (name, parts[1]))
    await m.answer(f"✅ Шаблон сохранён. В чате: <code>.{esc(name)}</code>")


@owner_r.message(Command("tlist"))
async def o_tlist(m: Message):
    rows = await db.all("SELECT * FROM templates ORDER BY name")
    if not rows:
        return await m.answer("Шаблонов нет. Добавить: /tadd имя текст")
    await m.answer("<b>Шаблоны</b>\n" + "\n".join(
        f".{esc(r['name'])} — {esc(r['text'][:60])}" for r in rows))


@owner_r.message(Command("tdel"))
async def o_tdel(m: Message, command: CommandObject):
    name = (command.args or "").strip().lower()
    cur = await db.run("DELETE FROM templates WHERE name=?", (name,))
    await m.answer("🗑 Удалено" if cur.rowcount else "Такого шаблона нет.")


@owner_r.message(Command("setstatus"))
async def o_setstatus(m: Message, command: CommandObject):
    parts = (command.args or "").split()
    if len(parts) != 2 or not parts[0].isdigit() or parts[1] not in STATUS:
        return await m.answer("Использование: /setstatus ID статус\nСтатусы: " + ", ".join(STATUS))
    if not await get_deal(int(parts[0])):
        return await m.answer("Сделка не найдена.")
    await set_status(int(parts[0]), parts[1])
    await m.answer(deal_card(await get_deal(int(parts[0])), private=True))


@owner_r.message(Command("dnote"))
async def o_dnote(m: Message, command: CommandObject):
    parts = (command.args or "").split(maxsplit=1)
    if len(parts) != 2 or not parts[0].isdigit():
        return await m.answer("Использование: /dnote ID текст")
    if not await get_deal(int(parts[0])):
        return await m.answer("Сделка не найдена.")
    await add_note(int(parts[0]), parts[1])
    await m.answer("📝 Заметка добавлена")


@owner_r.message(Command("stats"))
async def o_stats(m: Message):
    rows = await db.all(
        "SELECT status, COUNT(*) c, COALESCE(SUM(amount),0) s, COALESCE(SUM(fee),0) f "
        "FROM deals GROUP BY status")
    if not rows:
        return await m.answer("Данных пока нет.")
    lines, earned = [], 0.0
    for r in rows:
        lines.append(f"{STATUS[r['status']]}: {r['c']} шт. на {money(r['s'])}")
        if r["status"] == "done":
            earned = r["f"]
    cur = esc(await get_setting("currency"))
    await m.answer("<b>📊 Статистика</b>\n" + "\n".join(lines) +
                   f"\n\n💼 Заработано комиссий: <b>{money(earned)} {cur}</b>")


@owner_r.message(Command("export"))
async def o_export(m: Message):
    rows = await db.all("SELECT * FROM deals ORDER BY id")
    if not rows:
        return await m.answer("Нет сделок для выгрузки.")
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["id", "клиент", "chat_id", "сумма", "комиссия", "валюта", "условия",
                "статус", "создана", "обновлена", "заметки"])
    for d in rows:
        w.writerow([d["id"], d["client_name"], d["chat_id"], d["amount"], d["fee"], d["currency"],
                    d["description"], d["status"], fmt_ts(d["created_at"]),
                    fmt_ts(d["updated_at"]), d["note"]])
    await m.answer_document(BufferedInputFile(buf.getvalue().encode("utf-8-sig"),
                                              filename="deals.csv"))


# ---------- все остальные ----------
@fallback_r.message(Command("start", "id", "help"))
async def no_access(m: Message):
    await m.answer(f"⛔ Нет доступа. Ваш ID: <code>{m.from_user.id}</code>\n"
                   "Передайте его владельцу, если нужен доступ.")


# ======================= ЗАПУСК =======================
async def cleanup_loop():
    while True:
        try:
            await db.run("DELETE FROM messages WHERE ts < ?", (int(time.time()) - MSG_TTL,))
        except Exception:
            log.exception("cleanup")
        await asyncio.sleep(3600)


async def main():
    await db.init()
    for r in await db.all("SELECT user_id FROM allowed"):
        allowed_ids.add(r["user_id"])

    priv.include_router(owner_r)
    priv.include_router(user_r)
    priv.include_router(fallback_r)
    dp.include_router(biz)
    dp.include_router(priv)

    task = asyncio.create_task(cleanup_loop())
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        log.info("Бот запущен. Владелец: %s", OWNER_ID)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        task.cancel()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
