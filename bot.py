"""
Бизнес-бот: гарант / продажи / учёт сделок (aiogram 3 + Telegram Business). v2

Новое в v2:
  • мультивалютность: RUB, USD, STARS (⭐), TON, USDT, КБ — и свои через /cur;
  • типы сделок: гарант, продажа, покупка, другое (комиссия только у гаранта);
  • .add — мгновенно внести УЖЕ ЗАВЕРШЁННУЮ сделку в статистику (из чата или из ЛС бота);
  • статистика по периодам / валютам / типам, правка и удаление записей.
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
from datetime import datetime, timedelta
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
    "currency": "RUB",
    "default_kind": "garant",
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

# тип сделки: (эмодзи, название, алиасы)
KINDS = {
    "garant": ("🛡", "Гарант", "гарант гарантом гарантия garant"),
    "sale": ("💸", "Продажа", "продажа продал продано sale sell sold"),
    "buy": ("🛒", "Покупка", "покупка купил куплено buy bought"),
    "other": ("📦", "Другое", "другое прочее other"),
}

# код, символ, алиасы, знаков после запятой
DEFAULT_CURRENCIES = [
    ("RUB", "₽", "rub руб рубль рублей рубли р", 2),
    ("USD", "$", "usd бакс баксы баксов доллар долларов дол", 2),
    ("STARS", "⭐", "stars star звезды звёзды звезд звёзд звезда звёзда зв старс", 0),
    ("TON", "TON", "ton тон тонкоин", 4),
    ("USDT", "USDT", "usdt юсдт тезер tether", 2),
    ("KB", "КБ", "кб kb cb cryptobot криптобот", 4),
]

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS allowed(user_id INTEGER PRIMARY KEY, added_at INTEGER);
CREATE TABLE IF NOT EXISTS templates(name TEXT PRIMARY KEY, text TEXT);
CREATE TABLE IF NOT EXISTS currencies(
    code TEXT PRIMARY KEY, symbol TEXT, aliases TEXT, decimals INTEGER
);
CREATE TABLE IF NOT EXISTS messages(
    chat_id INTEGER, msg_id INTEGER, sender_id INTEGER, sender_name TEXT,
    text TEXT, kind TEXT, file_id TEXT, ts INTEGER,
    PRIMARY KEY(chat_id, msg_id)
);
CREATE TABLE IF NOT EXISTS deals(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER, client_name TEXT, amount REAL, fee REAL, currency TEXT,
    description TEXT, status TEXT, note TEXT, created_at INTEGER, updated_at INTEGER,
    kind TEXT DEFAULT 'garant', closed_at INTEGER
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
        # миграция со старой версии (v1)
        cols = {r["name"] for r in await self.all("PRAGMA table_info(deals)")}
        if "kind" not in cols:
            await self.run("ALTER TABLE deals ADD COLUMN kind TEXT DEFAULT 'garant'")
        if "closed_at" not in cols:
            await self.run("ALTER TABLE deals ADD COLUMN closed_at INTEGER")
            await self.run("UPDATE deals SET closed_at=updated_at WHERE status='done'")

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

# справочник валют (в памяти, грузится из БД)
CUR_SYM: dict[str, str] = {}
CUR_DEC: dict[str, int] = {}
ALIAS: dict[str, str] = {}
KIND_ALIAS: dict[str, str] = {
    a: k for k, (_, _, al) in KINDS.items() for a in al.split()
}


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


def norm(tok: str) -> str:
    return tok.replace("\ufe0f", "").strip(",.;:").lower()


def money(x: float) -> str:
    s = f"{x:,.8f}".replace(",", " ")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def fmt_amt(x: float, code: str) -> str:
    return f"{money(x)} {CUR_SYM.get(code, code)}"


def fmt_ts(ts: int | None) -> str:
    return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M") if ts else "—"


def parse_amount(s: str) -> float:
    if not s:
        raise CmdError("Укажите сумму, например: 500 stars")
    try:
        v = float(s.replace(",", ".").replace("_", ""))
    except ValueError:
        raise CmdError(f"Некорректная сумма: {esc(s)}")
    if not math.isfinite(v) or v <= 0 or v > 1e12:
        raise CmdError("Сумма должна быть положительным числом")
    return round(v, 8)


def kind_of(d) -> str:
    return d["kind"] if d["kind"] in KINDS else "garant"


async def load_currencies():
    rows = await db.all("SELECT * FROM currencies")
    if not rows:
        for code, sym, al, dec in DEFAULT_CURRENCIES:
            await db.run("INSERT INTO currencies VALUES (?,?,?,?)", (code, sym, al, dec))
        rows = await db.all("SELECT * FROM currencies")
    CUR_SYM.clear(), CUR_DEC.clear(), ALIAS.clear()
    for r in rows:
        code = r["code"]
        CUR_SYM[code] = r["symbol"]
        CUR_DEC[code] = r["decimals"]
        ALIAS[code.lower()] = code
        ALIAS[norm(r["symbol"])] = code
        for a in (r["aliases"] or "").split():
            ALIAS[norm(a)] = code


async def get_setting(key: str) -> str:
    row = await db.one("SELECT value FROM settings WHERE key=?", (key,))
    return row["value"] if row else DEFAULTS[key]


async def calc_fee(amount: float, cur: str) -> float:
    pct = float(await get_setting("fee_percent"))
    q = 10 ** CUR_DEC.get(cur, 2)
    return math.ceil(amount * pct / 100 * q - 1e-9) / q


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
MEDIA_RU = {
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


# ======================= РАЗБОР ПАРАМЕТРОВ СДЕЛКИ =======================
AMOUNT_RE = re.compile(r"^(?P<pre>[^\d\s]*)(?P<num>\d[\d.,_]*)(?P<post>[^\d\s]*)$")
FEE_RE = re.compile(r"^(?:fee|ком|комиссия)=(\d[\d.,]*)$", re.I)
DATE_RE = re.compile(r"^(?:date|дата)=(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?$", re.I)


def _date_ts(d: str, mo: str, y: str | None) -> int:
    now = datetime.now()
    year = int(y) if y else now.year
    if y and len(y) <= 2:
        year += 2000
    try:
        dt = datetime(year, int(mo), int(d), 12, 0)
    except ValueError:
        raise CmdError("Некорректная дата, формат: date=28.09 или date=28.09.2026")
    if not y and dt > now:
        dt = dt.replace(year=year - 1)
    return int(dt.timestamp())


def parse_deal_args(args: str) -> dict:
    """
    '500 stars продажа fee=10 date=28.09 описание'  /  '500⭐ продал аккаунт'  /  '$10 buy'
    Сначала сумма, затем в любом порядке валюта, тип, fee=, date=; всё остальное — описание.
    """
    rest = (args or "").strip()
    if not rest:
        raise CmdError("Укажите сумму, например: .add 500 stars продажа описание")
    first, _, rest = rest.partition(" ")
    mt = AMOUNT_RE.match(first)
    if not mt:
        raise CmdError(f"Некорректная сумма: {esc(first)}")
    res = {"amount": parse_amount(mt["num"]), "currency": None, "kind": None,
           "fee": None, "ts": None, "rest": ""}
    for part in (mt["pre"], mt["post"]):
        if part:
            code = ALIAS.get(norm(part))
            if not code:
                raise CmdError(f"Неизвестная валюта: {esc(part)}. Список: /cur")
            res["currency"] = code
    rest = rest.strip()
    while rest:
        tok, _, tail = rest.partition(" ")
        fm, dmt = FEE_RE.match(tok), DATE_RE.match(tok)
        if fm:
            try:
                res["fee"] = float(fm[1].replace(",", "."))
            except ValueError:
                raise CmdError("Некорректная комиссия")
        elif dmt:
            res["ts"] = _date_ts(*dmt.groups())
        elif res["currency"] is None and norm(tok) in ALIAS:
            res["currency"] = ALIAS[norm(tok)]
        elif res["kind"] is None and norm(tok) in KIND_ALIAS:
            res["kind"] = KIND_ALIAS[norm(tok)]
        else:
            break
        rest = tail.strip()
    res["rest"] = rest
    return res


async def finalize_params(p: dict) -> dict:
    """Подставляет значения по умолчанию, проверяет точность, считает комиссию."""
    cur = p["currency"] or await get_setting("currency")
    if cur not in CUR_SYM:
        raise CmdError(f"Валюта по умолчанию «{esc(cur)}» не найдена. Проверьте /cur и /set currency")
    kind = p["kind"] or await get_setting("default_kind")
    dec = CUR_DEC.get(cur, 2)
    if round(p["amount"], dec) != p["amount"]:
        raise CmdError(f"Для {esc(cur)} допустимо максимум {dec} знаков после запятой")
    if p["fee"] is not None:
        fee = p["fee"]
    elif kind == "garant":
        fee = await calc_fee(p["amount"], cur)
    else:
        fee = 0.0
    return {**p, "currency": cur, "kind": kind, "fee": fee}


# ======================= СДЕЛКИ =======================
async def create_deal(chat_id: int, client: str, p: dict, desc: str, status: str) -> int:
    now = int(time.time())
    ts = p["ts"] or now
    cur = await db.run(
        "INSERT INTO deals(chat_id, client_name, amount, fee, currency, description, status, "
        "note, created_at, updated_at, kind, closed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (chat_id, client, p["amount"], p["fee"], p["currency"], desc or "—", status, "",
         ts, now, p["kind"], ts if status == "done" else None))
    return cur.lastrowid


async def get_deal(deal_id: int):
    return await db.one("SELECT * FROM deals WHERE id=?", (deal_id,))


async def active_deal(chat_id: int):
    return await db.one(
        f"SELECT * FROM deals WHERE chat_id=? AND status IN ({','.join('?' * len(ACTIVE))}) "
        "ORDER BY id DESC LIMIT 1", (chat_id, *ACTIVE))


async def last_deal(chat_id: int):
    return await db.one("SELECT * FROM deals WHERE chat_id=? ORDER BY id DESC LIMIT 1", (chat_id,))


async def set_status(deal_id: int, status: str):
    now = int(time.time())
    await db.run("UPDATE deals SET status=?, updated_at=?, closed_at=? WHERE id=?",
                 (status, now, now if status == "done" else None, deal_id))


async def add_note(deal_id: int, text: str):
    d = await get_deal(deal_id)
    line = f"[{fmt_ts(int(time.time()))}] {text}"
    note = f"{d['note']}\n{line}" if d["note"] else line
    await db.run("UPDATE deals SET note=?, updated_at=? WHERE id=?",
                 (note, int(time.time()), deal_id))


def deal_card(d, private: bool = False) -> str:
    k = kind_of(d)
    emoji, label, _ = KINDS[k]
    lines = [
        f"{emoji} <b>{label} #{d['id']}</b>",
        f"👤 Клиент: {esc(d['client_name'])}",
        f"💵 Сумма: <b>{esc(fmt_amt(d['amount'], d['currency']))}</b>",
    ]
    if d["fee"]:
        who = "гаранта" if k == "garant" else ""
        lines.append(f"💼 Комиссия {who}: {esc(fmt_amt(d['fee'], d['currency']))}".replace("  ", " "))
    lines += [f"📝 Условия: {esc(d['description'])}", f"📊 Статус: {STATUS[d['status']]}"]
    if private:
        lines.append(f"🕒 Создана: {fmt_ts(d['created_at'])}")
        if d["closed_at"]:
            lines.append(f"🏁 Закрыта: {fmt_ts(d['closed_at'])}")
        if d["note"]:
            lines.append(f"🔒 Заметки:\n{esc(d['note'])}")
    return "\n".join(lines)


def deal_line(d) -> str:
    st = STATUS[d["status"]].split()[0]
    return (f"{st} <b>#{d['id']}</b> · {KINDS[kind_of(d)][0]} "
            f"{esc(fmt_amt(d['amount'], d['currency']))} · {esc(d['client_name'])}")


# ======================= ФИЛЬТРЫ (для /stats, /deals) =======================
PERIODS = {
    "today": "today", "сегодня": "today", "week": "week", "неделя": "week",
    "month": "month", "месяц": "month", "all": "all", "все": "all", "всё": "all",
}
PERIOD_RU = {"today": "сегодня", "week": "неделя", "month": "месяц", "all": "всё время"}


def parse_filters(args: str) -> dict:
    f = {"period": "all", "status": None, "kind": None, "currency": None}
    for t in (args or "").split():
        n = norm(t)
        if n in PERIODS:
            f["period"] = PERIODS[n]
        elif n in STATUS:
            f["status"] = n
        elif n in KIND_ALIAS:
            f["kind"] = KIND_ALIAS[n]
        elif n in ALIAS:
            f["currency"] = ALIAS[n]
        else:
            raise CmdError(f"Не понял «{esc(t)}». Периоды: today/week/month/all, "
                           f"статусы: {', '.join(STATUS)}, тип и валюта — по названию.")
    return f


def period_start(period: str) -> int:
    now = datetime.now()
    if period == "today":
        dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "week":
        dt = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "month":
        dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return 0
    return int(dt.timestamp())


def where_from(f: dict, with_period: bool = True) -> tuple[list[str], list]:
    conds, args = [], []
    if with_period and f["period"] != "all":
        conds.append("COALESCE(closed_at, created_at) >= ?")
        args.append(period_start(f["period"]))
    if f["kind"]:
        conds.append("COALESCE(kind,'garant') = ?")
        args.append(f["kind"])
    if f["currency"]:
        conds.append("currency = ?")
        args.append(f["currency"])
    return conds, args


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
    "<b>⚡ Быстрый учёт готовой сделки</b> (приватно, клиент ничего не увидит):\n"
    ".add 500 stars продал аккаунт\n"
    ".add 10$ купил подписку\n"
    ".add 1500 кб гарант fee=75 описание\n"
    ".add 2000 руб продажа date=28.09 описание\n"
    "Порядок: сумма → валюта/тип (любой порядок) → fee= и date= → описание.\n"
    ".undo — удалить последнюю запись в этом чате\n\n"
    "<b>Видны клиенту</b> (ваше сообщение заменяется ответом):\n"
    ".hi — приветствие · .rules — правила · .req — реквизиты\n"
    ".fee 500 stars — расчёт комиссии\n"
    ".new 500 stars [тип] описание — открыть сделку\n"
    ".deal — карточка · .paid — оплачено · .done — завершена\n"
    ".dispute — спор · .cancel — отмена\n"
    ".имя — ваш шаблон (/tadd в ЛС бота)\n\n"
    "<b>Приватные:</b> .info — сделка и история клиента, .note текст — заметка, .help\n\n"
    "<b>Типы:</b> гарант, продажа, покупка, другое\n"
    "<b>Валюты:</b> rub, usd/$/баксы, stars/⭐/звёзды, ton, usdt, кб (свои — /cur)"
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
    p = await finalize_params({**parse_deal_args(a), "kind": "garant", "fee": None})
    return (f"💼 Комиссия гаранта с суммы {esc(fmt_amt(p['amount'], p['currency']))}: "
            f"<b>{esc(fmt_amt(p['fee'], p['currency']))}</b>\n"
            f"Итого к оплате: <b>{esc(fmt_amt(p['amount'] + p['fee'], p['currency']))}</b>")


@reg(PUBLIC, "new", "сделка")
async def c_new(m, a):
    if await active_deal(m.chat.id):
        raise CmdError("В этом чате уже есть активная сделка. Закройте её: .done или .cancel")
    p = await finalize_params(parse_deal_args(a))
    did = await create_deal(m.chat.id, m.chat.full_name or str(m.chat.id), p,
                            p["rest"], "new")
    tail = "\n\nОжидаем оплату. Реквизиты: .req" if p["kind"] in ("garant", "sale") else ""
    return deal_card(await get_deal(did)) + tail


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
        m, "paid", "✅ <b>Оплата получена.</b> Средства удерживаются до выполнения условий.")


@reg(PUBLIC, "done", "готово")
async def c_done(m, a):
    return await change_status(
        m, "done", "🎉 <b>Сделка завершена.</b> Обязательства выполнены. Спасибо!")


@reg(PUBLIC, "dispute", "спор")
async def c_dispute(m, a):
    return await change_status(
        m, "dispute", "⚠️ <b>Открыт спор.</b> Средства заморожены до разбирательства.")


@reg(PUBLIC, "cancel", "отмена")
async def c_cancel(m, a):
    return await change_status(m, "cancelled", "❌ <b>Сделка отменена.</b>")


# ---------- приватные ----------
@reg(PRIVATE, "add", "добавить")
async def c_add(m, a):
    """Внести УЖЕ завершённую сделку в статистику — клиент ничего не увидит."""
    p = await finalize_params(parse_deal_args(a))
    did = await create_deal(m.chat.id, m.chat.full_name or str(m.chat.id), p, p["rest"], "done")
    return ("⚡ <b>Записано в статистику</b>\n" + deal_card(await get_deal(did), private=True) +
            f"\n\nОтменить запись: .undo или /ddel {did}")


@reg(PRIVATE, "undo", "откат")
async def c_undo(m, a):
    d = await last_deal(m.chat.id)
    if not d:
        raise CmdError("В этом чате нет записей для удаления.")
    await db.run("DELETE FROM deals WHERE id=?", (d["id"],))
    return "↩️ <b>Удалена запись</b>\n" + deal_card(d, private=True)


@reg(PRIVATE, "help", "помощь")
async def c_help(m, a):
    return BUSINESS_HELP


@reg(PRIVATE, "info", "инфо")
async def c_info(m, a):
    d = await active_deal(m.chat.id) or await last_deal(m.chat.id)
    done = await db.all(
        "SELECT currency, COUNT(*) c, SUM(amount) s FROM deals "
        "WHERE chat_id=? AND status='done' GROUP BY currency", (m.chat.id,))
    total = await db.one("SELECT COUNT(*) c FROM deals WHERE chat_id=?", (m.chat.id,))
    head = deal_card(d, private=True) if d else "Сделок с этим клиентом пока нет."
    hist = ", ".join(f"{r['c']} шт. на {fmt_amt(r['s'], r['currency'])}" for r in done) or "—"
    return f"{head}\n\n📈 История клиента: всего {total['c']}, завершено: {esc(hist)}"


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
            report += f"\n📎 Вложение: {MEDIA_RU.get(row['kind'], row['kind'])}"
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
    "<b>⚡ Учёт</b>\n"
    "/add 500 stars продажа Вася | описание — внести готовую сделку\n"
    "(в чате с клиентом проще: <code>.add 500 stars описание</code>)\n"
    "/stats [период] [валюта] [тип] — статистика\n"
    "/deals [статус] [период] [валюта] [тип] — список\n"
    "/deal ID · /find текст · /export\n"
    "/dedit ID поле значение — правка (amount, cur, kind, fee, desc, client, date)\n"
    "/ddel ID — удалить запись · /dnote ID текст · /setstatus ID статус\n"
    "/fee сумма [валюта] — комиссия\n\n"
    "<b>Валюты</b>\n"
    "/cur — список · /cur add КОД символ алиасы [dec=2] · /cur del КОД\n\n"
    "<b>Доступ</b>\n"
    "/allow ID · /deny ID · /users\n\n"
    "<b>Настройки и шаблоны</b>\n"
    "/settings · /set ключ значение\n"
    "/tadd имя текст · /tlist · /tdel имя\n\n"
    "/bhelp — команды для бизнес-чата · /id — ваш ID"
)
USER_HELP = (
    "<b>Доступные вам команды</b>\n"
    "/deals [статус] [период] [валюта] [тип] — список сделок\n"
    "/deal ID — карточка\n/find текст — поиск\n"
    "/fee сумма [валюта] — расчёт комиссии\n/id — ваш ID"
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
        p = await finalize_params({**parse_deal_args(command.args or ""), "kind": "garant", "fee": None})
    except CmdError as e:
        return await m.answer(f"⚠️ {e}")
    await m.answer(f"💼 Комиссия: <b>{esc(fmt_amt(p['fee'], p['currency']))}</b>\n"
                   f"Итого: <b>{esc(fmt_amt(p['amount'] + p['fee'], p['currency']))}</b>")


@user_r.message(Command("deals"))
async def u_deals(m: Message, command: CommandObject):
    try:
        f = parse_filters(command.args or "")
    except CmdError as e:
        return await m.answer(f"⚠️ {e}")
    conds, args = where_from(f)
    if f["status"]:
        conds.append("status = ?")
        args.append(f["status"])
    q = "SELECT * FROM deals" + (" WHERE " + " AND ".join(conds) if conds else "")
    rows = await db.all(q + " ORDER BY id DESC LIMIT 20", args)
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


@owner_r.message(Command("add"))
async def o_add(m: Message, command: CommandObject):
    """Внести готовую сделку прямо из ЛС с ботом: /add 500 stars продажа Вася | описание"""
    try:
        p = await finalize_params(parse_deal_args(command.args or ""))
        client, desc = "—", p["rest"]
        if "|" in desc:
            client, desc = (x.strip() for x in desc.split("|", 1))
            client = client or "—"
        did = await create_deal(0, client, p, desc, "done")
    except CmdError as e:
        return await m.answer(f"⚠️ {e}")
    await m.answer("⚡ <b>Записано в статистику</b>\n" + deal_card(await get_deal(did), private=True))


@owner_r.message(Command("ddel"))
async def o_ddel(m: Message, command: CommandObject):
    if not (command.args or "").strip().isdigit():
        return await m.answer("Использование: /ddel ID")
    d = await get_deal(int(command.args))
    if not d:
        return await m.answer("Сделка не найдена.")
    await db.run("DELETE FROM deals WHERE id=?", (d["id"],))
    await m.answer("🗑 <b>Удалено</b>\n" + deal_card(d, private=True))


@owner_r.message(Command("dedit"))
async def o_dedit(m: Message, command: CommandObject):
    parts = (command.args or "").split(maxsplit=2)
    usage = ("Использование: /dedit ID поле значение\n"
             "Поля: amount, cur, kind, fee, desc, client, date (28.09 или 28.09.2026)")
    if len(parts) < 3 or not parts[0].isdigit():
        return await m.answer(usage)
    d = await get_deal(int(parts[0]))
    if not d:
        return await m.answer("Сделка не найдена.")
    field, val = parts[1].lower(), parts[2].strip()
    try:
        if field in ("amount", "сумма"):
            amount = parse_amount(val)
            if round(amount, CUR_DEC.get(d["currency"], 2)) != amount:
                raise CmdError("Слишком много знаков после запятой для этой валюты")
            upd = ("amount", amount)
        elif field in ("cur", "currency", "валюта"):
            code = ALIAS.get(norm(val))
            if not code:
                raise CmdError("Неизвестная валюта, список: /cur")
            upd = ("currency", code)
        elif field in ("kind", "тип"):
            k = KIND_ALIAS.get(norm(val))
            if not k:
                raise CmdError("Типы: гарант, продажа, покупка, другое")
            upd = ("kind", k)
        elif field in ("fee", "комиссия"):
            try:
                upd = ("fee", float(val.replace(",", ".")))
            except ValueError:
                raise CmdError("Некорректная комиссия")
        elif field in ("desc", "описание"):
            upd = ("description", val)
        elif field in ("client", "клиент"):
            upd = ("client_name", val)
        elif field in ("date", "дата"):
            dm_ = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", val)
            if not dm_:
                raise CmdError("Формат даты: 28.09 или 28.09.2026")
            ts = _date_ts(*dm_.groups())
            await db.run("UPDATE deals SET created_at=?, closed_at=CASE WHEN status='done' "
                         "THEN ? ELSE closed_at END, updated_at=? WHERE id=?",
                         (ts, ts, int(time.time()), d["id"]))
            return await m.answer("✅ Обновлено\n" + deal_card(await get_deal(d["id"]), private=True))
        else:
            return await m.answer(usage)
    except CmdError as e:
        return await m.answer(f"⚠️ {e}")
    await db.run(f"UPDATE deals SET {upd[0]}=?, updated_at=? WHERE id=?",
                 (upd[1], int(time.time()), d["id"]))
    await m.answer("✅ Обновлено\n" + deal_card(await get_deal(d["id"]), private=True))


@owner_r.message(Command("stats"))
async def o_stats(m: Message, command: CommandObject):
    try:
        f = parse_filters(command.args or "")
    except CmdError as e:
        return await m.answer(f"⚠️ {e}")
    conds, args = where_from(f)
    cond_sql = (" AND " + " AND ".join(conds)) if conds else ""
    rows = await db.all(
        "SELECT currency, COALESCE(kind,'garant') k, COUNT(*) c, SUM(amount) s, SUM(fee) f "
        f"FROM deals WHERE status='done'{cond_sql} GROUP BY currency, k ORDER BY currency", args)

    by_cur: dict[str, dict] = {}
    for r in rows:
        d = by_cur.setdefault(r["currency"], {"c": 0, "s": 0.0, "f": 0.0, "kinds": []})
        d["c"] += r["c"]
        d["s"] += r["s"]
        d["f"] += r["f"] or 0
        d["kinds"].append(r)

    lines = [f"<b>📊 Статистика · {PERIOD_RU[f['period']]}</b>"]
    if not by_cur:
        lines.append("Завершённых сделок нет.")
    for cur, d in by_cur.items():
        lines.append(f"\n<b>{esc(CUR_SYM.get(cur, cur))} {esc(cur)}</b>: {d['c']} шт. на "
                     f"<b>{esc(fmt_amt(d['s'], cur))}</b>")
        if d["f"] > 0:
            lines.append(f"💼 Комиссии: {esc(fmt_amt(d['f'], cur))}")
        for r in d["kinds"]:
            emoji, label, _ = KINDS.get(r["k"], KINDS["other"])
            extra = f" · комиссии {esc(fmt_amt(r['f'], cur))}" if r["f"] else ""
            lines.append(f"  {emoji} {label}: {r['c']} · {esc(fmt_amt(r['s'], cur))}{extra}")

    a_conds, a_args = where_from(f, with_period=False)
    a_sql = (" AND " + " AND ".join(a_conds)) if a_conds else ""
    act = await db.all(
        f"SELECT currency, status, COUNT(*) c, SUM(amount) s FROM deals "
        f"WHERE status IN ('new','paid','dispute'){a_sql} GROUP BY currency, status", a_args)
    if act:
        lines.append("\n<b>⏳ В работе:</b>")
        for r in act:
            lines.append(f"  {STATUS[r['status']].split()[0]} {r['c']} шт. · "
                         f"{esc(fmt_amt(r['s'], r['currency']))}")
    await m.answer("\n".join(lines))


@owner_r.message(Command("export"))
async def o_export(m: Message):
    rows = await db.all("SELECT * FROM deals ORDER BY id")
    if not rows:
        return await m.answer("Нет сделок для выгрузки.")
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["id", "тип", "клиент", "chat_id", "сумма", "комиссия", "валюта", "условия",
                "статус", "создана", "закрыта", "заметки"])
    for d in rows:
        w.writerow([d["id"], kind_of(d), d["client_name"], d["chat_id"], d["amount"], d["fee"],
                    d["currency"], d["description"], d["status"], fmt_ts(d["created_at"]),
                    fmt_ts(d["closed_at"]), d["note"]])
    await m.answer_document(BufferedInputFile(buf.getvalue().encode("utf-8-sig"),
                                              filename="deals.csv"))


@owner_r.message(Command("cur"))
async def o_cur(m: Message, command: CommandObject):
    args = (command.args or "").split()
    if not args:
        rows = await db.all("SELECT * FROM currencies ORDER BY code")
        lines = [f"{esc(r['symbol'])} <code>{esc(r['code'])}</code> · знаков: {r['decimals']} · "
                 f"{esc(r['aliases'])}" for r in rows]
        return await m.answer(
            "<b>Валюты</b>\n" + "\n".join(lines) +
            "\n\nДобавить: /cur add КОД символ алиасы [dec=2]\nУдалить: /cur del КОД")
    if args[0] == "add" and len(args) >= 3:
        code = args[1].upper()
        if not re.fullmatch(r"\w{1,10}", code):
            return await m.answer("Код — до 10 букв/цифр.")
        dec, aliases = 2, []
        for t in args[3:]:
            mt = re.fullmatch(r"dec=(\d)", t.lower())
            if mt:
                dec = int(mt[1])
            else:
                aliases.append(t.lower())
        await db.run("INSERT OR REPLACE INTO currencies VALUES (?,?,?,?)",
                     (code, args[2], " ".join(aliases), dec))
        await load_currencies()
        return await m.answer(f"✅ Валюта <code>{esc(code)}</code> сохранена")
    if args[0] == "del" and len(args) == 2:
        code = args[1].upper()
        if code not in CUR_SYM:
            return await m.answer("Такой валюты нет.")
        used = await db.one("SELECT COUNT(*) c FROM deals WHERE currency=?", (code,))
        if used["c"]:
            return await m.answer(f"⚠️ Валюта используется в {used['c']} сделках — удалить нельзя.")
        if code == await get_setting("currency"):
            return await m.answer("⚠️ Это валюта по умолчанию. Сначала смените её: /set currency КОД")
        await db.run("DELETE FROM currencies WHERE code=?", (code,))
        await load_currencies()
        return await m.answer("🗑 Удалено")
    await m.answer("Использование: /cur · /cur add КОД символ алиасы [dec=2] · /cur del КОД")


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
            val = val.replace(",", ".")
            if not 0 <= float(val) <= 100:
                raise ValueError
        elif key == "currency":
            code = ALIAS.get(norm(val))
            if not code:
                raise ValueError
            val = code
        elif key == "default_kind":
            k = KIND_ALIAS.get(norm(val))
            if not k:
                raise ValueError
            val = k
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
    await load_currencies()
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
