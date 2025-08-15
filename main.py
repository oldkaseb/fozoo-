
# -*- coding: utf-8 -*-
# Fazol Bot — custom build with relationship wizard, operator-restricted actions,
# improved /start flows, crush list, and "my data" summary with popularity score.
# python-telegram-bot v21+

import os
import re
import random
import logging
import asyncio
import atexit
import hashlib
import datetime as dt
import urllib.parse as _up
from typing import Optional, List, Tuple, Dict, Any, Iterable, TypeVar

from zoneinfo import ZoneInfo

from sqlalchemy import (
    create_engine, select, text, Integer, BigInteger, String, DateTime,
    Date, Boolean, JSON, ForeignKey, Index
)
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application, MessageHandler, CallbackQueryHandler, ChatMemberHandler,
    CommandHandler, filters, ContextTypes
)
from telegram.error import Conflict as TgConflict

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)

# ---------- CONFIG ----------
TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
INSTANCE_TAG = os.getenv("INSTANCE_TAG", "").strip()  # for namespaced advisory lock

DEFAULT_TZ = "Asia/Tehran"
TZ_TEHRAN = ZoneInfo(DEFAULT_TZ)

OWNER_CONTACT_USERNAME = os.getenv("OWNER_CONTACT", "soulsownerbot")
AUTO_DELETE_SECONDS = int(os.getenv("AUTO_DELETE_SECONDS", "40"))
DISABLE_SINGLETON = os.getenv("DISABLE_SINGLETON", "0").strip().lower() in ("1", "true", "yes")

Base = declarative_base()

# ---------- PERSIAN TOOLS ----------
try:
    from persiantools.jdatetime import JalaliDateTime, JalaliDate
    from persiantools import digits as _digits
    HAS_PTOOLS = True
except Exception:
    HAS_PTOOLS = False

def fa_digits(x: str) -> str:
    s = str(x)
    if HAS_PTOOLS:
        try: return _digits.en_to_fa(s)
        except Exception: return s
    return s

def fa_to_en_digits(s: str) -> str:
    if HAS_PTOOLS:
        try: return _digits.fa_to_en(str(s))
        except Exception: ...
    return str(s)

def fmt_dt_fa(dt_utc: Optional[dt.datetime]) -> str:
    if dt_utc is None: return "-"
    if dt_utc.tzinfo is None: dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
    local = dt_utc.astimezone(TZ_TEHRAN)
    if HAS_PTOOLS:
        try:
            jdt = JalaliDateTime.fromgregorian(datetime=local)
            return fa_digits(jdt.strftime("%A %Y/%m/%d %H:%M"))
        except Exception: ...
    return local.strftime("%Y/%m/%d %H:%M")

def fmt_date_fa(d: Optional[dt.date]) -> str:
    if not d: return "-"
    if HAS_PTOOLS:
        try: return fa_digits(JalaliDate.fromgregorian(date=d).strftime("%Y/%m/%d"))
        except Exception: ...
    return d.strftime("%Y/%m/%d")

def jalali_now_year() -> int:
    now = dt.datetime.now(TZ_TEHRAN)
    if HAS_PTOOLS: return JalaliDateTime.fromgregorian(datetime=now).year
    return now.year

def jalali_month_len(y: int, m: int) -> int:
    if not HAS_PTOOLS:
        if m <= 6: return 31
        if m <= 11: return 30
        return 29
    for d in range(31, 27, -1):
        try:
            JalaliDate(y, m, d); return d
        except Exception: ...
    return 29

def today_jalali() -> Tuple[int,int,int]:
    now = dt.datetime.now(TZ_TEHRAN)
    if HAS_PTOOLS:
        j = JalaliDateTime.fromgregorian(datetime=now)
        return j.year, j.month, j.day
    d = now.date(); return d.year, d.month, d.day

def to_jalali_md(d: dt.date) -> Tuple[int,int]:
    if HAS_PTOOLS:
        j = JalaliDate.fromgregorian(date=d)
        return j.month, j.day
    return d.month, d.day

# ---------- TEXT NORMALIZE ----------
ARABIC_FIX_MAP = str.maketrans({"ي":"ی","ى":"ی","ئ":"ی","ك":"ک","ـ":""})
PUNCS = " \u200c\u200f\u200e\u2066\u2067\u2068\u2069\t\r\n.,!?؟،;:()[]{}«»\"'"
def fa_norm(s: str) -> str:
    if s is None: return ""
    s = str(s).translate(ARABIC_FIX_MAP)
    s = s.replace("\u200c"," ").replace("\u200f","").replace("\u200e","")
    s = s.replace("\u202a","").replace("\u202c","")
    s = re.sub(r"\s+"," ", s).strip()
    return s
def clean_text(s: str) -> str: return fa_norm(s)

RE_WORD_FAZOL = re.compile(rf"(?:^|[{re.escape(PUNCS)}])فضول(?:[{re.escape(PUNCS)}]|$)")

# ---------- DB ----------
try:
    import psycopg; _DRIVER="psycopg"
except Exception:
    try: import psycopg2; _DRIVER="psycopg2"
    except Exception: _DRIVER="psycopg"

raw_db_url = (os.getenv("DATABASE_URL") or "").strip()
if not raw_db_url:
    PGHOST=os.getenv("PGHOST"); PGPORT=os.getenv("PGPORT","5432")
    PGUSER=os.getenv("PGUSER"); PGPASSWORD=os.getenv("PGPASSWORD")
    PGDATABASE=os.getenv("PGDATABASE","railway")
    if all([PGHOST,PGUSER,PGPASSWORD]):
        raw_db_url = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    else:
        raise RuntimeError("DATABASE_URL or PG* envs are required.")

db_url = raw_db_url
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://","postgresql://",1)
if "+psycopg" not in db_url and "+psycopg2" not in db_url:
    db_url = db_url.replace("postgresql://", f"postgresql+{_DRIVER}://",1)
if "sslmode=" not in db_url:
    sep="&" if "?" in db_url else "?"
    db_url=f"{db_url}{sep}sslmode=require"

try:
    parsed=_up.urlsplit(db_url)
    logging.info(f"DB host={parsed.hostname} port={parsed.port} path={parsed.path} driver={_DRIVER}")
except Exception: ...

engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=300, future=True, connect_args={"sslmode":"require"})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# ---------- ORM ----------
class Group(Base):
    __tablename__="groups"
    id: Mapped[int]=mapped_column(BigInteger, primary_key=True)
    title: Mapped[Optional[str]]=mapped_column(String(255))
    owner_user_id: Mapped[Optional[int]]=mapped_column(BigInteger)
    timezone: Mapped[Optional[str]]=mapped_column(String(64))
    trial_started_at: Mapped[Optional[dt.datetime]]=mapped_column(DateTime)
    expires_at: Mapped[Optional[dt.datetime]]=mapped_column(DateTime)
    is_active: Mapped[bool]=mapped_column(Boolean, default=True)
    settings: Mapped[Optional[dict]]=mapped_column(JSON)

class User(Base):
    __tablename__="users"
    __table_args__=(
        Index("ix_users_chat_username","chat_id","username"),
        Index("ix_users_chat_tg","chat_id","tg_user_id", unique=True),
    )
    id: Mapped[int]=mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int]=mapped_column(BigInteger, index=True)
    tg_user_id: Mapped[int]=mapped_column(BigInteger, index=True)
    first_name: Mapped[Optional[str]]=mapped_column(String(128))
    last_name: Mapped[Optional[str]]=mapped_column(String(128))
    username: Mapped[Optional[str]]=mapped_column(String(128), index=True)
    gender: Mapped[str]=mapped_column(String(8), default="unknown")
    birthday: Mapped[Optional[dt.date]]=mapped_column(Date)

class GroupAdmin(Base):
    __tablename__="group_admins"
    __table_args__=(Index("ix_ga_unique","chat_id","tg_user_id", unique=True),)
    id: Mapped[int]=mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int]=mapped_column(BigInteger, index=True)
    tg_user_id: Mapped[int]=mapped_column(BigInteger, index=True)

class Relationship(Base):
    __tablename__="relationships"
    __table_args__=(Index("ix_rel_unique","chat_id","user_a_id","user_b_id", unique=True),)
    id: Mapped[int]=mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int]=mapped_column(BigInteger, index=True)
    user_a_id: Mapped[int]=mapped_column(ForeignKey("users.id"))
    user_b_id: Mapped[int]=mapped_column(ForeignKey("users.id"))
    started_at: Mapped[Optional[dt.date]]=mapped_column(Date)

class Crush(Base):
    __tablename__="crushes"
    __table_args__=(Index("ix_crush_unique","chat_id","from_user_id","to_user_id", unique=True),)
    id: Mapped[int]=mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int]=mapped_column(BigInteger, index=True)
    from_user_id: Mapped[int]=mapped_column(ForeignKey("users.id"))
    to_user_id: Mapped[int]=mapped_column(ForeignKey("users.id"))
    created_at: Mapped[dt.datetime]=mapped_column(DateTime, default=dt.datetime.utcnow)

class ReplyStatDaily(Base):
    __tablename__="reply_stat_daily"
    __table_args__=(Index("ix_reply_chat_date_user","chat_id","date","target_user_id", unique=True),)
    id: Mapped[int]=mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int]=mapped_column(BigInteger, index=True)
    date: Mapped[dt.date]=mapped_column(Date, index=True)
    target_user_id: Mapped[int]=mapped_column(ForeignKey("users.id"))
    reply_count: Mapped[int]=mapped_column(Integer, default=0)

class ShipHistory(Base):
    __tablename__="ship_history"
    __table_args__=(Index("ix_ship_chat_date","chat_id","date"),)
    id: Mapped[int]=mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int]=mapped_column(BigInteger, index=True)
    date: Mapped[dt.date]=mapped_column(Date, index=True)
    male_user_id: Mapped[int]=mapped_column(ForeignKey("users.id"))
    female_user_id: Mapped[int]=mapped_column(ForeignKey("users.id"))

class SubscriptionLog(Base):
    __tablename__="subscription_log"
    id: Mapped[int]=mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int]=mapped_column(BigInteger, index=True)
    actor_tg_user_id: Mapped[Optional[int]]=mapped_column(BigInteger)
    action: Mapped[str]=mapped_column(String(32))
    amount_days: Mapped[Optional[int]]=mapped_column(Integer)
    created_at: Mapped[dt.datetime]=mapped_column(DateTime, default=dt.datetime.utcnow)

class Seller(Base):
    __tablename__="sellers"
    id: Mapped[int]=mapped_column(Integer, primary_key=True, autoincrement=True)
    tg_user_id: Mapped[int]=mapped_column(BigInteger, unique=True, index=True)
    note: Mapped[Optional[str]]=mapped_column(String(255))
    is_active: Mapped[bool]=mapped_column(Boolean, default=True)

Base.metadata.create_all(bind=engine)
with engine.begin() as conn:
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_rel_unique ON relationships (chat_id, user_a_id, user_b_id);
        CREATE UNIQUE INDEX IF NOT EXISTS ix_crush_unique ON crushes (chat_id, from_user_id, to_user_id);
        CREATE UNIQUE INDEX IF NOT EXISTS ix_reply_chat_date_user ON reply_stat_daily (chat_id, date, target_user_id);
        CREATE INDEX IF NOT EXISTS ix_users_chat_username ON users (chat_id, username);
        CREATE UNIQUE INDEX IF NOT EXISTS ix_users_chat_tg ON users (chat_id, tg_user_id);
        CREATE INDEX IF NOT EXISTS ix_ship_chat_date ON ship_history (chat_id, date);
        CREATE UNIQUE INDEX IF NOT EXISTS ix_ga_unique ON group_admins (chat_id, tg_user_id);
    """))

# ---------- HELPERS ----------
def is_seller(session, tg_user_id: int) -> bool:
    try:
        s = session.query(Seller).filter_by(tg_user_id=tg_user_id, is_active=True).first()
        return bool(s)
    except Exception:
        return False

def is_group_admin(session, chat_id: int, tg_user_id: int) -> bool:
    if tg_user_id == OWNER_ID:
        return True
    row = session.execute(select(GroupAdmin).where(GroupAdmin.chat_id==chat_id, GroupAdmin.tg_user_id==tg_user_id)).scalar_one_or_none()
    return bool(row)

def is_operator(session, tg_user_id: int) -> bool:
    return (tg_user_id == OWNER_ID) or is_seller(session, tg_user_id)

T = TypeVar("T")
def chunked(seq: Iterable[T], n: int) -> List[List[T]]:
    buf: List[T] = []; out: List[List[T]] = []
    for x in seq:
        buf.append(x)
        if len(buf) == n: out.append(buf); buf=[]
    if buf: out.append(buf)
    return out

def mention_of(u: "User") -> str:
    name = u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
    return f'<a href="tg://user?id={u.tg_user_id}">{name}</a>'

def footer(text: str) -> str: return text

async def reply_temp(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str,
                     reply_markup: InlineKeyboardMarkup | None = None, keep: bool = False,
                     parse_mode: Optional[str] = None, reply_to_message_id: Optional[int] = None,
                     with_footer: bool = True):
    msg = await update.effective_chat.send_message(
        footer(text) if with_footer else text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        reply_to_message_id=reply_to_message_id,
        disable_web_page_preview=True,
    )
    if not keep:
        jq = context.application.job_queue
        if jq:
            jq.run_once(lambda c: c.bot.delete_message(msg.chat_id, msg.message_id), when=AUTO_DELETE_SECONDS)
    return msg

def ensure_group(session, chat) -> Group:
    g = session.get(Group, chat.id)
    if not g:
        g = Group(id=chat.id, title=getattr(chat, "title", None) or getattr(chat, "full_name", None),
                  timezone=DEFAULT_TZ, is_active=True)
        session.add(g)
    else:
        if getattr(chat, "title", None) and g.title != chat.title:
            g.title = chat.title
    session.flush(); return g

def upsert_user(session, chat_id: int, tg_user) -> User:
    u = session.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tg_user.id)).scalar_one_or_none()
    if not u:
        u = User(chat_id=chat_id, tg_user_id=tg_user.id)
        session.add(u)
    u.first_name = tg_user.first_name or u.first_name
    u.last_name = tg_user.last_name or u.last_name
    u.username = tg_user.username or u.username
    session.flush(); return u

def group_active(g: Group) -> bool:
    if g.expires_at is None: return True
    return g.expires_at > dt.datetime.utcnow()

# ---------- UI ----------
def kb_group_menu(is_group_admin_flag: bool, is_operator_flag: bool) -> List[List[InlineKeyboardButton]]:
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("👤 ثبت جنسیت", callback_data="ui:gset")],
        [InlineKeyboardButton("🎂 ثبت تولد", callback_data="ui:bd:start")],
        [InlineKeyboardButton("💘 ثبت کراش (ریپلای)", callback_data="ui:crush:add"),
         InlineKeyboardButton("🗑️ حذف کراش", callback_data="ui:crush:del")],
        [InlineKeyboardButton("💞 ثبت رابطه (راهنما)", callback_data="ui:rel:help")],
        [InlineKeyboardButton("👑 محبوب امروز", callback_data="ui:pop"),
         InlineKeyboardButton("💫 شیپ امشب", callback_data="ui:ship")],
        [InlineKeyboardButton("❤️ شیپم کن", callback_data="ui:shipme")],
        [InlineKeyboardButton("🏷️ تگ دخترها", callback_data="ui:tag:girls"),
         InlineKeyboardButton("🏷️ تگ پسرها", callback_data="ui:tag:boys")],
        [InlineKeyboardButton("🏷️ تگ همه", callback_data="ui:tag:all")],
        [InlineKeyboardButton("🔐 داده‌های من", callback_data="ui:privacy:me"),
         InlineKeyboardButton("🗑️ حذف من", callback_data="ui:privacy:delme")],
    ]
    if is_group_admin_flag or is_operator_flag:
        rows.append([InlineKeyboardButton("⚙️ پیکربندی فضول", callback_data="cfg:open")])
    return rows

def add_nav(rows: List[List[InlineKeyboardButton]], root: bool = False) -> InlineKeyboardMarkup:
    nav=[InlineKeyboardButton("✖️ بستن", callback_data="nav:close")]
    if not root: nav.insert(0, InlineKeyboardButton("⬅️ بازگشت", callback_data="nav:back"))
    return InlineKeyboardMarkup([nav]+rows)

# ---------- PANELS & STATE ----------
PANELS: Dict[Tuple[int,int], Dict[str, Any]] = {}
REL_WAIT: Dict[Tuple[int,int], Dict[str, Any]] = {}  # (chat_id, actor_tg) -> {"target_user_id": int}

def _panel_key(chat_id: int, message_id: int) -> Tuple[int,int]: return (chat_id, message_id)
def _panel_push(msg, owner_id: int, title: str, rows, root: bool):
    key=_panel_key(msg.chat.id, msg.message_id)
    meta=PANELS.get(key, {"owner": owner_id, "stack":[]})
    meta["owner"]=owner_id; meta["stack"].append((title, rows, root)); PANELS[key]=meta
def _panel_pop(msg):
    key=_panel_key(msg.chat.id, msg.message_id)
    meta=PANELS.get(key); 
    if not meta or not meta["stack"]: return None
    if len(meta["stack"])>1:
        meta["stack"].pop(); prev=meta["stack"][-1]; PANELS[key]=meta; return prev
    return None
def _set_rel_wait(chat_id: int, actor_tg: int, target_user_id: int):
    REL_WAIT[(chat_id, actor_tg)]={"target_user_id": target_user_id}
def _pop_rel_wait(chat_id: int, actor_tg: int):
    return REL_WAIT.pop((chat_id, actor_tg), None)

async def panel_open_initial(update: Update, context: ContextTypes.DEFAULT_TYPE, title: str, rows, root=True, parse_mode=None):
    msg = await update.effective_chat.send_message(footer(title), reply_markup=add_nav(rows, root=root),
                                                   disable_web_page_preview=True, parse_mode=parse_mode)
    _panel_push(msg, update.effective_user.id, title, rows, root)
    return msg

async def panel_edit(context: ContextTypes.DEFAULT_TYPE, qmsg, opener_id: int, title: str, rows, root=False, parse_mode=None):
    await qmsg.edit_text(footer(title), reply_markup=add_nav(rows, root=root),
                         disable_web_page_preview=True, parse_mode=parse_mode)
    _panel_push(qmsg, opener_id, title, rows, root)

# ---------- SINGLETON (PG advisory lock) ----------
SINGLETON_CONN=None; SINGLETON_KEY=None
def _advisory_key() -> int:
    if not TOKEN: return 0
    seed = TOKEN + ("|"+INSTANCE_TAG if INSTANCE_TAG else "")
    return int(hashlib.blake2b(seed.encode(), digest_size=8).hexdigest(), 16) % (2**31)

def _acquire_lock(conn, key: int) -> bool:
    cur=conn.cursor(); cur.execute("SELECT pg_try_advisory_lock(%s)", (key,)); ok=cur.fetchone()[0]; return bool(ok)

def acquire_singleton_or_exit():
    thash = hashlib.blake2b((TOKEN or "").encode(), digest_size=8).hexdigest()
    logging.info("TOKEN hash (last8) = %s", thash)
    logging.info("INSTANCE_TAG = %r", INSTANCE_TAG)

    global SINGLETON_CONN, SINGLETON_KEY
    if DISABLE_SINGLETON:
        logging.warning("⚠️ DISABLE_SINGLETON=1 → singleton guard disabled."); return
    SINGLETON_KEY=_advisory_key(); logging.info(f"Singleton key = {SINGLETON_KEY}")
    try:
        SINGLETON_CONN = engine.raw_connection()
        cur = SINGLETON_CONN.cursor()
        app_name = f"fazolbot:{INSTANCE_TAG or 'bot'}"
        cur.execute("SET application_name = %s", (app_name,))
        logging.info("application_name = %s", app_name)
        ok = _acquire_lock(SINGLETON_CONN, SINGLETON_KEY)
        if not ok:
            logging.error("Another instance is already running (PG advisory lock). Exiting.")
            os._exit(0)
        logging.info("Singleton advisory lock acquired.")
    except Exception as e:
        logging.error(f"Singleton lock failed: {e}"); os._exit(0)

    @atexit.register
    def _unlock():
        try:
            cur=SINGLETON_CONN.cursor(); cur.execute("SELECT pg_advisory_unlock(%s)", (SINGLETON_KEY,)); SINGLETON_CONN.close()
        except Exception: ...

async def singleton_watchdog(context: ContextTypes.DEFAULT_TYPE):
    if DISABLE_SINGLETON: return
    global SINGLETON_CONN, SINGLETON_KEY
    try:
        cur=SINGLETON_CONN.cursor(); cur.execute("SELECT 1"); cur.fetchone(); return
    except Exception as e:
        logging.warning(f"Singleton ping failed: {e}")
        try:
            try: SINGLETON_CONN.close()
            except Exception: ...
            SINGLETON_CONN=engine.raw_connection()
            cur=SINGLETON_CONN.cursor()
            app_name = f"fazolbot:{INSTANCE_TAG or 'bot'}"
        cur.execute("SET application_name = %s", (app_name,))
        logging.info("application_name = %s", app_name)
            cur.execute("SELECT pg_try_advisory_lock(%s)", (SINGLETON_KEY,)); ok=cur.fetchone()[0]
            if not ok: logging.error("Lost advisory lock, another instance holds it. Exiting."); os._exit(0)
            logging.info("Advisory lock re-acquired.")
        except Exception as e2:
            logging.error(f"Failed to re-acquire advisory lock: {e2}")

# ---------- HELP TEXT ----------
def user_help_text() -> str:
    return (
        "📘 راهنمای سریع:\n"
        "• «فضول» → تست سلامت\n"
        "• «فضول منو» → منوی دکمه‌ای\n"
        "• «ثبت جنسیت دختر/پسر» (ادمین: با ریپلای برای دیگران)\n"
        "• «ثبت تولد ۱۴۰۳/۰۵/۲۰» (ادمین: با ریپلای برای دیگران)\n"
        "• «ثبت رابطه» (ریپلای/@/آیدی) → ویزارد تاریخ\n"
        "• «کراشام» → لیست کراش‌ها\n"
        "• «داده‌های من» → پروفایل کامل + محبوبیت\n"
        "• «محبوب امروز»، «شیپم کن»، «شیپ امشب»\n"
    )

# ---------- CALLBACKS ----------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query
    if not q or not q.message: return
    await q.answer(); data=q.data or ""; msg=q.message
    user_id=q.from_user.id; chat_id=msg.chat.id; key=(chat_id, msg.message_id)

    meta=PANELS.get(key)
    if not meta: PANELS[key]={"owner": user_id, "stack":[]}; meta=PANELS[key]
    owner_id=meta.get("owner")
    if owner_id is not None and owner_id != user_id:
        await q.answer("این منو مخصوص کسی است که آن را باز کرده.", show_alert=True); return

    if data=="nav:close":
        try: await msg.delete()
        except Exception: ...
        PANELS.pop(key, None); return
    if data=="nav:back":
        prev=_panel_pop(msg)
        if not prev:
            try: await msg.delete()
            except Exception: ...
            PANELS.pop(key, None); return
        title, rows, root=prev; await panel_edit(context, msg, user_id, title, rows, root=root); return

    # config open (admins OR operators can open; but actions limited later)
    if data=="cfg:open":
        with SessionLocal() as s:
            gadmin = is_group_admin(s, chat_id, user_id)
            oper = is_operator(s, user_id)
            if not (gadmin or oper):
                await panel_edit(context, msg, user_id, "دسترسی نداری.",
                                 [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False)
                return
        rows=[
            [InlineKeyboardButton("⚡️ شارژ گروه", callback_data="ui:charge:open")],
            [InlineKeyboardButton("👥 مدیران گروه", callback_data="ga:list")],
            [InlineKeyboardButton("ℹ️ مشاهده انقضا", callback_data="ui:expiry")],
            [InlineKeyboardButton("🧹 پاکسازی گروه", callback_data=f"wipe:{chat_id}")],
        ]
        await panel_edit(context, msg, user_id, "⚙️ پیکربندی فضول", rows, root=False); return

    if data=="ga:list":
        with SessionLocal() as s:
            gas = s.query(GroupAdmin).filter_by(chat_id=chat_id).all()
            if not gas: txt="ادمینی ثبت نشده."
            else:
                mentions=[]
                for ga in gas[:50]:
                    u = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==ga.tg_user_id)).scalar_one_or_none()
                    if u: mentions.append(mention_of(u))
                txt="👥 ادمین‌های فضول:\n"+"\n".join(f"- {m}" for m in mentions)
        await panel_edit(context, msg, user_id, txt, [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False, parse_mode=ParseMode.HTML); return

    if data=="ui:expiry":
        with SessionLocal() as s:
            g=s.get(Group, chat_id); ex=g and g.expires_at and fmt_dt_fa(g.expires_at)
        await panel_edit(context, msg, user_id, f"⏳ اعتبار گروه تا: {ex or 'نامشخص'}",
                         [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False); return

    if data=="ui:charge:open":
        with SessionLocal() as s:
            if not is_operator(s, user_id):
                await panel_edit(context, msg, user_id, "فقط مالک/فروشنده مجاز است.",
                                 [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False); return
        kb=[[InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{chat_id}:30"),
             InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{chat_id}:90"),
             InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{chat_id}:180")]]
        await panel_edit(context, msg, user_id, "⌁ پنل شارژ گروه", kb, root=False); return

    m=re.match(r"^chg:(-?\d+):(\d+)$", data)
    if m:
        target_chat=int(m.group(1)); days=int(m.group(2))
        with SessionLocal() as s:
            if not is_operator(s, user_id):
                await panel_edit(context, msg, user_id, "فقط مالک/فروشنده مجاز است.",
                                 [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False); return
            g=s.get(Group, target_chat)
            if not g:
                await panel_edit(context, msg, user_id, "گروه پیدا نشد.",
                                 [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False); return
            base = g.expires_at if g.expires_at and g.expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
            g.expires_at = base + dt.timedelta(days=days)
            s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=user_id, action="extend", amount_days=days))
            s.commit()
            await panel_edit(context, msg, user_id, f"✅ تمدید شد تا {fmt_dt_fa(g.expires_at)}",
                             [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    m=re.match(r"^wipe:(-?\d+)$", data)
    if m:
        target_chat=int(m.group(1))
        with SessionLocal() as s:
            if not is_operator(s, user_id):
                await panel_edit(context, msg, user_id, "فقط مالک/فروشنده مجاز است.",
                                 [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False); return
            s.execute(Crush.__table__.delete().where(Crush.chat_id==target_chat))
            s.execute(Relationship.__table__.delete().where(Relationship.chat_id==target_chat))
            s.execute(ReplyStatDaily.__table__.delete().where(ReplyStatDaily.chat_id==target_chat))
            s.execute(User.__table__.delete().where(User.chat_id==target_chat))
            s.commit()
        await panel_edit(context, msg, user_id, "🧹 پاکسازی انجام شد.",
                         [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False); return

    # gender set
    if data=="ui:gset":
        rows=[[InlineKeyboardButton("👧 دختر", callback_data="gset:f")],
              [InlineKeyboardButton("👦 پسر", callback_data="gset:m")]]
        await panel_edit(context, msg, user_id, "جنسیتت چیه؟", rows, root=False); return
    if data.startswith("gset:"):
        is_female=data.endswith(":f")
        with SessionLocal() as s:
            u = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==user_id)).scalar_one_or_none()
            if not u: u=User(chat_id=chat_id, tg_user_id=user_id); s.add(u)
            u.gender="female" if is_female else "male"; s.commit()
        await panel_edit(context, msg, user_id, "ثبت شد ✅",
                         [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False); return

    # birthday wizard
    if data=="ui:bd:start":
        y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
        for ch in chunked(years, 4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"bd:y:{yy}") for yy in ch])
        rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"bd:yp:{y-16}")])
        await panel_edit(context, msg, user_id, "سال تولدت رو انتخاب کن", rows, root=False); return

    m=re.match(r"^bd:yp:(\d+)$", data)
    if m:
        start=int(m.group(1)); years=list(range(start, start-16, -1)); rows=[]
        for ch in chunked(years,4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"bd:y:{yy}") for yy in ch])
        rows.append([InlineKeyboardButton("قدیمی‌تر", callback_data=f"bd:yp:{start-16}")])
        await panel_edit(context, msg, user_id, "سال تولدت رو انتخاب کن", rows, root=False); return

    m=re.match(r"^bd:y:(\d+)$", data)
    if m:
        yy=int(m.group(1)); rows=[]
        for i in range(1,13): rows.append([InlineKeyboardButton(fa_digits(f"{i:02d}"), callback_data=f"bd:m:{yy}:{i}")])
        await panel_edit(context, msg, user_id, f"سال {fa_digits(yy)} — ماه تولد را انتخاب کن", rows, root=False); return

    m=re.match(r"^bd:m:(\d+):(\d+)$", data)
    if m:
        yy=int(m.group(1)); mm=int(m.group(2)); md=jalali_month_len(yy,mm); rows=[]
        for ch in chunked(list(range(1,md+1)),6):
            rows.append([InlineKeyboardButton(fa_digits(f"{d:02d}"), callback_data=f"bd:d:{yy}:{mm}:{d}") for d in ch])
        await panel_edit(context, msg, user_id, f"تاریخ: {fa_digits(yy)}/{fa_digits(mm)} — روز را انتخاب کن", rows, root=False); return

    m=re.match(r"^bd:d:(\d+):(\d+):(\d+)$", data)
    if m:
        yy,mm,dd=(int(m.group(1)),int(m.group(2)),int(m.group(3)))
        try:
            gdate = JalaliDate(yy,mm,dd).to_gregorian() if HAS_PTOOLS else dt.date(2000+yy%100, mm, dd)
        except Exception:
            await panel_edit(context, msg, user_id, "تاریخ نامعتبر شد. دوباره تلاش کن.", [[InlineKeyboardButton("برگشت", callback_data="ui:bd:start")]], root=False); return
        with SessionLocal() as s:
            u = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==user_id)).scalar_one_or_none()
            if not u: u=User(chat_id=chat_id, tg_user_id=user_id); s.add(u)
            u.birthday=gdate; s.commit()
        await panel_edit(context, msg, user_id, f"🎂 تولد ثبت شد: {fmt_date_fa(gdate)}",
                         [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False); return

    # relationship date picker
    m=re.match(r"^rel:yp:(\d+)$", data)
    if m:
        start=int(m.group(1)); years=list(range(start, start-16, -1)); rows=[]
        for ch in chunked(years,4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
        rows.append([InlineKeyboardButton("قدیمی‌تر", callback_data=f"rel:yp:{start-16}")])
        await panel_edit(context, msg, user_id, "شروع رابطه — سال را انتخاب کن", rows, root=False); return

    m=re.match(r"^rel:y:(\d+)$", data)
    if m:
        yy=int(m.group(1)); rows=[]
        for i in range(1,13): rows.append([InlineKeyboardButton(fa_digits(f"{i:02d}"), callback_data=f"rel:m:{yy}:{i}")])
        await panel_edit(context, msg, user_id, f"سال {fa_digits(yy)} — ماه را انتخاب کن", rows, root=False); return

    m=re.match(r"^rel:m:(\d+):(\d+)$", data)
    if m:
        yy=int(m.group(1)); mm=int(m.group(2)); md=jalali_month_len(yy,mm); rows=[]
        for ch in chunked(list(range(1,md+1)),6):
            rows.append([InlineKeyboardButton(fa_digits(f"{d:02d}"), callback_data=f"rel:d:{yy}:{mm}:{d}") for d in ch])
        await panel_edit(context, msg, user_id, f"تاریخ: {fa_digits(yy)}/{fa_digits(mm)} — روز را انتخاب کن", rows, root=False); return

    m=re.match(r"^rel:d:(\d+):(\d+):(\d+)$", data)
    if m:
        yy,mm,dd=(int(m.group(1)),int(m.group(2)),int(m.group(3)))
        wait=_pop_rel_wait(chat_id, user_id)
        if not wait:
            await panel_edit(context, msg, user_id, "درخواستی برای ثبت رابطه پیدا نشد. از نو شروع کن: «ثبت رابطه»",
                             [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False); return
        try:
            gdate = JalaliDate(yy,mm,dd).to_gregorian() if HAS_PTOOLS else dt.date(2000+yy%100, mm, dd)
        except Exception:
            await panel_edit(context, msg, user_id, "تاریخ نامعتبر شد. دوباره تلاش کن.",
                             [[InlineKeyboardButton("بازگشت", callback_data="nav:back")]], root=False); return
        with SessionLocal() as s2:
            g = s2.get(Group, chat_id)
            me = s2.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==user_id)).scalar_one_or_none()
            other = s2.get(User, wait["target_user_id"])
            if not (g and me and other):
                await panel_edit(context, msg, user_id, "کاربر/گروه پیدا نشد.", [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False); return
            a_id, b_id = sorted([me.id, other.id])
            old = s2.execute(select(Relationship).where(Relationship.chat_id==chat_id, Relationship.user_a_id==a_id, Relationship.user_b_id==b_id)).scalar_one_or_none()
            if not old: old=Relationship(chat_id=chat_id, user_a_id=a_id, user_b_id=b_id); s2.add(old)
            old.started_at=gdate; s2.commit()
        await panel_edit(context, msg, user_id, f"✅ رابطه ثبت شد از {fmt_date_fa(gdate)}",
                         [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False); return

    # quick UI helper tips
    if data in ("ui:crush:add","ui:crush:del","ui:rel:help","ui:tag:girls","ui:tag:boys","ui:tag:all","ui:pop","ui:ship","ui:privacy:me","ui:privacy:delme","ui:shipme"):
        hints={
            "ui:crush:add":"برای «ثبت کراش»، روی پیام شخص ریپلای کن و بنویس «ثبت کراش».",
            "ui:crush:del":"برای «حذف کراش»، روی پیام شخص ریپلای کن و بنویس «حذف کراش».",
            "ui:rel:help":"«ثبت رابطه» را با ریپلای/یوزرنیم/@ID بزن؛ بعد تاریخ را از کیبورد انتخاب کن.",
            "ui:tag:girls":"برای «تگ دخترها»، روی یک پیام ریپلای کن و بنویس: تگ دخترها",
            "ui:tag:boys":"برای «تگ پسرها»، روی یک پیام ریپلای کن و بنویس: تگ پسرها",
            "ui:tag:all":"برای «تگ همه»، روی یک پیام ریپلای کن و بنویس: تگ همه",
            "ui:pop":"برای «محبوب امروز»، همین دستور را در گروه بزن.",
            "ui:ship":"«شیپ امشب» آخر شب خودکار ارسال می‌شود.",
            "ui:shipme":"«شیپم کن» را در گروه بزن تا یک پارتنر پیشنهادی معرفی شود.",
            "ui:privacy:me":"برای «داده‌های من»، همین دستور را در گروه بزن.",
            "ui:privacy:delme":"برای «حذف من»، همین دستور را در گروه بزن.",
        }
        await panel_edit(context, msg, user_id, hints.get(data,"اوکی"),
                         [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False); return

    await panel_edit(context, msg, user_id, "دستور ناشناخته یا منقضی.",
                     [[InlineKeyboardButton("بازگشت", callback_data="nav:back")]], root=False)

# ---------- GROUP TEXT ----------
async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group","supergroup") or not update.message or not update.message.text: return
    text = clean_text(update.message.text)

    if RE_WORD_FAZOL.search(text):
        if "منو" in text or "فهرست" in text:
            with SessionLocal() as s:
                g=ensure_group(s, update.effective_chat)
                is_gadmin = is_group_admin(s, g.id, update.effective_user.id)
                oper = is_operator(s, update.effective_user.id)
            title="🕹 منوی فضول"
            rows=kb_group_menu(is_gadmin, oper)
            await panel_open_initial(update, context, title, rows, root=True); return
        if "کمک" in text or "راهنما" in text:
            await reply_temp(update, context, user_help_text()); return

    with SessionLocal() as s:
        g=ensure_group(s, update.effective_chat)
        me=upsert_user(s, g.id, update.effective_user)

    # gender
    m=re.match(r"^ثبت جنسیت (دختر|پسر)$", text)
    if m:
        gender_fa=m.group(1)
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat)
            if update.message.reply_to_message and is_group_admin(s, g.id, update.effective_user.id):
                target=upsert_user(s, g.id, update.message.reply_to_message.from_user)
            else:
                target=upsert_user(s, g.id, update.effective_user)
            target.gender = "female" if gender_fa=="دختر" else "male"
            s.commit()
            who="خودت" if target.tg_user_id==update.effective_user.id else f"{mention_of(target)}"
            await reply_temp(update, context, f"👤 جنسیت {who} ثبت شد: {'👧 دختر' if target.gender=='female' else '👦 پسر'}", parse_mode=ParseMode.HTML)
        return

    # relationship command (reply / @ / id) -> year picker
    m=re.match(r"^ثبت رابطه(?:\s+(.+))?$", text)
    if m:
        selector=(m.group(1) or "").strip()
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
            target_user=None
            if update.message.reply_to_message:
                target_user=upsert_user(s2, g.id, update.message.reply_to_message.from_user)
            elif selector:
                if selector.startswith("@"):
                    target_user=s2.execute(select(User).where(User.chat_id==g.id, User.username==selector[1:])).scalar_one_or_none()
                else:
                    try:
                        tgid=int(selector)
                        target_user=s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tgid)).scalar_one_or_none()
                    except Exception: target_user=None
            if not target_user:
                await reply_temp(update, context, "طرف مقابل پیدا نشد. با ریپلای یا @یوزرنیم یا آیدی عددی دوباره امتحان کن."); return
            if target_user.id==me.id:
                await reply_temp(update, context, "نمی‌تونی با خودت رابطه ثبت کنی."); return
            _set_rel_wait(g.id, me.tg_user_id, target_user.id)
            y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
            for ch in chunked(years,4):
                rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
            rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{y-16}")])
            await reply_temp(update, context, "شروع رابطه — سال را انتخاب کن", reply_markup=InlineKeyboardMarkup(rows), keep=True)
        return

    # birthday set
    m=re.match(r"^ثبت تولد ([\d\/\-]+)$", text)
    if m:
        date_str=m.group(1)
        try:
            ss=fa_to_en_digits(date_str).replace("/","-"); y,mn,d=(int(x) for x in ss.split("-"))
            if HAS_PTOOLS: gdate=JalaliDate(y,mn,d).to_gregorian()
            else: gdate=dt.date(2000 + (y%100), mn, d)
        except Exception:
            await reply_temp(update, context, "فرمت تاریخ نامعتبر است. نمونه: «ثبت تولد ۱۴۰۳/۰۵/۲۰»"); return
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat)
            if update.message.reply_to_message and is_group_admin(s, g.id, update.effective_user.id):
                target=upsert_user(s, g.id, update.message.reply_to_message.from_user)
            else:
                target=upsert_user(s, g.id, update.effective_user)
            target.birthday=gdate; s.commit()
            who="خودت" if target.tg_user_id==update.effective_user.id else f"{mention_of(target)}"
            await reply_temp(update, context, f"🎂 تولد {who} ثبت شد: {fmt_date_fa(gdate)}", parse_mode=ParseMode.HTML)
        return

    # my crushes
    if text=="کراشام":
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
            rows=s2.query(Crush).filter_by(chat_id=g.id, from_user_id=me.id).all()
            if not rows:
                await reply_temp(update, context, "هنوز کراشی ثبت نکردی."); return
            names=[]
            for r in rows[:20]:
                u=s2.get(User, r.to_user_id)
                if u: names.append(u.first_name or (u.username and "@"+u.username) or str(u.tg_user_id))
            await reply_temp(update, context, "💘 کراش‌های تو:\n" + "\n".join(f"- {n}" for n in names), keep=True)
        return

    # my data
    if text in ("داده های من","داده‌های من"):
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat)
            me=s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==update.effective_user.id)).scalar_one_or_none()
            if not me:
                await reply_temp(update, context, "چیزی از شما ذخیره نشده."); return
            my_crushes=s2.query(Crush).filter_by(chat_id=g.id, from_user_id=me.id).all()
            crush_list=[]
            for r in my_crushes[:20]:
                u=s2.get(User, r.to_user_id)
                if u: crush_list.append(u.first_name or (u.username and "@"+u.username) or str(u.tg_user_id))
            rel=s2.query(Relationship).filter_by(chat_id=g.id).filter((Relationship.user_a_id==me.id)|(Relationship.user_b_id==me.id)).first()
            rel_txt="-"
            if rel:
                other_id = rel.user_b_id if rel.user_a_id==me.id else rel.user_a_id
                other = s2.get(User, other_id)
                other_name = other and (other.first_name or (other.username and "@"+other.username) or str(other.tg_user_id))
                rel_txt = f"{other_name or '-'} از {fmt_date_fa(rel.started_at) if rel.started_at else '-'}"
            today=dt.datetime.now(TZ_TEHRAN).date()
            my_row=s2.execute(select(ReplyStatDaily).where(ReplyStatDaily.chat_id==g.id, ReplyStatDaily.date==today, ReplyStatDaily.target_user_id==me.id)).scalar_one_or_none()
            max_row=s2.execute(select(ReplyStatDaily).where(ReplyStatDaily.chat_id==g.id, ReplyStatDaily.date==today).order_by(ReplyStatDaily.reply_count.desc()).limit(1)).scalar_one_or_none()
            score=0
            if my_row and max_row and max_row.reply_count>0:
                score=round(10 * my_row.reply_count / max_row.reply_count)
            info=(
                f"👤 نام: {me.first_name or ''} @{me.username or ''}\n"
                f"جنسیت: {'دختر' if me.gender=='female' else ('پسر' if me.gender=='male' else 'نامشخص')}\n"
                f"تولد: {fmt_date_fa(me.birthday)}\n"
                f"کراش‌ها: {', '.join(crush_list) if crush_list else '-'}\n"
                f"رابطه: {rel_txt}\n"
                f"محبوبیت امروز: {score}/10"
            )
            await reply_temp(update, context, info, keep=True)
        return

    # popular today
    if text=="محبوب امروز":
        today=dt.datetime.now(TZ_TEHRAN).date()
        with SessionLocal() as s2:
            rows=s2.execute(select(ReplyStatDaily).where((ReplyStatDaily.chat_id==update.effective_chat.id)&(ReplyStatDaily.date==today)).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
        if not rows:
            await reply_temp(update, context, "امروز هنوز آماری نداریم.", keep=True); return
        lines=[]
        with SessionLocal() as s3:
            for i,r in enumerate(rows, start=1):
                u=s3.get(User, r.target_user_id)
                name=u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
        await reply_temp(update, context, "\n".join(lines), keep=True); return

    # ship today
    if text=="شیپ امشب":
        today=dt.datetime.now(TZ_TEHRAN).date()
        with SessionLocal() as s2:
            last=s2.execute(select(ShipHistory).where((ShipHistory.chat_id==update.effective_chat.id)&(ShipHistory.date==today)).order_by(ShipHistory.id.desc())).scalar_one_or_none()
        if not last:
            await reply_temp(update, context, "هنوز شیپ امشب ساخته نشده. آخر شب منتشر می‌شه 💫", keep=True); return
        with SessionLocal() as s3:
            muser, fuser = s3.get(User,last.male_user_id), s3.get(User,last.female_user_id)
        await reply_temp(update, context, f"💘 شیپِ امشب: {(muser.first_name or '@'+(muser.username or ''))} × {(fuser.first_name or '@'+(fuser.username or ''))}", keep=True); return

    # ship me
    if text=="شیپم کن":
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat); me=upsert_user(s,g.id,update.effective_user)
            if me.gender not in ("male","female"):
                await reply_temp(update, context, "اول جنسیتت رو ثبت کن: «ثبت جنسیت دختر/پسر»."); return
            rels=s.query(Relationship).filter_by(chat_id=g.id).all()
            in_rel=set([r.user_a_id for r in rels]+[r.user_b_id for r in rels])
            if me.id in in_rel:
                await reply_temp(update, context, "تو در رابطه‌ای. برای پیشنهاد باید سینگل باشی."); return
            opposite="female" if me.gender=="male" else "male"
            candidates=s.query(User).filter_by(chat_id=g.id, gender=opposite).all()
            candidates=[u for u in candidates if u.id not in in_rel and u.tg_user_id!=me.tg_user_id]
            if not candidates:
                await reply_temp(update, context, "کسی از جنس مخالفِ سینگل پیدا نشد."); return
            cand=random.choice(candidates)
            await reply_temp(update, context, f"❤️ پارتنر پیشنهادی برای شما: {mention_of(cand)}", keep=True, parse_mode=ParseMode.HTML); return

    # privacy info (short) kept for menu's quick action
    if text in ("حریم خصوصی","داده های من کوتاه"):
        with SessionLocal() as s2:
            u=s2.execute(select(User).where(User.chat_id==update.effective_chat.id, User.tg_user_id==update.effective_user.id)).scalar_one_or_none()
            if not u: await reply_temp(update, context, "چیزی از شما ذخیره نشده."); return
            info=f"👤 نام: {u.first_name or ''} @{u.username or ''}\nجنسیت: {u.gender}\nتولد: {fmt_date_fa(u.birthday)}"
        await reply_temp(update, context, info); return

    if text=="حذف من":
        with SessionLocal() as s2:
            u=s2.execute(select(User).where(User.chat_id==update.effective_chat.id, User.tg_user_id==update.effective_user.id)).scalar_one_or_none()
            if not u: await reply_temp(update, context, "اطلاعاتی از شما نداریم."); return
            s2.execute(Crush.__table__.delete().where((Crush.chat_id==update.effective_chat.id)&((Crush.from_user_id==u.id)|(Crush.to_user_id==u.id))))
            s2.execute(Relationship.__table__.delete().where((Relationship.chat_id==update.effective_chat.id)&((Relationship.user_a_id==u.id)|(Relationship.user_b_id==u.id))))
            s2.execute(ReplyStatDaily.__table__.delete().where((ReplyStatDaily.chat_id==update.effective_chat.id)&(ReplyStatDaily.target_user_id==u.id)))
            s2.execute(User.__table__.delete().where((User.chat_id==update.effective_chat.id)&(User.id==u.id)))
            s2.commit()
        await reply_temp(update, context, "✅ تمام داده‌های شما در این گروه حذف شد."); return

    # reply counting
    if update.message.reply_to_message:
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat)
            today=dt.datetime.now(TZ_TEHRAN).date()
            target=upsert_user(s, g.id, update.message.reply_to_message.from_user)
            upsert_user(s, g.id, update.effective_user)
            row=s.execute(select(ReplyStatDaily).where((ReplyStatDaily.chat_id==g.id)&(ReplyStatDaily.date==today)&(ReplyStatDaily.target_user_id==target.id))).scalar_one_or_none()
            if not row: row=ReplyStatDaily(chat_id=g.id, date=today, target_user_id=target.id, reply_count=0); s.add(row)
            row.reply_count += 1; s.commit()

# ---------- PRIVATE ----------
async def on_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type!="private" or not update.message or not update.message.text: return
    text=clean_text(update.message.text)
    bot_username=context.bot.username
    with SessionLocal() as s:
        uid=update.effective_user.id; seller=is_seller(s, uid)
        if uid!=OWNER_ID and not seller:
            if text in ("/start","start","کمک","راهنما"):
                txt=("سلام! 👋 من «فضول»م، ربات اجتماعی گروه‌های فارسی.\n"
                     "• منو و امکانات داخل گروه فعال می‌شن.\n"
                     "• برای شروع، منو رو با «فضول منو» باز کن.")
                rows=[[InlineKeyboardButton("➕ افزودن به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
                      [InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]]
                await reply_temp(update, context, txt, reply_markup=InlineKeyboardMarkup(rows), keep=True); return
            await reply_temp(update, context, "برای مدیریت باید مالک/فروشنده باشی. «/start» یا «کمک» بزن."); return

        # owner/seller panel
        if text in ("/start","start","پنل","مدیریت","کمک"):
            who = "👑 پنل مالک" if uid==OWNER_ID else "🛍️ پنل فروشنده"
            rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
                  [InlineKeyboardButton("➕ افزودن به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
                  [InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]]
            await panel_open_initial(update, context, who, rows, root=True); return

# ---------- CHAT MEMBER ----------
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat=update.my_chat_member.chat if update.my_chat_member else None
        if not chat: return
        with SessionLocal() as s: ensure_group(s, chat); s.commit()
    except Exception as e: logging.info(f"on_my_chat_member err: {e}")

# ---------- /start in groups ----------
async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username=context.bot.username
    if update.effective_chat.type!="private":
        txt=("سلام! من روشنم ✅\n"
             "• «فضول» → جانم (تست سلامت)\n"
             "• «فضول منو» → منوی دکمه‌ای\n"
             "• «فضول کمک» → راهنما")
        await reply_temp(update, context, txt); return
    # private handled in on_private_text

# ---------- ERROR ----------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err=context.error
    if isinstance(err, TgConflict):
        try:
            if OWNER_ID:
                await context.bot.send_message(OWNER_ID, "⚠️ Conflict 409: نمونهٔ دیگری از ربات در حال polling است. این نمونه خارج شد.")
        except Exception: ...
        logging.error("Conflict 409 detected. Exiting."); os._exit(0)
    logging.exception("Unhandled error", exc_info=err)

# ---------- ANY ----------
async def on_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m=update.effective_message
    if not m: return
    txt=clean_text((m.text or m.caption or "") or "")
    if txt=="فضول":
        try: await m.reply_text("جانم 👂")
        except Exception: ...

# ---------- JOBS ----------
async def job_midnight(context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as s:
        groups=s.query(Group).all(); today=dt.datetime.now(TZ_TEHRAN).date()
        for g in groups:
            if not group_active(g): continue
            # popular today
            top=s.execute(select(ReplyStatDaily).where((ReplyStatDaily.chat_id==g.id)&(ReplyStatDaily.date==today)).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if top:
                lines=[]
                for i,r in enumerate(top, start=1):
                    u=s.get(User, r.target_user_id)
                    name=u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                    lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
                try: await context.bot.send_message(g.id, footer("🌙 محبوب‌های امروز:\n"+"\n".join(lines)))
                except Exception: ...
            # ship tonight
            males=s.query(User).filter_by(chat_id=g.id, gender="male").all()
            females=s.query(User).filter_by(chat_id=g.id, gender="female").all()
            rels=s.query(Relationship).filter_by(chat_id=g.id).all()
            in_rel=set([r.user_a_id for r in rels]+[r.user_b_id for r in rels])
            males=[u for u in males if u.id not in in_rel]; females=[u for u in females if u.id not in in_rel]
            if males and females:
                muser=random.choice(males); fuser=random.choice(females)
                s.add(ShipHistory(chat_id=g.id, date=today, male_user_id=muser.id, female_user_id=fuser.id)); s.commit()
                try:
                    await context.bot.send_message(g.id, footer(f"💘 شیپِ امشب: {(muser.first_name or '@'+(muser.username or ''))} × {(fuser.first_name or '@'+(fuser.username or ''))}"))
                except Exception: ...

async def job_morning(context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as s:
        groups=s.query(Group).all(); jy,jm,jd=today_jalali()
        for g in groups:
            if not group_active(g): continue
            bdays=s.query(User).filter_by(chat_id=g.id).filter(User.birthday.isnot(None)).all()
            for u in bdays:
                um,ud=to_jalali_md(u.birthday)
                if um==jm and ud==jd:
                    try: await context.bot.send_message(g.id, footer(f"🎉🎂 تولدت مبارک {(u.first_name or '@'+(u.username or ''))}! ({fmt_date_fa(u.birthday)})"))
                    except Exception: ...
            rels=s.query(Relationship).filter_by(chat_id=g.id).all()
            for r in rels:
                if not r.started_at: continue
                rm, rd = to_jalali_md(r.started_at)
                if rd==jd:
                    ua, ub = s.get(User, r.user_a_id), s.get(User, r.user_b_id)
                    try: await context.bot.send_message(g.id, footer(f"💞 ماهگرد {(ua.first_name or '@'+(ua.username or ''))} و {(ub.first_name or '@'+(ub.username or ''))} مبارک! ({fmt_date_fa(r.started_at)})"))
                    except Exception: ...

# ---------- POST INIT ----------
async def _post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        logging.info("Webhook deleted. Polling is active.")
    except Exception as e:
        logging.warning(f"post_init webhook delete failed: {e}")
    logging.info(f"PersianTools enabled: {HAS_PTOOLS}")

# ---------- MAIN ----------
def main():
    if not TOKEN: raise RuntimeError("TELEGRAM_TOKEN env var is required.")
    acquire_singleton_or_exit()

    app = Application.builder().token(TOKEN).post_init(_post_init).build()

    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_group_text))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_private_text))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_error_handler(error_handler)

    app.add_handler(MessageHandler(filters.ALL, on_any), group=100)

    jq=app.job_queue
    if jq:
        jq.run_daily(job_morning, time=dt.time(6,0,0,tzinfo=TZ_TEHRAN))
        jq.run_daily(job_midnight, time=dt.time(0,1,0,tzinfo=TZ_TEHRAN))
        jq.run_repeating(singleton_watchdog, interval=60, first=60)

    logging.info("FazolBot running in POLLING mode…")
    allowed=["message","edited_message","callback_query","my_chat_member","chat_member","chat_join_request"]
    app.run_polling(allowed_updates=allowed, drop_pending_updates=True)

if __name__ == "__main__":
    main()
