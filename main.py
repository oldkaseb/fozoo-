
# -*- coding: utf-8 -*-
# Fazol Bot — complete build
# Features:
# - Relationship wizard (step-by-step): pick user (list/search/reply/@/id) → pick date (Jalali: year→month→day)
# - Crush add/remove + "کراشام"
# - "داده‌های من" (gender, birthday, crushes, relationship, popularity /10)
# - Owner panel (groups & sellers), "پنل اینجا" in-group quick panel
# - Group charge & wipe (owner/seller only), textual "فضول شارژ"
# - Menus hide admin-only options for normal users
# - Owner reports to PV
# - Polling mode with webhook deletion, PG advisory singleton
# Requires: python-telegram-bot[job-queue]>=21, SQLAlchemy, psycopg[binary], persiantools

import os
import re
# -*- coding: utf-8 -*-
# Fazol Bot — complete build
# Features:
# - Relationship wizard (step-by-step): pick user (list/search/reply/@/id) → pick date (Jalali: year→month→day)
# - Crush add/remove + "کراشام"
# - "داده‌های من" (gender, birthday, crushes, relationship, popularity /10)
# - Owner panel (groups & sellers), "پنل اینجا" in-group quick panel
# - Group charge & wipe (owner/seller only), textual "فضول شارژ"
# - Menus hide admin-only options for normal users
# - Owner reports to PV
# - Polling mode with webhook deletion, PG advisory singleton
# Requires: python-telegram-bot[job-queue]>=21, SQLAlchemy, psycopg[binary], persiantools

import os
import re
import random
import logging
import asyncio
import atexit
import hashlib
import datetime as dt
import time
import urllib.parse as _up
from typing import Optional, List, Tuple, Dict, Any, Iterable, TypeVar

from zoneinfo import ZoneInfo

from sqlalchemy import (
    create_engine, select, text, Integer, BigInteger, String, DateTime,
    Date, Boolean, JSON, ForeignKey, Index, func
)
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application, MessageHandler, CallbackQueryHandler, ChatMemberHandler,
    CommandHandler, filters, ContextTypes
)
from telegram.error import Conflict as TgConflict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)

TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
INSTANCE_TAG = os.getenv("INSTANCE_TAG", "").strip()

DEFAULT_TZ = "Asia/Tehran"
TZ_TEHRAN = ZoneInfo(DEFAULT_TZ)

OWNER_CONTACT_USERNAME = os.getenv("OWNER_CONTACT", "soulsownerbot")
AUTO_DELETE_SECONDS = int(os.getenv("AUTO_DELETE_SECONDS", "40"))
TTL_WAIT_SECONDS = int(os.getenv("TTL_WAIT_SECONDS", "1800"))  # 30 min
TTL_PANEL_SECONDS = int(os.getenv("TTL_PANEL_SECONDS", "7200"))  # 2 hours
DISABLE_SINGLETON = os.getenv("DISABLE_SINGLETON", "0").strip().lower() in ("1","true","yes")

Base = declarative_base()

try:
    from persiantools.jdatetime import JalaliDateTime, JalaliDate
    from persiantools import digits as _digits
    HAS_PTOOLS = True
except Exception:
    HAS_PTOOLS = False

def fa_digits(x: str) -> str:
    s=str(x)
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

engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=300, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

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
    last_seen: Mapped[Optional[dt.datetime]]=mapped_column(DateTime)
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
    conn.execute(text("ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS last_seen timestamp"))
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


def build_profile_caption(s, g, me) -> str:
    my_crushes = s.query(Crush).filter_by(chat_id=g.id, from_user_id=me.id).all()
    crush_list = []
    for r in my_crushes[:20]:
        u = s.get(User, r.to_user_id)
        if u: crush_list.append(mention_of(u))
    rel = s.query(Relationship).filter_by(chat_id=g.id).filter((Relationship.user_a_id==me.id)|(Relationship.user_b_id==me.id)).first()
    rel_txt = "-"
    if rel:
        other_id = rel.user_b_id if rel.user_a_id==me.id else rel.user_a_id
        other = s.get(User, other_id)
        other_name = other and mention_of(other)
        if other_name:
            rel_txt = f"{other_name} — از {fmt_date_fa(rel.started_at)}"
    today=dt.datetime.now(TZ_TEHRAN).date()
    my_row=s.execute(select(ReplyStatDaily).where(ReplyStatDaily.chat_id==g.id, ReplyStatDaily.date==today, ReplyStatDaily.target_user_id==me.id)).scalar_one_or_none()
    max_row=s.execute(select(ReplyStatDaily).where(ReplyStatDaily.chat_id==g.id, ReplyStatDaily.date==today).order_by(ReplyStatDaily.reply_count.desc()).limit(1)).scalar_one_or_none()
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
    return info

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
            jq.run_once(lambda c: asyncio.create_task(c.bot.delete_message(msg.chat_id, msg.message_id)), when=AUTO_DELETE_SECONDS)
    return msg

def ensure_group(session, chat) -> "Group":
    g = session.get(Group, chat.id)
    if not g:
        g = Group(id=chat.id, title=getattr(chat, "title", None) or getattr(chat, "full_name", None),
                  timezone=DEFAULT_TZ, is_active=True)
        session.add(g)
    else:
        if getattr(chat, "title", None) and g.title != chat.title:
            g.title = chat.title
    session.flush(); return g

def upsert_user(session, chat_id: int, tg_user) -> "User":
    u = session.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tg_user.id)).scalar_one_or_none()
    if not u:
        u = User(chat_id=chat_id, tg_user_id=tg_user.id)
        session.add(u)
    u.first_name = tg_user.first_name or u.first_name
    u.last_name = tg_user.last_name or u.last_name
    u.username = tg_user.username or u.username
    u.last_seen = dt.datetime.utcnow()
    session.flush(); return u

def group_active(g: "Group") -> bool:
    if g.expires_at is None: return True
    return g.expires_at > dt.datetime.utcnow()

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

PANELS: Dict[Tuple[int,int], Dict[str, Any]] = {}
REL_WAIT: Dict[Tuple[int,int], Dict[str, Any]] = {}
SELLER_WAIT: Dict[int, Dict[str, Any]] = {}
REL_USER_WAIT: Dict[Tuple[int,int], Dict[str, Any]] = {}

def _panel_key(chat_id: int, message_id: int) -> Tuple[int,int]: return (chat_id, message_id)
def _panel_push(msg, owner_id: int, title: str, rows, root: bool):
    key=_panel_key(msg.chat.id, msg.message_id)
    meta=PANELS.get(key, {"owner": owner_id, "stack":[]})
    meta["owner"]=owner_id; meta["stack"].append((title, rows, root)); PANELS[key]=meta
    meta["ts"] = time.time()
def _panel_pop(msg):
    key=_panel_key(msg.chat.id, msg.message_id)
    meta=PANELS.get(key); 
    if not meta or not meta["stack"]: return None
    if len(meta["stack"])>1:
        meta["stack"].pop(); prev=meta["stack"][-1]; PANELS[key]=meta; return prev
    return None
def _set_rel_wait(chat_id: int, actor_tg: int, target_user_id: int, target_tgid: int | None = None):
    ctx={"target_user_id": target_user_id};
    if target_tgid: ctx["target_tgid"]=target_tgid
    ctx["ts"] = dt.datetime.utcnow().timestamp()
    REL_WAIT[(chat_id, actor_tg)] = ctx
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
    # --- lightweight in-memory GC for stale waits/panels ---
    try:
        now = time.time()
        # REL_USER_WAIT: has 'ts' and optional 'panel_key'
        for k, v in list(REL_USER_WAIT.items()):
            ts = v.get("ts")
            if ts and (now - ts) > TTL_WAIT_SECONDS:
                pk = v.get("panel_key")
                try:
                    if pk: asyncio.create_task(context.bot.delete_message(pk[0], pk[1]))
                except Exception:
                    ...
                REL_USER_WAIT.pop(k, None)
        # REL_WAIT: we stamped ts when setting
        for k, v in list(REL_WAIT.items()):
            ts = v.get("ts")
            if ts and (now - ts) > TTL_WAIT_SECONDS:
                REL_WAIT.pop(k, None)
        # PANELS: clear very old stacks
        for k, meta in list(PANELS.items()):
            ts = meta.get("ts")
            if ts and (now - ts) > TTL_PANEL_SECONDS:
                PANELS.pop(k, None)
    except Exception:
        ...

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

def user_help_text() -> str:
    return (
        "📘 راهنمای سریع:\n"
        "• «فضول» → تست سلامت\n"
        "• «فضول منو» → منوی دکمه‌ای\n"
        "• «ثبت جنسیت دختر/پسر» (ادمین: با ریپلای برای دیگران)\n"
        "• «ثبت تولد ۱۴۰۳/۰۵/۲۰» (ادمین: با ریپلای برای دیگران)\n"
        "• «ثبت رابطه» → انتخاب از لیست/جستجو → سال/ماه/روز\n"
        "• «کراشام» → لیست کراش‌ها\n"
        "• «داده‌های من» → پروفایل کامل + محبوبیت\n"
        "• «محبوب امروز»، «شیپم کن»، «شیپ امشب»\n"
    )


async def notify_owner(context, text: str):
    try:
        if not OWNER_ID:
            return
        import re as _re
        # detect group id like "گروه -1001234567890"
        group_id = None
        m = _re.search(r"(?:گروه|group)\s+(-?\d{6,})", text)
        chat_title = None; chat_username = None; invite_link = None
        if m:
            try:
                group_id = int(m.group(1))
                chat = await context.bot.get_chat(group_id)
                chat_title = getattr(chat, "title", None)
                chat_username = getattr(chat, "username", None)
                invite_link = getattr(chat, "invite_link", None)
                if chat_title:
                    text = text.replace(m.group(0), f"گروه {chat_title}")
            except Exception:
                group_id = None
        # autolink user IDs (7+ digits, positive)
        def _mentionify(mt):
            uid = mt.group(0)
            try:
                if uid.startswith("0"):
                    return uid
                if len(uid) >= 7:
                    return f'<a href="tg://user?id={uid}">{uid}</a>'
            except Exception:
                pass
            return uid
        text_html = _re.sub(r"(?<!-)\b\d{7,}\b", _mentionify, text)
        # prepare group button if resolvable
        url = None
        try:
            if chat_username:
                url = f"https://t.me/{chat_username}"
            elif invite_link:
                url = invite_link
        except Exception:
            url = None
        kb = None
        if url:
            from telegram import InlineKeyboardMarkup, InlineKeyboardButton
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("ورود به گروه", url=url)]])
        await context.bot.send_message(OWNER_ID, text_html, disable_web_page_preview=False, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logging.warning(f"notify_owner failed: {e}")


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

    # --- Relationship extra selectors ---
    m=re.match(r"^rel:list:(\d+)$", data)
    if m:
        page=int(m.group(1)); per=10; offset=page*per
        with SessionLocal() as s:
            me=s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==user_id)).scalar_one_or_none()
            q=select(User).where(User.chat_id==chat_id)
            if me: q=q.where(User.id!=me.id)
            rows_db=s.execute(q.order_by(User.last_seen.desc().nullslast()).offset(offset).limit(per)).scalars().all()
            total_cnt=s.execute(select(func.count()).select_from(User).where(User.chat_id==chat_id)).scalar() or 0
        if not rows_db:
            await panel_edit(context, msg, user_id, "کسی در لیست نیست. از «جستجو» استفاده کن.", [[InlineKeyboardButton("جستجو", callback_data="rel:ask")]], root=False); return
        btns=[[InlineKeyboardButton((u.first_name or (u.username and "@"+u.username) or str(u.tg_user_id))[:30], callback_data=f"rel:picktg:{u.tg_user_id}")] for u in rows_db]
        nav=[]
        if page>0: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"rel:list:{page-1}"))
        if total_cnt > offset+per: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"rel:list:{page+1}"))
        if nav: btns.append(nav)
        btns.append([InlineKeyboardButton("🔎 جستجو", callback_data="rel:ask")])
        await panel_open_initial(update, context, "از لیست انتخاب کن", btns, root=True); return


    m=re.match(r"^rel:picktg:(\d+)$", data)
    if m:
        tgid=int(m.group(1))
        with SessionLocal() as s:
            target = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tgid)).scalar_one_or_none()
            me = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==user_id)).scalar_one_or_none()
        if not target or not me:
            await panel_edit(context, msg, user_id, "کاربر پیدا نشد. ممکن است از گروه خارج شده باشد.", [[InlineKeyboardButton("برگشت", callback_data="rel:list:0")]], root=False); return
        if target.tg_user_id==user_id:
            await panel_edit(context, msg, user_id, "نمی‌تونی با خودت رابطه ثبت کنی.", [[InlineKeyboardButton("برگشت", callback_data="rel:list:0")]], root=False); return
        _set_rel_wait(chat_id, user_id, target.id, target.tg_user_id)
        y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
        for ch in chunked(years,4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
        rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{y-16}")])
        await panel_edit(context, msg, user_id, "شروع رابطه — سال را انتخاب کن", rows, root=False); return
    m=re.match(r"^rel:pick:(\d+)$", data)
    if m:
        target_user_id=int(m.group(1))
        _set_rel_wait(chat_id, user_id, target_user_id)
        y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
        for ch in chunked(years,4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
        rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{y-16}")])
        await panel_edit(context, msg, user_id, "شروع رابطه — سال را انتخاب کن", rows, root=False); return

    if data=="rel:ask":
        REL_USER_WAIT[(chat_id, user_id)]={"ts": dt.datetime.utcnow().timestamp(), "panel_key": (msg.chat.id, msg.message_id)}
        await panel_edit(context, msg, user_id, "یوزرنیم را با @ یا آیدی عددی را بفرست (یا بنویس «لغو»).", [[InlineKeyboardButton("انصراف", callback_data="nav:close")]], root=False); return

    # --- Relationship date wizard ---
    m=re.match(r"^rel:yp:(\d+)$", data)
    if m:
        start=int(m.group(1))
        years=list(range(start, start-16, -1))
        rows=[[InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in years[i:i+4]] for i in range(0,len(years),4)]
        rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{start-16}")])
        await panel_edit(context, msg, user_id, "شروع رابطه — سال را انتخاب کن", rows, root=False); return

    m=re.match(r"^rel:y:(\d{4})$", data)
    if m:
        y=int(m.group(1))
        months=list(range(1,13))
        rows=[[InlineKeyboardButton(fa_digits(str(mm)), callback_data=f"rel:m:{y}-{mm}") for mm in months[i:i+4]] for i in range(0,12,4)]
        await panel_edit(context, msg, user_id, f"سال {fa_digits(y)} — ماه را انتخاب کن", rows, root=False); return

    m=re.match(r"^rel:m:(\d{4})-(\d{1,2})$", data)
    if m:
        y=int(m.group(1)); mth=int(m.group(2))
        try:
            mdays=jalali_month_len(y, mth)
        except Exception:
            mdays=31 if mth<=6 else (30 if mth<=11 else 29)
        days=list(range(1, mdays+1))
        rows=[[InlineKeyboardButton(fa_digits(str(dd)), callback_data=f"rel:d:{y}-{mth}-{dd}") for dd in days[i:i+7]] for i in range(0,len(days),7)]
        await panel_edit(context, msg, user_id, f"{fa_digits(y)}/{fa_digits(mth)} — روز را انتخاب کن", rows, root=False); return

    m=re.match(r"^rel:d:(\d{4})-(\d{1,2})-(\d{1,2})$", data)
    if m:
        y=int(m.group(1)); mth=int(m.group(2)); dd=int(m.group(3))
        ctx=_pop_rel_wait(chat_id, user_id)
        if not ctx:
            await panel_edit(context, msg, user_id, "جلسه پیدا نشد. دوباره «ثبت رابطه» را بزن.", [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False); return
        target_user_id = ctx.get("target_user_id")
        with SessionLocal() as s:
            me = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==user_id)).scalar_one_or_none()
            other = s.get(User, target_user_id) if target_user_id else None
            if not other:
                tgid = ctx.get('target_tgid') if ctx else None
                if tgid:
                    other = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tgid)).scalar_one_or_none()
            if not (me and other):
                await panel_edit(context, msg, user_id, "کاربرها پیدا نشدند. از او بخواه یک پیام بدهد یا دوباره تلاش کن.", [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False); return
            try:
                if HAS_PTOOLS:
                    gdate=JalaliDate(y,mth,dd).to_gregorian()
                else:
                    gdate=dt.date(y, mth, dd)
            except Exception:
                await panel_edit(context, msg, user_id, "تاریخ نامعتبر بود.", [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False); return
            # remove previous relationships for both
            s.execute(Relationship.__table__.delete().where((Relationship.chat_id==chat_id) & ((Relationship.user_a_id==me.id) | (Relationship.user_b_id==me.id) | (Relationship.user_a_id==other.id) | (Relationship.user_b_id==other.id))))
            ua, ub = (me.id, other.id) if me.id < other.id else (other.id, me.id)
            s.add(Relationship(chat_id=chat_id, user_a_id=ua, user_b_id=ub, started_at=gdate))
            s.commit()
        await panel_edit(context, msg, user_id, f"✅ رابطه ثبت شد از {fmt_date_fa(gdate)}", [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False)
        try:
            await notify_owner(context, f"[گزارش] رابطه در گروه {chat_id} ثبت شد: {me.tg_user_id} با {other.tg_user_id} از {fmt_date_fa(gdate)}")
        except Exception: ...
        return

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
            await notify_owner(context, f"[گزارش] شارژ {days}روزه برای گروه {g.id} انجام شد. انقضا: {fmt_dt_fa(g.expires_at)}")
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
                         [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False)
        await notify_owner(context, f"[گزارش] پاکسازی گروه {target_chat} انجام شد.")
        return

    # --- Owner panel: groups & sellers ---
    if data.startswith("adm:"):
        with SessionLocal() as s:
            if not (q.from_user.id == OWNER_ID or is_seller(s, q.from_user.id)):
                await q.answer("دسترسی مالک/فروشنده لازم است.", show_alert=True); return

        if data == "adm:home":
            rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
                  [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")]]
            await panel_edit(context, msg, user_id, "پنل مالک", rows, root=True); return

        m = re.match(r"^adm:groups:(\d+)$", data)
        if m:
            page=int(m.group(1)); per=8; offset=page*per
            with SessionLocal() as s:
                rows_db=s.execute(select(Group).order_by(Group.id).offset(offset).limit(per)).scalars().all()
                total_cnt=s.execute(text("SELECT COUNT(*) FROM groups")).scalar() or 0
                btns=[]
                for g in rows_db:
                    ttl=(g.title or "-")[:28]
                    btns.append([InlineKeyboardButton(f"{ttl} ({g.id})", callback_data=f"adm:g:{g.id}")])
                nav=[]
                if page>0: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"adm:groups:{page-1}"))
                if total_cnt > offset+per: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"adm:groups:{page+1}"))
                if nav: btns.append(nav)
                btns.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:home")])
            await panel_edit(context, msg, user_id, "📋 لیست گروه‌ها", btns or [[InlineKeyboardButton("بازگشت", callback_data="adm:home")]], root=True); return

        m = re.match(r"^adm:g:(-?\d+)$", data)
        if m:
            gid=int(m.group(1))
            with SessionLocal() as s:
                g=s.get(Group, gid)
                if not g:
                    await panel_edit(context, msg, user_id, "گروه پیدا نشد.", [[InlineKeyboardButton("بازگشت", callback_data="adm:groups:0")]], root=True); return
                ex=fmt_dt_fa(g.expires_at); title=g.title or "-"
            rows=[
                [InlineKeyboardButton("➕ ۳۰", callback_data=f"chg:{gid}:30"),
                 InlineKeyboardButton("➕ ۹۰", callback_data=f"chg:{gid}:90"),
                 InlineKeyboardButton("➕ ۱۸۰", callback_data=f"chg:{gid}:180")],
                [InlineKeyboardButton("⏱ صفر کردن", callback_data=f"adm:zero:{gid}")],
                [InlineKeyboardButton("🚪 خروج از گروه", callback_data=f"adm:leave:{gid}")],
                [InlineKeyboardButton("🧹 پاکسازی داده‌ها", callback_data=f"wipe:{gid}")],
                [InlineKeyboardButton("🗑 حذف از لیست", callback_data=f"adm:delgroup:{gid}")],
                [InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:groups:0")]
            ]
            await panel_edit(context, msg, user_id, f"مدیریت گروه\n{title}\nID: {gid}\nانقضا: {ex}", rows, root=True); return

        m = re.match(r"^adm:zero:(-?\d+)$", data)
        if m:
            gid=int(m.group(1))
            with SessionLocal() as s:
                if not (user_id==OWNER_ID or is_seller(s, user_id)):
                    await panel_edit(context, msg, user_id, "فقط مالک/فروشنده.", [[InlineKeyboardButton("بازگشت", callback_data="adm:groups:0")]], root=True); return
                g=s.get(Group, gid)
                if not g: await panel_edit(context, msg, user_id, "گروه پیدا نشد.", [[InlineKeyboardButton("بازگشت", callback_data="adm:groups:0")]], root=True); return
                g.expires_at = dt.datetime.utcnow(); s.commit()
            await notify_owner(context, f"[گزارش] انقضای گروه {gid} صفر شد.")
            await panel_edit(context, msg, user_id, "⏱ صفر شد.", [[InlineKeyboardButton("بازگشت", callback_data=f"adm:g:{gid}")]], root=True); return

        m = re.match(r"^adm:leave:(-?\d+)$", data)
        if m:
            gid=int(m.group(1))
            try:
                await context.bot.leave_chat(gid)
                await notify_owner(context, f"[گزارش] ربات از گروه {gid} خارج شد.")
                await panel_edit(context, msg, user_id, "🚪 از گروه خارج شد.", [[InlineKeyboardButton("بازگشت", callback_data=f"adm:g:{gid}")]], root=True); return
            except Exception as e:
                await panel_edit(context, msg, user_id, f"خروج ناموفق: {e}", [[InlineKeyboardButton("بازگشت", callback_data=f"adm:g:{gid}")]], root=True); return

        m = re.match(r"^adm:delgroup:(-?\d+)$", data)
        if m:
            gid=int(m.group(1))
            with SessionLocal() as s:
                s.execute(Crush.__table__.delete().where(Crush.chat_id==gid))
                s.execute(Relationship.__table__.delete().where(Relationship.chat_id==gid))
                s.execute(ReplyStatDaily.__table__.delete().where(ReplyStatDaily.chat_id==gid))
                s.execute(User.__table__.delete().where(User.chat_id==gid))
                s.execute(GroupAdmin.__table__.delete().where(GroupAdmin.chat_id==gid))
                s.execute(Group.__table__.delete().where(Group.id==gid))
                s.commit()
            await notify_owner(context, f"[گزارش] گروه {gid} از لیست حذف شد.")
            await panel_edit(context, msg, user_id, "🗑 حذف شد.", [[InlineKeyboardButton("بازگشت", callback_data="adm:groups:0")]], root=True); return

        if data=="adm:sellers":
            with SessionLocal() as s:
                sellers=s.query(Seller).filter_by(is_active=True).all()
                btns=[[InlineKeyboardButton(f"حذف {sl.tg_user_id}", callback_data=f"adm:seller:del:{sl.tg_user_id}")] for sl in sellers[:25]]
                btns.append([InlineKeyboardButton("➕ افزودن فروشنده", callback_data="adm:seller:add")])
                btns.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:home")])
            await panel_edit(context, msg, user_id, "🛍️ فروشنده‌ها", btns, root=True); return

        if data=="adm:seller:add":
            SELLER_WAIT[user_id]={"mode":"add"}
            await panel_edit(context, msg, user_id, "آیدی عددی فروشنده را بفرست.",
                             [[InlineKeyboardButton("انصراف", callback_data="adm:sellers")]], root=True); return

        m = re.match(r"^adm:seller:del:(\d+)$", data)
        if m:
            sid=int(m.group(1))
            with SessionLocal() as s:
                row=s.query(Seller).filter_by(tg_user_id=sid, is_active=True).first()
                if row: row.is_active=False; s.commit()
            await notify_owner(context, f"[گزارش] فروشنده {sid} عزل شد.")
            await panel_edit(context, msg, user_id, "فروشنده حذف شد.", [[InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:sellers")]], root=True); return

    if data in ("ui:crush:add","ui:crush:del","ui:rel:help","ui:tag:girls","ui:tag:boys","ui:tag:all","ui:pop","ui:ship","ui:privacy:me","ui:privacy:delme","ui:shipme"):
        hints={
            "ui:crush:add":"برای «ثبت کراش»، روی پیام شخص ریپلای کن و بنویس «ثبت کراش». یا: «ثبت کراش @username / 123456»",
            "ui:crush:del":"برای «حذف کراش»، مانند بالا عمل کن.",
            "ui:rel:help":"«ثبت رابطه» را بزن؛ از لیست انتخاب کن یا جستجو کن؛ سپس تاریخ را انتخاب کن.",
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

async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group","supergroup") or not update.message or not update.message.text: return
    text = clean_text(update.message.text)
    # Allow 'انتخاب از لیست' to open chooser
    if text.replace("‌","").strip() in ("انتخاب از لیست","انتخاب از ليست","از لیست","از ليست"):
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
            page=0; per=10; offset=0
            rows_db=s2.execute(select(User).where(User.chat_id==g.id, User.id!=me.id).order_by(func.lower(User.first_name).asc(), User.id.asc()).offset(offset).limit(per)).scalars().all()
            total_cnt=s2.execute(select(func.count()).select_from(User).where(User.chat_id==g.id)).scalar() or 0
        if not rows_db:
            await reply_temp(update, context, "کسی در لیست نیست. از طرف مقابل بخواه یک پیام بدهد یا «جستجو» را بزن."); return
        btns=[[InlineKeyboardButton((u.first_name or (u.username and "@"+u.username) or str(u.tg_user_id))[:30], callback_data=f"rel:picktg:{u.tg_user_id}")] for u in rows_db]
        nav=[]
        if total_cnt > per: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"rel:list:{1}"))
        if nav: btns.append(nav)
        btns.append([InlineKeyboardButton("🔎 جستجو", callback_data="rel:ask")])
        msg = await panel_open_initial(update, context, "از لیست انتخاب کن", btns, root=True)
        REL_USER_WAIT[(update.effective_chat.id, update.effective_user.id)] = {"ts": dt.datetime.utcnow().timestamp(), "panel_key": (msg.chat.id, msg.message_id)}
        return

    # EARLY: waiting for username/id from "rel:ask"
    key_wait=(update.effective_chat.id, update.effective_user.id)
    if REL_USER_WAIT.get(key_wait):
        sel=text.strip()
        if sel.replace("‌","").strip() in ("انتخاب از لیست","انتخاب از ليست","از لیست","از ليست"):
            with SessionLocal() as s2:
                g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
                page=0; per=10; offset=0
                rows_db=s2.execute(select(User).where(User.chat_id==g.id, User.id!=me.id).order_by(func.lower(User.first_name).asc(), User.id.asc()).offset(offset).limit(per)).scalars().all()
                total_cnt=s2.execute(select(func.count()).select_from(User).where(User.chat_id==g.id)).scalar() or 0
            if not rows_db:
                await reply_temp(update, context, "کسی در لیست نیست. از «جستجو» استفاده کن یا از طرف مقابل بخواه یک پیام بدهد."); return
            btns=[[InlineKeyboardButton((u.first_name or (u.username and "@"+u.username) or str(u.tg_user_id))[:30], callback_data=f"rel:picktg:{u.tg_user_id}")] for u in rows_db]
            nav=[]
            if total_cnt > per: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"rel:list:{1}"))
            if nav: btns.append(nav)
            btns.append([InlineKeyboardButton("🔎 جستجو", callback_data="rel:ask")])
            await panel_open_initial(update, context, "از لیست انتخاب کن", btns, root=True)
            return
    
        if sel in ("لغو","انصراف"):
            REL_USER_WAIT.pop(key_wait, None)
            await reply_temp(update, context, "لغو شد."); 
            return
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
            target_user=None
            if sel.startswith("@"):
                uname=sel[1:].lower()
                target_user=s2.execute(select(User).where(User.chat_id==g.id, func.lower(User.username)==uname)).scalar_one_or_none()
            else:
                try:
                    tgid=int(sel)
                    target_user=s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tgid)).scalar_one_or_none()
                except Exception: target_user=None
            if not target_user:
                await reply_temp(update, context, "کاربر پیدا نشد. از او بخواه یک پیام بدهد یا از «انتخاب از لیست» استفاده کن.", keep=True); 
                return
            if target_user.tg_user_id==update.effective_user.id:
                await reply_temp(update, context, "نمی‌تونی با خودت رابطه ثبت کنی."); 
                return
            REL_USER_WAIT.pop(key_wait, None)
            _set_rel_wait(g.id, me.tg_user_id, target_user.id, target_user.tg_user_id)
            y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
            for ch in chunked(years,4):
                rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
            rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{y-16}")])
            await reply_temp(update, context, "شروع رابطه — سال را انتخاب کن", reply_markup=InlineKeyboardMarkup(rows), keep=True)
        return


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

    # owner quick panel for THIS group
    if text == "پنل اینجا":
        with SessionLocal() as s:
            if not (update.effective_user.id==OWNER_ID or is_seller(s, update.effective_user.id)):
                return
            g=ensure_group(s, update.effective_chat)
            ex=fmt_dt_fa(g.expires_at); title=g.title or "-"
        rows=[
            [InlineKeyboardButton("➕ ۳۰", callback_data=f"chg:{g.id}:30"),
             InlineKeyboardButton("➕ ۹۰", callback_data=f"chg:{g.id}:90"),
             InlineKeyboardButton("➕ ۱۸۰", callback_data=f"chg:{g.id}:180")],
            [InlineKeyboardButton("⏱ صفر کردن", callback_data=f"adm:zero:{g.id}")],
            [InlineKeyboardButton("🚪 خروج از گروه", callback_data=f"adm:leave:{g.id}")],
            [InlineKeyboardButton("🧹 پاکسازی داده‌ها", callback_data=f"wipe:{g.id}")],
        ]
        await panel_open_initial(update, context, f"مدیریت گروه\n{title}\nID: {g.id}\nانقضا: {ex}", rows, root=True)
        return

    with SessionLocal() as s:
        g=ensure_group(s, update.effective_chat)
        me=upsert_user(s, g.id, update.effective_user)

    # textual open charge
    if "فضول" in text and "شارژ" in text:
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat)
            if not (is_operator(s, update.effective_user.id) or is_group_admin(s, g.id, update.effective_user.id)):
                await reply_temp(update, context, "دسترسی نداری.")
                return
        kb=[[InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{update.effective_chat.id}:30"),
             InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{update.effective_chat.id}:90"),
             InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{update.effective_chat.id}:180")]]
        await panel_open_initial(update, context, "⌁ پنل شارژ گروه", kb, root=True)
        return

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

    # relationship start (reply/@/id) -> or open chooser
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
                    uname=selector[1:].lower()
                    target_user=s2.execute(select(User).where(User.chat_id==g.id, func.lower(User.username)==uname)).scalar_one_or_none()
                else:
                    try:
                        tgid=int(selector)
                        target_user=s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tgid)).scalar_one_or_none()
                    except Exception: target_user=None
            # if target_user already resolved, open date wizard now
            if target_user:
                if target_user.tg_user_id==update.effective_user.id:
                    await reply_temp(update, context, "نمی‌تونی با خودت رابطه ثبت کنی."); return
                _set_rel_wait(g.id, me.tg_user_id, target_user.id, target_user.tg_user_id)
                y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
                for ch in chunked(years,4):
                    rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
                rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{y-16}")])
                await reply_temp(update, context, "شروع رابطه — سال را انتخاب کن", reply_markup=InlineKeyboardMarkup(rows), keep=True); return
            
            if not target_user:
                # Open chooser LIST immediately (page 0)
                page=0; per=10; offset=page*per
                with SessionLocal() as s_list:
                    me=upsert_user(s_list, g.id, update.effective_user)
                    rows_db=s_list.execute(
                        select(User).where(User.chat_id==g.id, User.id!=me.id)
                        .order_by(func.lower(User.first_name).asc(), User.id.asc())
                        .offset(offset).limit(per)
                    ).scalars().all()
                    total_cnt=s_list.execute(select(func.count()).select_from(User).where(User.chat_id==g.id)).scalar() or 0
                btns=[[InlineKeyboardButton((u.first_name or (u.username and "@"+u.username) or str(u.tg_user_id))[:30], callback_data=f"rel:picktg:{u.tg_user_id}")] for u in rows_db]
                nav=[]
                if total_cnt > offset+per: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"rel:list:{page+1}"))
                if nav: btns.append(nav)
                btns.append([InlineKeyboardButton("🔎 جستجو", callback_data="rel:ask"), InlineKeyboardButton("انصراف", callback_data="nav:close")])
                msg = await panel_open_initial(update, context, "از لیست انتخاب کن", btns, root=True)
                # Put user in waiting mode so further @/id text works too
                REL_USER_WAIT[(update.effective_chat.id, update.effective_user.id)] = {"ts": dt.datetime.utcnow().timestamp(), "panel_key": (msg.chat.id, msg.message_id)}
                return

    # birthday set# birthday set
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

    # crush add/remove
    m = re.match(r"^(ثبت|حذف) کراش(?:\s+(.+))?$", text)
    if m:
        action = m.group(1); selector = (m.group(2) or "").strip()
        with SessionLocal() as s2:
            g = ensure_group(s2, update.effective_chat)
            me = upsert_user(s2, g.id, update.effective_user)
            target_user = None
            if update.message.reply_to_message:
                target_user = upsert_user(s2, g.id, update.message.reply_to_message.from_user)
            elif selector:
                if selector.startswith("@"):
                    target_user = s2.execute(select(User).where(User.chat_id==g.id, func.lower(User.username)==selector[1:].lower())).scalar_one_or_none()
                else:
                    try:
                        tgid = int(selector)
                        target_user = s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tgid)).scalar_one_or_none()
                    except Exception:
                        target_user = None
            if not target_user:
                await reply_temp(update, context, "طرف مقابل پیدا نشد. با ریپلای یا @یوزرنیم یا آیدی عددی دوباره امتحان کن."); return
            if target_user.id == me.id:
                await reply_temp(update, context, "نمی‌تونی روی خودت کراش بزنی."); return

            existed = s2.execute(select(Crush).where(Crush.chat_id==g.id, Crush.from_user_id==me.id, Crush.to_user_id==target_user.id)).scalar_one_or_none()
            if action == "ثبت":
                if existed:
                    await reply_temp(update, context, "از قبل کراش ثبت شده بود."); return
                s2.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=target_user.id))
                s2.commit()
                await notify_owner(context, f"[گزارش] کراش ثبت شد: {me.tg_user_id} -> {target_user.tg_user_id} در گروه {g.id}")
                await reply_temp(update, context, f"✅ کراش ثبت شد روی {mention_of(target_user)}", parse_mode=ParseMode.HTML); return
            else:
                if not existed:
                    await reply_temp(update, context, "چیزی برای حذف پیدا نشد."); return
                s2.execute(Crush.__table__.delete().where((Crush.chat_id==g.id)&(Crush.from_user_id==me.id)&(Crush.to_user_id==target_user.id)))
                s2.commit()
                await notify_owner(context, f"[گزارش] کراش حذف شد: {me.tg_user_id} -/-> {target_user.tg_user_id} در گروه {g.id}")
                await reply_temp(update, context, f"🗑️ کراش حذف شد روی {mention_of(target_user)}", parse_mode=ParseMode.HTML); return

    if text=="کراشام":
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
            rows=s2.query(Crush).filter_by(chat_id=g.id, from_user_id=me.id).all()
            if not rows:
                await reply_temp(update, context, "هنوز کراشی ثبت نکردی."); return
            names=[]
            for r in rows[:20]:
                u=s2.get(User, r.to_user_id)
                if u: names.append(mention_of(u))
            await reply_temp(update, context, "💘 کراش‌های تو:\n" + "\n".join(f"- {n}" for n in names), keep=True, parse_mode=ParseMode.HTML)
        return

    # tag commands (reply-based): تگ دخترها / تگ پسرها / تگ همه (با/بی فاصله)
    if text in ("تگ دخترها","تگ دختر ها","تگ پسرها","تگ پسر ها","تگ همه"):
        if not update.message.reply_to_message:
            await reply_temp(update, context, "باید روی یک پیام ریپلای کنی."); return
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat)
            gender=None
            if text in ("تگ دخترها","تگ دختر ها"): gender="female"
            elif text in ("تگ پسرها","تگ پسر ها"): gender="male"
            q = s2.query(User).filter_by(chat_id=g.id)
            if gender: q = q.filter(User.gender==gender)
            users=q.limit(500).all()
            if not users:
                await reply_temp(update, context, "کسی با این معیار پیدا نکردم."); return
            mentions=[mention_of(u) for u in users]
        buf=""; out=[]
        for m_ in mentions:
            if len(buf)+len(m_)+1>3500:
                out.append(buf); buf=""
            buf += ("" if not buf else " ") + m_
        if buf: out.append(buf)
        for part in out[:6]:
            await reply_temp(update, context, part, keep=True, parse_mode=ParseMode.HTML, reply_to_message_id=update.message.reply_to_message.message_id)
        return


    if text.startswith("آیدی") or text.startswith("ایدی"):
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat)
            me=upsert_user(s2, g.id, update.effective_user)
            parts=text.split(maxsplit=1)
            selector=(parts[1].strip() if len(parts)>1 else "")
            target_user=None
            if update.message.reply_to_message:
                target_user=upsert_user(s2, g.id, update.message.reply_to_message.from_user)
            elif selector in ("داده های من","داده‌های من","me","خودم","خود",""):
                target_user=me
            elif selector.startswith("@"):
                uname=selector[1:].lower()
                target_user=s2.execute(select(User).where(User.chat_id==g.id, func.lower(User.username)==uname)).scalar_one_or_none()
            else:
                try:
                    tgid=int(selector)
                    target_user=s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tgid)).scalar_one_or_none()
                except Exception: target_user=None
            if not target_user:
                await reply_temp(update, context, "کاربر پیدا نشد. ریپلای کن یا «آیدی داده های من» یا @/آیدی بده."); return
            if target_user.tg_user_id != me.tg_user_id:
                if not (is_group_admin(s2, g.id, me.tg_user_id) or is_operator(s2, me.tg_user_id)):
                    await reply_temp(update, context, "این بخش برای دیگران فقط مخصوص ادمین‌هاست."); return
            info = build_profile_caption(s2, g, target_user)
        try:
            photos = await context.bot.get_user_profile_photos(target_user.tg_user_id, limit=1)
            if photos.total_count>0:
                file_id = photos.photos[0][-1].file_id
                await context.bot.send_photo(update.effective_chat.id, file_id, caption=info, parse_mode=ParseMode.HTML, reply_to_message_id=update.message.message_id)
            else:
                await reply_temp(update, context, info, keep=True, parse_mode=ParseMode.HTML, reply_to_message_id=update.message.message_id)
        except Exception:
            await reply_temp(update, context, info, keep=True, parse_mode=ParseMode.HTML, reply_to_message_id=update.message.message_id)
        return
    # (deprecated) داده‌های من → حالا از طریق «آیدی/ایدی» انجام می‌شود
    if text in ("داده های من","داده‌های من","ایدی داده های من"):
        text = "آیدی داده های من"
        # fallthrough to آیدی handler below


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
                name=mention_of(u)
                lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
        await reply_temp(update, context, "\n".join(lines), keep=True, parse_mode=ParseMode.HTML); return

    if text=="شیپ امشب":
        today=dt.datetime.now(TZ_TEHRAN).date()
        with SessionLocal() as s2:
            last=s2.execute(select(ShipHistory).where((ShipHistory.chat_id==update.effective_chat.id)&(ShipHistory.date==today)).order_by(ShipHistory.id.desc())).scalar_one_or_none()
        if not last:
            await reply_temp(update, context, "هنوز شیپ امشب ساخته نشده. آخر شب منتشر می‌شه 💫", keep=True); return
        with SessionLocal() as s3:
            muser, fuser = s3.get(User,last.male_user_id), s3.get(User,last.female_user_id)
        await reply_temp(update, context, f"💘 شیپِ امشب: {(muser.first_name or '@'+(muser.username or ''))} × {(fuser.first_name or '@'+(fuser.username or ''))}", keep=True); return

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

    if update.message.reply_to_message:
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat)
            today=dt.datetime.now(TZ_TEHRAN).date()
            target=upsert_user(s, g.id, update.message.reply_to_message.from_user)
            upsert_user(s, g.id, update.effective_user)
            row=s.execute(select(ReplyStatDaily).where((ReplyStatDaily.chat_id==g.id)&(ReplyStatDaily.date==today)&(ReplyStatDaily.target_user_id==target.id))).scalar_one_or_none()
            if not row: row=ReplyStatDaily(chat_id=g.id, date=today, target_user_id=target.id, reply_count=0); s.add(row)
            row.reply_count += 1; s.commit()

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

        # quick list of groups in PV
        if text in ("لیست گروه ها","لیست گروه‌ها"):
            rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")]]
            await panel_open_initial(update, context, "📋 لیست گروه‌ها", rows, root=True); return

        # quick open owner panel by text
        if text in ("پنل مالک","پنل","مدیریت"):
            rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
                  [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")]]
            await panel_open_initial(update, context, "پنل مالک", rows, root=True); return

        if SELLER_WAIT.get(uid):
            sel = text.strip()
            target_id = None
            if sel.startswith("@"):
                await reply_temp(update, context, "لطفاً آیدی عددی تلگرام را بفرست (username کافی نیست).", keep=True); return
            else:
                try: target_id=int(sel)
                except Exception: await reply_temp(update, context, "فرمت نامعتبر. یک عدد بفرست.", keep=True); return
            with SessionLocal() as s2:
                ex=s2.query(Seller).filter_by(tg_user_id=target_id, is_active=True).first()
                if ex: await reply_temp(update, context, "این فروشنده از قبل فعال است.", keep=True)
                else:
                    row=s2.query(Seller).filter_by(tg_user_id=target_id).first()
                    if not row: row=Seller(tg_user_id=target_id, is_active=True); s2.add(row)
                    else: row.is_active=True
                    s2.commit()
            SELLER_WAIT.pop(uid, None)
            await notify_owner(context, f"[گزارش] فروشنده {target_id} افزوده شد.")
            await reply_temp(update, context, "✅ فروشنده اضافه شد.", keep=True); return

        if text in ("/start","start","پنل","مدیریت","کمک"):
            who = "👑 پنل مالک" if uid==OWNER_ID else "🛍️ پنل فروشنده"
            rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
                  [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")],
                  [InlineKeyboardButton("➕ افزودن به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
                  [InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]]
            await panel_open_initial(update, context, who, rows, root=True); return

async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat=update.my_chat_member.chat if update.my_chat_member else None
        if not chat: return
        with SessionLocal() as s: ensure_group(s, chat); s.commit()
    except Exception as e: logging.info(f"on_my_chat_member err: {e}")

async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username=context.bot.username
    if update.effective_chat.type!="private":
        txt=("سلام! من روشنم ✅\n"
             "• «فضول» → جانم (تست سلامت)\n"
             "• «فضول منو» → منوی دکمه‌ای\n"
             "• «فضول کمک» → راهنما")
        await reply_temp(update, context, txt); return
    # private
    uid = update.effective_user.id
    with SessionLocal() as s:
        seller = is_seller(s, uid)
    if uid!=OWNER_ID and not seller:
        txt=("سلام! 👋 من «فضول»م، ربات اجتماعی گروه‌های فارسی.\n"
             "• منو و امکانات داخل گروه فعال می‌شن.\n"
             "• برای شروع، منو رو با «فضول منو» باز کن.")
        rows=[[InlineKeyboardButton("➕ افزودن به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
              [InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]]
        await reply_temp(update, context, txt, reply_markup=InlineKeyboardMarkup(rows), keep=True); return
    rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
          [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")],
          [InlineKeyboardButton("➕ افزودن به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
          [InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]]
    who = "👑 پنل مالک" if uid==OWNER_ID else "🛍️ پنل فروشنده"
    await panel_open_initial(update, context, who, rows, root=True); return

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err=context.error
    if isinstance(err, TgConflict):
        try:
            if OWNER_ID:
                await context.bot.send_message(OWNER_ID, "⚠️ Conflict 409: نمونهٔ دیگری از ربات در حال polling است. این نمونه خارج شد.")
        except Exception: ...
        logging.error("Conflict 409 detected. Exiting."); os._exit(0)
    logging.exception("Unhandled error", exc_info=err)

async def on_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m=update.effective_message
    if not m: return
    txt=clean_text((m.text or m.caption or "") or "")
    if txt=="فضول":
        try: await m.reply_text("جانم 👂")
        except Exception: ...

async def job_midnight(context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as s:
        groups=s.query(Group).all(); today=dt.datetime.now(TZ_TEHRAN).date()
        for g in groups:
            if not group_active(g): continue
            top=s.execute(select(ReplyStatDaily).where((ReplyStatDaily.chat_id==g.id)&(ReplyStatDaily.date==today)).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if top:
                lines=[]
                for i,r in enumerate(top, start=1):
                    u=s.get(User, r.target_user_id)
                    name=u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                    lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
                try: await context.bot.send_message(g.id, footer("🌙 محبوب‌های امروز:\n"+"\n".join(lines)))
                except Exception: ...
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

async def _post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        logging.info("Webhook deleted. Polling is active.")
    except Exception as e:
        logging.warning(f"post_init webhook delete failed: {e}")
    logging.info(f"PersianTools enabled: {HAS_PTOOLS}")

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username=context.bot.username
    if update.effective_chat.type in ("group","supergroup"):
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat)
            is_gadmin = is_group_admin(s, g.id, update.effective_user.id)
            oper = is_operator(s, update.effective_user.id)
        title="🕹 منوی فضول"
        rows=kb_group_menu(is_gadmin, oper)
        await panel_open_initial(update, context, title, rows, root=True); return
    await on_start(update, context)

async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id
    with SessionLocal() as s:
        if not (uid==OWNER_ID or is_seller(s, uid)):
            await reply_temp(update, context, "این دستور مخصوص مالک/فروشنده است."); return
    rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
          [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")]]
    await panel_open_initial(update, context, "پنل مالک", rows, root=True); return

async def cmd_charge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group","supergroup"):
        await reply_temp(update, context, "این دستور مخصوص داخل گروه است."); return
    with SessionLocal() as s:
        if not is_operator(s, update.effective_user.id):
            await reply_temp(update, context, "فقط مالک/فروشنده مجاز است."); return
    kb=[[InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{update.effective_chat.id}:30"),
         InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{update.effective_chat.id}:90"),
         InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{update.effective_chat.id}:180")]]
    await panel_open_initial(update, context, "⌁ پنل شارژ گروه", kb, root=True); return

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await reply_temp(update, context, user_help_text(), keep=True)

def main():
    if not TOKEN: raise RuntimeError("TELEGRAM_TOKEN env var is required.")
    acquire_singleton_or_exit()

    app = Application.builder().token(TOKEN).post_init(_post_init).build()

    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("panel", cmd_panel))
    app.add_handler(CommandHandler("charge", cmd_charge))
    app.add_handler(CommandHandler("help", cmd_help))

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

import random
import logging
import asyncio
import atexit
import hashlib
import datetime as dt
import time
import urllib.parse as _up
from typing import Optional, List, Tuple, Dict, Any, Iterable, TypeVar

from zoneinfo import ZoneInfo

from sqlalchemy import (
    create_engine, select, text, Integer, BigInteger, String, DateTime,
    Date, Boolean, JSON, ForeignKey, Index, func
)
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application, MessageHandler, CallbackQueryHandler, ChatMemberHandler,
    CommandHandler, filters, ContextTypes
)
from telegram.error import Conflict as TgConflict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)

TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
INSTANCE_TAG = os.getenv("INSTANCE_TAG", "").strip()

DEFAULT_TZ = "Asia/Tehran"
TZ_TEHRAN = ZoneInfo(DEFAULT_TZ)

OWNER_CONTACT_USERNAME = os.getenv("OWNER_CONTACT", "soulsownerbot")
AUTO_DELETE_SECONDS = int(os.getenv("AUTO_DELETE_SECONDS", "40"))
TTL_WAIT_SECONDS = int(os.getenv("TTL_WAIT_SECONDS", "1800"))  # 30 min
TTL_PANEL_SECONDS = int(os.getenv("TTL_PANEL_SECONDS", "7200"))  # 2 hours
DISABLE_SINGLETON = os.getenv("DISABLE_SINGLETON", "0").strip().lower() in ("1","true","yes")

Base = declarative_base()

try:
    from persiantools.jdatetime import JalaliDateTime, JalaliDate
    from persiantools import digits as _digits
    HAS_PTOOLS = True
except Exception:
    HAS_PTOOLS = False

def fa_digits(x: str) -> str:
    s=str(x)
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

engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=300, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

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
    last_seen: Mapped[Optional[dt.datetime]]=mapped_column(DateTime)
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
    conn.execute(text("ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS last_seen timestamp"))
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


def build_profile_caption(s, g, me) -> str:
    my_crushes = s.query(Crush).filter_by(chat_id=g.id, from_user_id=me.id).all()
    crush_list = []
    for r in my_crushes[:20]:
        u = s.get(User, r.to_user_id)
        if u: crush_list.append(mention_of(u))
    rel = s.query(Relationship).filter_by(chat_id=g.id).filter((Relationship.user_a_id==me.id)|(Relationship.user_b_id==me.id)).first()
    rel_txt = "-"
    if rel:
        other_id = rel.user_b_id if rel.user_a_id==me.id else rel.user_a_id
        other = s.get(User, other_id)
        other_name = other and mention_of(other)
        if other_name:
            rel_txt = f"{other_name} — از {fmt_date_fa(rel.started_at)}"
    today=dt.datetime.now(TZ_TEHRAN).date()
    my_row=s.execute(select(ReplyStatDaily).where(ReplyStatDaily.chat_id==g.id, ReplyStatDaily.date==today, ReplyStatDaily.target_user_id==me.id)).scalar_one_or_none()
    max_row=s.execute(select(ReplyStatDaily).where(ReplyStatDaily.chat_id==g.id, ReplyStatDaily.date==today).order_by(ReplyStatDaily.reply_count.desc()).limit(1)).scalar_one_or_none()
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
    return info

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
            jq.run_once(lambda c: asyncio.create_task(c.bot.delete_message(msg.chat_id, msg.message_id)), when=AUTO_DELETE_SECONDS)
    return msg

def ensure_group(session, chat) -> "Group":
    g = session.get(Group, chat.id)
    if not g:
        g = Group(id=chat.id, title=getattr(chat, "title", None) or getattr(chat, "full_name", None),
                  timezone=DEFAULT_TZ, is_active=True)
        session.add(g)
    else:
        if getattr(chat, "title", None) and g.title != chat.title:
            g.title = chat.title
    session.flush(); return g

def upsert_user(session, chat_id: int, tg_user) -> "User":
    u = session.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tg_user.id)).scalar_one_or_none()
    if not u:
        u = User(chat_id=chat_id, tg_user_id=tg_user.id)
        session.add(u)
    u.first_name = tg_user.first_name or u.first_name
    u.last_name = tg_user.last_name or u.last_name
    u.username = tg_user.username or u.username
    u.last_seen = dt.datetime.utcnow()
    session.flush(); return u

def group_active(g: "Group") -> bool:
    if g.expires_at is None: return True
    return g.expires_at > dt.datetime.utcnow()

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

PANELS: Dict[Tuple[int,int], Dict[str, Any]] = {}
REL_WAIT: Dict[Tuple[int,int], Dict[str, Any]] = {}
SELLER_WAIT: Dict[int, Dict[str, Any]] = {}
REL_USER_WAIT: Dict[Tuple[int,int], Dict[str, Any]] = {}

def _panel_key(chat_id: int, message_id: int) -> Tuple[int,int]: return (chat_id, message_id)
def _panel_push(msg, owner_id: int, title: str, rows, root: bool):
    key=_panel_key(msg.chat.id, msg.message_id)
    meta=PANELS.get(key, {"owner": owner_id, "stack":[]})
    meta["owner"]=owner_id; meta["stack"].append((title, rows, root)); PANELS[key]=meta
    meta["ts"] = time.time()
def _panel_pop(msg):
    key=_panel_key(msg.chat.id, msg.message_id)
    meta=PANELS.get(key); 
    if not meta or not meta["stack"]: return None
    if len(meta["stack"])>1:
        meta["stack"].pop(); prev=meta["stack"][-1]; PANELS[key]=meta; return prev
    return None
def _set_rel_wait(chat_id: int, actor_tg: int, target_user_id: int, target_tgid: int | None = None):
    ctx={"target_user_id": target_user_id};
    if target_tgid: ctx["target_tgid"]=target_tgid
    ctx["ts"] = dt.datetime.utcnow().timestamp()
    REL_WAIT[(chat_id, actor_tg)] = ctx
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
    # --- lightweight in-memory GC for stale waits/panels ---
    try:
        now = time.time()
        # REL_USER_WAIT: has 'ts' and optional 'panel_key'
        for k, v in list(REL_USER_WAIT.items()):
            ts = v.get("ts")
            if ts and (now - ts) > TTL_WAIT_SECONDS:
                pk = v.get("panel_key")
                try:
                    if pk: asyncio.create_task(context.bot.delete_message(pk[0], pk[1]))
                except Exception:
                    ...
                REL_USER_WAIT.pop(k, None)
        # REL_WAIT: we stamped ts when setting
        for k, v in list(REL_WAIT.items()):
            ts = v.get("ts")
            if ts and (now - ts) > TTL_WAIT_SECONDS:
                REL_WAIT.pop(k, None)
        # PANELS: clear very old stacks
        for k, meta in list(PANELS.items()):
            ts = meta.get("ts")
            if ts and (now - ts) > TTL_PANEL_SECONDS:
                PANELS.pop(k, None)
    except Exception:
        ...

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

def user_help_text() -> str:
    return (
        "📘 راهنمای سریع:\n"
        "• «فضول» → تست سلامت\n"
        "• «فضول منو» → منوی دکمه‌ای\n"
        "• «ثبت جنسیت دختر/پسر» (ادمین: با ریپلای برای دیگران)\n"
        "• «ثبت تولد ۱۴۰۳/۰۵/۲۰» (ادمین: با ریپلای برای دیگران)\n"
        "• «ثبت رابطه» → انتخاب از لیست/جستجو → سال/ماه/روز\n"
        "• «کراشام» → لیست کراش‌ها\n"
        "• «داده‌های من» → پروفایل کامل + محبوبیت\n"
        "• «محبوب امروز»، «شیپم کن»، «شیپ امشب»\n"
    )


async def notify_owner(context, text: str):
    try:
        if not OWNER_ID:
            return
        import re as _re
        # detect group id like "گروه -1001234567890"
        group_id = None
        m = _re.search(r"(?:گروه|group)\s+(-?\d{6,})", text)
        chat_title = None; chat_username = None; invite_link = None
        if m:
            try:
                group_id = int(m.group(1))
                chat = await context.bot.get_chat(group_id)
                chat_title = getattr(chat, "title", None)
                chat_username = getattr(chat, "username", None)
                invite_link = getattr(chat, "invite_link", None)
                if chat_title:
                    text = text.replace(m.group(0), f"گروه {chat_title}")
            except Exception:
                group_id = None
        # autolink user IDs (7+ digits, positive)
        def _mentionify(mt):
            uid = mt.group(0)
            try:
                if uid.startswith("0"):
                    return uid
                if len(uid) >= 7:
                    return f'<a href="tg://user?id={uid}">{uid}</a>'
            except Exception:
                pass
            return uid
        text_html = _re.sub(r"(?<!-)\b\d{7,}\b", _mentionify, text)
        # prepare group button if resolvable
        url = None
        try:
            if chat_username:
                url = f"https://t.me/{chat_username}"
            elif invite_link:
                url = invite_link
        except Exception:
            url = None
        kb = None
        if url:
            from telegram import InlineKeyboardMarkup, InlineKeyboardButton
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("ورود به گروه", url=url)]])
        await context.bot.send_message(OWNER_ID, text_html, disable_web_page_preview=False, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logging.warning(f"notify_owner failed: {e}")


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

    # --- Relationship extra selectors ---
    m=re.match(r"^rel:list:(\d+)$", data)
    if m:
        page=int(m.group(1)); per=10; offset=page*per
        with SessionLocal() as s:
            me=s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==user_id)).scalar_one_or_none()
            q=select(User).where(User.chat_id==chat_id)
            if me: q=q.where(User.id!=me.id)
            rows_db=s.execute(q.order_by(User.last_seen.desc().nullslast()).offset(offset).limit(per)).scalars().all()
            total_cnt=s.execute(select(func.count()).select_from(User).where(User.chat_id==chat_id)).scalar() or 0
        if not rows_db:
            await panel_edit(context, msg, user_id, "کسی در لیست نیست. از «جستجو» استفاده کن.", [[InlineKeyboardButton("جستجو", callback_data="rel:ask")]], root=False); return
        btns=[[InlineKeyboardButton((u.first_name or (u.username and "@"+u.username) or str(u.tg_user_id))[:30], callback_data=f"rel:picktg:{u.tg_user_id}")] for u in rows_db]
        nav=[]
        if page>0: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"rel:list:{page-1}"))
        if total_cnt > offset+per: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"rel:list:{page+1}"))
        if nav: btns.append(nav)
        btns.append([InlineKeyboardButton("🔎 جستجو", callback_data="rel:ask")])
        await panel_open_initial(update, context, "از لیست انتخاب کن", btns, root=True); return


    m=re.match(r"^rel:picktg:(\d+)$", data)
    if m:
        tgid=int(m.group(1))
        with SessionLocal() as s:
            target = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tgid)).scalar_one_or_none()
            me = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==user_id)).scalar_one_or_none()
        if not target or not me:
            await panel_edit(context, msg, user_id, "کاربر پیدا نشد. ممکن است از گروه خارج شده باشد.", [[InlineKeyboardButton("برگشت", callback_data="rel:list:0")]], root=False); return
        if target.tg_user_id==user_id:
            await panel_edit(context, msg, user_id, "نمی‌تونی با خودت رابطه ثبت کنی.", [[InlineKeyboardButton("برگشت", callback_data="rel:list:0")]], root=False); return
        _set_rel_wait(chat_id, user_id, target.id, target.tg_user_id)
        y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
        for ch in chunked(years,4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
        rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{y-16}")])
        await panel_edit(context, msg, user_id, "شروع رابطه — سال را انتخاب کن", rows, root=False); return
    m=re.match(r"^rel:pick:(\d+)$", data)
    if m:
        target_user_id=int(m.group(1))
        _set_rel_wait(chat_id, user_id, target_user_id)
        y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
        for ch in chunked(years,4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
        rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{y-16}")])
        await panel_edit(context, msg, user_id, "شروع رابطه — سال را انتخاب کن", rows, root=False); return

    if data=="rel:ask":
        REL_USER_WAIT[(chat_id, user_id)]={"ts": dt.datetime.utcnow().timestamp(), "panel_key": (msg.chat.id, msg.message_id)}
        await panel_edit(context, msg, user_id, "یوزرنیم را با @ یا آیدی عددی را بفرست (یا بنویس «لغو»).", [[InlineKeyboardButton("انصراف", callback_data="nav:close")]], root=False); return

    # --- Relationship date wizard ---
    m=re.match(r"^rel:yp:(\d+)$", data)
    if m:
        start=int(m.group(1))
        years=list(range(start, start-16, -1))
        rows=[[InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in years[i:i+4]] for i in range(0,len(years),4)]
        rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{start-16}")])
        await panel_edit(context, msg, user_id, "شروع رابطه — سال را انتخاب کن", rows, root=False); return

    m=re.match(r"^rel:y:(\d{4})$", data)
    if m:
        y=int(m.group(1))
        months=list(range(1,13))
        rows=[[InlineKeyboardButton(fa_digits(str(mm)), callback_data=f"rel:m:{y}-{mm}") for mm in months[i:i+4]] for i in range(0,12,4)]
        await panel_edit(context, msg, user_id, f"سال {fa_digits(y)} — ماه را انتخاب کن", rows, root=False); return

    m=re.match(r"^rel:m:(\d{4})-(\d{1,2})$", data)
    if m:
        y=int(m.group(1)); mth=int(m.group(2))
        try:
            mdays=jalali_month_len(y, mth)
        except Exception:
            mdays=31 if mth<=6 else (30 if mth<=11 else 29)
        days=list(range(1, mdays+1))
        rows=[[InlineKeyboardButton(fa_digits(str(dd)), callback_data=f"rel:d:{y}-{mth}-{dd}") for dd in days[i:i+7]] for i in range(0,len(days),7)]
        await panel_edit(context, msg, user_id, f"{fa_digits(y)}/{fa_digits(mth)} — روز را انتخاب کن", rows, root=False); return

    m=re.match(r"^rel:d:(\d{4})-(\d{1,2})-(\d{1,2})$", data)
    if m:
        y=int(m.group(1)); mth=int(m.group(2)); dd=int(m.group(3))
        ctx=_pop_rel_wait(chat_id, user_id)
        if not ctx:
            await panel_edit(context, msg, user_id, "جلسه پیدا نشد. دوباره «ثبت رابطه» را بزن.", [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False); return
        target_user_id = ctx.get("target_user_id")
        with SessionLocal() as s:
            me = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==user_id)).scalar_one_or_none()
            other = s.get(User, target_user_id) if target_user_id else None
            if not other:
                tgid = ctx.get('target_tgid') if ctx else None
                if tgid:
                    other = s.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tgid)).scalar_one_or_none()
            if not (me and other):
                await panel_edit(context, msg, user_id, "کاربرها پیدا نشدند. از او بخواه یک پیام بدهد یا دوباره تلاش کن.", [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False); return
            try:
                if HAS_PTOOLS:
                    gdate=JalaliDate(y,mth,dd).to_gregorian()
                else:
                    gdate=dt.date(y, mth, dd)
            except Exception:
                await panel_edit(context, msg, user_id, "تاریخ نامعتبر بود.", [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False); return
            # remove previous relationships for both
            s.execute(Relationship.__table__.delete().where((Relationship.chat_id==chat_id) & ((Relationship.user_a_id==me.id) | (Relationship.user_b_id==me.id) | (Relationship.user_a_id==other.id) | (Relationship.user_b_id==other.id))))
            ua, ub = (me.id, other.id) if me.id < other.id else (other.id, me.id)
            s.add(Relationship(chat_id=chat_id, user_a_id=ua, user_b_id=ub, started_at=gdate))
            s.commit()
        await panel_edit(context, msg, user_id, f"✅ رابطه ثبت شد از {fmt_date_fa(gdate)}", [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False)
        try:
            await notify_owner(context, f"[گزارش] رابطه در گروه {chat_id} ثبت شد: {me.tg_user_id} با {other.tg_user_id} از {fmt_date_fa(gdate)}")
        except Exception: ...
        return

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
            await notify_owner(context, f"[گزارش] شارژ {days}روزه برای گروه {g.id} انجام شد. انقضا: {fmt_dt_fa(g.expires_at)}")
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
                         [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False)
        await notify_owner(context, f"[گزارش] پاکسازی گروه {target_chat} انجام شد.")
        return

    # --- Owner panel: groups & sellers ---
    if data.startswith("adm:"):
        with SessionLocal() as s:
            if not (q.from_user.id == OWNER_ID or is_seller(s, q.from_user.id)):
                await q.answer("دسترسی مالک/فروشنده لازم است.", show_alert=True); return

        if data == "adm:home":
            rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
                  [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")]]
            await panel_edit(context, msg, user_id, "پنل مالک", rows, root=True); return

        m = re.match(r"^adm:groups:(\d+)$", data)
        if m:
            page=int(m.group(1)); per=8; offset=page*per
            with SessionLocal() as s:
                rows_db=s.execute(select(Group).order_by(Group.id).offset(offset).limit(per)).scalars().all()
                total_cnt=s.execute(text("SELECT COUNT(*) FROM groups")).scalar() or 0
                btns=[]
                for g in rows_db:
                    ttl=(g.title or "-")[:28]
                    btns.append([InlineKeyboardButton(f"{ttl} ({g.id})", callback_data=f"adm:g:{g.id}")])
                nav=[]
                if page>0: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"adm:groups:{page-1}"))
                if total_cnt > offset+per: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"adm:groups:{page+1}"))
                if nav: btns.append(nav)
                btns.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:home")])
            await panel_edit(context, msg, user_id, "📋 لیست گروه‌ها", btns or [[InlineKeyboardButton("بازگشت", callback_data="adm:home")]], root=True); return

        m = re.match(r"^adm:g:(-?\d+)$", data)
        if m:
            gid=int(m.group(1))
            with SessionLocal() as s:
                g=s.get(Group, gid)
                if not g:
                    await panel_edit(context, msg, user_id, "گروه پیدا نشد.", [[InlineKeyboardButton("بازگشت", callback_data="adm:groups:0")]], root=True); return
                ex=fmt_dt_fa(g.expires_at); title=g.title or "-"
            rows=[
                [InlineKeyboardButton("➕ ۳۰", callback_data=f"chg:{gid}:30"),
                 InlineKeyboardButton("➕ ۹۰", callback_data=f"chg:{gid}:90"),
                 InlineKeyboardButton("➕ ۱۸۰", callback_data=f"chg:{gid}:180")],
                [InlineKeyboardButton("⏱ صفر کردن", callback_data=f"adm:zero:{gid}")],
                [InlineKeyboardButton("🚪 خروج از گروه", callback_data=f"adm:leave:{gid}")],
                [InlineKeyboardButton("🧹 پاکسازی داده‌ها", callback_data=f"wipe:{gid}")],
                [InlineKeyboardButton("🗑 حذف از لیست", callback_data=f"adm:delgroup:{gid}")],
                [InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:groups:0")]
            ]
            await panel_edit(context, msg, user_id, f"مدیریت گروه\n{title}\nID: {gid}\nانقضا: {ex}", rows, root=True); return

        m = re.match(r"^adm:zero:(-?\d+)$", data)
        if m:
            gid=int(m.group(1))
            with SessionLocal() as s:
                if not (user_id==OWNER_ID or is_seller(s, user_id)):
                    await panel_edit(context, msg, user_id, "فقط مالک/فروشنده.", [[InlineKeyboardButton("بازگشت", callback_data="adm:groups:0")]], root=True); return
                g=s.get(Group, gid)
                if not g: await panel_edit(context, msg, user_id, "گروه پیدا نشد.", [[InlineKeyboardButton("بازگشت", callback_data="adm:groups:0")]], root=True); return
                g.expires_at = dt.datetime.utcnow(); s.commit()
            await notify_owner(context, f"[گزارش] انقضای گروه {gid} صفر شد.")
            await panel_edit(context, msg, user_id, "⏱ صفر شد.", [[InlineKeyboardButton("بازگشت", callback_data=f"adm:g:{gid}")]], root=True); return

        m = re.match(r"^adm:leave:(-?\d+)$", data)
        if m:
            gid=int(m.group(1))
            try:
                await context.bot.leave_chat(gid)
                await notify_owner(context, f"[گزارش] ربات از گروه {gid} خارج شد.")
                await panel_edit(context, msg, user_id, "🚪 از گروه خارج شد.", [[InlineKeyboardButton("بازگشت", callback_data=f"adm:g:{gid}")]], root=True); return
            except Exception as e:
                await panel_edit(context, msg, user_id, f"خروج ناموفق: {e}", [[InlineKeyboardButton("بازگشت", callback_data=f"adm:g:{gid}")]], root=True); return

        m = re.match(r"^adm:delgroup:(-?\d+)$", data)
        if m:
            gid=int(m.group(1))
            with SessionLocal() as s:
                s.execute(Crush.__table__.delete().where(Crush.chat_id==gid))
                s.execute(Relationship.__table__.delete().where(Relationship.chat_id==gid))
                s.execute(ReplyStatDaily.__table__.delete().where(ReplyStatDaily.chat_id==gid))
                s.execute(User.__table__.delete().where(User.chat_id==gid))
                s.execute(GroupAdmin.__table__.delete().where(GroupAdmin.chat_id==gid))
                s.execute(Group.__table__.delete().where(Group.id==gid))
                s.commit()
            await notify_owner(context, f"[گزارش] گروه {gid} از لیست حذف شد.")
            await panel_edit(context, msg, user_id, "🗑 حذف شد.", [[InlineKeyboardButton("بازگشت", callback_data="adm:groups:0")]], root=True); return

        if data=="adm:sellers":
            with SessionLocal() as s:
                sellers=s.query(Seller).filter_by(is_active=True).all()
                btns=[[InlineKeyboardButton(f"حذف {sl.tg_user_id}", callback_data=f"adm:seller:del:{sl.tg_user_id}")] for sl in sellers[:25]]
                btns.append([InlineKeyboardButton("➕ افزودن فروشنده", callback_data="adm:seller:add")])
                btns.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:home")])
            await panel_edit(context, msg, user_id, "🛍️ فروشنده‌ها", btns, root=True); return

        if data=="adm:seller:add":
            SELLER_WAIT[user_id]={"mode":"add"}
            await panel_edit(context, msg, user_id, "آیدی عددی فروشنده را بفرست.",
                             [[InlineKeyboardButton("انصراف", callback_data="adm:sellers")]], root=True); return

        m = re.match(r"^adm:seller:del:(\d+)$", data)
        if m:
            sid=int(m.group(1))
            with SessionLocal() as s:
                row=s.query(Seller).filter_by(tg_user_id=sid, is_active=True).first()
                if row: row.is_active=False; s.commit()
            await notify_owner(context, f"[گزارش] فروشنده {sid} عزل شد.")
            await panel_edit(context, msg, user_id, "فروشنده حذف شد.", [[InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:sellers")]], root=True); return

    if data in ("ui:crush:add","ui:crush:del","ui:rel:help","ui:tag:girls","ui:tag:boys","ui:tag:all","ui:pop","ui:ship","ui:privacy:me","ui:privacy:delme","ui:shipme"):
        hints={
            "ui:crush:add":"برای «ثبت کراش»، روی پیام شخص ریپلای کن و بنویس «ثبت کراش». یا: «ثبت کراش @username / 123456»",
            "ui:crush:del":"برای «حذف کراش»، مانند بالا عمل کن.",
            "ui:rel:help":"«ثبت رابطه» را بزن؛ از لیست انتخاب کن یا جستجو کن؛ سپس تاریخ را انتخاب کن.",
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

async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group","supergroup") or not update.message or not update.message.text: return
    text = clean_text(update.message.text)
    # Allow 'انتخاب از لیست' to open chooser
    if text.replace("‌","").strip() in ("انتخاب از لیست","انتخاب از ليست","از لیست","از ليست"):
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
            page=0; per=10; offset=0
            rows_db=s2.execute(select(User).where(User.chat_id==g.id, User.id!=me.id).order_by(func.lower(User.first_name).asc(), User.id.asc()).offset(offset).limit(per)).scalars().all()
            total_cnt=s2.execute(select(func.count()).select_from(User).where(User.chat_id==g.id)).scalar() or 0
        if not rows_db:
            await reply_temp(update, context, "کسی در لیست نیست. از طرف مقابل بخواه یک پیام بدهد یا «جستجو» را بزن."); return
        btns=[[InlineKeyboardButton((u.first_name or (u.username and "@"+u.username) or str(u.tg_user_id))[:30], callback_data=f"rel:picktg:{u.tg_user_id}")] for u in rows_db]
        nav=[]
        if total_cnt > per: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"rel:list:{1}"))
        if nav: btns.append(nav)
        btns.append([InlineKeyboardButton("🔎 جستجو", callback_data="rel:ask")])
        msg = await panel_open_initial(update, context, "از لیست انتخاب کن", btns, root=True)
        REL_USER_WAIT[(update.effective_chat.id, update.effective_user.id)] = {"ts": dt.datetime.utcnow().timestamp(), "panel_key": (msg.chat.id, msg.message_id)}
        return

    # EARLY: waiting for username/id from "rel:ask"
    key_wait=(update.effective_chat.id, update.effective_user.id)
    if REL_USER_WAIT.get(key_wait):
        sel=text.strip()
        if sel.replace("‌","").strip() in ("انتخاب از لیست","انتخاب از ليست","از لیست","از ليست"):
            with SessionLocal() as s2:
                g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
                page=0; per=10; offset=0
                rows_db=s2.execute(select(User).where(User.chat_id==g.id, User.id!=me.id).order_by(func.lower(User.first_name).asc(), User.id.asc()).offset(offset).limit(per)).scalars().all()
                total_cnt=s2.execute(select(func.count()).select_from(User).where(User.chat_id==g.id)).scalar() or 0
            if not rows_db:
                await reply_temp(update, context, "کسی در لیست نیست. از «جستجو» استفاده کن یا از طرف مقابل بخواه یک پیام بدهد."); return
            btns=[[InlineKeyboardButton((u.first_name or (u.username and "@"+u.username) or str(u.tg_user_id))[:30], callback_data=f"rel:picktg:{u.tg_user_id}")] for u in rows_db]
            nav=[]
            if total_cnt > per: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"rel:list:{1}"))
            if nav: btns.append(nav)
            btns.append([InlineKeyboardButton("🔎 جستجو", callback_data="rel:ask")])
            await panel_open_initial(update, context, "از لیست انتخاب کن", btns, root=True)
            return
    
        if sel in ("لغو","انصراف"):
            REL_USER_WAIT.pop(key_wait, None)
            await reply_temp(update, context, "لغو شد."); 
            return
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
            target_user=None
            if sel.startswith("@"):
                uname=sel[1:].lower()
                target_user=s2.execute(select(User).where(User.chat_id==g.id, func.lower(User.username)==uname)).scalar_one_or_none()
            else:
                try:
                    tgid=int(sel)
                    target_user=s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tgid)).scalar_one_or_none()
                except Exception: target_user=None
            if not target_user:
                await reply_temp(update, context, "کاربر پیدا نشد. از او بخواه یک پیام بدهد یا از «انتخاب از لیست» استفاده کن.", keep=True); 
                return
            if target_user.tg_user_id==update.effective_user.id:
                await reply_temp(update, context, "نمی‌تونی با خودت رابطه ثبت کنی."); 
                return
            REL_USER_WAIT.pop(key_wait, None)
            _set_rel_wait(g.id, me.tg_user_id, target_user.id, target_user.tg_user_id)
            y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
            for ch in chunked(years,4):
                rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
            rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{y-16}")])
            await reply_temp(update, context, "شروع رابطه — سال را انتخاب کن", reply_markup=InlineKeyboardMarkup(rows), keep=True)
        return


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

    # owner quick panel for THIS group
    if text == "پنل اینجا":
        with SessionLocal() as s:
            if not (update.effective_user.id==OWNER_ID or is_seller(s, update.effective_user.id)):
                return
            g=ensure_group(s, update.effective_chat)
            ex=fmt_dt_fa(g.expires_at); title=g.title or "-"
        rows=[
            [InlineKeyboardButton("➕ ۳۰", callback_data=f"chg:{g.id}:30"),
             InlineKeyboardButton("➕ ۹۰", callback_data=f"chg:{g.id}:90"),
             InlineKeyboardButton("➕ ۱۸۰", callback_data=f"chg:{g.id}:180")],
            [InlineKeyboardButton("⏱ صفر کردن", callback_data=f"adm:zero:{g.id}")],
            [InlineKeyboardButton("🚪 خروج از گروه", callback_data=f"adm:leave:{g.id}")],
            [InlineKeyboardButton("🧹 پاکسازی داده‌ها", callback_data=f"wipe:{g.id}")],
        ]
        await panel_open_initial(update, context, f"مدیریت گروه\n{title}\nID: {g.id}\nانقضا: {ex}", rows, root=True)
        return

    with SessionLocal() as s:
        g=ensure_group(s, update.effective_chat)
        me=upsert_user(s, g.id, update.effective_user)

    # textual open charge
    if "فضول" in text and "شارژ" in text:
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat)
            if not (is_operator(s, update.effective_user.id) or is_group_admin(s, g.id, update.effective_user.id)):
                await reply_temp(update, context, "دسترسی نداری.")
                return
        kb=[[InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{update.effective_chat.id}:30"),
             InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{update.effective_chat.id}:90"),
             InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{update.effective_chat.id}:180")]]
        await panel_open_initial(update, context, "⌁ پنل شارژ گروه", kb, root=True)
        return

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

    # relationship start (reply/@/id) -> or open chooser
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
                    uname=selector[1:].lower()
                    target_user=s2.execute(select(User).where(User.chat_id==g.id, func.lower(User.username)==uname)).scalar_one_or_none()
                else:
                    try:
                        tgid=int(selector)
                        target_user=s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tgid)).scalar_one_or_none()
                    except Exception: target_user=None
            # if target_user already resolved, open date wizard now
            if target_user:
                if target_user.tg_user_id==update.effective_user.id:
                    await reply_temp(update, context, "نمی‌تونی با خودت رابطه ثبت کنی."); return
                _set_rel_wait(g.id, me.tg_user_id, target_user.id, target_user.tg_user_id)
                y=jalali_now_year(); years=list(range(y, y-16, -1)); rows=[]
                for ch in chunked(years,4):
                    rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{yy}") for yy in ch])
                rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"rel:yp:{y-16}")])
                await reply_temp(update, context, "شروع رابطه — سال را انتخاب کن", reply_markup=InlineKeyboardMarkup(rows), keep=True); return
            if not target_user:
                rows=[[InlineKeyboardButton("انصراف", callback_data="nav:close")]]
                msg = await panel_open_initial(update, context, "ثبت رابطه — @یوزرنیم یا آیدی عددی طرف مقابل را بفرست", rows, root=True)
                REL_USER_WAIT[(update.effective_chat.id, update.effective_user.id)] = {"ts": dt.datetime.utcnow().timestamp(), "panel_key": (msg.chat.id, msg.message_id)}
                return
    # birthday set# birthday set
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

    # crush add/remove
    m = re.match(r"^(ثبت|حذف) کراش(?:\s+(.+))?$", text)
    if m:
        action = m.group(1); selector = (m.group(2) or "").strip()
        with SessionLocal() as s2:
            g = ensure_group(s2, update.effective_chat)
            me = upsert_user(s2, g.id, update.effective_user)
            target_user = None
            if update.message.reply_to_message:
                target_user = upsert_user(s2, g.id, update.message.reply_to_message.from_user)
            elif selector:
                if selector.startswith("@"):
                    target_user = s2.execute(select(User).where(User.chat_id==g.id, func.lower(User.username)==selector[1:].lower())).scalar_one_or_none()
                else:
                    try:
                        tgid = int(selector)
                        target_user = s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tgid)).scalar_one_or_none()
                    except Exception:
                        target_user = None
            if not target_user:
                await reply_temp(update, context, "طرف مقابل پیدا نشد. با ریپلای یا @یوزرنیم یا آیدی عددی دوباره امتحان کن."); return
            if target_user.id == me.id:
                await reply_temp(update, context, "نمی‌تونی روی خودت کراش بزنی."); return

            existed = s2.execute(select(Crush).where(Crush.chat_id==g.id, Crush.from_user_id==me.id, Crush.to_user_id==target_user.id)).scalar_one_or_none()
            if action == "ثبت":
                if existed:
                    await reply_temp(update, context, "از قبل کراش ثبت شده بود."); return
                s2.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=target_user.id))
                s2.commit()
                await notify_owner(context, f"[گزارش] کراش ثبت شد: {me.tg_user_id} -> {target_user.tg_user_id} در گروه {g.id}")
                await reply_temp(update, context, f"✅ کراش ثبت شد روی {mention_of(target_user)}", parse_mode=ParseMode.HTML); return
            else:
                if not existed:
                    await reply_temp(update, context, "چیزی برای حذف پیدا نشد."); return
                s2.execute(Crush.__table__.delete().where((Crush.chat_id==g.id)&(Crush.from_user_id==me.id)&(Crush.to_user_id==target_user.id)))
                s2.commit()
                await notify_owner(context, f"[گزارش] کراش حذف شد: {me.tg_user_id} -/-> {target_user.tg_user_id} در گروه {g.id}")
                await reply_temp(update, context, f"🗑️ کراش حذف شد روی {mention_of(target_user)}", parse_mode=ParseMode.HTML); return

    if text=="کراشام":
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat); me=upsert_user(s2, g.id, update.effective_user)
            rows=s2.query(Crush).filter_by(chat_id=g.id, from_user_id=me.id).all()
            if not rows:
                await reply_temp(update, context, "هنوز کراشی ثبت نکردی."); return
            names=[]
            for r in rows[:20]:
                u=s2.get(User, r.to_user_id)
                if u: names.append(mention_of(u))
            await reply_temp(update, context, "💘 کراش‌های تو:\n" + "\n".join(f"- {n}" for n in names), keep=True, parse_mode=ParseMode.HTML)
        return

    # tag commands (reply-based): تگ دخترها / تگ پسرها / تگ همه (با/بی فاصله)
    if text in ("تگ دخترها","تگ دختر ها","تگ پسرها","تگ پسر ها","تگ همه"):
        if not update.message.reply_to_message:
            await reply_temp(update, context, "باید روی یک پیام ریپلای کنی."); return
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat)
            gender=None
            if text in ("تگ دخترها","تگ دختر ها"): gender="female"
            elif text in ("تگ پسرها","تگ پسر ها"): gender="male"
            q = s2.query(User).filter_by(chat_id=g.id)
            if gender: q = q.filter(User.gender==gender)
            users=q.limit(500).all()
            if not users:
                await reply_temp(update, context, "کسی با این معیار پیدا نکردم."); return
            mentions=[mention_of(u) for u in users]
        buf=""; out=[]
        for m_ in mentions:
            if len(buf)+len(m_)+1>3500:
                out.append(buf); buf=""
            buf += ("" if not buf else " ") + m_
        if buf: out.append(buf)
        for part in out[:6]:
            await reply_temp(update, context, part, keep=True, parse_mode=ParseMode.HTML, reply_to_message_id=update.message.reply_to_message.message_id)
        return


    if text.startswith("آیدی") or text.startswith("ایدی"):
        with SessionLocal() as s2:
            g=ensure_group(s2, update.effective_chat)
            me=upsert_user(s2, g.id, update.effective_user)
            parts=text.split(maxsplit=1)
            selector=(parts[1].strip() if len(parts)>1 else "")
            target_user=None
            if update.message.reply_to_message:
                target_user=upsert_user(s2, g.id, update.message.reply_to_message.from_user)
            elif selector in ("داده های من","داده‌های من","me","خودم","خود",""):
                target_user=me
            elif selector.startswith("@"):
                uname=selector[1:].lower()
                target_user=s2.execute(select(User).where(User.chat_id==g.id, func.lower(User.username)==uname)).scalar_one_or_none()
            else:
                try:
                    tgid=int(selector)
                    target_user=s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tgid)).scalar_one_or_none()
                except Exception: target_user=None
            if not target_user:
                await reply_temp(update, context, "کاربر پیدا نشد. ریپلای کن یا «آیدی داده های من» یا @/آیدی بده."); return
            if target_user.tg_user_id != me.tg_user_id:
                if not (is_group_admin(s2, g.id, me.tg_user_id) or is_operator(s2, me.tg_user_id)):
                    await reply_temp(update, context, "این بخش برای دیگران فقط مخصوص ادمین‌هاست."); return
            info = build_profile_caption(s2, g, target_user)
        try:
            photos = await context.bot.get_user_profile_photos(target_user.tg_user_id, limit=1)
            if photos.total_count>0:
                file_id = photos.photos[0][-1].file_id
                await context.bot.send_photo(update.effective_chat.id, file_id, caption=info, parse_mode=ParseMode.HTML, reply_to_message_id=update.message.message_id)
            else:
                await reply_temp(update, context, info, keep=True, parse_mode=ParseMode.HTML, reply_to_message_id=update.message.message_id)
        except Exception:
            await reply_temp(update, context, info, keep=True, parse_mode=ParseMode.HTML, reply_to_message_id=update.message.message_id)
        return
    # (deprecated) داده‌های من → حالا از طریق «آیدی/ایدی» انجام می‌شود
    if text in ("داده های من","داده‌های من","ایدی داده های من"):
        text = "آیدی داده های من"
        # fallthrough to آیدی handler below


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
                name=mention_of(u)
                lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
        await reply_temp(update, context, "\n".join(lines), keep=True, parse_mode=ParseMode.HTML); return

    if text=="شیپ امشب":
        today=dt.datetime.now(TZ_TEHRAN).date()
        with SessionLocal() as s2:
            last=s2.execute(select(ShipHistory).where((ShipHistory.chat_id==update.effective_chat.id)&(ShipHistory.date==today)).order_by(ShipHistory.id.desc())).scalar_one_or_none()
        if not last:
            await reply_temp(update, context, "هنوز شیپ امشب ساخته نشده. آخر شب منتشر می‌شه 💫", keep=True); return
        with SessionLocal() as s3:
            muser, fuser = s3.get(User,last.male_user_id), s3.get(User,last.female_user_id)
        await reply_temp(update, context, f"💘 شیپِ امشب: {(muser.first_name or '@'+(muser.username or ''))} × {(fuser.first_name or '@'+(fuser.username or ''))}", keep=True); return

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

    if update.message.reply_to_message:
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat)
            today=dt.datetime.now(TZ_TEHRAN).date()
            target=upsert_user(s, g.id, update.message.reply_to_message.from_user)
            upsert_user(s, g.id, update.effective_user)
            row=s.execute(select(ReplyStatDaily).where((ReplyStatDaily.chat_id==g.id)&(ReplyStatDaily.date==today)&(ReplyStatDaily.target_user_id==target.id))).scalar_one_or_none()
            if not row: row=ReplyStatDaily(chat_id=g.id, date=today, target_user_id=target.id, reply_count=0); s.add(row)
            row.reply_count += 1; s.commit()

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

        # quick list of groups in PV
        if text in ("لیست گروه ها","لیست گروه‌ها"):
            rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")]]
            await panel_open_initial(update, context, "📋 لیست گروه‌ها", rows, root=True); return

        # quick open owner panel by text
        if text in ("پنل مالک","پنل","مدیریت"):
            rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
                  [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")]]
            await panel_open_initial(update, context, "پنل مالک", rows, root=True); return

        if SELLER_WAIT.get(uid):
            sel = text.strip()
            target_id = None
            if sel.startswith("@"):
                await reply_temp(update, context, "لطفاً آیدی عددی تلگرام را بفرست (username کافی نیست).", keep=True); return
            else:
                try: target_id=int(sel)
                except Exception: await reply_temp(update, context, "فرمت نامعتبر. یک عدد بفرست.", keep=True); return
            with SessionLocal() as s2:
                ex=s2.query(Seller).filter_by(tg_user_id=target_id, is_active=True).first()
                if ex: await reply_temp(update, context, "این فروشنده از قبل فعال است.", keep=True)
                else:
                    row=s2.query(Seller).filter_by(tg_user_id=target_id).first()
                    if not row: row=Seller(tg_user_id=target_id, is_active=True); s2.add(row)
                    else: row.is_active=True
                    s2.commit()
            SELLER_WAIT.pop(uid, None)
            await notify_owner(context, f"[گزارش] فروشنده {target_id} افزوده شد.")
            await reply_temp(update, context, "✅ فروشنده اضافه شد.", keep=True); return

        if text in ("/start","start","پنل","مدیریت","کمک"):
            who = "👑 پنل مالک" if uid==OWNER_ID else "🛍️ پنل فروشنده"
            rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
                  [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")],
                  [InlineKeyboardButton("➕ افزودن به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
                  [InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]]
            await panel_open_initial(update, context, who, rows, root=True); return

async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat=update.my_chat_member.chat if update.my_chat_member else None
        if not chat: return
        with SessionLocal() as s: ensure_group(s, chat); s.commit()
    except Exception as e: logging.info(f"on_my_chat_member err: {e}")

async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username=context.bot.username
    if update.effective_chat.type!="private":
        txt=("سلام! من روشنم ✅\n"
             "• «فضول» → جانم (تست سلامت)\n"
             "• «فضول منو» → منوی دکمه‌ای\n"
             "• «فضول کمک» → راهنما")
        await reply_temp(update, context, txt); return
    # private
    uid = update.effective_user.id
    with SessionLocal() as s:
        seller = is_seller(s, uid)
    if uid!=OWNER_ID and not seller:
        txt=("سلام! 👋 من «فضول»م، ربات اجتماعی گروه‌های فارسی.\n"
             "• منو و امکانات داخل گروه فعال می‌شن.\n"
             "• برای شروع، منو رو با «فضول منو» باز کن.")
        rows=[[InlineKeyboardButton("➕ افزودن به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
              [InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]]
        await reply_temp(update, context, txt, reply_markup=InlineKeyboardMarkup(rows), keep=True); return
    rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
          [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")],
          [InlineKeyboardButton("➕ افزودن به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
          [InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]]
    who = "👑 پنل مالک" if uid==OWNER_ID else "🛍️ پنل فروشنده"
    await panel_open_initial(update, context, who, rows, root=True); return

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err=context.error
    if isinstance(err, TgConflict):
        try:
            if OWNER_ID:
                await context.bot.send_message(OWNER_ID, "⚠️ Conflict 409: نمونهٔ دیگری از ربات در حال polling است. این نمونه خارج شد.")
        except Exception: ...
        logging.error("Conflict 409 detected. Exiting."); os._exit(0)
    logging.exception("Unhandled error", exc_info=err)

async def on_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m=update.effective_message
    if not m: return
    txt=clean_text((m.text or m.caption or "") or "")
    if txt=="فضول":
        try: await m.reply_text("جانم 👂")
        except Exception: ...

async def job_midnight(context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as s:
        groups=s.query(Group).all(); today=dt.datetime.now(TZ_TEHRAN).date()
        for g in groups:
            if not group_active(g): continue
            top=s.execute(select(ReplyStatDaily).where((ReplyStatDaily.chat_id==g.id)&(ReplyStatDaily.date==today)).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if top:
                lines=[]
                for i,r in enumerate(top, start=1):
                    u=s.get(User, r.target_user_id)
                    name=u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                    lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
                try: await context.bot.send_message(g.id, footer("🌙 محبوب‌های امروز:\n"+"\n".join(lines)))
                except Exception: ...
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

async def _post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        logging.info("Webhook deleted. Polling is active.")
    except Exception as e:
        logging.warning(f"post_init webhook delete failed: {e}")
    logging.info(f"PersianTools enabled: {HAS_PTOOLS}")

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username=context.bot.username
    if update.effective_chat.type in ("group","supergroup"):
        with SessionLocal() as s:
            g=ensure_group(s, update.effective_chat)
            is_gadmin = is_group_admin(s, g.id, update.effective_user.id)
            oper = is_operator(s, update.effective_user.id)
        title="🕹 منوی فضول"
        rows=kb_group_menu(is_gadmin, oper)
        await panel_open_initial(update, context, title, rows, root=True); return
    await on_start(update, context)

async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id
    with SessionLocal() as s:
        if not (uid==OWNER_ID or is_seller(s, uid)):
            await reply_temp(update, context, "این دستور مخصوص مالک/فروشنده است."); return
    rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
          [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")]]
    await panel_open_initial(update, context, "پنل مالک", rows, root=True); return

async def cmd_charge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group","supergroup"):
        await reply_temp(update, context, "این دستور مخصوص داخل گروه است."); return
    with SessionLocal() as s:
        if not is_operator(s, update.effective_user.id):
            await reply_temp(update, context, "فقط مالک/فروشنده مجاز است."); return
    kb=[[InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{update.effective_chat.id}:30"),
         InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{update.effective_chat.id}:90"),
         InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{update.effective_chat.id}:180")]]
    await panel_open_initial(update, context, "⌁ پنل شارژ گروه", kb, root=True); return

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await reply_temp(update, context, user_help_text(), keep=True)

def main():
    if not TOKEN: raise RuntimeError("TELEGRAM_TOKEN env var is required.")
    acquire_singleton_or_exit()

    app = Application.builder().token(TOKEN).post_init(_post_init).build()

    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("panel", cmd_panel))
    app.add_handler(CommandHandler("charge", cmd_charge))
    app.add_handler(CommandHandler("help", cmd_help))

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
