# main.py
import os, logging, re, random, datetime as dt, asyncio, atexit, hashlib, urllib.parse as _up, math
from typing import Optional, List, Tuple, Dict, Any
from zoneinfo import ZoneInfo

from sqlalchemy import select, text, Integer, BigInteger, String, DateTime, Date, Boolean, JSON, ForeignKey, Index, func
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
from sqlalchemy import create_engine

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application, MessageHandler, CallbackQueryHandler, ChatMemberHandler,
    CommandHandler, filters, ContextTypes
)
from telegram.error import Conflict as TgConflict

# ================== CONFIG ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)   # خاموش‌تر کردن لاگ HTTP
logging.getLogger("telegram").setLevel(logging.INFO)   # لاگ سبک تلگرام برای دیباگ

TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

# فقط ایران
DEFAULT_TZ = "Asia/Tehran"
TZ_TEHRAN = ZoneInfo(DEFAULT_TZ)

OWNER_CONTACT_USERNAME = os.getenv("OWNER_CONTACT", "soulsownerbot")
AUTO_DELETE_SECONDS = int(os.getenv("AUTO_DELETE_SECONDS", "40"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
PORT = int(os.getenv("PORT", "8080"))

Base = declarative_base()

# Persian dates
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
        except Exception: pass
    return str(s)

def fmt_dt_fa(dt_utc: Optional[dt.datetime]) -> str:
    if dt_utc is None: return "-"
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
    local = dt_utc.astimezone(TZ_TEHRAN)
    if HAS_PTOOLS:
        try:
            jdt = JalaliDateTime.fromgregorian(datetime=local)
            s = jdt.strftime("%A %Y/%m/%d %H:%M")
            return fa_digits(s) + " (تهران)"
        except Exception: ...
    return local.strftime("%Y/%m/%d %H:%M") + " (Tehran)"

def fmt_date_fa(d: Optional[dt.date]) -> str:
    if not d: return "-"
    if HAS_PTOOLS:
        try:
            jd = JalaliDate.fromgregorian(date=d)
            return fa_digits(jd.strftime("%Y/%m/%d"))
        except Exception: ...
    return d.strftime("%Y/%m/%d")

def parse_jalali_date_input(s: str) -> dt.date:
    ss = fa_to_en_digits(s).strip().replace("/", "-")
    parts = ss.split("-")
    if len(parts) != 3: raise ValueError("Bad date format")
    y, m, d = (int(parts[0]), int(parts[1]), int(parts[2]))
    if y >= 1700:
        raise ValueError("Gregorian not allowed")
    if HAS_PTOOLS:
        return JalaliDate(y, m, d).to_gregorian()
    return dt.date(2000+y%100, m, d)

def jalali_now_year() -> int:
    now = dt.datetime.now(TZ_TEHRAN)
    if HAS_PTOOLS:
        return JalaliDateTime.fromgregorian(datetime=now).year
    return now.year

def jalali_month_len(y: int, m: int) -> int:
    if not HAS_PTOOLS:
        if m <= 6: return 31
        if m <= 11: return 30
        return 29
    for d in range(31, 27, -1):
        try:
            JalaliDate(y, m, d)
            return d
        except Exception:
            continue
    return 29

def today_jalali() -> Tuple[int,int,int]:
    now = dt.datetime.now(TZ_TEHRAN)
    if HAS_PTOOLS:
        j = JalaliDateTime.fromgregorian(datetime=now)
        return j.year, j.month, j.day
    d = now.date()
    return d.year, d.month, d.day

def to_jalali_md(d: dt.date) -> Tuple[int,int]:
    if HAS_PTOOLS:
        j = JalaliDate.fromgregorian(date=d)
        return j.month, j.day
    return d.month, d.day

# — بدون واترمارک
def footer(text: str) -> str:
    return text

def contact_kb(extra_rows: List[List[InlineKeyboardButton]]|None=None, bot_username: Optional[str]=None) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if extra_rows: rows.extend([r for r in extra_rows if r])
    rows.append([InlineKeyboardButton("📞 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")])
    if bot_username:
        rows.append([InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"https://t.me/{bot_username}?startgroup=true")])
    return InlineKeyboardMarkup(rows)

# ================== Auto-Delete ==================
async def _job_delete_message(context: ContextTypes.DEFAULT_TYPE):
    chat_id, msg_id = context.job.data
    try:
        await context.bot.delete_message(chat_id, msg_id)
    except Exception: ...

def schedule_autodelete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, keep: bool=False):
    if keep: return
    jq = context.application.job_queue if hasattr(context, "application") else None
    if jq: jq.run_once(_job_delete_message, when=AUTO_DELETE_SECONDS, data=(chat_id, message_id))

async def reply_temp(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str,
                     reply_markup: InlineKeyboardMarkup|None=None, keep: bool=False,
                     parse_mode: Optional[str]=None, reply_to_message_id: Optional[int]=None, with_footer: bool=True):
    msg = await update.effective_chat.send_message(
        footer(text) if with_footer else text,
        reply_markup=reply_markup, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id
    )
    schedule_autodelete(context, msg.chat_id, msg.message_id, keep=keep)
    return msg

# ================== DB URL ==================
def _mask_url(u: str) -> str:
    try:
        parts = _up.urlsplit(u)
        if parts.username or parts.password:
            netloc = parts.hostname or ""
            if parts.port: netloc += f":{parts.port}"
            return _up.urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception: ...
    return "<unparsable>"

_DRIVER = None
try:
    import psycopg; _DRIVER = "psycopg"
except Exception:
    try:
        import psycopg2; _DRIVER = "psycopg2"
    except Exception:
        _DRIVER = "psycopg"

raw_db_url = (os.getenv("DATABASE_URL") or "").strip()
if not raw_db_url:
    PGHOST = os.getenv("PGHOST"); PGPORT = os.getenv("PGPORT", "5432")
    PGUSER = os.getenv("PGUSER"); PGPASSWORD = os.getenv("PGPASSWORD")
    PGDATABASE = os.getenv("PGDATABASE", "railway")
    if all([PGHOST, PGUSER, PGPASSWORD]):
        raw_db_url = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    else:
        raise RuntimeError("DATABASE_URL یافت نشد و متغیرهای PGHOST/PGUSER/PGPASSWORD هم ست نیستند.")

db_url = raw_db_url
if db_url.startswith("postgres://"): db_url = db_url.replace("postgres://", "postgresql://", 1)
if "+psycopg" not in db_url and "+psycopg2" not in db_url:
    db_url = db_url.replace("postgresql://", f"postgresql+{_DRIVER}://", 1)
if "sslmode=" not in db_url:
    sep = "&" if "?" in db_url else "?"
    db_url = f"{db_url}{sep}sslmode=require"

try:
    parsed = _up.urlsplit(db_url)
    logging.info(f"DB host: {parsed.hostname}, port: {parsed.port}, db: {parsed.path}")
except Exception:
    pass

engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_recycle=300,
    future=True,
    connect_args={"sslmode":"require"}
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# --- Singleton guard ---
SINGLETON_CONN = None
SINGLETON_KEY = None

def _advisory_key() -> int:
    return int(hashlib.blake2b(TOKEN.encode(), digest_size=8).hexdigest(), 16) % (2**31)

def _acquire_lock(conn, key: int) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
    ok = cur.fetchone()[0]
    return bool(ok)

def acquire_singleton_or_exit():
    global SINGLETON_CONN, SINGLETON_KEY
    SINGLETON_KEY = _advisory_key()
    try:
        SINGLETON_CONN = engine.raw_connection()
        cur = SINGLETON_CONN.cursor()
        cur.execute("SET application_name = 'fazolbot'")
        ok = _acquire_lock(SINGLETON_CONN, SINGLETON_KEY)
        if not ok:
            logging.error("Another instance is already running (PG advisory lock). Exiting.")
            os._exit(0)
        logging.info("Singleton advisory lock acquired. This is the only polling instance.")
    except Exception as e:
        logging.error(f"Singleton lock failed: {e}")
        os._exit(0)

    @atexit.register
    def _unlock():
        try:
            cur = SINGLETON_CONN.cursor()
            cur.execute("SELECT pg_advisory_unlock(%s)", (SINGLETON_KEY,))
            SINGLETON_CONN.close()
        except Exception:
            pass

async def singleton_watchdog(context: ContextTypes.DEFAULT_TYPE):
    global SINGLETON_CONN, SINGLETON_KEY
    try:
        cur = SINGLETON_CONN.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        return
    except Exception as e:
        logging.warning(f"Singleton connection ping failed, trying re-acquire: {e}")
        try:
            try:
                SINGLETON_CONN.close()
            except Exception:
                pass
            SINGLETON_CONN = engine.raw_connection()
            cur = SINGLETON_CONN.cursor()
            cur.execute("SELECT pg_try_advisory_lock(%s)", (SINGLETON_KEY,))
            ok = cur.fetchone()[0]
            if not ok:
                logging.error("Lost advisory lock, another instance holds it now. Exiting.")
                os._exit(0)
            logging.info("Advisory lock re-acquired after DB restart.")
        except Exception as e2:
            logging.error(f"Failed to re-acquire advisory lock: {e2}")

# ================== MODELS ==================
class Group(Base):
    __tablename__ = "groups"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    owner_user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    timezone: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    trial_started_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, nullable=True)
    expires_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    settings: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        Index("ix_users_chat_username", "chat_id", "username"),
        Index("ix_users_chat_tg", "chat_id", "tg_user_id", unique=True),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(128))
    last_name: Mapped[Optional[str]] = mapped_column(String(128))
    username: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    gender: Mapped[str] = mapped_column(String(8), default="unknown")
    birthday: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)

class GroupAdmin(Base):
    __tablename__ = "group_admins"
    __table_args__ = (Index("ix_ga_unique", "chat_id", "tg_user_id", unique=True),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, index=True)

class Relationship(Base):
    __tablename__ = "relationships"
    __table_args__ = (Index("ix_rel_unique", "chat_id", "user_a_id", "user_b_id", unique=True),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    user_a_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user_b_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    started_at: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)

class Crush(Base):
    __tablename__ = "crushes"
    __table_args__ = (Index("ix_crush_unique", "chat_id", "from_user_id", "to_user_id", unique=True),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    from_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    to_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

class ReplyStatDaily(Base):
    __tablename__ = "reply_stat_daily"
    __table_args__ = (Index("ix_reply_chat_date_user", "chat_id", "date", "target_user_id", unique=True),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    date: Mapped[dt.date] = mapped_column(Date, index=True)
    target_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    reply_count: Mapped[int] = mapped_column(Integer, default=0)

class ShipHistory(Base):
    __tablename__ = "ship_history"
    __table_args__ = (Index("ix_ship_chat_date", "chat_id", "date"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    date: Mapped[dt.date] = mapped_column(Date, index=True)
    male_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    female_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

class SubscriptionLog(Base):
    __tablename__ = "subscription_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    actor_tg_user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    action: Mapped[str] = mapped_column(String(32))
    amount_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

class Seller(Base):
    __tablename__ = "sellers"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    note: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

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

# ================== HELPERS ==================
def try_send_owner(text_msg: str):
    from telegram import Bot
    if not TOKEN or not OWNER_ID: return
    try: Bot(TOKEN).send_message(OWNER_ID, footer(text_msg))
    except Exception as e: logging.info(f"Owner DM failed: {e}")

def ensure_group(session, chat) -> 'Group':
    created = False
    g = session.get(Group, chat.id)
    if not g:
        created = True
        g = Group(
            id=chat.id, title=getattr(chat, "title", None) or str(chat.id),
            owner_user_id=None, timezone=DEFAULT_TZ,
            trial_started_at=dt.datetime.utcnow(),
            expires_at=dt.datetime.utcnow() + dt.timedelta(days=7),
            is_active=True, settings={}
        )
        session.add(g)
        session.add(SubscriptionLog(chat_id=chat.id, actor_tg_user_id=None, action="trial_start", amount_days=7))
        session.commit()
        try_send_owner(f"➕ ربات به گروه جدید اضافه شد:\n• {g.title}\n• chat_id: {g.id}\n• پلن: ۷ روز رایگان فعال شد.")
    else:
        if g.timezone != DEFAULT_TZ:
            g.timezone = DEFAULT_TZ
            session.commit()
    g._just_created = created
    return g

def upsert_user(session, chat_id, tg_user) -> 'User':
    u = session.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tg_user.id)).scalar_one_or_none()
    if not u:
        u = User(chat_id=chat_id, tg_user_id=tg_user.id,
                 first_name=tg_user.first_name, last_name=tg_user.last_name,
                 username=tg_user.username, gender="unknown")
        session.add(u); session.commit()
    else:
        changed = False
        if u.first_name != tg_user.first_name: u.first_name = tg_user.first_name; changed = True
        if u.last_name != tg_user.last_name: u.last_name = tg_user.last_name; changed = True
        if u.username != tg_user.username: u.username = tg_user.username; changed = True
        if changed: session.commit()
    return u

def is_seller(session, tg_user_id: int) -> bool:
    s = session.execute(select(Seller).where(Seller.tg_user_id==tg_user_id, Seller.is_active==True)).scalar_one_or_none()
    return bool(s)

def is_group_admin(session, chat_id: int, tg_user_id: int) -> bool:
    if tg_user_id == OWNER_ID: return True
    g = session.get(Group, chat_id)
    blocked = (g.settings or {}).get("blocked_sellers", []) if g else []
    if is_seller(session, tg_user_id) and tg_user_id not in blocked: return True
    row = session.execute(select(GroupAdmin).where(
        (GroupAdmin.chat_id==chat_id) & (GroupAdmin.tg_user_id==tg_user_id)
    )).scalar_one_or_none()
    return bool(row)

def group_active(g: Group) -> bool:
    return bool(g.expires_at and g.expires_at > dt.datetime.utcnow())

def mention_of(u: 'User') -> str:
    if u.username: return f"@{u.username}"
    name = u.first_name or "کاربر"
    return f'<a href="tg://user?id={u.tg_user_id}">{name}</a>'

def mention_by_tgid(session, chat_id: int, tg_user_id: int) -> str:
    u = session.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tg_user_id)).scalar_one_or_none()
    if u: return mention_of(u)
    return f'<a href="tg://user?id={tg_user_id}">کاربر</a>'

# --------- نرمال‌سازی ----------
ARABIC_FIX_MAP = str.maketrans({
    "ي": "ی", "ى": "ی", "ئ": "ی", "ك": "ک",
    "ـ": "",
})
PUNCS = " \u200c\u200f\u200e\u2066\u2067\u2068\u2069\t\r\n.,!?؟،;:()[]{}«»\"'"

def fa_norm(s: str) -> str:
    if s is None: return ""
    s = str(s).translate(ARABIC_FIX_MAP)
    s = s.replace("\u200c", " ").replace("\u200f", "").replace("\u200e","")
    s = s.replace("\u202a","").replace("\u202c","")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_text(s: str) -> str:
    return fa_norm(s)

RE_WORD_FAZOL = re.compile(rf"(?:^|[{re.escape(PUNCS)}])فضول(?:[{re.escape(PUNCS)}]|$)")

def chunked(lst: List, n: int):
    for i in range(0, len(lst), n): yield lst[i:i+n]

# ================== PANELS (state) ==================
PANELS: Dict[Tuple[int,int], Dict[str, Any]] = {}

# ================== INTERACTION WAITERS (expect target) ==================
# منتظر ماندن برای پیام بعدیِ همان کاربر جهت گرفتن @username یا آیدی عددی
WAITERS: Dict[Tuple[int,int], Dict[str, Any]] = {}  # key = (chat_id, user_id) -> {"for": str, "created_at": dt.datetime}
WAITER_TTL_SECONDS = 180  # سه دقیقه فرصت

def _waiter_key(chat_id: int, user_id: int) -> Tuple[int,int]:
    return (chat_id, user_id)

def _set_waiter(chat_id: int, user_id: int, what: str):
    WAITERS[_waiter_key(chat_id, user_id)] = {"for": what, "created_at": dt.datetime.utcnow()}

def _pop_waiter(chat_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    return WAITERS.pop(_waiter_key(chat_id, user_id), None)

def _peek_waiter(chat_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    data = WAITERS.get(_waiter_key(chat_id, user_id))
    if not data: return None
    if (dt.datetime.utcnow() - data["created_at"]).total_seconds() > WAITER_TTL_SECONDS:
        WAITERS.pop(_waiter_key(chat_id, user_id), None)
        return None
    return data

# ورودی‌های هدف: ریپلای، @username، یا آیدی عددی TG
USERNAME_RE = re.compile(r"^@?(\w+)$")
DIGITS_RE = re.compile(r"^\d{5,}$")  # آیدی‌های تلگرام معمولاً ۵ رقم به بالا هستند

def parse_target_token(s: str) -> Tuple[str, str]:
    """('username','uname') یا ('tgid','12345') یا ('bad','') برمی‌گرداند"""
    s = fa_to_en_digits(fa_norm(s or "")).strip()
    if not s: return ("bad","")
    m = USERNAME_RE.match(s)
    if m: return ("username", m.group(1))
    if DIGITS_RE.match(s): return ("tgid", s)
    return ("bad","")

def find_user_by_selector(session, chat_id: int, sel_type: str, sel_val: str) -> Optional['User']:
    if sel_type == "username":
        return session.execute(select(User).where(User.chat_id==chat_id, User.username==sel_val)).scalar_one_or_none()
    if sel_type == "tgid":
        try:
            tg_id = int(sel_val)
        except Exception:
            return None
        return session.execute(select(User).where(User.chat_id==chat_id, User.tg_user_id==tg_id)).scalar_one_or_none()
    return None

async def prompt_target(update: Update, context: ContextTypes.DEFAULT_TYPE, purpose_fa: str):
    """از کاربر می‌خواهیم @یوزرنیم یا آیدی عددی را بفرستد (فقط همان کاربر بعدی معتبر است)"""
    txt = f"لطفاً @یوزرنیم یا آیدی عددی کاربرِ هدف را برای «{purpose_fa}» بفرست.\nمثال: @ali یا 123456789"
    await reply_temp(update, context, txt)

def _target_from_reply(s, chat_id: int, update: Update) -> Optional['User']:
    if update.message and update.message.reply_to_message:
        return upsert_user(s, chat_id, update.message.reply_to_message.from_user)
    return None

# ---------- رابطه: ویزارد تاریخ با User.id داخلی ----------
async def open_relation_wizard_by_uid(update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
    y = jalali_now_year()
    years = list(range(y, y-16, -1))
    rows: List[List[InlineKeyboardButton]] = []
    for chunk in chunked(years, 4):
        rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"relid:y:{target_user_id}:{yy}") for yy in chunk])
    rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"relid:yp:{target_user_id}:{y-16}")])
    await panel_open_initial(update, context, "سال شمسی شروع رابطه را انتخاب کن", rows, root=False)

def add_nav(rows: List[List[InlineKeyboardButton]], root: bool=False) -> InlineKeyboardMarkup:
    nav = [InlineKeyboardButton("✖️ بستن", callback_data="nav:close")]
    if not root:
        nav.insert(0, InlineKeyboardButton("⬅️ بازگشت", callback_data="nav:back"))
    return InlineKeyboardMarkup([nav] + rows)

def _panel_key(chat_id: int, message_id: int) -> Tuple[int,int]:
    return (chat_id, message_id)

def _panel_push(msg, owner_id: int, title: str, rows: List[List[InlineKeyboardButton]], root: bool):
    key = _panel_key(msg.chat.id, msg.message_id)
    meta = PANELS.get(key, {"owner": owner_id, "stack": []})
    meta["owner"] = owner_id
    meta["stack"].append((title, rows, root))
    PANELS[key] = meta

def _panel_pop(msg) -> Optional[Tuple[str, List[List[InlineKeyboardButton]], bool]]:
    key = _panel_key(msg.chat.id, msg.message_id)
    meta = PANELS.get(key)
    if not meta or not meta["stack"]: return None
    if len(meta["stack"]) > 1:
        meta["stack"].pop()
        prev = meta["stack"][-1]
        PANELS[key] = meta
        return prev
    return None

async def panel_open_initial(update: Update, context: ContextTypes.DEFAULT_TYPE, title: str, rows: List[List[InlineKeyboardButton]], root: bool=True):
    msg = await update.effective_chat.send_message(footer(title), reply_markup=add_nav(rows, root=root))
    _panel_push(msg, update.effective_user.id, title, rows, root)
    schedule_autodelete(context, msg.chat_id, msg.message_id, keep=True)  # منو پاک نشه
    return msg

async def panel_edit(context: ContextTypes.DEFAULT_TYPE, qmsg, opener_id: int, title: str, rows: List[List[InlineKeyboardButton]], root: bool=False, parse_mode: Optional[str]=None):
    await qmsg.edit_text(footer(title), reply_markup=add_nav(rows, root=root), disable_web_page_preview=True, parse_mode=parse_mode)
    _panel_push(qmsg, opener_id, title, rows, root)
    schedule_autodelete(context, qmsg.chat.id, qmsg.message_id, keep=True)  # منو پاک نشه

def kb_group_menu(is_group_admin_flag: bool) -> List[List[InlineKeyboardButton]]:
    rows = [
        [InlineKeyboardButton("👤 ثبت جنسیت", callback_data="ui:gset")],
        [InlineKeyboardButton("🎂 ثبت تولد", callback_data="ui:bd:start")],
        [InlineKeyboardButton("💘 ثبت کراش (ریپلای)", callback_data="ui:crush:add"),
         InlineKeyboardButton("🗑️ حذف کراش", callback_data="ui:crush:del")],
        [InlineKeyboardButton("💞 ثبت رابطه (با @ و انتخاب تاریخ)", callback_data="ui:rel:help")],
        [InlineKeyboardButton("👑 محبوب امروز", callback_data="ui:pop"),
         InlineKeyboardButton("💫 شیپ امشب", callback_data="ui:ship")],
        [InlineKeyboardButton("❤️ شیپم کن", callback_data="ui:shipme")],
        [InlineKeyboardButton("🏷️ تگ دخترها", callback_data="ui:tag:girls"),
         InlineKeyboardButton("🏷️ تگ پسرها", callback_data="ui:tag:boys")],
        [InlineKeyboardButton("🏷️ تگ همه", callback_data="ui:tag:all")],
        [InlineKeyboardButton("🔐 داده‌های من", callback_data="ui:privacy:me"),
         InlineKeyboardButton("🗑️ حذف من", callback_data="ui:privacy:delme")],
    ]
    if is_group_admin_flag:
        rows.append([InlineKeyboardButton("⚙️ پیکربندی فضول", callback_data="cfg:open")])
    return rows

def kb_config(chat_id: int, bot_username: str) -> List[List[InlineKeyboardButton]]:
    return [
        [InlineKeyboardButton("⚡️ شارژ گروه", callback_data="ui:charge:open")],
        [InlineKeyboardButton("👥 مدیران گروه", callback_data="ga:list")],
        [InlineKeyboardButton("ℹ️ مشاهده انقضا", callback_data="ui:expiry")],
        [InlineKeyboardButton("⛔️ صفر کردن شارژ", callback_data=f"chg:{chat_id}:0")],
        [InlineKeyboardButton("🚪 خروج ربات", callback_data=f"grp:{chat_id}:leave")],
        [InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
        [InlineKeyboardButton("🧹 پاکسازی گروه", callback_data=f"wipe:{chat_id}")],
    ]
# ================== TARGET HELPERS (waiters / selectors) ==================
# نگه‌داشتن وضعیت «در انتظار هدف» برای هر کاربر در هر گروه
WAITERS: Dict[Tuple[int, int], Dict[str, Any]] = {}

def _wkey(chat_id: int, user_id: int) -> Tuple[int, int]:
    return (chat_id, user_id)

def _set_waiter(chat_id: int, user_id: int, purpose: str) -> None:
    # purpose یکی از: relation_set | relation_del | crush_add | crush_del | admin_add | admin_del
    WAITERS[_wkey(chat_id, user_id)] = {"for": purpose, "at": dt.datetime.utcnow()}

def _peek_waiter(chat_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    return WAITERS.get(_wkey(chat_id, user_id))

def _pop_waiter(chat_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    return WAITERS.pop(_wkey(chat_id, user_id), None)

def parse_target_token(s: str) -> Tuple[str, Any]:
    """
    ورودی کاربر برای اشاره به هدف را پارس می‌کند.
    خروجی:
      ("username", "foo")  وقتی مثل @foo یا foo است (فقط حروف/عدد/آندرلاین)
      ("id", 123456789)    وقتی عددی است
      ("bad", None)        در غیر این صورت
    """
    t = fa_to_en_digits(clean_text(s))
    if not t:
        return ("bad", None)
    # اگر با @ شروع شد
    if t.startswith("@"):
        uname = t[1:].strip()
        if re.fullmatch(r"\w{3,}", uname or ""):
            return ("username", uname)
        return ("bad", None)
    # اگر فقط یک توکن مثل foo بود
    if " " not in t and re.fullmatch(r"\w{3,}", t):
        # به‌عنوان یوزرنیم قبول می‌کنیم
        return ("username", t)
    # اگر عددی بود (آیدی تلگرام)
    digits = t.replace(" ", "")
    if re.fullmatch(r"\d{5,}", digits):
        try:
            return ("id", int(digits))
        except Exception:
            return ("bad", None)
    return ("bad", None)

def find_user_by_selector(session, chat_id: int, sel_type: str, sel_val: Any) -> Optional['User']:
    """
    sel_type: "username" یا "id"
    در دیتابیس همان گروه به‌دنبالش می‌گردد. کاربر باید قبلاً در گروه پیام داده باشد.
    """
    if sel_type == "username":
        return session.execute(
            select(User).where(User.chat_id == chat_id, User.username == str(sel_val))
        ).scalar_one_or_none()
    if sel_type == "id":
        return session.execute(
            select(User).where(User.chat_id == chat_id, User.tg_user_id == int(sel_val))
        ).scalar_one_or_none()
    return None

def _target_from_reply(session, chat_id: int, update: Update) -> Optional['User']:
    """اگر روی پیام کسی ریپلای شده بود، همان فرد را (در DB گروه) برمی‌گرداند/می‌سازد."""
    if not update.message or not update.message.reply_to_message:
        return None
    try:
        return upsert_user(session, chat_id, update.message.reply_to_message.from_user)
    except Exception:
        return None

async def prompt_target(update: Update, context: ContextTypes.DEFAULT_TYPE, title: str):
    """وقتی هدف مشخص نیست از کاربر می‌خواهیم @یوزرنیم یا آیدی عددی بفرستد."""
    txt = (
        f"🔎 {title}\n"
        "لطفاً @یوزرنیم یا آیدی عددی طرف مقابل را ارسال کن.\n"
        "مثال: @foo یا 123456789"
    )
    await reply_temp(update, context, txt, keep=False)

async def open_relation_wizard_by_uid(update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_internal_id: int):
    """
    ویزارد تاریخ شروع رابطه را بر اساس user.id داخلی (جدول users) باز می‌کند.
    از مسیر relid:* در کال‌بک‌ها استفاده می‌کنیم.
    """
    y = jalali_now_year()
    years = list(range(y, y-16, -1))
    rows: List[List[InlineKeyboardButton]] = []
    for chunk in chunked(years, 4):
        rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"relid:y:{target_user_internal_id}:{yy}") for yy in chunk])
    rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"relid:yp:{target_user_internal_id}:{y-16}")])
    await panel_open_initial(update, context, "سال شمسی شروع رابطه را انتخاب کن", rows, root=False)

# ================== PATTERNS ==================
PAT_GROUP = {
    "menu": re.compile(r"^(?:فضول منو|منو)$"),
    "help": re.compile(r"^(?:فضول کمک|راهنما|کمک)$"),
    "config": re.compile(r"^(?:پیکربندی فضول|فضول پیکربندی|فضول تنظیمات|تنظیمات فضول)$"),
    "bot_stats": re.compile(r"^(?:آمار فضول|فضول آمار|آمار ربات)$"),
    "admin_add": re.compile(r"^فضول ادمین(?: @?(\w+))?$"),
    "admin_del": re.compile(r"^حذف فضول ادمین(?: @?(\w+))?$"),
    "gender": re.compile(r"^ثبت جنسیت (دختر|پسر)$"),
    "birthday_wizard": re.compile(r"^ثبت تولد$"),
    "birthday_set": re.compile(r"^ثبت تولد ([\d\/\-]+)$"),
    "birthday_del": re.compile(r"^حذف تولد$"),
    "relation_set_wizard": re.compile(r"^ثبت رابطه @?(\w+)$"),
    "relation_set": re.compile(r"^ثبت رابطه @?(\w+)\s+([\d\/\-]+)$"),
    "relation_del": re.compile(r"^حذف رابطه @?(\w+)$"),
    "crush_add": re.compile(r"^ثبت کراش$"),
    "crush_del": re.compile(r"^حذف کراش$"),
    "popular_today": re.compile(r"^محبوب امروز$"),
    "ship_tonight": re.compile(r"^شیپ امشب$"),
    "ship_me": re.compile(r"^شیپم کن$"),
    "expiry": re.compile(r"^فضول انقضا$"),
    "charge": re.compile(r"^فضول شارژ$"),
    "tag_girls": re.compile(r"^تگ دخترها$"),
    "tag_boys": re.compile(r"^تگ پسرها$"),
    "tag_all": re.compile(r"^تگ همه$"),
    "privacy_me": re.compile(r"^حذف من$"),
    "privacy_info": re.compile(r"^(?:داده(?:‌| )های من|حریم خصوصی)$"),
    "wipe_group": re.compile(r"^پاکسازی گروه$"),
}

# الگوهای انعطاف‌پذیر جدید برای کار با ریپلای / @یوزرنیم / آیدی عددی
PAT_GROUP.update({
    # ادمین‌ها
    "admin_add_any": re.compile(r"^فضول ادمین(?:\s+(?:@?(\w+)|(\d+)))?$"),
    "admin_del_any": re.compile(r"^حذف فضول ادمین(?:\s+(?:@?(\w+)|(\d+)))?$"),

    # رابطه
    "relation_any": re.compile(r"^ثبت رابطه(?:\s+(?:@?(\w+)|(\d+)))?$"),
    "relation_del_any": re.compile(r"^حذف رابطه(?:\s+(?:@?(\w+)|(\d+)))?$"),

    # کراش
    "crush_add_any": re.compile(r"^ثبت کراش(?:\s+(?:@?(\w+)|(\d+)))?$"),
    "crush_del_any": re.compile(r"^حذف کراش(?:\s+(?:@?(\w+)|(\d+)))?$"),
})

PAT_DM = {
    "panel": re.compile(r"^(?:پنل|مدیریت|کمک)$"),
    "groups": re.compile(r"^گروه‌ها$"),
    "manage": re.compile(r"^مدیریت (\-?\d+)$"),
    "extend": re.compile(r"^تمدید (\-?\d+)\s+(\d+)$"),
    "add_seller": re.compile(r"^افزودن فروشنده (\d+)(?:\s+(.+))?$"),
    "del_seller": re.compile(r"^حذف فروشنده (\d+)$"),
    "list_sellers": re.compile(r"^لیست فروشنده‌ها$"),
    "bot_stats": re.compile(r"^(?:آمار فضول|فضول آمار|آمار ربات)$"),
}

# ================== GROUP TEXT ==================
async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group","supergroup") or not update.message or not update.message.text:
        return
    logging.info(f"[grp] msg from {update.effective_user.id if update.effective_user else '-'}: {update.message.text}")
    text = clean_text(update.message.text)

    # «فضول منو» یا «فضول کمک» با وجود کلمهٔ فضول
    if RE_WORD_FAZOL.search(text):
        if "منو" in text or "فهرست" in text:
            with SessionLocal() as s:
                g = ensure_group(s, update.effective_chat)
                is_gadmin = is_group_admin(s, g.id, update.effective_user.id)
            title = "🕹 منوی فضول"
            rows = kb_group_menu(is_gadmin)
            await panel_open_initial(update, context, title, rows, root=True)
            return
        if "کمک" in text or "راهنما" in text:
            await reply_temp(update, context, user_help_text())
            return

    # اگر منتظر هدف از همین کاربر هستیم
    waiter = _peek_waiter(update.effective_chat.id, update.effective_user.id)
    if waiter:
        with SessionLocal() as s:
            g = ensure_group(s, update.effective_chat)
            sel_type, sel_val = parse_target_token(text)
            if sel_type == "bad":
                await reply_temp(update, context, "قابل فهم نبود. یک @یوزرنیم یا آیدی عددی مثل 123456789 بفرست.")
                return
            target = find_user_by_selector(s, g.id, sel_type, sel_val)
            if not target:
                await reply_temp(update, context, "کاربر هدف در دیتابیس گروه پیدا نشد. باید قبلاً در گروه پیام داده باشد.")
                _pop_waiter(g.id, update.effective_user.id)
                return

            purpose = waiter["for"]
            _pop_waiter(g.id, update.effective_user.id)
            me = upsert_user(s, g.id, update.effective_user)

            if purpose == "relation_set":
                await open_relation_wizard_by_uid(update, context, target.id)
                return
            if purpose == "relation_del":
                s.execute(Relationship.__table__.delete().where(
                    (Relationship.chat_id==g.id) & (
                        ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==target.id)) |
                        ((Relationship.user_a_id==target.id) & (Relationship.user_b_id==me.id))
                    )
                )); s.commit()
                await reply_temp(update, context, "رابطه حذف شد 🗑️"); return
            if purpose == "crush_add":
                if me.id == target.id: await reply_temp(update, context, "روی خودت نمی‌شه 😅"); return
                try:
                    s.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=target.id)); s.commit()
                    await reply_temp(update, context, "کراش ثبت شد 💘")
                except Exception:
                    await reply_temp(update, context, "از قبل ثبت شده بود.")
                return
            if purpose == "crush_del":
                s.execute(Crush.__table__.delete().where(
                    (Crush.chat_id==g.id) & (Crush.from_user_id==me.id) & (Crush.to_user_id==target.id)
                )); s.commit()
                await reply_temp(update, context, "کراش حذف شد 🗑️"); return
            if purpose == "admin_add":
                if not is_group_admin(s, g.id, update.effective_user.id):
                    await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک می‌تواند."); return
                try:
                    s.add(GroupAdmin(chat_id=g.id, tg_user_id=target.tg_user_id)); s.commit()
                    await reply_temp(update, context, "✅ به‌عنوان ادمین گروه اضافه شد.")
                except Exception:
                    await reply_temp(update, context, "قبلاً ادمین بوده یا خطا رخ داد.")
                return
            if purpose == "admin_del":
                if not is_group_admin(s, g.id, update.effective_user.id):
                    await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک می‌تواند."); return
                if target.tg_user_id == OWNER_ID or is_seller(s, target.tg_user_id):
                    await reply_temp(update, context, "نمی‌توان مالک/فروشنده را حذف کرد."); return
                s.execute(GroupAdmin.__table__.delete().where(
                    (GroupAdmin.chat_id==g.id) & (GroupAdmin.tg_user_id==target.tg_user_id)
                )); s.commit()
                await reply_temp(update, context, "🗑️ ادمین گروه حذف شد."); return

    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)
        is_gadmin = is_group_admin(s, g.id, update.effective_user.id)

    # منو/راهنما/آمار/پیکربندی مثل قبل … (بدون تغییر)

    # ===== سایر دستورات متنی =====
    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)

        # … جنسیت/تولد/… مثل قبل (بدون تغییر)

        # ---------------- ثبت رابطه — انعطاف ----------------
        if PAT_GROUP["relation_any"].match(text):
            if not group_active(g):
                await reply_temp(update, context, "⌛️ اعتبار گروه تمام شده."); return

            m = PAT_GROUP["relation_any"].match(text)
            target_user = _target_from_reply(s, g.id, update)

            if not target_user:
                uname = m.group(1); did = m.group(2)
                if uname:
                    target_user = s.execute(select(User).where(User.chat_id==g.id, User.username==uname)).scalar_one_or_none()
                elif did:
                    try:
                        tid = int(did)
                        target_user = s.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tid)).scalar_one_or_none()
                    except Exception:
                        target_user = None

            if not target_user:
                _set_waiter(g.id, update.effective_user.id, "relation_set")
                await prompt_target(update, context, "ثبت رابطه")
                return

            await open_relation_wizard_by_uid(update, context, target_user.id)
            return

        # ---------------- حذف رابطه — انعطاف ----------------
        if PAT_GROUP["relation_del_any"].match(text):
            m = PAT_GROUP["relation_del_any"].match(text)
            me = upsert_user(s, g.id, update.effective_user)
            target_user = _target_from_reply(s, g.id, update)

            if not target_user:
                uname = m.group(1); did = m.group(2)
                if uname:
                    target_user = s.execute(select(User).where(User.chat_id==g.id, User.username==uname)).scalar_one_or_none()
                elif did:
                    try:
                        tid = int(did)
                        target_user = s.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tid)).scalar_one_or_none()
                    except Exception:
                        target_user = None

            if not target_user:
                _set_waiter(g.id, update.effective_user.id, "relation_del")
                await prompt_target(update, context, "حذف رابطه")
                return

            s.execute(Relationship.__table__.delete().where(
                (Relationship.chat_id==g.id) & (
                    ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==target_user.id)) |
                    ((Relationship.user_a_id==target_user.id) & (Relationship.user_b_id==me.id))
                )
            )); s.commit()
            await reply_temp(update, context, "رابطه حذف شد 🗑️")
            return

        # … بقیهٔ دستورات (کراش/تگ/… و حریم خصوصی) مثل قبل
    # شمارش ریپلای‌ها (مثل قبل)

        # ---------------- کراش — انعطاف (ریپلای/یوزرنیم/آیدی/درخواست هدف) ----------------
        if PAT_GROUP["crush_add_any"].match(text):
            if not group_active(g):
                await reply_temp(update, context, "⌛️ اعتبار گروه تمام شده."); return
            me = upsert_user(s, g.id, update.effective_user)

            m = PAT_GROUP["crush_add_any"].match(text)
            target_user = _target_from_reply(s, g.id, update)
            if not target_user:
                uname = m.group(1); did = m.group(2)
                if uname:
                    target_user = s.execute(select(User).where(User.chat_id==g.id, User.username==uname)).scalar_one_or_none()
                elif did:
                    try:
                        tid = int(did)
                        target_user = s.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tid)).scalar_one_or_none()
                    except Exception:
                        target_user = None

            if not target_user:
                _set_waiter(g.id, update.effective_user.id, "crush_add")
                await prompt_target(update, context, "ثبت کراش")
                return

            if me.id == target_user.id:
                await reply_temp(update, context, "روی خودت نمی‌شه 😅"); return
            try:
                s.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=target_user.id)); s.commit()
                await reply_temp(update, context, "کراش ثبت شد 💘")
            except Exception:
                await reply_temp(update, context, "از قبل ثبت شده بود.")
            return

        if PAT_GROUP["crush_del_any"].match(text):
            me = upsert_user(s, g.id, update.effective_user)

            m = PAT_GROUP["crush_del_any"].match(text)
            target_user = _target_from_reply(s, g.id, update)
            if not target_user:
                uname = m.group(1); did = m.group(2)
                if uname:
                    target_user = s.execute(select(User).where(User.chat_id==g.id, User.username==uname)).scalar_one_or_none()
                elif did:
                    try:
                        tid = int(did)
                        target_user = s.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==tid)).scalar_one_or_none()
                    except Exception:
                        target_user = None

            if not target_user:
                _set_waiter(g.id, update.effective_user.id, "crush_del")
                await prompt_target(update, context, "حذف کراش")
                return

            s.execute(Crush.__table__.delete().where(
                (Crush.chat_id==g.id) & (Crush.from_user_id==me.id) & (Crush.to_user_id==target_user.id)
            )); s.commit()
            await reply_temp(update, context, "کراش حذف شد 🗑️")
            return

        # محبوب امروز
        if PAT_GROUP["popular_today"].match(text):
            today = dt.datetime.now(TZ_TEHRAN).date()
            with SessionLocal() as s2:
                rows = s2.execute(select(ReplyStatDaily).where(
                    (ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.date==today)
                ).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if not rows:
                await reply_temp(update, context, "امروز هنوز آماری نداریم.", keep=True); return
            lines=[]
            with SessionLocal() as s3:
                for i,r in enumerate(rows, start=1):
                    u = s3.get(User, r.target_user_id)
                    name = u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                    lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
            await reply_temp(update, context, "\n".join(lines), keep=True); return

        # شیپ امشب (آخرین ثبت)
        if PAT_GROUP["ship_tonight"].match(text):
            today = dt.datetime.now(TZ_TEHRAN).date()
            with SessionLocal() as s2:
                last = s2.execute(select(ShipHistory).where(
                    (ShipHistory.chat_id==g.id) & (ShipHistory.date==today)
                ).order_by(ShipHistory.id.desc())).scalar_one_or_none()
            if not last:
                await reply_temp(update, context, "هنوز شیپ امشب ساخته نشده. آخر شب منتشر می‌شه 💫", keep=True); return
            with SessionLocal() as s3:
                m, f = s3.get(User, last.male_user_id), s3.get(User, last.female_user_id)
            await reply_temp(update, context,
                             f"💘 شیپِ امشب: {(m.first_name or '@'+(m.username or ''))} × {(f.first_name or '@'+(f.username or ''))}",
                             keep=True)
            return

        # شیپم کن — پارتنر پیشنهادی
        if PAT_GROUP["ship_me"].match(text):
            me = upsert_user(s, g.id, update.effective_user)
            if me.gender not in ("male","female"):
                await reply_temp(update, context, "اول جنسیتت رو ثبت کن: «ثبت جنسیت دختر/پسر»."); return
            rels = s.query(Relationship).filter_by(chat_id=g.id).all()
            in_rel = set([r.user_a_id for r in rels] + [r.user_b_id for r in rels])
            my_rel = me.id in in_rel
            if my_rel:
                await reply_temp(update, context, "تو در رابطه‌ای. برای پیشنهاد باید سینگل باشی."); return
            opposite = "female" if me.gender=="male" else "male"
            candidates = s.query(User).filter_by(chat_id=g.id, gender=opposite).all()
            candidates = [u for u in candidates if u.id not in in_rel and u.tg_user_id != me.tg_user_id]
            if not candidates:
                await reply_temp(update, context, "کسی از جنس مخالفِ سینگل پیدا نشد."); return
            cand = random.choice(candidates)
            await reply_temp(update, context, f"❤️ پارتنر پیشنهادی برای شما: {mention_of(cand)}", keep=True, parse_mode=ParseMode.HTML)
            return

        # انقضا
        if PAT_GROUP["expiry"].match(text):
            ex = g.expires_at and fmt_dt_fa(g.expires_at)
            await reply_temp(update, context, f"⏳ اعتبار این گروه تا: {ex or 'نامشخص'}"); return

        # شارژ منو
        if PAT_GROUP["charge"].match(text):
            with SessionLocal() as s2:
                if not is_group_admin(s2, g.id, update.effective_user.id):
                    await reply_temp(update, context, "دسترسی نداری."); return
            chat_id = update.effective_chat.id
            kb = [
                [InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{chat_id}:30"),
                 InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{chat_id}:90"),
                 InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{chat_id}:180")],
                [InlineKeyboardButton("⛔️ صفر کردن شارژ", callback_data=f"chg:{chat_id}:0")]
            ]
            await panel_open_initial(update, context, "⌁ پنل شارژ گروه", kb, root=False)
            return

        # تگ‌ها
        if PAT_GROUP["tag_girls"].match(text) or PAT_GROUP["tag_boys"].match(text) or PAT_GROUP["tag_all"].match(text):
            if not update.message.reply_to_message:
                await reply_temp(update, context, "برای تگ کردن، روی یک پیام ریپلای کن."); return
            reply_to = update.message.reply_to_message.message_id
            with SessionLocal() as s2:
                if PAT_GROUP["tag_girls"].match(text):
                    users = s2.query(User).filter_by(chat_id=g.id, gender="female").all()
                elif PAT_GROUP["tag_boys"].match(text):
                    users = s2.query(User).filter_by(chat_id=g.id, gender="male").all()
                else:
                    users = s2.query(User).filter_by(chat_id=g.id).all()
            if not users:
                await reply_temp(update, context, "کسی برای تگ پیدا نشد."); return
            mentions = [mention_of(u) for u in users]
            for pack in chunked(mentions, 4):
                try:
                    await context.bot.send_message(
                        chat_id=g.id, text=" ".join(pack),
                        reply_to_message_id=reply_to, parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.8)
                except Exception as e:
                    logging.info(f"Tag batch send failed: {e}")
            return

        # حریم خصوصی
        if PAT_GROUP["privacy_me"].match(text):
            me_id = update.effective_user.id
            with SessionLocal() as s2:
                u = s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==me_id)).scalar_one_or_none()
                if not u:
                    await reply_temp(update, context, "اطلاعاتی از شما نداریم."); return
                s2.execute(Crush.__table__.delete().where((Crush.chat_id==g.id) & ((Crush.from_user_id==u.id) | (Crush.to_user_id==u.id))))
                s2.execute(Relationship.__table__.delete().where((Relationship.chat_id==g.id) & ((Relationship.user_a_id==u.id) | (Relationship.user_b_id==u.id))))
                s2.execute(ReplyStatDaily.__table__.delete().where((ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.target_user_id==u.id)))
                s2.execute(User.__table__.delete().where((User.chat_id==g.id) & (User.id==u.id)))
                s2.commit()
            await reply_temp(update, context, "✅ تمام داده‌های شما در این گروه حذف شد."); return

        if PAT_GROUP["privacy_info"].match(text):
            me_id = update.effective_user.id
            with SessionLocal() as s2:
                u = s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==me_id)).scalar_one_or_none()
                if not u:
                    await reply_temp(update, context, "چیزی از شما ذخیره نشده."); return
                info = f"👤 نام: {u.first_name or ''} @{u.username or ''}\nجنسیت: {u.gender}\nتولد (شمسی): {fmt_date_fa(u.birthday)}"
            await reply_temp(update, context, info); return

        # پاکسازی
        if PAT_GROUP["wipe_group"].match(text):
            with SessionLocal() as s2:
                if not is_group_admin(s2, g.id, update.effective_user.id):
                    await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک."); return
            kb = [[InlineKeyboardButton("🧹 تایید پاکسازی", callback_data=f"wipe:{g.id}"),
                   InlineKeyboardButton("انصراف", callback_data="noop")]]
            await panel_open_initial(update, context, "⚠️ مطمئنی کل داده‌های گروه حذف شود؟", kb, root=False)
            return

    # شمارش ریپلای‌ها
    if update.message.reply_to_message:
        with SessionLocal() as s:
            g = ensure_group(s, update.effective_chat)
            today = dt.datetime.now(TZ_TEHRAN).date()
            target = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            upsert_user(s, g.id, update.effective_user)
            row = s.execute(select(ReplyStatDaily).where(
                (ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.date==today) & (ReplyStatDaily.target_user_id==target.id)
            )).scalar_one_or_none()
            if not row:
                row = ReplyStatDaily(chat_id=g.id, date=today, target_user_id=target.id, reply_count=0)
                s.add(row)
            row.reply_count += 1
            s.commit()

# ================== CALLBACKS ==================# ================== CALLBACKS ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q: return
    data = q.data or ""
    msg = q.message
    if not msg:
        await q.answer("پیام یافت نشد.", show_alert=True)
        return

    user_id = q.from_user.id
    chat_id = msg.chat.id
    key = _panel_key(chat_id, msg.message_id)

    # لاگ کال‌بک برای دیباگ
    logging.info(f"[cb] chat={chat_id} user={user_id} data={data}")

    # اگر state از بین رفته (مثلاً بعد از ری‌استارت)، خودمان بسازیم تا دکمه‌ها از کار نیفتند
    meta = PANELS.get(key)
    if not meta:
        PANELS[key] = {"owner": user_id, "stack": []}
        meta = PANELS[key]

    # فقط صاحب منو اجازه دارد
    owner_id = meta.get("owner")
    if owner_id is not None and owner_id != user_id:
        await q.answer("این منو مخصوص کسی است که آن را باز کرده.", show_alert=True)
        return

    await q.answer()

    # ناوبری
    if data == "nav:close":
        try:
            await msg.delete()
        except Exception: ...
        PANELS.pop(key, None)
        return

    if data == "nav:back":
        prev = _panel_pop(msg)
        if not prev:
            try:
                await msg.delete()
            except Exception: ...
            PANELS.pop(key, None)
            return
        title, rows, root = prev
        await panel_edit(context, msg, user_id, title, rows, root=root)
        return

    # کمک کاربر در PV
    if data == "usr:help":
        await panel_edit(context, msg, user_id, user_help_text(), [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False)
        return

    # پنل گروه: پیکربندی
    if data == "cfg:open":
        with SessionLocal() as s:
            if not is_group_admin(s, chat_id, user_id):
                await panel_edit(context, msg, user_id, "دسترسی نداری.", [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False)
                return
        rows = kb_config(chat_id, context.bot.username)
        await panel_edit(context, msg, user_id, "⚙️ پیکربندی فضول", rows, root=False)
        return

    if data == "cfg:sync":
        added = await sync_group_admins(context.bot, chat_id)
        await panel_edit(context, msg, user_id, f"✅ همگام شد. ادمین‌های جدید: {fa_digits(added)}", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    # مشاهده انقضا
    if data == "ui:expiry":
        with SessionLocal() as s:
            g = s.get(Group, chat_id)
            ex = g and g.expires_at and fmt_dt_fa(g.expires_at)
        await panel_edit(context, msg, user_id, f"⏳ اعتبار گروه تا: {ex or 'نامشخص'}", [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False)
        return

    # شارژ گروه (پنل)
    if data == "ui:charge:open":
        kb = [
            [InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{chat_id}:30"),
             InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{chat_id}:90"),
             InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{chat_id}:180")],
            [InlineKeyboardButton("⛔️ صفر کردن شارژ", callback_data=f"chg:{chat_id}:0")]
        ]
        await panel_edit(context, msg, user_id, "⌁ پنل شارژ گروه", kb, root=False)
        return

    # اعمال شارژ/صفرکردن
    m = re.match(r"^chg:(-?\d+):(\d+)$", data)
    if m:
        target_chat = int(m.group(1)); days = int(m.group(2))
        with SessionLocal() as s:
            if not is_group_admin(s, target_chat, user_id):
                await panel_edit(context, msg, user_id, "دسترسی نداری.", [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False)
                return
            g = s.get(Group, target_chat)
            if not g:
                await panel_edit(context, msg, user_id, "گروه پیدا نشد.", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False); return
            if days <= 0:
                g.expires_at = dt.datetime.utcnow()
                s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=user_id, action="reset", amount_days=0))
                s.commit()
                await panel_edit(context, msg, user_id, "⛔️ شارژ گروه صفر شد.", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
            else:
                base = g.expires_at if g.expires_at and g.expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
                g.expires_at = base + dt.timedelta(days=days)
                s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=user_id, action="extend", amount_days=days))
                s.commit()
                await panel_edit(context, msg, user_id, f"✅ تمدید شد تا {fmt_dt_fa(g.expires_at)}", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    # خروج ربات
    m = re.match(r"^grp:(-?\d+):leave$", data)
    if m:
        target_chat = int(m.group(1))
        with SessionLocal() as s:
            if not is_group_admin(s, target_chat, user_id):
                await panel_edit(context, msg, user_id, "دسترسی نداری.", [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False); return
        await panel_edit(context, msg, user_id, "در حال ترک گروه…", [[InlineKeyboardButton("باشه", callback_data="nav:close")]], root=False)
        try:
            await context.bot.leave_chat(target_chat)
        except Exception: ...
        return

    # پاکسازی گروه (تایید)
    m = re.match(r"^wipe:(-?\d+)$", data)
    if m:
        target_chat = int(m.group(1))
        with SessionLocal() as s:
            if not is_group_admin(s, target_chat, user_id):
                await panel_edit(context, msg, user_id, "دسترسی نداری.", [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False); return
            s.execute(Crush.__table__.delete().where(Crush.chat_id==target_chat))
            s.execute(Relationship.__table__.delete().where(Relationship.chat_id==target_chat))
            s.execute(ReplyStatDaily.__table__.delete().where(ReplyStatDaily.chat_id==target_chat))
            s.execute(User.__table__.delete().where(User.chat_id==target_chat))
            s.commit()
        await panel_edit(context, msg, user_id, "🧹 پاکسازی انجام شد.", [[InlineKeyboardButton("باشه", callback_data="nav:back")]], root=False)
        return

    # لیست ادمین‌های گروه — با منشن
    if data == "ga:list":
        with SessionLocal() as s:
            gas = s.query(GroupAdmin).filter_by(chat_id=chat_id).all()
            if not gas:
                txt = "ادمینی ثبت نشده. «پیکربندی فضول» را بزن تا ادمین‌ها همگام شوند."
            else:
                mentions = [mention_by_tgid(s, chat_id, ga.tg_user_id) for ga in gas[:50]]
                txt = "👥 ادمین‌های فضول:\n" + "\n".join(f"- {m}" for m in mentions)
        await panel_edit(context, msg, user_id, txt, [[InlineKeyboardButton("همگام‌سازی مجدد", callback_data="cfg:sync")]], root=False, parse_mode=ParseMode.HTML)
        return

    # ثبت جنسیت
    if data == "ui:gset":
        rows = [[InlineKeyboardButton("👧 دختر", callback_data="gset:f")],
                [InlineKeyboardButton("👦 پسر", callback_data="gset:m")]]
        await panel_edit(context, msg, user_id, "جنسیتت چیه؟", rows, root=False)
        return

    if data.startswith("gset:"):
        is_female = data.endswith(":f")
        with SessionLocal() as s:
            u = upsert_user(s, chat_id, update.effective_user)
            u.gender = "female" if is_female else "male"; s.commit()
        await panel_edit(context, msg, user_id, "ثبت شد ✅", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    # ثبت تولد — ویزارد سال/ماه/روز (شمسی)
    if data == "ui:bd:start":
        y = jalali_now_year()
        years = list(range(y, y-16, -1))
        rows=[]
        for chunk in chunked(years, 4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"bd:y:{yy}") for yy in chunk])
        rows.append([InlineKeyboardButton("سال‌های قدیمی‌تر", callback_data=f"bd:yp:{y-16}")])
        await panel_edit(context, msg, user_id, "سال تولدت رو انتخاب کن (شمسی)", rows, root=False)
        return

    m = re.match(r"^bd:yp:(\d+)$", data)
    if m:
        start = int(m.group(1))
        years = list(range(start, start-16, -1))
        rows=[]
        for chunk in chunked(years, 4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"bd:y:{yy}") for yy in chunk])
        rows.append([InlineKeyboardButton("قدیمی‌تر", callback_data=f"bd:yp:{start-16}")])
        await panel_edit(context, msg, user_id, "سال تولدت رو انتخاب کن (شمسی)", rows, root=False)
        return

    m = re.match(r"^bd:y:(\d+)$", data)
    if m:
        yy = int(m.group(1))
        rows=[]
        for i in range(1, 13):
            rows.append([InlineKeyboardButton(fa_digits(f"{i:02d}"), callback_data=f"bd:m:{yy}:{i}")])
        await panel_edit(context, msg, user_id, f"سال {fa_digits(yy)} — ماه تولد را انتخاب کن", rows, root=False)
        return

    m = re.match(r"^bd:m:(\d+):(\d+)$", data)
    if m:
        yy = int(m.group(1)); mm = int(m.group(2))
        md = jalali_month_len(yy, mm)
        rows=[]
        for chunk in chunked(list(range(1, md+1)), 6):
            rows.append([InlineKeyboardButton(fa_digits(f"{d:02d}"), callback_data=f"bd:d:{yy}:{mm}:{d}") for d in chunk])
        await panel_edit(context, msg, user_id, f"تاریخ: {fa_digits(yy)}/{fa_digits(mm)} — روز را انتخاب کن", rows, root=False)
        return

    m = re.match(r"^bd:d:(\d+):(\d+):(\d+)$", data)
    if m:
        yy, mm, dd = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        try:
            gdate = JalaliDate(yy, mm, dd).to_gregorian() if HAS_PTOOLS else dt.date(2000+yy%100, mm, dd)
        except Exception:
            await panel_edit(context, msg, user_id, "تاریخ نامعتبر شد. دوباره تلاش کن.", [[InlineKeyboardButton("برگشت", callback_data="ui:bd:start")]], root=False)
            return
        with SessionLocal() as s:
            u = upsert_user(s, chat_id, update.effective_user)
            u.birthday = gdate; s.commit()
        await panel_edit(context, msg, user_id, f"🎂 تولد ثبت شد: {fmt_date_fa(gdate)}", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    # ===== ثبت رابطه — ویزارد تاریخ بر مبنای user_id داخلی (relid:*) =====
    m = re.match(r"^relid:yp:(\d+):(\d+)$", data)
    if m:
        uid = int(m.group(1)); start = int(m.group(2))
        years = list(range(start, start-16, -1))
        rows=[]
        for chunk in chunked(years, 4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"relid:y:{uid}:{yy}") for yy in chunk])
        rows.append([InlineKeyboardButton("قدیمی‌تر", callback_data=f"relid:yp:{uid}:{start-16}")])
        await panel_edit(context, msg, user_id, "سال شمسی شروع رابطه را انتخاب کن", rows, root=False)
        return

    m = re.match(r"^relid:y:(\d+):(\d+)$", data)
    if m:
        uid = int(m.group(1)); yy = int(m.group(2))
        rows=[]
        for i in range(1, 13):
            rows.append([InlineKeyboardButton(fa_digits(f"{i:02d}"), callback_data=f"relid:m:{uid}:{yy}:{i}")])
        await panel_edit(context, msg, user_id, f"سال {fa_digits(yy)} — ماه را انتخاب کن", rows, root=False)
        return

    m = re.match(r"^relid:m:(\d+):(\d+):(\d+)$", data)
    if m:
        uid = int(m.group(1)); yy = int(m.group(2)); mm = int(m.group(3))
        md = jalali_month_len(yy, mm)
        rows=[]
        for chunk in chunked(list(range(1, md+1)), 6):
            rows.append([InlineKeyboardButton(fa_digits(f"{d:02d}"), callback_data=f"relid:d:{uid}:{yy}:{mm}:{d}") for d in chunk])
        await panel_edit(context, msg, user_id, f"تاریخ {fa_digits(yy)}/{fa_digits(mm)} — روز را انتخاب کن", rows, root=False)
        return

    m = re.match(r"^relid:d:(\d+):(\d+):(\d+):(\d+)$", data)
    if m:
        uid = int(m.group(1)); yy = int(m.group(2)); mm = int(m.group(3)); dd = int(m.group(4))
        try:
            started = JalaliDate(yy, mm, dd).to_gregorian() if HAS_PTOOLS else dt.date(2000+yy%100, mm, dd)
        except Exception:
            await panel_edit(context, msg, user_id, "تاریخ نامعتبر شد. دوباره تلاش کن.", [[InlineKeyboardButton("برگشت", callback_data=f"relid:y:{uid}:{yy}")]], root=False)
            return
        with SessionLocal() as s:
            me = upsert_user(s, chat_id, update.effective_user)
            to = s.get(User, uid)
            if not to:
                await panel_edit(context, msg, user_id, "کاربر هدف پیدا نشد.", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
                return
            s.execute(Relationship.__table__.delete().where(
                (Relationship.chat_id==chat_id) & (
                    ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==to.id)) |
                    ((Relationship.user_a_id==to.id) & (Relationship.user_b_id==me.id))
                )
            ))
            s.add(Relationship(chat_id=chat_id, user_a_id=min(me.id,to.id), user_b_id=max(me.id,to.id), started_at=started))
            s.commit()
        await panel_edit(context, msg, user_id, f"💞 رابطه ثبت شد — تاریخ شمسی: {fa_digits(f'{yy}/{mm:02d}/{dd:02d}')}", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    # ===== نسخهٔ قدیمی ویزارد رابطه بر پایه username (rel:*) — سازگاری =====
    m = re.match(r"^rel:yp:(\w+):(\d+)$", data)
    if m:
        uname = m.group(1); start = int(m.group(2))
        years = list(range(start, start-16, -1))
        rows=[]
        for chunk in chunked(years, 4):
            rows.append([InlineKeyboardButton(fa_digits(str(yy)), callback_data=f"rel:y:{uname}:{yy}") for yy in chunk])
        rows.append([InlineKeyboardButton("قدیمی‌تر", callback_data=f"rel:yp:{uname}:{start-16}")])
        await panel_edit(context, msg, user_id, f"ثبت رابطه با @{uname} — سال شمسی را انتخاب کن", rows, root=False)
        return

    m = re.match(r"^rel:y:(\w+):(\d+)$", data)
    if m:
        uname = m.group(1); yy = int(m.group(2))
        rows=[]
        for i in range(1, 13):
            rows.append([InlineKeyboardButton(fa_digits(f"{i:02d}"), callback_data=f"rel:m:{uname}:{yy}:{i}")])
        await panel_edit(context, msg, user_id, f"@{uname} — سال {fa_digits(yy)} — ماه را انتخاب کن", rows, root=False)
        return

    m = re.match(r"^rel:m:(\w+):(\d+):(\d+)$", data)
    if m:
        uname = m.group(1); yy = int(m.group(2)); mm = int(m.group(3))
        md = jalali_month_len(yy, mm)
        rows=[]
        for chunk in chunked(list(range(1, md+1)), 6):
            rows.append([InlineKeyboardButton(fa_digits(f"{d:02d}"), callback_data=f"rel:d:{uname}:{yy}:{mm}:{d}") for d in chunk])
        await panel_edit(context, msg, user_id, f"@{uname} — تاریخ {fa_digits(yy)}/{fa_digits(mm)} — روز را انتخاب کن", rows, root=False)
        return

    m = re.match(r"^rel:d:(\w+):(\d+):(\d+):(\d+)$", data)
    if m:
        uname = m.group(1); yy, mm, dd = (int(m.group(2)), int(m.group(3)), int(m.group(4)))
        try:
            started = JalaliDate(yy, mm, dd).to_gregorian() if HAS_PTOOLS else dt.date(2000+yy%100, mm, dd)
        except Exception:
            await panel_edit(context, msg, user_id, "تاریخ نامعتبر شد. دوباره تلاش کن.", [[InlineKeyboardButton("برگشت", callback_data=f"rel:y:{uname}:{yy}")]], root=False)
            return
        with SessionLocal() as s:
            me = upsert_user(s, chat_id, update.effective_user)
            to = s.execute(select(User).where(User.chat_id==chat_id, User.username==uname)).scalar_one_or_none()
            if not to:
                await panel_edit(context, msg, user_id, "کاربر هدف پیدا نشد.", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
                return
            s.execute(Relationship.__table__.delete().where(
                (Relationship.chat_id==chat_id) & (
                    ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==to.id)) |
                    ((Relationship.user_a_id==to.id) & (Relationship.user_b_id==me.id))
                )
            ))
            s.add(Relationship(chat_id=chat_id, user_a_id=min(me.id,to.id), user_b_id=max(me.id,to.id), started_at=started))
            s.commit()
        await panel_edit(context, msg, user_id, f"💞 رابطه ثبت شد — تاریخ شمسی: {fa_digits(f'{yy}/{mm:02d}/{dd:02d}')}", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    # راهنمای عملیات‌هایی که ریپلای می‌خواهند
    if data in ("ui:crush:add","ui:crush:del","ui:rel:help","ui:tag:girls","ui:tag:boys","ui:tag:all","ui:pop","ui:ship","ui:privacy:me","ui:privacy:delme","ui:shipme"):
        hints = {
            "ui:crush:add": "برای «ثبت کراش»، روی پیام شخص ریپلای کن و بنویس «ثبت کراش».",
            "ui:crush:del": "برای «حذف کراش»، روی پیام شخص ریپلای کن و بنویس «حذف کراش».",
            "ui:rel:help": "برای «ثبت رابطه»، بنویس: «ثبت رابطه @username» یا بدون هدف بزن و من ازت هدف می‌پرسم؛ بعد تاریخ را از ویزارد انتخاب کن.",
            "ui:tag:girls": "برای «تگ دخترها»، روی یک پیام ریپلای کن و بنویس: تگ دخترها",
            "ui:tag:boys": "برای «تگ پسرها»، روی یک پیام ریپلای کن و بنویس: تگ پسرها",
            "ui:tag:all": "برای «تگ همه»، روی یک پیام ریپلای کن و بنویس: تگ همه",
            "ui:pop": "برای «محبوب امروز»، همین دستور را در گروه بزن.",
            "ui:ship": "«شیپ امشب» آخر شب خودکار ارسال می‌شود.",
            "ui:shipme": "«شیپم کن» را در گروه بزن تا یک پارتنر پیشنهادی معرفی شود.",
            "ui:privacy:me": "برای «حذف من»، همین دستور را در گروه بزن.",
            "ui:privacy:delme": "برای «حذف من»، همین دستور را در گروه بزن.",
        }
        await panel_edit(context, msg, user_id, hints.get(data, "اوکی"), [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    # مدیریت — لیست گروه‌ها (PV)
    m = re.match(r"^adm:groups:(\d+)$", data)
    if m:
        page = int(m.group(1))
        with SessionLocal() as s:
            uid = q.from_user.id
            if uid != OWNER_ID and not is_seller(s, uid):
                await panel_edit(context, msg, user_id, "فقط مالک/فروشنده.", [[InlineKeyboardButton("بستن", callback_data="nav:close")]], root=False); return
            groups = s.query(Group).order_by(Group.id.desc()).all()
        page_size = 6
        total = len(groups)
        pages = max(1, math.ceil(total/page_size))
        page = max(0, min(page, pages-1))
        start = page*page_size
        subset = groups[start:start+page_size]
        rows=[]
        for g in subset:
            ex = fmt_dt_fa(g.expires_at) if g.expires_at else "نامشخص"
            rows.append([InlineKeyboardButton(f"{g.title} — {fa_digits(g.id)} — {ex}", callback_data=f"adm:g:{g.id}")])
        nav=[]
        if page>0: nav.append(InlineKeyboardButton("◀️", callback_data=f"adm:groups:{page-1}"))
        if page<pages-1: nav.append(InlineKeyboardButton("▶️", callback_data=f"adm:groups:{page+1}"))
        if nav: rows.append(nav)
        await panel_edit(context, msg, user_id, f"📋 لیست گروه‌ها (صفحه {fa_digits(page+1)}/{fa_digits(pages)})", rows or [[InlineKeyboardButton("بازگشت", callback_data="nav:back")]], root=False)
        return

    m = re.match(r"^adm:g:(-?\d+)$", data)
    if m:
        target = int(m.group(1))
        with SessionLocal() as s:
            g = s.get(Group, target)
        if not g:
            await panel_edit(context, msg, user_id, "گروه پیدا نشد.", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False); return
        ex = g.expires_at and fmt_dt_fa(g.expires_at)
        title = f"🧩 پنل گروه: {g.title}\nchat_id: {g.id}\nانقضا: {ex or 'نامشخص'}"
        kb = [
            [InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{g.id}:30"),
             InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{g.id}:90"),
             InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{g.id}:180")],
            [InlineKeyboardButton("⛔️ صفر کردن شارژ", callback_data=f"chg:{g.id}:0")],
            [InlineKeyboardButton("ℹ️ انقضا", callback_data="ui:expiry"),
             InlineKeyboardButton("🚪 خروج ربات", callback_data=f"grp:{g.id}:leave")],
            [InlineKeyboardButton("بازگشت به لیست", callback_data="adm:groups:0")],
        ]
        await panel_edit(context, msg, user_id, title, kb, root=False)
        return

    # فروشنده‌ها
    if data == "adm:sellers":
        with SessionLocal() as s:
            uid = q.from_user.id
            if uid != OWNER_ID:
                await panel_edit(context, msg, user_id, "فقط مالک دسترسی دارد.", [[InlineKeyboardButton("بستن", callback_data="nav:close")]], root=False); return
            sellers = s.query(Seller).order_by(Seller.id.asc()).all()
        rows=[]
        if sellers:
            for sl in sellers[:50]:
                cap = f"{sl.tg_user_id} | {'فعال' if sl.is_active else 'غیرفعال'}"
                rows.append([InlineKeyboardButton(f"📈 آمار {cap}", callback_data=f"sl:stat:{sl.tg_user_id}"),
                             InlineKeyboardButton("❌ عزل", callback_data=f"sl:del:{sl.tg_user_id}")])
        rows.append([InlineKeyboardButton("➕ راهنمای افزودن فروشنده", callback_data="sl:add:help")])
        await panel_edit(context, msg, user_id, "🛍️ لیست فروشنده‌ها", rows, root=False)
        return

    m = re.match(r"^sl:stat:(\d+)$", data)
    if m:
        sid = int(m.group(1))
        with SessionLocal() as s:
            rows = s.query(SubscriptionLog).filter(SubscriptionLog.actor_tg_user_id==sid).all()
        total_ops = len(rows)
        total_days = sum([r.amount_days or 0 for r in rows])
        await panel_edit(context, msg, user_id, f"📈 آمار فروشنده {sid}\nتعداد عملیات: {fa_digits(total_ops)}\nمجموع روزهای شارژ: {fa_digits(total_days)}", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    m = re.match(r"^sl:del:(\d+)$", data)
    if m:
        sid = int(m.group(1))
        with SessionLocal() as s:
            if user_id != OWNER_ID:
                await panel_edit(context, msg, user_id, "فقط مالک می‌تواند.", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False); return
            ex = s.execute(select(Seller).where(Seller.tg_user_id==sid)).scalar_one_or_none()
            if not ex:
                await panel_edit(context, msg, user_id, "فروشنده پیدا نشد.", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False); return
            ex.is_active = False; s.commit()
        await panel_edit(context, msg, user_id, "🗑️ فروشنده عزل شد.", [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    if data == "sl:add:help":
        txt = "برای افزودن فروشنده (فقط مالک):\n«افزودن فروشنده 123456789 یادداشت‌اختیاری»\nبرای عزل: «حذف فروشنده 123456789»"
        await panel_edit(context, msg, user_id, txt, [[InlineKeyboardButton("برگشت", callback_data="nav:back")]], root=False)
        return

    # پیش‌فرض
    await panel_edit(context, msg, user_id, "دستور ناشناخته یا منقضی.", [[InlineKeyboardButton("بازگشت", callback_data="nav:back")]], root=False)

# ================== JOBS ==================
async def job_midnight(context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as s:
        groups = s.query(Group).all()
        for g in groups:
            if not group_active(g): continue
            today = dt.datetime.now(TZ_TEHRAN).date()
            top = s.execute(select(ReplyStatDaily).where(
                (ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.date==today)
            ).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if top:
                lines=[]
                for i,r in enumerate(top, start=1):
                    u = s.get(User, r.target_user_id)
                    name = u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                    lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
                try:
                    await context.bot.send_message(g.id, footer("🌙 محبوب‌های امروز:\n" + "\n".join(lines)))
                except: ...
            males = s.query(User).filter_by(chat_id=g.id, gender="male").all()
            females = s.query(User).filter_by(chat_id=g.id, gender="female").all()
            rels = s.query(Relationship).filter_by(chat_id=g.id).all()
            in_rel = set([r.user_a_id for r in rels] + [r.user_b_id for r in rels])
            males = [u for u in males if u.id not in in_rel]
            females = [u for u in females if u.id not in in_rel]
            if males and females:
                m = random.choice(males); f = random.choice(females)
                s.add(ShipHistory(chat_id=g.id, date=today, male_user_id=m.id, female_user_id=f.id)); s.commit()
                try:
                    await context.bot.send_message(g.id, footer(
                        f"💘 شیپِ امشب: {(m.first_name or '@'+(m.username or ''))} × {(f.first_name or '@'+(f.username or ''))}"
                    ))
                except: ...

async def job_morning(context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as s:
        groups = s.query(Group).all()
        for g in groups:
            if not group_active(g): continue
            jy, jm, jd = today_jalali()
            bdays = s.query(User).filter_by(chat_id=g.id).filter(User.birthday.isnot(None)).all()
            for u in bdays:
                um, ud = to_jalali_md(u.birthday)
                if um==jm and ud==jd:
                    try:
                        await context.bot.send_message(g.id, footer(f"🎉🎂 تولدت مبارک {(u.first_name or '@'+(u.username or ''))}! ({fmt_date_fa(u.birthday)})"))
                    except: ...
            rels = s.query(Relationship).filter_by(chat_id=g.id).all()
            for r in rels:
                if not r.started_at: continue
                rm, rd = to_jalali_md(r.started_at)
                if rd==jd:
                    ua, ub = s.get(User, r.user_a_id), s.get(User, r.user_b_id)
                    try:
                        await context.bot.send_message(
                            g.id, footer(f"💞 ماهگرد {(ua.first_name or '@'+(ua.username or ''))} و {(ub.first_name or '@'+(ub.username or ''))} مبارک! ({fmt_date_fa(r.started_at)})")
                        )
                    except: ...

# ================== ERROR HANDLER ==================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    if isinstance(err, TgConflict):
        try:
            if OWNER_ID:
                await context.bot.send_message(OWNER_ID, "⚠️ Conflict 409: نمونهٔ دیگری از ربات در حال polling است. این نمونه خارج شد.")
        except Exception: ...
        logging.error("Conflict 409 detected. Exiting this instance to avoid duplicate polling.")
        os._exit(0)
    logging.exception("Unhandled error", exc_info=err)

# ================== FALLBACK PING (ALWAYS) ==================
async def on_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """اگر هیچ هندلر دیگری جواب نداد، گفتن «فضول» جواب «جانم» می‌گیرد."""
    m = update.effective_message
    if not m: return
    txt = clean_text((m.text or m.caption or "") or "")
    if txt == "فضول":
        try:
            await m.reply_text("جانم 👂")
        except Exception:
            pass

# ================== INTRO TEXT ==================
def group_intro_text(bot_username: str) -> str:
    return (
        "سلام! من «فضول» هستم 🤖\n"
        "برای شروع توی گروه بنویس: «فضول منو»\n"
        "راهنما: «فضول کمک»\n"
        "ادمین‌ها می‌تونن «پیکربندی فضول» رو بزنن تا همهٔ ادمین‌های تلگرام به ادمین فضول اضافه بشن.\n"
        "همهٔ تاریخ‌ها شمسی و ساعت‌ها ایران هستن.\n"
        "برای افزودنم به گروه‌های دیگه از دکمهٔ زیر استفاده کن."
    )

# ============== SYNC TG ADMINS => GroupAdmin ==============
async def sync_group_admins(bot, chat_id: int):
    admins = await bot.get_chat_administrators(chat_id)
    tg_ids = [a.user.id for a in admins if not a.user.is_bot]
    if not tg_ids: return 0
    added = 0
    with SessionLocal() as s:
        for uid in tg_ids:
            exists = s.execute(select(GroupAdmin).where(GroupAdmin.chat_id==chat_id, GroupAdmin.tg_user_id==uid)).scalar_one_or_none()
            if not exists:
                s.add(GroupAdmin(chat_id=chat_id, tg_user_id=uid)); added += 1
        s.commit()
    return added

# ================== /start ==================
async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username = context.bot.username
    if update.effective_chat.type != "private":
        txt = (
            "سلام! من روشنم ✅\n"
            "• «فضول» → جانم (تست سلامت)\n"
            "• «فضول منو» → منوی دکمه‌ای\n"
            "• «فضول کمک» → راهنمای کامل"
        )
        await reply_temp(update, context, txt)
        return

    with SessionLocal() as s:
        uid = update.effective_user.id
        if uid == OWNER_ID:
            txt = (
                "👑 به پنل مالک خوش آمدی!\n"
                "• «📋 لیست گروه‌ها» برای شارژ/انقضا/خروج/افزودن\n"
                "• «🛍️ لیست فروشنده‌ها» برای آمار/عزل/افزودن\n"
                "• «آمار فضول» برای آمار کلی ربات\n"
                "• «فضول» → پاسخ سلامت: جانم"
            )
            await panel_open_initial(update, context, txt,
                [[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0"),
                  InlineKeyboardButton("🛍️ لیست فروشنده‌ها", callback_data="adm:sellers")]],
                root=True)
            return
        elif is_seller(s, uid):
            txt = (
                "🛍️ راهنمای فروشنده:\n"
                "• «گروه‌ها» برای مدیریت\n"
                "• «فضول» → پاسخ سلامت: جانم"
            )
            await panel_open_initial(update, context, txt,
                [[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")]],
                root=True)
            return
        else:
            txt = (
                "سلام! 👋 این ربات برای گروه‌هاست.\n"
                "➕ با دکمهٔ زیر ربات را به گروه اضافه کن و ۷ روز رایگان استفاده کن.\n"
                "در گروه «فضول» بزن (لایو‌چک) و بعد «فضول منو»."
            )
            await reply_temp(update, context, txt, reply_markup=contact_kb(
                extra_rows=[[InlineKeyboardButton("🧭 راهنمای کاربر", callback_data="usr:help")]],
                bot_username=bot_username
            ), keep=True)
            return

# ================== HELP TEXT ==================
def user_help_text() -> str:
    return (
        "📘 راهنمای کامل کاربر (شمسی):\n"
        "• «فضول» → تست سلامت (جانم)\n"
        "• «فضول منو» → منوی دکمه‌ای\n"
        "• «ثبت جنسیت دختر/پسر»\n"
        "• «ثبت تولد» (ویزارد دکمه‌ای) یا «🎂 ثبت تولد ۱۴۰۳-۰۵-۲۰»\n"
        "• «حذف تولد»\n"
        "• «ثبت کراش» (ریپلای)\n"
        "• «حذف کراش» (ریپلای)\n"
        "• «ثبت رابطه @username» (ویزارد تاریخ) / «ثبت رابطه @username ۱۴۰۲/۱۲/۰۱» / «حذف رابطه @username»\n"
        "• «شیپم کن» (پارتنر پیشنهادی برای شما)\n"
        "• «محبوب امروز» / «شیپ امشب»\n"
        "• «تگ دخترها|پسرها|همه» (ریپلای؛ هر پیام ۴ نفر)\n"
        "• «حریم خصوصی» / «حذف من»\n"
        "• «فضول شارژ» (فقط مدیر/فروشنده/مالک)\n"
        "• «فضول انقضا» نمایش پایان اعتبار گروه"
    )

# ================== BOT STATS (OWNER ONLY) ==================
def build_bot_stats_text(s) -> str:
    now = dt.datetime.utcnow()
    total_groups = s.query(func.count(Group.id)).scalar() or 0
    active_groups = s.query(func.count(Group.id)).filter(Group.expires_at != None, Group.expires_at > now).scalar() or 0
    expired_groups = total_groups - active_groups

    total_users = s.query(func.count(User.id)).scalar() or 0
    male = s.query(func.count(User.id)).filter(User.gender=="male").scalar() or 0
    female = s.query(func.count(User.id)).filter(User.gender=="female").scalar() or 0
    unknown = total_users - male - female

    rels = s.query(func.count(Relationship.id)).scalar() or 0
    crushes = s.query(func.count(Crush.id)).scalar() or 0
    ships = s.query(func.count(ShipHistory.id)).scalar() or 0

    today = dt.datetime.now(TZ_TEHRAN).date()
    today_stats = s.query(func.count(ReplyStatDaily.id)).filter(ReplyStatDaily.date==today).scalar() or 0

    sellers_total = s.query(func.count(Seller.id)).scalar() or 0
    sellers_active = s.query(func.count(Seller.id)).filter(Seller.is_active==True).scalar() or 0

    lines = [
        f"📊 آمار کلی ربات:",
        f"• گروه‌ها: {fa_digits(total_groups)} (فعال: {fa_digits(active_groups)} | منقضی: {fa_digits(expired_groups)})",
        f"• کاربران: {fa_digits(total_users)} (دختر: {fa_digits(female)} | پسر: {fa_digits(male)} | نامشخص: {fa_digits(unknown)})",
        f"• روابط: {fa_digits(rels)} | کراش‌ها: {fa_digits(crushes)} | شیپ‌ها: {fa_digits(ships)}",
        f"• ردیابی ریپلای امروز: {fa_digits(today_stats)} رکورد",
        f"• فروشنده‌ها: {fa_digits(sellers_total)} (فعال: {fa_digits(sellers_active)})",
    ]
    return "\n".join(lines)

# ================== PRIVATE (OWNER/SELLER/USER) ==================
async def on_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private" or not update.message or not update.message.text:
        return
    logging.info(f"[pv] msg from {update.effective_user.id if update.effective_user else '-'}: {update.message.text}")
    text = clean_text(update.message.text)

    bot_username = context.bot.username
    with SessionLocal() as s:
        uid = update.effective_user.id
        seller = is_seller(s, uid)
        if uid != OWNER_ID and not seller:
            if text in ("/start","start","کمک","راهنما"):
                await reply_temp(update, context,
                                 "این ربات مخصوص گروه‌هاست. با دکمهٔ زیر اضافه کن و ۷ روز رایگان استفاده کن.\nدر گروه «فضول» و «فضول منو» را بزن.",
                                 reply_markup=contact_kb(bot_username=bot_username), keep=True)
                return
            if PAT_DM["bot_stats"].match(text):
                await reply_temp(update, context, "فقط مالک می‌تواند این دستور را اجرا کند."); return
            # fallback help
            await reply_temp(update, context, "برای مدیریت باید مالک/فروشنده باشی. «/start» یا «کمک» بزن."); return

        # پنل
        if PAT_DM["panel"].match(text):
            who = "👑 پنل مالک" if uid==OWNER_ID else "🛍️ پنل فروشنده"
            await panel_open_initial(update, context, who,
                                     [[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
                                      [InlineKeyboardButton("🛍️ لیست فروشنده‌ها", callback_data="adm:sellers")] if uid==OWNER_ID else []],
                                     root=True)
            return

        # آمار فضول (فقط مالک)
        if PAT_DM["bot_stats"].match(text):
            if uid != OWNER_ID:
                await reply_temp(update, context, "فقط مالک می‌تواند این دستور را اجرا کند."); return
            await reply_temp(update, context, build_bot_stats_text(s), keep=True)
            return

        if PAT_DM["groups"].match(text):
            await panel_open_initial(update, context, "📋 لیست گروه‌ها",
                                     [[InlineKeyboardButton("نمایش", callback_data="adm:groups:0")]],
                                     root=True)
            return

        if m := PAT_DM["manage"].match(text):
            chat_id = int(m.group(1))
            g = s.get(Group, chat_id)
            if not g: await reply_temp(update, context, "گروه پیدا نشد."); return
            ex = g.expires_at and fmt_dt_fa(g.expires_at)
            title = f"🧩 پنل گروه: {g.title}\nchat_id: {g.id}\nانقضا: {ex or 'نامشخص'}"
            kb = [
                [InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{g.id}:30"),
                 InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{g.id}:90"),
                 InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{g.id}:180")],
                [InlineKeyboardButton("⛔️ صفر کردن شارژ", callback_data=f"chg:{g.id}:0")],
                [InlineKeyboardButton("ℹ️ انقضا", callback_data="ui:expiry"),
                 InlineKeyboardButton("🚪 خروج ربات", callback_data=f"grp:{g.id}:leave")],
                [InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
            ]
            await panel_open_initial(update, context, title, kb, root=False)
            return

        if m := PAT_DM["extend"].match(text):
            chat_id = int(m.group(1)); days = int(m.group(2))
            g = s.get(Group, chat_id)
            if not g: await reply_temp(update, context, "گروه پیدا نشد."); return
            if days <= 0:
                g.expires_at = dt.datetime.utcnow()
                s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=uid, action="reset", amount_days=0))
            else:
                base = g.expires_at if g.expires_at and g.expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
                g.expires_at = base + dt.timedelta(days=days)
                s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=uid, action="extend", amount_days=days))
            s.commit()
            await reply_temp(update, context, f"✅ تنظیم شد: {fmt_dt_fa(g.expires_at)}"); return

        if PAT_DM["list_sellers"].match(text):
            sellers = s.query(Seller).order_by(Seller.id.asc()).all()
            if not sellers:
                await reply_temp(update, context, "هیچ فروشنده‌ای ثبت نشده."); return
            rows=[]
            for sl in sellers[:50]:
                cap = f"{sl.tg_user_id} | {'فعال' if sl.is_active else 'غیرفعال'}"
                r = [InlineKeyboardButton(f"📈 آمار {cap}", callback_data=f"sl:stat:{sl.tg_user_id}")]
                if uid==OWNER_ID:
                    r.append(InlineKeyboardButton("❌ عزل", callback_data=f"sl:del:{sl.tg_user_id}"))
                rows.append(r)
            rows.append([InlineKeyboardButton("➕ راهنمای افزودن فروشنده", callback_data="sl:add:help")])
            await panel_open_initial(update, context, "🛍️ لیست فروشنده‌ها", rows, root=True); return

        if m := PAT_DM["add_seller"].match(text):
            if uid != OWNER_ID: await reply_temp(update, context, "فقط مالک می‌تواند فروشنده اضافه کند."); return
            seller_id = int(m.group(1)); note = m.group(2)
            ex = s.execute(select(Seller).where(Seller.tg_user_id==seller_id)).scalar_one_or_none()
            if ex:
                ex.is_active = True
                if note: ex.note = note
            else:
                s.add(Seller(tg_user_id=seller_id, note=note, is_active=True))
            s.commit()
            await reply_temp(update, context, "✅ فروشنده اضافه/فعال شد."); return

        if m := PAT_DM["del_seller"].match(text):
            if uid != OWNER_ID: await reply_temp(update, context, "فقط مالک می‌تواند فروشنده را عزل کند."); return
            seller_id = int(m.group(1))
            ex = s.execute(select(Seller).where(Seller.tg_user_id==seller_id)).scalar_one_or_none()
            if not ex: await reply_temp(update, context, "فروشنده پیدا نشد."); return
            ex.is_active = False; s.commit()
            await reply_temp(update, context, "🗑️ فروشنده عزل شد."); return

# ================== BOOT ==================
async def _post_init(app: Application):
    try:
        info = await app.bot.get_webhook_info()
        if WEBHOOK_URL:
            logging.info("Webhook mode enabled; leaving webhook to PTB in run_webhook.")
        else:
            if info.url:
                logging.info(f"Webhook was set to: {info.url} — deleting…")
            await app.bot.delete_webhook(drop_pending_updates=True)
            logging.info("Webhook deleted. Polling will receive ALL updates.")
    except Exception as e:
        logging.warning(f"post_init webhook check failed: {e}")
    logging.info(f"PersianTools enabled: {HAS_PTOOLS}")

def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN env var is required.")

    acquire_singleton_or_exit()

    app = Application.builder().token(TOKEN).post_init(_post_init).build()

    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_group_text))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_private_text))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_error_handler(error_handler)

    # فول‌بک «فضول» ← «جانم»
    app.add_handler(MessageHandler(filters.ALL, on_any), group=100)

    jq = app.job_queue
    if jq is None:
        logging.warning('JobQueue فعال نیست. نصب کن: pip install "python-telegram-bot[job-queue]==21.6"')
    else:
        jq.run_daily(job_morning, time=dt.time(6,0,0, tzinfo=TZ_TEHRAN))
        jq.run_daily(job_midnight, time=dt.time(0,1,0, tzinfo=TZ_TEHRAN))
        jq.run_repeating(singleton_watchdog, interval=60, first=60)

    logging.info("FazolBot running…")
    allowed = ["message","edited_message","callback_query","my_chat_member","chat_member","chat_join_request"]
    if WEBHOOK_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN, webhook_url=f"{WEBHOOK_URL}/{TOKEN}",
                        allowed_updates=allowed, drop_pending_updates=True)
    else:
        app.run_polling(allowed_updates=allowed, drop_pending_updates=True)

if __name__ == "__main__":
    main()
