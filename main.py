# main.py
import os
import re
import math
import random
import logging
import asyncio
import atexit
import hashlib
import datetime as dt
import urllib.parse as _up
from typing import Optional, List, Tuple, Dict, Any
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

# ========== CONFIG ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)

TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

DEFAULT_TZ = "Asia/Tehran"
TZ_TEHRAN = ZoneInfo(DEFAULT_TZ)

OWNER_CONTACT_USERNAME = os.getenv("OWNER_CONTACT", "soulsownerbot")
AUTO_DELETE_SECONDS = int(os.getenv("AUTO_DELETE_SECONDS", "40"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
PORT = int(os.getenv("PORT", "8080"))

Base = declarative_base()

# ---------- Persian dates & digits ----------
# Persian dates (optional dependency)
try:
    from persiantools.jdatetime import JalaliDateTime, JalaliDate
    from persiantools import digits as _digits
    HAS_PTOOLS = True
except Exception:
    HAS_PTOOLS = False

def fa_digits(x: str) -> str:
    """به اعداد فارسی تبدیل می‌کند (اگر persiantools نصب باشد)."""
    s = str(x)
    if HAS_PTOOLS:
        try:
            return _digits.en_to_fa(s)
        except Exception:
            return s
    return s

def fa_to_en_digits(s: str) -> str:
    """اعداد فارسی/عربی را به انگلیسی برمی‌گرداند (برای پارس‌های ورودی)."""
    if HAS_PTOOLS:
        try:
            return _digits.fa_to_en(str(s))
        except Exception:
            pass
    return str(s)

def fmt_dt_fa(dt_utc: Optional[dt.datetime]) -> str:
    """فرمت تاریخ‌-زمان UTC به منطقه تهران، با تبدیل شمسی در صورت امکان."""
    if dt_utc is None:
        return "-"
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
    local = dt_utc.astimezone(TZ_TEHRAN)
    if HAS_PTOOLS:
        try:
            jdt = JalaliDateTime.fromgregorian(datetime=local)
            s = jdt.strftime("%A %Y/%m/%d %H:%M")
            return fa_digits(s) + " (تهران)"
        except Exception:
            pass
    return local.strftime("%Y/%m/%d %H:%M") + " (Tehran)"

def fmt_date_fa(d: Optional[dt.date]) -> str:
    """فرمت تاریخ به شمسی (YYYY/MM/DD) در صورت امکان؛ در غیر این‌صورت میلادی."""
    if not d:
        return "-"
    if HAS_PTOOLS:
        try:
            jd = JalaliDate.fromgregorian(date=d)
            return fa_digits(jd.strftime("%Y/%m/%d"))
        except Exception:
            pass
    return d.strftime("%Y/%m/%d")

def parse_jalali_date_input(s: str) -> dt.date:
    """
    ورودی کاربر مثل ۱۴۰۳/۰۵/۲۰ یا 1403-05-20 را به تاریخ میلادی (date) تبدیل می‌کند.
    فقط تاریخ شمسی مجاز است؛ اگر سال >= 1700 باشد، به‌عنوان میلادی رد می‌شود.
    """
    ss = fa_to_en_digits(str(s)).strip().replace("/", "-")
    parts = ss.split("-")
    if len(parts) != 3:
        raise ValueError("فرمت تاریخ نامعتبر است.")
    y, m, d = (int(parts[0]), int(parts[1]), int(parts[2]))
    if y >= 1700:
        raise ValueError("تاریخ میلادی مجاز نیست؛ شمسی وارد کن.")
    if HAS_PTOOLS:
        return JalaliDate(y, m, d).to_gregorian()
    # fallback ساده (بدون persiantools) — تقریبی نیست؛ از قرن ۱۴۰۰ فقط سال‌های 20xx را می‌سازد
    return dt.date(2000 + (y % 100), m, d)

def jalali_now_year() -> int:
    now = dt.datetime.now(TZ_TEHRAN)
    if HAS_PTOOLS:
        return JalaliDateTime.fromgregorian(datetime=now).year
    return now.year

def jalali_month_len(y: int, m: int) -> int:
    """تعداد روزهای ماه شمسی (با persiantools دقیق؛ بدون آن: تخمینی استاندارد)."""
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

def today_jalali() -> Tuple[int, int, int]:
    now = dt.datetime.now(TZ_TEHRAN)
    if HAS_PTOOLS:
        j = JalaliDateTime.fromgregorian(datetime=now)
        return j.year, j.month, j.day
    d = now.date()
    return d.year, d.month, d.day

def to_jalali_md(d: dt.date) -> Tuple[int, int]:
    """ماه/روز شمسی تاریخ میلادی ورودی را برمی‌گرداند (برای تبریک‌ها/ماهگرد)."""
    if HAS_PTOOLS:
        j = JalaliDate.fromgregorian(date=d)
        return j.month, j.day
    return d.month, d.day

# ---------- Footer / Contact keyboard / Temp replies ----------

def footer(text: str) -> str:
    # اگر خواستی واترمارک یا امضا اضافه کنی همین‌جا انجام بده
    return text

def contact_kb(
    extra_rows: List[List[InlineKeyboardButton]] | None = None,
    bot_username: Optional[str] = None
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if extra_rows:
        rows.extend([r for r in extra_rows if r])
    rows.append([InlineKeyboardButton("📞 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")])
    if bot_username:
        rows.append([InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"https://t.me/{bot_username}?startgroup=true")])
    return InlineKeyboardMarkup(rows)

# --- Auto-Delete helper for ephemeral messages ---
async def _job_delete_message(context: ContextTypes.DEFAULT_TYPE):
    chat_id, msg_id = context.job.data
    try:
        await context.bot.delete_message(chat_id, msg_id)
    except Exception:
        ...

def schedule_autodelete(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    keep: bool = False
):
    if keep:
        return
    jq = getattr(context.application, "job_queue", None)
    if jq:
        jq.run_once(_job_delete_message, when=AUTO_DELETE_SECONDS, data=(chat_id, message_id))

async def reply_temp(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    keep: bool = False,
    parse_mode: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
    with_footer: bool = True
):
    msg = await update.effective_chat.send_message(
        footer(text) if with_footer else text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        reply_to_message_id=reply_to_message_id,
        disable_web_page_preview=True,
    )
    schedule_autodelete(context, msg.chat_id, msg.message_id, keep=keep)
    return msg

# ---------- Database URL, engine, session, singleton lock ----------

def _mask_url(u: str) -> str:
    try:
        parts = _up.urlsplit(u)
        if parts.username or parts.password:
            netloc = parts.hostname or ""
            if parts.port:
                netloc += f":{parts.port}"
            return _up.urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        ...
    return "<unparsable>"

# انتخاب درایور postgres (psycopg یا psycopg2)
_DRIVER = None
try:
    import psycopg  # type: ignore
    _DRIVER = "psycopg"
except Exception:
    try:
        import psycopg2  # type: ignore
        _DRIVER = "psycopg2"
    except Exception:
        _DRIVER = "psycopg"

raw_db_url = (os.getenv("DATABASE_URL") or "").strip()
if not raw_db_url:
    PGHOST = os.getenv("PGHOST")
    PGPORT = os.getenv("PGPORT", "5432")
    PGUSER = os.getenv("PGUSER")
    PGPASSWORD = os.getenv("PGPASSWORD")
    PGDATABASE = os.getenv("PGDATABASE", "railway")
    if all([PGHOST, PGUSER, PGPASSWORD]):
        raw_db_url = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    else:
        raise RuntimeError("DATABASE_URL تنظیم نشده و PGHOST/PGUSER/PGPASSWORD هم موجود نیست.")

db_url = raw_db_url
# سازگاری با postgres://
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)
# تزریق درایور
if "+psycopg" not in db_url and "+psycopg2" not in db_url:
    db_url = db_url.replace("postgresql://", f"postgresql+{_DRIVER}://", 1)
# اجباری‌کردن SSL مگر اینکه قبلاً ست شده باشد
if "sslmode=" not in db_url:
    sep = "&" if "?" in db_url else "?"
    db_url = f"{db_url}{sep}sslmode=require"

try:
    parsed = _up.urlsplit(db_url)
    logging.info(f"DB host={parsed.hostname} port={parsed.port} path={parsed.path} driver={_DRIVER}")
except Exception:
    logging.info("DB URL parsed with issues.")

engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_recycle=300,
    future=True,
    connect_args={"sslmode": "require"},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

Base = declarative_base()  # اگر قبلاً تعریف شده بود، این خط را حذف کن؛ فقط یکبار باشد

# ---------- Singleton polling guard via PG advisory lock ----------
SINGLETON_CONN = None
SINGLETON_KEY = None

def _advisory_key() -> int:
    # کلید پایدار براساس توکن ربات
    if not TOKEN:
        return 0
    return int(hashlib.blake2b(TOKEN.encode(), digest_size=8).hexdigest(), 16) % (2**31)

def _acquire_lock(conn, key: int) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
    ok = cur.fetchone()[0]
    return bool(ok)

def acquire_singleton_or_exit():
    """اجازه نمی‌دهیم دو نمونه همزمان polling کنند (قفل مشورتی PG)."""
    global SINGLETON_CONN, SINGLETON_KEY
    SINGLETON_KEY = _advisory_key()
    try:
        SINGLETON_CONN = engine.raw_connection()
        cur = SINGLETON_CONN.cursor()
        cur.execute("SET application_name = 'fazolbot'")
        ok = _acquire_lock(SINGLETON_CONN, SINGLETON_KEY)
        if not ok:
            logging.error("نمونه‌ی دیگری در حال اجراست (PG advisory lock). خروج.")
            os._exit(0)
        logging.info("Singleton lock گرفته شد؛ این نمونه تنها polling instance است.")
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
            ...

async def singleton_watchdog(context: ContextTypes.DEFAULT_TYPE):
    """سلامت اتصال قفل را پایش می‌کند و در صورت قطع، دوباره سعی به گرفتن قفل می‌کند."""
    global SINGLETON_CONN, SINGLETON_KEY
    try:
        cur = SINGLETON_CONN.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        return
    except Exception as e:
        logging.warning(f"Singleton ping failed, trying re-acquire: {e}")
        try:
            try:
                SINGLETON_CONN.close()
            except Exception:
                ...
            SINGLETON_CONN = engine.raw_connection()
            cur = SINGLETON_CONN.cursor()
            cur.execute("SELECT pg_try_advisory_lock(%s)", (SINGLETON_KEY,))
            ok = cur.fetchone()[0]
            if not ok:
                logging.error("از دست رفتن قفل و تصاحب توسط نمونه‌ی دیگر. خروج.")
                os._exit(0)
            logging.info("قفل پس از ری‌استارت DB دوباره گرفته شد.")
        except Exception as e2:
            logging.error(f"Re-acquire advisory lock failed: {e2}")

# ---------- ORM Models & Indexes ----------

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

# ایجاد جداول و ایندکس‌های مکمل (idempotent)
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

# ---------- Helper functions ----------

def try_send_owner(text_msg: str):
    """ارسال پیام به مالک ربات (اگر OWNER_ID تنظیم شده باشد)."""
    from telegram import Bot
    if not TOKEN or not OWNER_ID:
        return
    try:
        Bot(TOKEN).send_message(OWNER_ID, footer(text_msg))
    except Exception as e:
        logging.info(f"Owner DM failed: {e}")

def ensure_group(session, chat) -> 'Group':
    """در صورت نبود، گروه را در DB می‌سازد و پلن آزمایشی فعال می‌کند."""
    created = False
    g = session.get(Group, chat.id)
    if not g:
        created = True
        g = Group(
            id=chat.id,
            title=getattr(chat, "title", None) or str(chat.id),
            owner_user_id=None,
            timezone=DEFAULT_TZ,
            trial_started_at=dt.datetime.utcnow(),
            expires_at=dt.datetime.utcnow() + dt.timedelta(days=7),
            is_active=True,
            settings={}
        )
        session.add(g)
        session.add(SubscriptionLog(
            chat_id=chat.id,
            actor_tg_user_id=None,
            action="trial_start",
            amount_days=7
        ))
        session.commit()
        try_send_owner(
            f"➕ ربات به گروه جدید اضافه شد:\n• {g.title}\n• chat_id: {g.id}\n• پلن: ۷ روز رایگان فعال شد."
        )
    else:
        if g.timezone != DEFAULT_TZ:
            g.timezone = DEFAULT_TZ
            session.commit()
    g._just_created = created
    return g

def upsert_user(session, chat_id: int, tg_user) -> 'User':
    """کاربر را در گروه ذخیره یا آپدیت می‌کند."""
    u = session.execute(
        select(User).where(User.chat_id == chat_id, User.tg_user_id == tg_user.id)
    ).scalar_one_or_none()
    if not u:
        u = User(
            chat_id=chat_id,
            tg_user_id=tg_user.id,
            first_name=tg_user.first_name,
            last_name=tg_user.last_name,
            username=tg_user.username,
            gender="unknown"
        )
        session.add(u)
        session.commit()
    else:
        changed = False
        if u.first_name != tg_user.first_name:
            u.first_name = tg_user.first_name; changed = True
        if u.last_name != tg_user.last_name:
            u.last_name = tg_user.last_name; changed = True
        if u.username != tg_user.username:
            u.username = tg_user.username; changed = True
        if changed:
            session.commit()
    return u

def is_seller(session, tg_user_id: int) -> bool:
    """آیا کاربر فروشنده فعال است؟"""
    s = session.execute(
        select(Seller).where(Seller.tg_user_id == tg_user_id, Seller.is_active == True)
    ).scalar_one_or_none()
    return bool(s)

def is_group_admin(session, chat_id: int, tg_user_id: int) -> bool:
    """آیا کاربر ادمین فضول در گروه است؟"""
    if tg_user_id == OWNER_ID:
        return True
    g = session.get(Group, chat_id)
    blocked = (g.settings or {}).get("blocked_sellers", []) if g else []
    if is_seller(session, tg_user_id) and tg_user_id not in blocked:
        return True
    row = session.execute(select(GroupAdmin).where(
        (GroupAdmin.chat_id == chat_id) & (GroupAdmin.tg_user_id == tg_user_id)
    )).scalar_one_or_none()
    return bool(row)

def group_active(g: Group) -> bool:
    """آیا گروه فعال است؟"""
    return bool(g.expires_at and g.expires_at > dt.datetime.utcnow())

def mention_of(u: 'User') -> str:
    """لینک/منشن کاربر در تلگرام."""
    if u.username:
        return f"@{u.username}"
    name = u.first_name or "کاربر"
    return f'<a href="tg://user?id={u.tg_user_id}">{name}</a>'

def mention_by_tgid(session, chat_id: int, tg_user_id: int) -> str:
    """لینک/منشن کاربر بر اساس tg_user_id."""
    u = session.execute(
        select(User).where(User.chat_id == chat_id, User.tg_user_id == tg_user_id)
    ).scalar_one_or_none()
    return mention_of(u) if u else f'<a href="tg://user?id={tg_user_id}">کاربر</a>'

# --- نرمال‌سازی متن فارسی ---
ARABIC_FIX_MAP = str.maketrans({
    "ي": "ی", "ى": "ی", "ئ": "ی", "ك": "ک",
    "ـ": "",
})
PUNCS = " \u200c\u200f\u200e\u2066\u2067\u2068\u2069\t\r\n.,!?؟،;:()[]{}«»\"'"

def fa_norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).translate(ARABIC_FIX_MAP)
    s = s.replace("\u200c", " ").replace("\u200f", "").replace("\u200e", "")
    s = s.replace("\u202a", "").replace("\u202c", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_text(s: str) -> str:
    return fa_norm(s)

# --- تشخیص کلمه «فضول» در متن ---
RE_WORD_FAZOL = re.compile(rf"(?:^|[{re.escape(PUNCS)}])فضول(?:[{re.escape(PUNCS)}]|$)")

def chunked(lst: List, n: int):
    """تقسیم لیست به زیرلیست‌های طول n."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ---------- Target selection (reply / @username / numeric id) & Waiters ----------

# نگه‌داشتن وضعیت «در انتظار هدف» برای هر کاربر در هر گروه
WAITERS: Dict[Tuple[int, int], Dict[str, Any]] = {}
WAITER_TTL_SECONDS = 180  # سه دقیقه مهلت

def _wkey(chat_id: int, user_id: int) -> Tuple[int, int]:
    return (chat_id, user_id)

def _set_waiter(chat_id: int, user_id: int, purpose: str) -> None:
    """
    purpose یکی از این‌هاست:
      relation_set | relation_del | crush_add | crush_del | admin_add | admin_del
    (در صورت نیاز می‌تونی مورد جدید اضافه کنی)
    """
    WAITERS[_wkey(chat_id, user_id)] = {"for": purpose, "at": dt.datetime.utcnow()}

def _peek_waiter(chat_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    data = WAITERS.get(_wkey(chat_id, user_id))
    if not data:
        return None
    # انقضای waiter
    if (dt.datetime.utcnow() - data["at"]).total_seconds() > WAITER_TTL_SECONDS:
        WAITERS.pop(_wkey(chat_id, user_id), None)
        return None
    return data

def _pop_waiter(chat_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    return WAITERS.pop(_wkey(chat_id, user_id), None)

def parse_target_token(s: str) -> Tuple[str, Any]:
    """
    ورودی برای اشاره به هدف را پارس می‌کند.
    خروجی:
      ("username", "foo")  وقتی مثل @foo یا foo است (فقط حروف/عدد/آندرلاین، حداقل 3)
      ("id", 123456789)    وقتی عددی است (۵ رقم یا بیشتر)
      ("bad", None)        در غیر این صورت
    """
    t = fa_to_en_digits(clean_text(s or ""))
    if not t:
        return ("bad", None)
    # @username
    if t.startswith("@"):
        uname = t[1:].strip()
        if re.fullmatch(r"\w{3,}", uname or ""):
            return ("username", uname)
        return ("bad", None)
    # توکن یکتا بدون فاصله -> به عنوان username
    if " " not in t and re.fullmatch(r"\w{3,}", t):
        return ("username", t)
    # اعداد
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
    در دیتابیس همان گروه به‌دنبالش می‌گردد. (باید قبلاً در گروه پیام داده باشد.)
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
    """
    اگر روی پیام کسی ریپلای شده بود، همان فرد را (در DB گروه) برمی‌گرداند/می‌سازد.
    """
    if not update.message or not update.message.reply_to_message:
        return None
    try:
        return upsert_user(session, chat_id, update.message.reply_to_message.from_user)
    except Exception:
        return None

async def prompt_target(update: Update, context: ContextTypes.DEFAULT_TYPE, title: str):
    """
    وقتی هدف مشخص نیست از کاربر می‌خواهیم @یوزرنیم یا آیدی عددی بفرستد.
    """
    txt = (
        f"🔎 {title}\n"
        "لطفاً @یوزرنیم یا آیدی عددی طرف مقابل را ارسال کن.\n"
        "مثال: @foo یا 123456789"
    )
    await reply_temp(update, context, txt, keep=False)

# ================== GROUP TEXT ==================
async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # فقط پیام‌های متنی در گروه/سوپرگروه
    if update.effective_chat.type not in ("group", "supergroup") or not update.message or not update.message.text:
        return

    logging.info(f"[grp] msg from {update.effective_user.id if update.effective_user else '-'}: {update.message.text}")
    text = clean_text(update.message.text)

    # «فضول منو» یا «فضول کمک» وقتی کلمه‌ی «فضول» در متن هست
    if RE_WORD_FAZOL.search(text):
        if "منو" in text or "فهرست" در text:
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

    # اگر منتظر هدف از همین کاربر هستیم (برای ویزاردها/انتخاب هدف)
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
                    (Relationship.chat_id == g.id) & (
                        ((Relationship.user_a_id == me.id) & (Relationship.user_b_id == target.id)) |
                        ((Relationship.user_a_id == target.id) & (Relationship.user_b_id == me.id))
                    )
                ))
                s.commit()
                await reply_temp(update, context, "رابطه حذف شد 🗑️")
                return
            if purpose == "crush_add":
                if me.id == target.id:
                    await reply_temp(update, context, "روی خودت نمی‌شه 😅")
                    return
                try:
                    s.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=target.id))
                    s.commit()
                    await reply_temp(update, context, "کراش ثبت شد 💘")
                except Exception:
                    await reply_temp(update, context, "از قبل ثبت شده بود.")
                return
            if purpose == "crush_del":
                s.execute(Crush.__table__.delete().where(
                    (Crush.chat_id == g.id) & (Crush.from_user_id == me.id) & (Crush.to_user_id == target.id)
                ))
                s.commit()
                await reply_temp(update, context, "کراش حذف شد 🗑️")
                return
            if purpose == "admin_add":
                if not is_group_admin(s, g.id, update.effective_user.id):
                    await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک می‌تواند.")
                    return
                try:
                    s.add(GroupAdmin(chat_id=g.id, tg_user_id=target.tg_user_id))
                    s.commit()
                    await reply_temp(update, context, "✅ به‌عنوان ادمین گروه اضافه شد.")
                except Exception:
                    await reply_temp(update, context, "قبلاً ادمین بوده یا خطا رخ داد.")
                return
            if purpose == "admin_del":
                if not is_group_admin(s, g.id, update.effective_user.id):
                    await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک می‌تواند.")
                    return
                if target.tg_user_id == OWNER_ID or is_seller(s, target.tg_user_id):
                    await reply_temp(update, context, "نمی‌توان مالک/فروشنده را حذف کرد.")
                    return
                s.execute(GroupAdmin.__table__.delete().where(
                    (GroupAdmin.chat_id == g.id) & (GroupAdmin.tg_user_id == target.tg_user_id)
                ))
                s.commit()
                await reply_temp(update, context, "🗑️ ادمین گروه حذف شد.")
                return

    # وضعیت گروه و نقش کاربر فعلی
    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)
        is_gadmin = is_group_admin(s, g.id, update.effective_user.id)
        me = upsert_user(s, g.id, update.effective_user)

    # ===== (تغییر اصلی) ثبت جنسیت با ریپلای توسط ادمین =====
    m = PAT_GROUP["gender"].match(text)
    if m:
        gender_fa = m.group(1)
        target_user: Optional[User] = None
        with SessionLocal() as s:
            g = ensure_group(s, update.effective_chat)

            # اگر ریپلای شده و فرستنده ادمین گروه است → روی هدف
            if update.message.reply_to_message and is_group_admin(s, g.id, update.effective_user.id):
                target_user = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            else:
                # در غیر این صورت روی خودِ فرستنده
                target_user = upsert_user(s, g.id, update.effective_user)

            # نگاشت دختر/پسر
            gcode = "female" if gender_fa == "دختر" else "male"
            target_user.gender = gcode
            s.commit()

            who = "خودت" if target_user.tg_user_id == update.effective_user.id else f"{mention_of(target_user)}"
            await reply_temp(
                update, context,
                f"👤 جنسیت {who} ثبت شد: {'👧 دختر' if gcode=='female' else '👦 پسر'}",
                parse_mode=ParseMode.HTML
            )
        return

    # ===== (تغییر اصلی) ثبت تولد با ریپلای + تاریخ توسط ادمین =====
    m = PAT_GROUP["birthday_set"].match(text)
    if m:
        date_str = m.group(1)
        try:
            gdate = parse_jalali_date_input(date_str)
        except Exception:
            await reply_temp(update, context, "فرمت تاریخ نامعتبر است. نمونه: «ثبت تولد ۱۴۰۳/۰۵/۲۰»")
            return

        with SessionLocal() as s:
            g = ensure_group(s, update.effective_chat)

            # اگر ریپلای شده و فرستنده ادمین گروه است → روی هدف
            if update.message.reply_to_message and is_group_admin(s, g.id, update.effective_user.id):
                target_user = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            else:
                # در غیر این صورت روی خودِ فرستنده
                target_user = upsert_user(s, g.id, update.effective_user)

            target_user.birthday = gdate
            s.commit()

            who = "خودت" if target_user.tg_user_id == update.effective_user.id else f"{mention_of(target_user)}"
            await reply_temp(
                update, context,
                f"🎂 تولد {who} ثبت شد: {fmt_date_fa(gdate)}",
                parse_mode=ParseMode.HTML
            )
        return

    # ===== باقیِ دستورات قبلی (بدون تغییر ساختاری) =====
    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)

        # ---------------- ثبت رابطه — انعطاف ----------------
        if PAT_GROUP["relation_any"].match(text):
            if not group_active(g):
                await reply_temp(update, context, "⌛️ اعتبار گروه تمام شده.")
                return

            m = PAT_GROUP["relation_any"].match(text)
            target_user = _target_from_reply(s, g.id, update)

            if not target_user:
                uname = m.group(1); did = m.group(2)
                if uname:
                    target_user = s.execute(select(User).where(User.chat_id == g.id, User.username == uname)).scalar_one_or_none()
                elif did:
                    try:
                        tid = int(did)
                        target_user = s.execute(select(User).where(User.chat_id == g.id, User.tg_user_id == tid)).scalar_one_or_none()
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
                    target_user = s.execute(select(User).where(User.chat_id == g.id, User.username == uname)).scalar_one_or_none()
                elif did:
                    try:
                        tid = int(did)
                        target_user = s.execute(select(User).where(User.chat_id == g.id, User.tg_user_id == tid)).scalar_one_or_none()
                    except Exception:
                        target_user = None

            if not target_user:
                _set_waiter(g.id, update.effective_user.id, "relation_del")
                await prompt_target(update, context, "حذف رابطه")
                return

            s.execute(Relationship.__table__.delete().where(
                (Relationship.chat_id == g.id) & (
                    ((Relationship.user_a_id == me.id) & (Relationship.user_b_id == target_user.id)) |
                    ((Relationship.user_a_id == target_user.id) & (Relationship.user_b_id == me.id))
                )
            ))
            s.commit()
            await reply_temp(update, context, "رابطه حذف شد 🗑️")
            return

        # ---------------- کراش — انعطاف ----------------
        if PAT_GROUP["crush_add_any"].match(text):
            if not group_active(g):
                await reply_temp(update, context, "⌛️ اعتبار گروه تمام شده.")
                return
            me = upsert_user(s, g.id, update.effective_user)

            m = PAT_GROUP["crush_add_any"].match(text)
            target_user = _target_from_reply(s, g.id, update)
            if not target_user:
                uname = m.group(1); did = m.group(2)
                if uname:
                    target_user = s.execute(select(User).where(User.chat_id == g.id, User.username == uname)).scalar_one_or_none()
                elif did:
                    try:
                        tid = int(did)
                        target_user = s.execute(select(User).where(User.chat_id == g.id, User.tg_user_id == tid)).scalar_one_or_none()
                    except Exception:
                        target_user = None

            if not target_user:
                _set_waiter(g.id, update.effective_user.id, "crush_add")
                await prompt_target(update, context, "ثبت کراش")
                return

            if me.id == target_user.id:
                await reply_temp(update, context, "روی خودت نمی‌شه 😅")
                return
            try:
                s.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=target_user.id))
                s.commit()
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
                    target_user = s.execute(select(User).where(User.chat_id == g.id, User.username == uname)).scalar_one_or_none()
                elif did:
                    try:
                        tid = int(did)
                        target_user = s.execute(select(User).where(User.chat_id == g.id, User.tg_user_id == tid)).scalar_one_or_none()
                    except Exception:
                        target_user = None

            if not target_user:
                _set_waiter(g.id, update.effective_user.id, "crush_del")
                await prompt_target(update, context, "حذف کراش")
                return

            s.execute(Crush.__table__.delete().where(
                (Crush.chat_id == g.id) & (Crush.from_user_id == me.id) & (Crush.to_user_id == target_user.id)
            ))
            s.commit()
            await reply_temp(update, context, "کراش حذف شد 🗑️")
            return

        # محبوب امروز
        if PAT_GROUP["popular_today"].match(text):
            today = dt.datetime.now(TZ_TEHRAN).date()
            with SessionLocal() as s2:
                rows = s2.execute(select(ReplyStatDaily).where(
                    (ReplyStatDaily.chat_id == g.id) & (ReplyStatDaily.date == today)
                ).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if not rows:
                await reply_temp(update, context, "امروز هنوز آماری نداریم.", keep=True)
                return
            lines = []
            with SessionLocal() as s3:
                for i, r in enumerate(rows, start=1):
                    u = s3.get(User, r.target_user_id)
                    name = u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                    lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
            await reply_temp(update, context, "\n".join(lines), keep=True)
            return

        # شیپ امشب (آخرین ثبت)
        if PAT_GROUP["ship_tonight"].match(text):
            today = dt.datetime.now(TZ_TEHRAN).date()
            with SessionLocal() as s2:
                last = s2.execute(select(ShipHistory).where(
                    (ShipHistory.chat_id == g.id) & (ShipHistory.date == today)
                ).order_by(ShipHistory.id.desc())).scalar_one_or_none()
            if not last:
                await reply_temp(update, context, "هنوز شیپ امشب ساخته نشده. آخر شب منتشر می‌شه 💫", keep=True)
                return
            with SessionLocal() as s3:
                m, f = s3.get(User, last.male_user_id), s3.get(User, last.female_user_id)
            await reply_temp(
                update, context,
                f"💘 شیپِ امشب: {(m.first_name or '@'+(m.username or ''))} × {(f.first_name or '@'+(f.username or ''))}",
                keep=True
            )
            return

        # شیپم کن — پارتنر پیشنهادی
        if PAT_GROUP["ship_me"].match(text):
            me = upsert_user(s, g.id, update.effective_user)
            if me.gender not in ("male", "female"):
                await reply_temp(update, context, "اول جنسیتت رو ثبت کن: «ثبت جنسیت دختر/پسر».")
                return
            rels = s.query(Relationship).filter_by(chat_id=g.id).all()
            in_rel = set([r.user_a_id for r in rels] + [r.user_b_id for r in rels])
            if me.id in in_rel:
                await reply_temp(update, context, "تو در رابطه‌ای. برای پیشنهاد باید سینگل باشی.")
                return
            opposite = "female" if me.gender == "male" else "male"
            candidates = s.query(User).filter_by(chat_id=g.id, gender=opposite).all()
            candidates = [u for u in candidates if u.id not in in_rel and u.tg_user_id != me.tg_user_id]
            if not candidates:
                await reply_temp(update, context, "کسی از جنس مخالفِ سینگل پیدا نشد.")
                return
            cand = random.choice(candidates)
            await reply_temp(
                update, context,
                f"❤️ پارتنر پیشنهادی برای شما: {mention_of(cand)}",
                keep=True, parse_mode=ParseMode.HTML
            )
            return

        # انقضا
        if PAT_GROUP["expiry"].match(text):
            ex = g.expires_at and fmt_dt_fa(g.expires_at)
            await reply_temp(update, context, f"⏳ اعتبار این گروه تا: {ex or 'نامشخص'}")
            return

        # شارژ منو (فقط ادمین فضول/فروشنده/مالک)
        if PAT_GROUP["charge"].match(text):
            with SessionLocal() as s2:
                if not is_group_admin(s2, g.id, update.effective_user.id):
                    await reply_temp(update, context, "دسترسی نداری.")
                    return
            chat_id = update.effective_chat.id
            kb = [
                [InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{chat_id}:30"),
                 InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{chat_id}:90"),
                 InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{chat_id}:180")],
                [InlineKeyboardButton("⛔️ صفر کردن شارژ", callback_data=f"chg:{chat_id}:0")]
            ]
            await panel_open_initial(update, context, "⌁ پنل شارژ گروه", kb, root=False)
            return

        # تگ‌ها (نیاز به ریپلای)
        if PAT_GROUP["tag_girls"].match(text) or PAT_GROUP["tag_boys"].match(text) or PAT_GROUP["tag_all"].match(text):
            if not update.message.reply_to_message:
                await reply_temp(update, context, "برای تگ کردن، روی یک پیام ریپلای کن.")
                return
            reply_to = update.message.reply_to_message.message_id
            with SessionLocal() as s2:
                if PAT_GROUP["tag_girls"].match(text):
                    users = s2.query(User).filter_by(chat_id=g.id, gender="female").all()
                elif PAT_GROUP["tag_boys"].match(text):
                    users = s2.query(User).filter_by(chat_id=g.id, gender="male").all()
                else:
                    users = s2.query(User).filter_by(chat_id=g.id).all()
            if not users:
                await reply_temp(update, context, "کسی برای تگ پیدا نشد.")
                return
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

        # حریم خصوصی: حذف من
        if PAT_GROUP["privacy_me"].match(text):
            me_id = update.effective_user.id
            with SessionLocal() as s2:
                u = s2.execute(select(User).where(User.chat_id == g.id, User.tg_user_id == me_id)).scalar_one_or_none()
                if not u:
                    await reply_temp(update, context, "اطلاعاتی از شما نداریم.")
                    return
                s2.execute(Crush.__table__.delete().where(
                    (Crush.chat_id == g.id) & ((Crush.from_user_id == u.id) | (Crush.to_user_id == u.id))
                ))
                s2.execute(Relationship.__table__.delete().where(
                    (Relationship.chat_id == g.id) & ((Relationship.user_a_id == u.id) | (Relationship.user_b_id == u.id))
                ))
                s2.execute(ReplyStatDaily.__table__.delete().where(
                    (ReplyStatDaily.chat_id == g.id) & (ReplyStatDaily.target_user_id == u.id)
                ))
                s2.execute(User.__table__.delete().where((User.chat_id == g.id) & (User.id == u.id)))
                s2.commit()
            await reply_temp(update, context, "✅ تمام داده‌های شما در این گروه حذف شد.")
            return

        if PAT_GROUP["privacy_info"].match(text):
            me_id = update.effective_user.id
            with SessionLocal() as s2:
                u = s2.execute(select(User).where(User.chat_id == g.id, User.tg_user_id == me_id)).scalar_one_or_none()
                if not u:
                    await reply_temp(update, context, "چیزی از شما ذخیره نشده.")
                    return
                info = f"👤 نام: {u.first_name or ''} @{u.username or ''}\nجنسیت: {u.gender}\nتولد (شمسی): {fmt_date_fa(u.birthday)}"
            await reply_temp(update, context, info)
            return

        # پاکسازی (فقط ادمین فضول/فروشنده/مالک)
        if PAT_GROUP["wipe_group"].match(text):
            with SessionLocal() as s2:
                if not is_group_admin(s2, g.id, update.effective_user.id):
                    await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک.")
                    return
            kb = [[InlineKeyboardButton("🧹 تایید پاکسازی", callback_data=f"wipe:{g.id}"),
                   InlineKeyboardButton("انصراف", callback_data="noop")]]
            await panel_open_initial(update, context, "⚠️ مطمئنی کل داده‌های گروه حذف شود؟", kb, root=False)
            return

    # شمارش ریپلای‌ها (برای آمار محبوب امروز)
    if update.message.reply_to_message:
        with SessionLocal() as s:
            g = ensure_group(s, update.effective_chat)
            today = dt.datetime.now(TZ_TEHRAN).date()
            target = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            upsert_user(s, g.id, update.effective_user)
            row = s.execute(select(ReplyStatDaily).where(
                (ReplyStatDaily.chat_id == g.id) & (ReplyStatDaily.date == today) & (ReplyStatDaily.target_user_id == target.id)
            )).scalar_one_or_none()
            if not row:
                row = ReplyStatDaily(chat_id=g.id, date=today, target_user_id=target.id, reply_count=0)
                s.add(row)
            row.reply_count += 1
            s.commit()

def user_help_text() -> str:
    return (
        "📘 راهنمای کامل کاربر (شمسی):\n"
        "• «فضول» → تست سلامت (جانم)\n"
        "• «فضول منو» → منوی دکمه‌ای\n"
        "• «ثبت جنسیت دختر/پسر» — اگر ادمین هستی و روی پیام کسی ریپلای کنی، برای او ثبت می‌شود.\n"
        "• «ثبت تولد ۱۴۰۳-۰۵-۲۰» — اگر ادمین هستی و ریپلای کنی، برای او ثبت می‌شود.\n"
        "• «حذف تولد» (برای خودت)\n"
        "• «ثبت کراش» (ریپلای) / «حذف کراش» (ریپلای)\n"
        "• «ثبت رابطه @username» (ویزارد تاریخ) / «ثبت رابطه @username ۱۴۰۲/۱۲/۰۱» / «حذف رابطه @username»\n"
        "• «شیپم کن» (پارتنر پیشنهادی برای شما)\n"
        "• «محبوب امروز» / «شیپ امشب»\n"
        "• «تگ دخترها|پسرها|همه» (ریپلای؛ هر پیام ۴ نفر)\n"
        "• «حریم خصوصی» / «حذف من»\n"
        "• «فضول شارژ» (فقط مدیر/فروشنده/مالک)\n"
        "• «فضول انقضا» نمایش پایان اعتبار گروه"
    )

# ================== MY_CHAT_MEMBER (presence) ==================
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ثبت/حذف گروه هنگام اضافه/حذف ربات."""
    try:
        chat = update.my_chat_member.chat if update.my_chat_member else None
        if not chat:
            return
        with SessionLocal() as s:
            g = ensure_group(s, chat)
            # اگر ربات حذف شد، می‌تونی اینجا g.is_active = False هم کنی (دلخواه)
            s.commit()
    except Exception as e:
        logging.info(f"on_my_chat_member err: {e}")

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
    if not tg_ids:
        return 0
    added = 0
    with SessionLocal() as s:
        for uid in tg_ids:
            exists = s.execute(select(GroupAdmin).where(
                GroupAdmin.chat_id == chat_id, GroupAdmin.tg_user_id == uid
            )).scalar_one_or_none()
            if not exists:
                s.add(GroupAdmin(chat_id=chat_id, tg_user_id=uid)); added += 1
        s.commit()
    return added

# ================== POST INIT ==================
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

# ================== ERROR HANDLER ==================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    if isinstance(err, TgConflict):
        try:
            if OWNER_ID:
                await context.bot.send_message(OWNER_ID, "⚠️ Conflict 409: نمونهٔ دیگری از ربات در حال polling است. این نمونه خارج شد.")
        except Exception:
            ...
        logging.error("Conflict 409 detected. Exiting this instance to avoid duplicate polling.")
        os._exit(0)
    logging.exception("Unhandled error", exc_info=err)

# ================== FALLBACK PING (ALWAYS) ==================
async def on_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """اگر هیچ هندلر دیگری جواب نداد، گفتن «فضول» جواب «جانم» می‌گیرد."""
    m = update.effective_message
    if not m:
        return
    txt = clean_text((m.text or m.caption or "") or "")
    if txt == "فضول":
        try:
            await m.reply_text("جانم 👂")
        except Exception:
            pass

# ================== BOOT ==================
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
        seller = is_seller(s, uid)
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
        elif seller:
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
        jq.run_daily(job_morning, time=dt.time(6, 0, 0, tzinfo=TZ_TEHRAN))
        jq.run_daily(job_midnight, time=dt.time(0, 1, 0, tzinfo=TZ_TEHRAN))
        jq.run_repeating(singleton_watchdog, interval=60, first=60)

    logging.info("FazolBot running…")
    allowed = ["message","edited_message","callback_query","my_chat_member","chat_member","chat_join_request"]
    if WEBHOOK_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN, webhook_url=f"{WEBHOOK_URL}/{TOKEN}",
                        allowed_updates=allowed, drop_pending_updates=True)
    else:
        app.run_polling(allowed_updates=allowed, drop_pending_updates=True)
