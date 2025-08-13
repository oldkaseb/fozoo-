import os, logging, re, random, datetime as dt, asyncio
from typing import Optional, List, Tuple, Dict
from zoneinfo import ZoneInfo
import urllib.parse as _up

from sqlalchemy import select, text
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
from sqlalchemy import create_engine, Integer, BigInteger, String, DateTime, Date, Boolean, JSON, ForeignKey, Index

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application, MessageHandler, CallbackQueryHandler, ChatMemberHandler,
    CommandHandler, filters, ContextTypes
)

# ====== CONFIG ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DEFAULT_TZ = os.getenv("DEFAULT_TZ", "Asia/Tehran")
OWNER_CONTACT_USERNAME = os.getenv("OWNER_CONTACT", "soulsownerbot")
AUTO_DELETE_SECONDS = int(os.getenv("AUTO_DELETE_SECONDS", "40"))

Base = declarative_base()

# ====== Persian Date/Time ======
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

def fmt_dt_fa(dt_utc: Optional[dt.datetime], tz_name: Optional[str] = None) -> str:
    if dt_utc is None: return "-"
    tz = ZoneInfo(tz_name or DEFAULT_TZ)
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
    local = dt_utc.astimezone(tz)
    if HAS_PTOOLS:
        try:
            jdt = JalaliDateTime.fromgregorian(datetime=local)
            s = jdt.strftime("%Y/%m/%d %H:%M")
            return fa_digits(s) + f" ({tz.key.split('/')[-1]})"
        except Exception: ...
    return local.strftime("%Y/%m/%d %H:%M") + f" ({tz.key.split('/')[-1]})"

def fmt_date_fa(d: Optional[dt.date]) -> str:
    if not d: return "-"
    if HAS_PTOOLS:
        try:
            jd = JalaliDate.fromgregorian(date=d)
            return fa_digits(jd.strftime("%Y/%m/%d"))
        except Exception: ...
    return d.strftime("%Y/%m/%d")

def today_jalali(tz: ZoneInfo) -> Tuple[int,int,int]:
    now = dt.datetime.now(tz)
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

def footer(text: str) -> str:
    return f"{text}\n\n— ساخته شده توسط تیم souls"

def contact_kb(extra_rows: List[List[InlineKeyboardButton]]|None=None, bot_username: Optional[str]=None) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if extra_rows: rows.extend(extra_rows)
    rows.append([InlineKeyboardButton("📞 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")])
    if bot_username:
        rows.append([InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"https://t.me/{bot_username}?startgroup=true")])
    return InlineKeyboardMarkup(rows)

# ====== Auto-Delete ======
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
                     parse_mode: Optional[str]=None, with_footer: bool=True, reply_to_message_id: Optional[int]=None):
    msg = await update.effective_chat.send_message(
        footer(text) if with_footer else text,
        reply_markup=reply_markup, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id
    )
    schedule_autodelete(context, msg.chat_id, msg.message_id, keep=keep)
    return msg

def autodel_qmessage(context: ContextTypes.DEFAULT_TYPE, qmsg):
    if not qmsg: return
    schedule_autodelete(context, qmsg.chat.id, qmsg.message_id, keep=False)

# ====== DB URL ======
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
    host_ok = bool(parsed.hostname)
    logging.info(f"DB host: {parsed.hostname}, port: {parsed.port}, db: {parsed.path}")
except Exception:
    host_ok = False
if not host_ok:
    masked = _mask_url(raw_db_url)
    raise RuntimeError(
        "DATABASE_URL نامعتبر است (هاست ندارد). مقدار فعلی (بدون پسورد): "
        f"{masked}\nاز Postgres → Connect → External Connection String کپی کن و با کلید DATABASE_URL ست کن."
    )

engine = create_engine(db_url, pool_pre_ping=True, future=True, connect_args={"sslmode":"require"})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# ====== MODELS ======
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

# ====== HELPERS ======
def try_send_owner(text_msg: str):
    from telegram import Bot
    if not TOKEN or not OWNER_ID: return
    try: Bot(TOKEN).send_message(OWNER_ID, footer(text_msg))
    except Exception as e: logging.info(f"Owner DM failed: {e}")

def ensure_group(session, chat) -> 'Group':
    g = session.get(Group, chat.id)
    if not g:
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

async def require_active_or_warn(update: Update, context: ContextTypes.DEFAULT_TYPE, session, g: Group) -> bool:
    if group_active(g): return True
    kb = contact_kb(bot_username=context.bot.username)
    msg = await update.effective_chat.send_message(
        footer("⌛️ اعتبار ربات در این گروه تمام شده. از «فضول شارژ» یا پنل پیکربندی برای تمدید استفاده کنید."),
        reply_markup=kb
    )
    schedule_autodelete(context, msg.chat_id, msg.message_id, keep=False)
    return False

def clean_text(s: str) -> str: return re.sub(r"\s+", " ", s.strip())

def chunked(lst: List, n: int):
    for i in range(0, len(lst), n): yield lst[i:i+n]

def mention_of(u: 'User') -> str:
    if u.username: return f"@{u.username}"
    name = u.first_name or "کاربر"
    return f'<a href="tg://user?id={u.tg_user_id}">{name}</a>'

# ====== STATE/PATTERNS ======
PENDING_REL: Dict[Tuple[int,int], Dict] = {}
TAG_DELAY_SECONDS = 0.8

PAT_GROUP = {
    "help": re.compile(r"^(?:فضول کمک|راهنما|کمک|فضول منو|منو)$"),
    "config": re.compile(r"^(?:پیکربندی فضول|فضول پیکربندی|فضول تنظیمات|تنظیمات فضول)$"),
    "admin_add": re.compile(r"^فضول ادمین(?: @?(\w+))?$"),
    "admin_del": re.compile(r"^حذف فضول ادمین(?: @?(\w+))?$"),
    "seller_block": re.compile(r"^(?:مسدود فروشنده)(?: @?(\w+))?$"),
    "seller_unblock": re.compile(r"^(?:آزاد فروشنده)(?: @?(\w+))?$"),
    "gender": re.compile(r"^ثبت جنسیت (دختر|پسر)$"),
    "birthday_set": re.compile(r"^ثبت تولد (\d{4}-\d{2}-\d{2})$"),
    "birthday_del": re.compile(r"^حذف تولد$"),
    "relation_set": re.compile(r"^ثبت رابطه @?(\w+)\s+(\d{4}-\d{2}-\d{2})$"),
    "relation_del": re.compile(r"^حذف رابطه @?(\w+)$"),
    "crush_add": re.compile(r"^ثبت کراش$"),
    "crush_del": re.compile(r"^حذف کراش$"),
    "popular_today": re.compile(r"^محبوب امروز$"),
    "ship_tonight": re.compile(r"^شیپ امشب$"),
    "expiry": re.compile(r"^فضول انقضا$"),
    "charge": re.compile(r"^فضول شارژ$"),
    "tag_girls": re.compile(r"^تگ دخترها$"),
    "tag_boys": re.compile(r"^تگ پسرها$"),
    "tag_all": re.compile(r"^تگ همه$"),
    "privacy_me": re.compile(r"^حذف من$"),
    "privacy_info": re.compile(r"^(?:داده(?:‌| )های من|حریم خصوصی)$"),
    "wipe_group": re.compile(r"^پاکسازی گروه$"),
}

PAT_DM = {
    "panel": re.compile(r"^(?:پنل|مدیریت|کمک)$"),
    "groups": re.compile(r"^گروه‌ها$"),
    "manage": re.compile(r"^مدیریت (\-?\d+)$"),
    "extend": re.compile(r"^تمدید (\-?\d+)\s+(\d+)$"),
    "set_tz": re.compile(r"^تنظیم زمان (\-?\d+)\s+([\w\/]+)$"),
    "add_seller": re.compile(r"^افزودن فروشنده (\d+)(?:\s+(.+))?$"),
    "del_seller": re.compile(r"^حذف فروشنده (\d+)$"),
    "list_sellers": re.compile(r"^لیست فروشنده‌ها$"),
}

# ====== UI ======
def build_group_menu(is_group_admin_flag: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("👤 ثبت جنسیت", callback_data="ui:gset")],
        [InlineKeyboardButton("🎂 ثبت تولد", callback_data="ui:bd:start")],
        [InlineKeyboardButton("💘 ثبت کراش (ریپلای)", callback_data="ui:crush:add"),
         InlineKeyboardButton("🗑️ حذف کراش", callback_data="ui:crush:del")],
        [InlineKeyboardButton("💞 ثبت رابطه (ریپلای)", callback_data="ui:rel:add"),
         InlineKeyboardButton("🗑️ حذف رابطه", callback_data="ui:rel:del")],
        [InlineKeyboardButton("👑 محبوب امروز", callback_data="ui:pop"),
         InlineKeyboardButton("💫 شیپ امشب", callback_data="ui:ship")],
        [InlineKeyboardButton("🏷️ تگ دخترها", callback_data="ui:tag:girls"),
         InlineKeyboardButton("🏷️ تگ پسرها", callback_data="ui:tag:boys")],
        [InlineKeyboardButton("🏷️ تگ همه", callback_data="ui:tag:all")],
        [InlineKeyboardButton("🔐 داده‌های من", callback_data="ui:privacy:me"),
         InlineKeyboardButton("🗑️ حذف من", callback_data="ui:privacy:delme")],
    ]
    if is_group_admin_flag:
        rows.append([InlineKeyboardButton("⚙️ پیکربندی فضول", callback_data="cfg:open")])
    return InlineKeyboardMarkup(rows)

def build_config_panel(chat_id: int, bot_username: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡️ شارژ گروه", callback_data="ui:charge:open")],
        [InlineKeyboardButton("👥 مدیران گروه", callback_data="ga:list")],
        [InlineKeyboardButton("⏱ تنظیم تایم‌زون", callback_data="tz:menu")],
        [InlineKeyboardButton("ℹ️ مشاهده انقضا", callback_data="ui:expiry")],
        [InlineKeyboardButton("🚪 خروج ربات", callback_data=f"grp:{chat_id}:leave")],
        [InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
        [InlineKeyboardButton("🧹 پاکسازی گروه", callback_data=f"wipe:{chat_id}")],
    ])

def build_owner_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
        [InlineKeyboardButton("🛍️ لیست فروشنده‌ها", callback_data="adm:sellers")],
        [InlineKeyboardButton("ℹ️ راهنما", callback_data="adm:help")],
    ])

# ====== /start ======
async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await reply_temp(update, context, "سلام! در گروه «فضول منو» یا «فضول کمک» بزن.")
        return
    bot_username = context.bot.username
    with SessionLocal() as s:
        uid = update.effective_user.id
        if uid == OWNER_ID:
            txt = (
                "👑 «راهنمای مالک»\n"
                "• «📋 لیست گروه‌ها» → پنل گروه (شارژ/تمدید/انقضا/خروج/افزودن).\n"
                "• «🛍️ لیست فروشنده‌ها» → آمار/عزل/افزودن.\n"
                "• «تنظیم زمان <chat_id> <Area/City>»، «تمدید <chat_id> <days>».\n"
                "• ۷ روز تست رایگان برای هر گروه جدید فعال می‌شود."
            )
            await reply_temp(update, context, txt, reply_markup=contact_kb(
                extra_rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0"),
                             InlineKeyboardButton("🛍️ لیست فروشنده‌ها", callback_data="adm:sellers")]],
                bot_username=bot_username
            ))
            return
        elif is_seller(s, uid):
            txt = (
                "🛍️ «راهنمای فروشنده»\n"
                "• در گروه: «فضول شارژ» یا «⚙️ پیکربندی فضول».\n"
                "• در پی‌وی: «📋 لیست گروه‌ها» → پنل هر گروه → شارژ ۳۰/۹۰/۱۸۰ روزه.\n"
                "• به مشتری بگو در گروه «فضول منو» بزنند؛ همه‌چیز با دکمه‌هاست."
            )
            await reply_temp(update, context, txt, reply_markup=contact_kb(
                extra_rows=[[InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")]],
                bot_username=bot_username
            ))
            return
        else:
            txt = (
                "سلام! 👋\n"
                "این ربات مخصوص گروه‌هاست. می‌تونی اضافه‌ش کنی و ۷ روز تست رایگان داشته باشی.\n"
                "بعد از افزودن، در گروه «فضول منو» بزن تا با دکمه‌ها کار کنی."
            )
            await reply_temp(update, context, txt, reply_markup=contact_kb(
                extra_rows=[[InlineKeyboardButton("🧭 راهنمای استفاده در گروه", callback_data="usr:help")]],
                bot_username=bot_username
            ))
            return

# ====== GROUP TEXT ======
async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group","supergroup") or not update.message or not update.message.text:
        return
    text = clean_text(update.message.text)

    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)
        is_gadmin = is_group_admin(s, g.id, update.effective_user.id)

    if PAT_GROUP["help"].match(text):
        await reply_temp(update, context, "🕹 منوی فضول:", reply_markup=build_group_menu(is_gadmin))
        return

    if PAT_GROUP["config"].match(text):
        with SessionLocal() as s:
            if not is_group_admin(s, update.effective_chat.id, update.effective_user.id):
                await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک می‌تواند.")
                return
        await reply_temp(update, context, "⚙️ پیکربندی فضول:", reply_markup=build_config_panel(update.effective_chat.id, context.bot.username))
        return

    # ادمین اضافه/حذف
    if PAT_GROUP["admin_add"].match(text) or PAT_GROUP["admin_del"].match(text):
        m_add = PAT_GROUP["admin_add"].match(text)
        m_del = PAT_GROUP["admin_del"].match(text)
        with SessionLocal() as s:
            if not is_group_admin(s, update.effective_chat.id, update.effective_user.id):
                await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک می‌تواند.")
                return
            target_id = None
            m = m_add or m_del
            if update.message.reply_to_message:
                target_id = update.message.reply_to_message.from_user.id
            elif m and m.group(1):
                uname = m.group(1)
                urow = s.execute(select(User).where(User.chat_id==update.effective_chat.id, User.username==uname)).scalar_one_or_none()
                if urow: target_id = urow.tg_user_id
            if not target_id:
                await reply_temp(update, context, "روی پیام کاربر ریپلای کن یا «فضول ادمین @username / حذف فضول ادمین @username» بزن.")
                return
            if m_add:
                try:
                    s.add(GroupAdmin(chat_id=update.effective_chat.id, tg_user_id=target_id)); s.commit()
                    await reply_temp(update, context, "✅ به‌عنوان ادمین گروه اضافه شد.")
                except Exception:
                    await reply_temp(update, context, "قبلاً ادمین بوده یا خطا رخ داد.")
            else:
                if target_id == OWNER_ID or is_seller(s, target_id):
                    await reply_temp(update, context, "نمی‌توان مالک/فروشنده را حذف کرد.")
                else:
                    s.execute(GroupAdmin.__table__.delete().where(
                        (GroupAdmin.chat_id==update.effective_chat.id) & (GroupAdmin.tg_user_id==target_id)
                    )); s.commit()
                    await reply_temp(update, context, "🗑️ ادمین گروه حذف شد.")
        return

    # مسدود/آزاد فروشنده
    if PAT_GROUP["seller_block"].match(text) or PAT_GROUP["seller_unblock"].match(text):
        block = bool(PAT_GROUP["seller_block"].match(text))
        with SessionLocal() as s:
            if not is_group_admin(s, update.effective_chat.id, update.effective_user.id):
                await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک می‌تواند.")
                return
            target = None
            m = PAT_GROUP["seller_block"].match(text) if block else PAT_GROUP["seller_unblock"].match(text)
            if update.message.reply_to_message:
                target = update.message.reply_to_message.from_user
            elif m and m.group(1):
                uname = m.group(1)
                urow = s.execute(select(User).where(User.chat_id==update.effective_chat.id, User.username==uname)).scalar_one_or_none()
                if urow:
                    class _Tmp: id=urow.tg_user_id
                    target = _Tmp()
            if not target:
                await reply_temp(update, context, "روی پیام فروشنده ریپلای کن یا با @username مشخص کن.")
                return
            if not is_seller(s, target.id):
                await reply_temp(update, context, "این کاربر فروشنده نیست.")
                return
            g = s.get(Group, update.effective_chat.id)
            g.settings = g.settings or {}
            bl = set(g.settings.get("blocked_sellers", []))
            if block: bl.add(target.id)
            else: bl.discard(target.id)
            g.settings["blocked_sellers"] = list(bl); s.commit()
        await reply_temp(update, context, "✅ اعمال شد.")
        return

    # ===== سایر دستورات =====
    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)

        if m := PAT_GROUP["gender"].match(text):
            if not await require_active_or_warn(update, context, s, g): return
            gender = "female" if m.group(1)=="دختر" else "male"
            u = upsert_user(s, g.id, update.effective_user)
            u.gender = gender; s.commit()
            await reply_temp(update, context, "ثبت شد ✅"); return

        if m := PAT_GROUP["birthday_set"].match(text):
            if not await require_active_or_warn(update, context, s, g): return
            try: d = dt.date.fromisoformat(m.group(1))
            except ValueError:
                await reply_temp(update, context, "تاریخ نامعتبر. فرمت YYYY-MM-DD"); return
            u = upsert_user(s, g.id, update.effective_user)
            u.birthday = d; s.commit()
            await reply_temp(update, context, f"تولد ثبت شد 🎂 (شمسی: {fmt_date_fa(d)})"); return

        if PAT_GROUP["birthday_del"].match(text):
            u = upsert_user(s, g.id, update.effective_user)
            u.birthday = None; s.commit()
            await reply_temp(update, context, "تولد حذف شد 🗑️"); return

        if m := PAT_GROUP["relation_set"].match(text):
            if not await require_active_or_warn(update, context, s, g): return
            target_username, date_str = m.group(1), m.group(2)
            try: started = dt.date.fromisoformat(date_str)
            except ValueError:
                await reply_temp(update, context, "تاریخ نامعتبر."); return
            me = upsert_user(s, g.id, update.effective_user)
            to = s.execute(select(User).where(User.chat_id==g.id, User.username==target_username)).scalar_one_or_none()
            if not to:
                await reply_temp(update, context, "کاربر هدف پیدا نشد."); return
            s.execute(Relationship.__table__.delete().where(
                (Relationship.chat_id==g.id) & (
                    ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==to.id)) |
                    ((Relationship.user_a_id==to.id) & (Relationship.user_b_id==me.id))
                )
            ))
            s.add(Relationship(chat_id=g.id, user_a_id=min(me.id,to.id), user_b_id=max(me.id,to.id), started_at=started)); s.commit()
            await reply_temp(update, context, f"رابطه ثبت شد 💞 (تاریخ شمسی: {fmt_date_fa(started)})"); return

        if m := PAT_GROUP["relation_del"].match(text):
            target_username = m.group(1)
            me = upsert_user(s, g.id, update.effective_user)
            to = s.execute(select(User).where(User.chat_id==g.id, User.username==target_username)).scalar_one_or_none()
            if not to: await reply_temp(update, context, "کاربر هدف پیدا نشد."); return
            s.execute(Relationship.__table__.delete().where(
                (Relationship.chat_id==g.id) & (
                    ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==to.id)) |
                    ((Relationship.user_a_id==to.id) & (Relationship.user_b_id==me.id))
                )
            )); s.commit()
            await reply_temp(update, context, "رابطه حذف شد 🗑️"); return

        if PAT_GROUP["crush_add"].match(text):
            if not update.message.reply_to_message:
                await reply_temp(update, context, "روی پیام طرف ریپلای کن یا از دکمه منو استفاده کن."); return
            if not await require_active_or_warn(update, context, s, g): return
            me = upsert_user(s, g.id, update.effective_user)
            to = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            if me.id == to.id:
                await reply_temp(update, context, "روی خودت نمی‌شه 😅"); return
            try:
                s.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=to.id)); s.commit()
                await reply_temp(update, context, "کراش ثبت شد 💘")
            except Exception:
                await reply_temp(update, context, "از قبل ثبت شده بود.")
            return

        if PAT_GROUP["crush_del"].match(text):
            if not update.message.reply_to_message:
                await reply_temp(update, context, "روی پیام طرف ریپلای کن یا از دکمه منو استفاده کن."); return
            me = upsert_user(s, g.id, update.effective_user)
            to = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            s.execute(Crush.__table__.delete().where(
                (Crush.chat_id==g.id) & (Crush.from_user_id==me.id) & (Crush.to_user_id==to.id)
            )); s.commit()
            await reply_temp(update, context, "کراش حذف شد 🗑️"); return

        if PAT_GROUP["popular_today"].match(text):
            tz = ZoneInfo(g.timezone or DEFAULT_TZ)
            today = dt.datetime.now(tz).date()
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

        if PAT_GROUP["ship_tonight"].match(text):
            tz = ZoneInfo(g.timezone or DEFAULT_TZ); today = dt.datetime.now(tz).date()
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

        if PAT_GROUP["expiry"].match(text):
            ex = g.expires_at and fmt_dt_fa(g.expires_at, g.timezone)
            await reply_temp(update, context, f"⏳ اعتبار این گروه تا: {ex or 'نامشخص'}"); return

        if PAT_GROUP["charge"].match(text):
            with SessionLocal() as s2:
                if not is_group_admin(s2, g.id, update.effective_user.id):
                    await reply_temp(update, context, "دسترسی نداری."); return
            chat_id = update.effective_chat.id
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{chat_id}:30"),
                InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{chat_id}:90"),
                InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{chat_id}:180"),
            ]])
            await reply_temp(update, context, "⌁ پنل شارژ گروه:", reply_markup=kb); return

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
                    await asyncio.sleep(TAG_DELAY_SECONDS)
                except Exception as e:
                    logging.info(f"Tag batch send failed: {e}")
            return

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

        if PAT_GROUP["wipe_group"].match(text):
            with SessionLocal() as s2:
                if not is_group_admin(s2, g.id, update.effective_user.id):
                    await reply_temp(update, context, "فقط مدیر گروه/فروشنده/مالک."); return
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🧹 تایید پاکسازی", callback_data=f"wipe:{g.id}"),
                                        InlineKeyboardButton("انصراف", callback_data="noop")]])
            await reply_temp(update, context, "⚠️ مطمئنی کل داده‌های گروه حذف شود؟", reply_markup=kb); return

    # شمارش ریپلای‌ها
    if update.message.reply_to_message:
        with SessionLocal() as s:
            g = ensure_group(s, update.effective_chat)
            tz = ZoneInfo(g.timezone or DEFAULT_TZ); today = dt.datetime.now(tz).date()
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

# ====== PRIVATE (OWNER/SELLER) ======
async def on_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private" or not update.message or not update.message.text:
        return
    text = clean_text(update.message.text)
    bot_username = context.bot.username
    with SessionLocal() as s:
        uid = update.effective_user.id
        seller = is_seller(s, uid)
        if uid != OWNER_ID and not seller:
            if text in ("/start","start","کمک","راهنما"):
                txt = (
                    "این ربات مخصوص گروه‌هاست. برای شروع، ربات را به گروه اضافه کن تا ۷ روز رایگان فعال شود.\n"
                    "در گروه «فضول منو» را بزن تا با دکمه‌ها کار کنی."
                )
                await reply_temp(update, context, txt, reply_markup=contact_kb(bot_username=bot_username)); return
            await reply_temp(update, context, "برای مدیریت باید مالک/فروشنده باشی. «/start» یا «کمک» بزن."); return

        if PAT_DM["panel"].match(text):
            who = "👑 پنل مالک" if uid==OWNER_ID else "🛍️ پنل فروشنده"
            await reply_temp(update, context, who, reply_markup=build_owner_panel()); return

        if PAT_DM["groups"].match(text):
            await reply_temp(update, context, "📋 لیست گروه‌ها (صفحه ۱)",
                             reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("نمایش", callback_data="adm:groups:0")]])); return

        if m := PAT_DM["manage"].match(text):
            chat_id = int(m.group(1))
            g = s.get(Group, chat_id)
            if not g: await reply_temp(update, context, "گروه پیدا نشد."); return
            ex = g.expires_at and fmt_dt_fa(g.expires_at, g.timezone)
            txt = f"🧩 پنل گروه: {g.title}\nchat_id: {g.id}\nانقضا: {ex or 'نامشخص'}"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{g.id}:30"),
                 InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{g.id}:90"),
                 InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{g.id}:180")],
                [InlineKeyboardButton("ℹ️ انقضا", callback_data="ui:expiry"),
                 InlineKeyboardButton("🚪 خروج ربات", callback_data=f"grp:{g.id}:leave")],
                [InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"https://t.me/{bot_username}?startgroup=true")],
            ])
            await reply_temp(update, context, txt, reply_markup=kb); return

        if m := PAT_DM["extend"].match(text):
            chat_id = int(m.group(1)); days = int(m.group(2))
            g = s.get(Group, chat_id)
            if not g: await reply_temp(update, context, "گروه پیدا نشد."); return
            base = g.expires_at if g.expires_at and g.expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
            g.expires_at = base + dt.timedelta(days=days)
            s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=uid, action="extend", amount_days=days)); s.commit()
            await reply_temp(update, context, f"✅ تمدید شد تا {fmt_dt_fa(g.expires_at, g.timezone)}"); return

        if m := PAT_DM["set_tz"].match(text):
            chat_id = int(m.group(1)); tzname = m.group(2)
            g = s.get(Group, chat_id)
            if not g: await reply_temp(update, context, "گروه پیدا نشد."); return
            try: ZoneInfo(tzname)
            except Exception: await reply_temp(update, context, "نام منطقه زمانی نامعتبر. مثال: Asia/Tehran"); return
            g.timezone = tzname; s.commit()
            await reply_temp(update, context, f"⏱ تایم‌زون گروه تنظیم شد: {tzname}"); return

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
            await reply_temp(update, context, "🛍️ لیست فروشنده‌ها", reply_markup=InlineKeyboardMarkup(rows)); return

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

# ====== CALLBACKS ======
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.data: return

    # پاسخ فوری تا مطمئن شوید «کلیک» کار می‌کند حتی اگر ادامهٔ پردازش به هر دلیلی خطا بدهد
    try:
        await q.answer("✅ کلیک ثبت شد", cache_time=0, show_alert=False)
    except Exception: ...

    autodel_qmessage(context, q.message)  # تایمر حذف برای پیام پنل

    if q.data == "usr:help":
        txt = (
            "راهنمای سریع کاربر:\n"
            "• در گروه «فضول منو» بزن → دکمه‌ها برای ثبت جنسیت/تولد/کراش/رابطه/تگ‌ها.\n"
            "• دستورات متنی هم پشتیبانی می‌شود (مثلاً «تگ همه» روی ریپلای).\n"
            "• ۷ روز تست رایگان پس از افزودن ربات به گروه."
        )
        await q.message.reply_text(footer(txt), reply_markup=contact_kb(bot_username=context.bot.username))
        return

    if q.data == "cfg:open":
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                await q.answer("فقط مدیر گروه/فروشنده/مالک.", show_alert=True); return
        try:
            await q.edit_message_reply_markup(reply_markup=build_config_panel(q.message.chat.id, context.bot.username))
        except Exception:
            await q.message.reply_text(footer("⚙️ پیکربندی فضول:"), reply_markup=build_config_panel(q.message.chat.id, context.bot.username))
        return

    # شارژ
    if q.data.startswith("chg:"):
        _, chat_id_str, days_str = q.data.split(":")
        target_chat_id = int(chat_id_str); days = int(days_str)
        with SessionLocal() as s:
            if not is_group_admin(s, target_chat_id, q.from_user.id):
                await q.answer("دسترسی نداری.", show_alert=True); return
            g = s.get(Group, target_chat_id) or ensure_group(s, q.message.chat)
            base = g.expires_at if g.expires_at and g.expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
            g.expires_at = base + dt.timedelta(days=days)
            s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=q.from_user.id, action="extend", amount_days=days))
            s.commit()
            ex_str = fmt_dt_fa(g.expires_at, g.timezone)
        await q.answer(f"✅ تمدید شد تا {ex_str}", show_alert=True)
        return

    if q.data == "ui:charge:open":
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                await q.answer("فقط مدیر گروه/فروشنده/مالک.", show_alert=True); return
        chat_id = q.message.chat.id
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{chat_id}:30"),
            InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{chat_id}:90"),
            InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{chat_id}:180"),
        ]])
        try:
            await q.edit_message_reply_markup(reply_markup=kb)
        except Exception:
            await q.message.reply_text(footer("⌁ پنل شارژ گروه:"), reply_markup=kb)
        return

    # لیست گروه‌ها
    if q.data.startswith("adm:groups"):
        parts = q.data.split(":")
        page = int(parts[2]) if len(parts)>=3 else 0
        PAGE_SIZE = 5
        with SessionLocal() as s:
            groups = s.query(Group).order_by(Group.id.asc()).all()
        if not groups:
            await q.message.reply_text("گروهی ثبت نشده.")
            return
        total_pages = (len(groups)+PAGE_SIZE-1)//PAGE_SIZE
        page = max(0, min(page, total_pages-1))
        start = page*PAGE_SIZE
        subset = groups[start:start+PAGE_SIZE]
        lines = []
        for g in subset:
            ex = g.expires_at and fmt_dt_fa(g.expires_at, g.timezone)
            stat = "فعال ✅" if group_active(g) else "منقضی ⛔️"
            lines.append(f"{g.title} | chat_id: {g.id} | تا: {ex or 'نامشخص'} | {stat}")
        rows = [[InlineKeyboardButton(f"🧩 پنل «{g.title[:18]}»", callback_data=f"grp:{g.id}:panel")] for g in subset]
        nav=[]
        if page>0: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"adm:groups:{page-1}"))
        if page<total_pages-1: nav.append(InlineKeyboardButton("➡️ بعدی", callback_data=f"adm:groups:{page+1}"))
        if nav: rows.append(nav)
        rows.append([InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"https://t.me/{context.bot.username}?startgroup=true")])
        rows.append([InlineKeyboardButton("📞 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")])
        try:
            await q.edit_message_text(footer(f"📋 لیست گروه‌ها (صفحه {fa_digits(page+1)}/{fa_digits(total_pages)})\n" + "\n".join(lines)))
            await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            await q.message.reply_text(footer("\n".join(lines)), reply_markup=InlineKeyboardMarkup(rows))
        return

    if q.data.startswith("grp:"):
        _, chat_id_str, action = q.data.split(":")
        chat_id = int(chat_id_str)
        if action == "panel":
            with SessionLocal() as s:
                g = s.get(Group, chat_id)
            if not g:
                await q.answer("گروه یافت نشد.", show_alert=True); return
            ex = g.expires_at and fmt_dt_fa(g.expires_at, g.timezone)
            txt = f"🧩 پنل گروه: {g.title}\nchat_id: {g.id}\nانقضا: {ex or 'نامشخص'}"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{g.id}:30"),
                 InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{g.id}:90"),
                 InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{g.id}:180")],
                [InlineKeyboardButton("ℹ️ انقضا", callback_data="ui:expiry"),
                 InlineKeyboardButton("🚪 خروج ربات", callback_data=f"grp:{g.id}:leave")],
                [InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"https://t.me/{context.bot.username}?startgroup=true")],
                [InlineKeyboardButton("⬅️ بازگشت به لیست", callback_data="adm:groups:0")]
            ])
            await q.message.reply_text(footer(txt), reply_markup=kb)
            return
        if action == "leave":
            try:
                await context.bot.leave_chat(chat_id)
                await q.answer("✅ ربات از گروه خارج شد.", show_alert=True)
            except Exception:
                await q.answer("خطا در خروج (ممکن است عضو نباشم).", show_alert=True)
            return

    # فروشنده‌ها
    if q.data == "adm:sellers":
        with SessionLocal() as s:
            sellers = s.query(Seller).order_by(Seller.id.asc()).all()
        if not sellers:
            await q.message.reply_text("هیچ فروشنده‌ای ثبت نشده.")
            return
        rows=[]
        for sl in sellers[:50]:
            cap = f"{sl.tg_user_id} | {'فعال' if sl.is_active else 'غیرفعال'}"
            r = [InlineKeyboardButton(f"📈 آمار {cap}", callback_data=f"sl:stat:{sl.tg_user_id}")]
            if q.from_user.id==OWNER_ID:
                r.append(InlineKeyboardButton("❌ عزل", callback_data=f"sl:del:{sl.tg_user_id}"))
            rows.append(r)
        rows.append([InlineKeyboardButton("➕ راهنمای افزودن فروشنده", callback_data="sl:add:help")])
        await q.message.reply_text(footer("🛍️ لیست فروشنده‌ها"), reply_markup=InlineKeyboardMarkup(rows))
        return

    if q.data == "adm:help":
        txt = (
            "راهنمای مالک/فروشنده:\n"
            "• 📋 لیست گروه‌ها → پنل هر گروه: شارژ ۳۰/۹۰/۱۸۰، مشاهده انقضا، خروج، افزودن به گروه.\n"
            "• 🛍️ لیست فروشنده‌ها: آمار، عزل فروشنده (فقط مالک)، راهنمای افزودن.\n"
            "• دستورات پی‌وی: «تمدید <chat_id> <days>»، «تنظیم زمان <chat_id> <Area/City>»، «افزودن فروشنده <id> [یادداشت]»، «حذف فروشنده <id>»."
        )
        await q.message.reply_text(footer(txt), reply_markup=contact_kb())
        return

    if q.data.startswith("sl:"):
        _, sub, arg = q.data.split(":")
        with SessionLocal() as s:
            if sub == "stat":
                tid = int(arg)
                now = dt.datetime.utcnow()
                def _count(days):
                    since = now - dt.timedelta(days=days)
                    rows = s.execute(select(SubscriptionLog).where(
                        (SubscriptionLog.actor_tg_user_id==tid) &
                        (SubscriptionLog.action=="extend") &
                        (SubscriptionLog.created_at>=since)
                    )).scalars().all()
                    return len(rows), sum([r.amount_days or 0 for r in rows])
                c7,d7 = _count(7); c30,d30 = _count(30)
                rows_all = s.execute(select(SubscriptionLog).where(
                    (SubscriptionLog.actor_tg_user_id==tid) & (SubscriptionLog.action=="extend")
                )).scalars().all()
                call, dall = len(rows_all), sum([r.amount_days or 0 for r in rows_all])
                txt = (f"📈 آمار فروشنده {tid}:\n"
                       f"۷ روز اخیر: {fa_digits(c7)} تمدید / {fa_digits(d7)} روز\n"
                       f"۳۰ روز اخیر: {fa_digits(c30)} تمدید / {fa_digits(d30)} روز\n"
                       f"مجموع: {fa_digits(call)} تمدید / {fa_digits(dall)} روز")
                await q.message.reply_text(footer(txt))
                return
            elif sub == "del":
                if q.from_user.id != OWNER_ID:
                    await q.answer("فقط مالک می‌تواند.", show_alert=True); return
                tid = int(arg)
                ex = s.execute(select(Seller).where(Seller.tg_user_id==tid)).scalar_one_or_none()
                if not ex: await q.answer("فروشنده پیدا نشد.", show_alert=True); return
                ex.is_active = False; s.commit()
                await q.answer("🗑️ فروشنده عزل شد.", show_alert=True); return
            elif sub == "add" and arg=="help":
                txt = "برای افزودن فروشنده: در همین چت بفرست:\n«افزودن فروشنده <tg_user_id> [یادداشت]»"
                await q.message.reply_text(footer(txt)); return

    # گروه: مدیران
    if q.data == "ga:list":
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                await q.answer("فقط مدیر گروه/فروشنده/مالک.", show_alert=True); return
            admins = s.query(GroupAdmin).filter_by(chat_id=q.message.chat.id).all()
        names = [str(a.tg_user_id) for a in admins] or ["—"]
        txt = ("👥 مدیران محلی این گروه:\n"
               f"{fa_digits('، '.join(names))}\n\n"
               "افزودن: «فضول ادمین» (ریپلای) یا «فضول ادمین @username»\n"
               "حذف: «حذف فضول ادمین» (ریپلای) یا «حذف فضول ادمین @username»")
        await q.message.reply_text(footer(txt))
        return

    # جنسیت
    if q.data == "ui:gset":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("👧 دختر", callback_data="gset:female"),
             InlineKeyboardButton("👦 پسر", callback_data="gset:male")]
        ])
        try:
            await q.edit_message_reply_markup(reply_markup=kb)
        except Exception:
            await q.message.reply_text(footer("جنسیت؟"), reply_markup=kb)
        return

    if q.data.startswith("gset:"):
        gender = q.data.split(":")[1]
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            u = upsert_user(s, g.id, q.from_user)
            u.gender = "female" if gender=="female" else "male"
            s.commit()
        await q.answer("ثبت شد ✅", show_alert=False)
        return

    # ثبت تولد با دکمه
    def _year_page(start_year: int) -> InlineKeyboardMarkup:
        years = [start_year+i for i in range(-8, 9)]
        rows = []
        for i in range(0, len(years), 3):
            chunk = years[i:i+3]
            rows.append([InlineKeyboardButton(fa_digits(y), callback_data=f"bd:y:{y}") for y in chunk])
        rows.append([InlineKeyboardButton("⏪", callback_data=f"bd:yp:{years[0]-17}"),
                     InlineKeyboardButton("انصراف", callback_data="bd:cancel"),
                     InlineKeyboardButton("⏩", callback_data=f"bd:yn:{years[-1]+17}")])
        return InlineKeyboardMarkup(rows)

    def _month_kb(year: int) -> InlineKeyboardMarkup:
        rows = []
        for r in (1,4,7,10):
            rows.append([InlineKeyboardButton(fa_digits(f"{m:02d}"), callback_data=f"bd:m:{year}:{m}") for m in range(r, r+3)])
        rows.append([InlineKeyboardButton("↩️ سال", callback_data="ui:bd:start")])
        return InlineKeyboardMarkup(rows)

    def _days_kb(year:int, month:int) -> InlineKeyboardMarkup:
        import calendar
        nd = calendar.monthrange(year, month)[1]
        rows = []; row=[]
        for d in range(1, nd+1):
            row.append(InlineKeyboardButton(fa_digits(f"{d:02d}"), callback_data=f"bd:d:{year}:{month}:{d}"))
            if len(row)==7: rows.append(row); row=[]
        if row: rows.append(row)
        rows.append([InlineKeyboardButton("↩️ ماه", callback_data=f"bd:m:{year}:{month}")])
        return InlineKeyboardMarkup(rows)

    if q.data in ("ui:bd:start","bd:start"):
        try:
            await q.edit_message_reply_markup(reply_markup=_year_page(dt.datetime.utcnow().year-5))
        except Exception:
            await q.message.reply_text(footer("سال تولد را انتخاب کن:"), reply_markup=_year_page(dt.datetime.utcnow().year-5))
        return

    if q.data.startswith("bd:yp:") or q.data.startswith("bd:yn:"):
        base = int(q.data.split(":")[2])
        try:
            await q.edit_message_reply_markup(reply_markup=_year_page(base))
        except Exception:
            await q.message.reply_text(footer("سال تولد:"), reply_markup=_year_page(base))
        return

    if q.data.startswith("bd:y:"):
        y = int(q.data.split(":")[2])
        try:
            await q.edit_message_reply_markup(reply_markup=_month_kb(y))
        except Exception:
            await q.message.reply_text(footer("ماه تولد:"), reply_markup=_month_kb(y))
        return

    if q.data.startswith("bd:m:"):
        parts = q.data.split(":")
        if len(parts)==4:
            y = int(parts[2]); m = int(parts[3])
            try:
                await q.edit_message_reply_markup(reply_markup=_days_kb(y,m))
            except Exception:
                await q.message.reply_text(footer("روز تولد:"), reply_markup=_days_kb(y,m))
        else:
            await q.edit_message_reply_markup(reply_markup=_month_kb(int(parts[2])))
        return

    if q.data.startswith("bd:d:"):
        _,_, y,m,d = q.data.split(":")
        Y,M,D = int(y), int(m), int(d)
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            u = upsert_user(s, g.id, q.from_user)
            u.birthday = dt.date(Y,M,D); s.commit()
        await q.answer("تولد ثبت شد 🎂", show_alert=True)
        try:
            await q.edit_message_text(f"🎂 تاریخ تولد شما (شمسی): {fmt_date_fa(dt.date(Y,M,D))}")
        except Exception: ...
        if (q.message.chat.id, q.from_user.id) in PENDING_REL:
            pend = PENDING_REL.pop((q.message.chat.id, q.from_user.id), {})
            target_tg_id = pend.get("target_tg_id")
            with SessionLocal() as s:
                g = ensure_group(s, q.message.chat)
                me = upsert_user(s, g.id, q.from_user)
                to = s.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==target_tg_id)).scalar_one_or_none()
                if not to and q.message.reply_to_message:
                    to = upsert_user(s, g.id, q.message.reply_to_message.from_user)
                s.execute(Relationship.__table__.delete().where(
                    (Relationship.chat_id==g.id) & (
                        ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==to.id)) |
                        ((Relationship.user_a_id==to.id) & (Relationship.user_b_id==me.id))
                    )
                ))
                s.add(Relationship(chat_id=g.id, user_a_id=min(me.id,to.id), user_b_id=max(me.id,to.id), started_at=dt.date(Y,M,D)))
                s.commit()
            try:
                await q.message.reply_text(f"💞 رابطه ثبت شد از تاریخ (شمسی) {fmt_date_fa(dt.date(Y,M,D))}")
            except: ...
        return

    if q.data == "bd:cancel":
        await q.answer("لغو شد", show_alert=False); return

    # کراش/رابطه با دکمه
    if q.data in ("ui:crush:add","ui:crush:del","ui:rel:add","ui:rel:del"):
        if not q.message or not q.message.reply_to_message:
            await q.answer("روی پیام طرف ریپلای کن و دوباره بزن.", show_alert=True); return

    if q.data == "ui:crush:add":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            if not group_active(g): await q.answer("اعتبار گروه تمام شده.", show_alert=True); return
            me = upsert_user(s, g.id, q.from_user)
            to = upsert_user(s, g.id, q.message.reply_to_message.from_user)
            if me.id == to.id: await q.answer("روی خودت نمی‌شه 😅", show_alert=True); return
            try:
                s.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=to.id)); s.commit()
                await q.answer("کراش ثبت شد 💘", show_alert=True)
            except Exception:
                await q.answer("قبلاً ثبت شده.", show_alert=True)
        return

    if q.data == "ui:crush:del":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            me = upsert_user(s, g.id, q.from_user)
            to = upsert_user(s, g.id, q.message.reply_to_message.from_user)
            s.execute(Crush.__table__.delete().where(
                (Crush.chat_id==g.id) & (Crush.from_user_id==me.id) & (Crush.to_user_id==to.id)
            )); s.commit()
        await q.answer("کراش حذف شد 🗑️", show_alert=True)
        return

    if q.data == "ui:rel:add":
        target_tg_id = q.message.reply_to_message.from_user.id
        PENDING_REL[(q.message.chat.id, q.from_user.id)] = {"target_tg_id": target_tg_id}
        try:
            await q.edit_message_reply_markup(reply_markup=_year_page(dt.datetime.utcnow().year-5))
        except Exception:
            await q.message.reply_text(footer("تاریخ شروع رابطه را انتخاب کن:"), reply_markup=_year_page(dt.datetime.utcnow().year-5))
        return

    if q.data == "ui:rel:del":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            me = upsert_user(s, g.id, q.from_user)
            to = upsert_user(s, g.id, q.message.reply_to_message.from_user)
            s.execute(Relationship.__table__.delete().where(
                (Relationship.chat_id==g.id) & (
                    ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==to.id)) |
                    ((Relationship.user_a_id==to.id) & (Relationship.user_b_id==me.id))
                )
            )); s.commit()
        await q.answer("رابطه حذف شد 🗑️", show_alert=True)
        return

    # محبوب/شیپ (ماندگار)
    if q.data == "ui:pop":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            tz = ZoneInfo(g.timezone or DEFAULT_TZ); today = dt.datetime.now(tz).date()
            rows = s.execute(select(ReplyStatDaily).where(
                (ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.date==today)
            ).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if not rows:
                await q.answer("امروز آماری نداریم.", show_alert=True); return
            lines=[]
            for i,r in enumerate(rows, start=1):
                u = s.get(User, r.target_user_id)
                name = u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                lines.append(f"{fa_digits(i)}) {name} — {fa_digits(r.reply_count)} ریپلای")
        await q.message.reply_text("\n".join(lines))
        return

    if q.data == "ui:ship":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            tz = ZoneInfo(g.timezone or DEFAULT_TZ); today = dt.datetime.now(tz).date()
            last = s.execute(select(ShipHistory).where(
                (ShipHistory.chat_id==g.id) & (ShipHistory.date==today)
            ).order_by(ShipHistory.id.desc())).scalar_one_or_none()
        if not last:
            await q.answer("هنوز شیپ امشب ساخته نشده.", show_alert=True); return
        with SessionLocal() as s:
            m, f = s.get(User, last.male_user_id), s.get(User, last.female_user_id)
        await q.message.reply_text(f"💘 شیپ امشب: {(m.first_name or '@'+(m.username or ''))} × {(f.first_name or '@'+(f.username or ''))}")
        return

    # تگ (ماندگار)
    if q.data.startswith("ui:tag:"):
        kind = q.data.split(":")[2]
        if not q.message or not q.message.reply_to_message:
            await q.answer("برای تگ، روی پیام هدف ریپلای کن و دوباره بزن.", show_alert=True); return
        reply_to = q.message.reply_to_message.message_id
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            if kind=="girls":
                users = s.query(User).filter_by(chat_id=g.id, gender="female").all()
            elif kind=="boys":
                users = s.query(User).filter_by(chat_id=g.id, gender="male").all()
            else:
                users = s.query(User).filter_by(chat_id=g.id).all()
        if not users: await q.answer("کسی برای تگ موجود نیست.", show_alert=True); return
        mentions = [mention_of(u) for u in users]
        for pack in chunked(mentions, 4):
            try:
                await q.bot.send_message(q.message.chat.id, " ".join(pack),
                                         reply_to_message_id=reply_to,
                                         parse_mode=ParseMode.HTML,
                                         disable_web_page_preview=True)
                await asyncio.sleep(TAG_DELAY_SECONDS)
            except Exception as e:
                logging.info(f"Tag batch send failed: {e}")
        return

    # پرایوسی
    if q.data == "ui:privacy:me":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            u = s.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==q.from_user.id)).scalar_one_or_none()
        if not u:
            await q.answer("چیزی ذخیره نشده.", show_alert=True); return
        txt = f"👤 نام: {u.first_name or ''} @{u.username or ''}\nجنسیت: {u.gender}\nتولد (شمسی): {fmt_date_fa(u.birthday)}"
        await q.message.reply_text(txt)
        return

    if q.data == "ui:privacy:delme":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            u = s.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==q.from_user.id)).scalar_one_or_none()
            if not u:
                await q.answer("چیزی برای حذف نیست.", show_alert=True); return
            s.execute(Crush.__table__.delete().where((Crush.chat_id==g.id) & ((Crush.from_user_id==u.id) | (Crush.to_user_id==u.id))))
            s.execute(Relationship.__table__.delete().where((Relationship.chat_id==g.id) & ((Relationship.user_a_id==u.id) | (Relationship.user_b_id==u.id))))
            s.execute(ReplyStatDaily.__table__.delete().where((ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.target_user_id==u.id)))
            s.execute(User.__table__.delete().where((User.chat_id==g.id) & (User.id==u.id)))
            s.commit()
        await q.answer("✅ حذف شد.", show_alert=True); return

    # تایم‌زون
    if q.data == "tz:menu":
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                await q.answer("دسترسی نداری.", show_alert=True); return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Asia/Tehran", callback_data="tz:set:Asia/Tehran"),
             InlineKeyboardButton("Europe/Istanbul", callback_data="tz:set:Europe/Istanbul")],
            [InlineKeyboardButton("UTC", callback_data="tz:set:UTC")]
        ])
        try:
            await q.edit_message_reply_markup(reply_markup=kb)
        except Exception:
            await q.message.reply_text(footer("تایم‌زون:"), reply_markup=kb)
        return

    if q.data.startswith("tz:set:"):
        tzname = q.data.split(":",2)[2]
        try: ZoneInfo(tzname)
        except Exception: await q.answer("TZ نامعتبر.", show_alert=True); return
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                await q.answer("دسترسی نداری.", show_alert=True); return
            g = ensure_group(s, q.message.chat)
            g.timezone = tzname; s.commit()
        await q.answer(f"تایم‌زون = {tzname}", show_alert=True); return

    if q.data == "ui:expiry":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
        await q.answer(f"انقضا: {fmt_dt_fa(g.expires_at, g.timezone) if g.expires_at else 'نامشخص'}", show_alert=True); return

    # پاکسازی گروه
    if q.data.startswith("wipe:"):
        chat_id = int(q.data.split(":")[1])
        with SessionLocal() as s:
            if not is_group_admin(s, chat_id, q.from_user.id):
                await q.answer("فقط مدیر گروه/فروشنده/مالک.", show_alert=True); return
            s.execute(Crush.__table__.delete().where(Crush.chat_id==chat_id))
            s.execute(Relationship.__table__.delete().where(Relationship.chat_id==chat_id))
            s.execute(ReplyStatDaily.__table__.delete().where(ReplyStatDaily.chat_id==chat_id))
            s.execute(ShipHistory.__table__.delete().where(ShipHistory.chat_id==chat_id))
            s.execute(GroupAdmin.__table__.delete().where(GroupAdmin.chat_id==chat_id))
            s.execute(User.__table__.delete().where(User.chat_id==chat_id))
            s.commit()
        try:
            await q.edit_message_text(footer("🧹 کل داده‌های این گروه پاک شد."))
        except: ...
        return

    if q.data == "noop":
        await q.answer("لغو شد", show_alert=False); return

# ====== INSTALL/UNINSTALL REPORTS ======
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.my_chat_member: return
    chat = update.my_chat_member.chat
    new_status = update.my_chat_member.new_chat_member.status
    old_status = update.my_chat_member.old_chat_member.status
    if chat.type in ("group","supergroup"):
        with SessionLocal() as s:
            if new_status in ("member","administrator"):
                ensure_group(s, chat)
                try_send_owner(f"➕ ربات اضافه شد به گروه:\n• {chat.title}\n• chat_id: {chat.id}")
            elif new_status in ("left","kicked") and old_status in ("member","administrator"):
                try_send_owner(f"➖ ربات از گروه حذف شد:\n• {chat.title}\n• chat_id: {chat.id}")

# ====== JOBS ======
async def job_midnight(context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as s:
        groups = s.query(Group).all()
        for g in groups:
            if not group_active(g): continue
            tz = ZoneInfo(g.timezone or DEFAULT_TZ); today = dt.datetime.now(tz).date()
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
            # شیپ فقط بین مجردها
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
            tz = ZoneInfo(g.timezone or DEFAULT_TZ)
            jy, jm, jd = today_jalali(tz)
            # تولدها (بر مبنای جلالی)
            bdays = s.query(User).filter_by(chat_id=g.id).filter(User.birthday.isnot(None)).all()
            for u in bdays:
                um, ud = to_jalali_md(u.birthday)
                if um==jm and ud==jd:
                    try:
                        await context.bot.send_message(g.id, footer(f"🎉🎂 تولدت مبارک {(u.first_name or '@'+(u.username or ''))}! ({fmt_date_fa(u.birthday)})"))
                    except: ...
            # ماهگرد رابطه‌ها (جلالی)
            rels = s.query(Relationship).filter_by(chat_id=g.id).all()
            for r in rels:
                if not r.started_at: continue
                rm, rd = to_jalali_md(r.started_at)
                if rd==jd:  # هر ماه، همان روز
                    ua, ub = s.get(User, r.user_a_id), s.get(User, r.user_b_id)
                    try:
                        await context.bot.send_message(
                            g.id, footer(f"💞 ماهگرد {(ua.first_name or '@'+(ua.username or ''))} و {(ub.first_name or '@'+(ub.username or ''))} مبارک! ({fmt_date_fa(r.started_at)})")
                        )
                    except: ...

# ====== BOOT ======
async def _post_init(app: Application):
    try:
        info = await app.bot.get_webhook_info()
        if info.url:
            logging.info(f"Webhook was set to: {info.url} — deleting…")
        await app.bot.delete_webhook(drop_pending_updates=True)
        logging.info("Webhook deleted. Polling will receive ALL updates.")
    except Exception as e:
        logging.warning(f"delete_webhook failed: {e}")
    logging.info(f"PersianTools enabled: {HAS_PTOOLS}")

def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN env var is required.")
    app = Application.builder().token(TOKEN).post_init(_post_init).build()

    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_group_text))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_private_text))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    jq = app.job_queue
    if jq is None:
        logging.warning('JobQueue فعال نیست. نصب کن: pip install "python-telegram-bot[job-queue]==21.6"')
    else:
        jq.run_daily(job_morning, time=dt.time(6,0,0))   # 6 UTC
        jq.run_daily(job_midnight, time=dt.time(21,0,0)) # 21 UTC ~ حوالی آخر شب ایران

    logging.info("FazolBot running…")
    # allowed_updates صریح: کالبک‌ها تضمینی دریافت می‌شن
    app.run_polling(
        allowed_updates=["message","edited_message","callback_query","my_chat_member","chat_member","chat_join_request"],
        drop_pending_updates=True
    )

if __name__ == "__main__":
    main()
