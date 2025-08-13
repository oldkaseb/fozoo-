import os, logging, re, random, datetime as dt, asyncio
from typing import Optional, List, Tuple, Dict
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
from sqlalchemy import create_engine, Integer, BigInteger, String, DateTime, Date, Boolean, JSON, ForeignKey, Index

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application, MessageHandler, CallbackQueryHandler, ChatMemberHandler,
    CommandHandler, filters, ContextTypes
)

# -------------------- CONFIG --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DEFAULT_TZ = os.getenv("DEFAULT_TZ", "Asia/Tehran")

# -------------------- DB (Railway + SSL Patch + Validation) --------------------
import urllib.parse as _up
Base = declarative_base()

def _mask_url(u: str) -> str:
    try:
        parts = _up.urlsplit(u)
        if parts.username or parts.password:
            netloc = parts.hostname or ""
            if parts.port: netloc += f":{parts.port}"
            return _up.urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        pass
    return "<unparsable>"

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
        raise RuntimeError("DATABASE_URL یافت نشد و متغیرهای PGHOST/PGUSER/PGPASSWORD هم ست نیستند.")

db_url = raw_db_url
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
elif db_url.startswith("postgresql://"):
    db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
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

engine = create_engine(
    db_url,
    pool_pre_ping=True,
    future=True,
    connect_args={"sslmode": "require"},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# -------------------- MODELS --------------------
class Group(Base):
    __tablename__ = "groups"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)  # chat_id
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
    gender: Mapped[str] = mapped_column(String(8), default="unknown")  # male/female/unknown
    birthday: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)

class GroupAdmin(Base):
    __tablename__ = "group_admins"
    __table_args__ = (
        Index("ix_ga_unique", "chat_id", "tg_user_id", unique=True),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, index=True)

class Relationship(Base):
    __tablename__ = "relationships"
    __table_args__ = (
        Index("ix_rel_unique", "chat_id", "user_a_id", "user_b_id", unique=True),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    user_a_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user_b_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    started_at: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)

class Crush(Base):
    __tablename__ = "crushes"
    __table_args__ = (
        Index("ix_crush_unique", "chat_id", "from_user_id", "to_user_id", unique=True),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    from_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    to_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

class ReplyStatDaily(Base):
    __tablename__ = "reply_stat_daily"
    __table_args__ = (
        Index("ix_reply_chat_date_user", "chat_id", "date", "target_user_id", unique=True),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    date: Mapped[dt.date] = mapped_column(Date, index=True)
    target_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    reply_count: Mapped[int] = mapped_column(Integer, default=0)

class ShipHistory(Base):
    __tablename__ = "ship_history"
    __table_args__ = (
        Index("ix_ship_chat_date", "chat_id", "date"),
    )
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
    action: Mapped[str] = mapped_column(String(32))  # trial_start/extend/expire
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

# -------------------- HELPERS --------------------
def get_tz(group: Group) -> ZoneInfo:
    return ZoneInfo(group.timezone or DEFAULT_TZ)

def try_send_owner(text: str):
    from telegram import Bot
    if not TOKEN or not OWNER_ID: return
    try:
        Bot(TOKEN).send_message(OWNER_ID, text)
    except Exception as e:
        logging.info(f"Owner DM failed: {e}")

def ensure_group(session, chat) -> Group:
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

def is_global_admin(session, tg_user_id: int) -> bool:
    return tg_user_id == OWNER_ID or is_seller(session, tg_user_id)

def is_group_admin(session, chat_id: int, tg_user_id: int) -> bool:
    if is_global_admin(session, tg_user_id):
        return True
    row = session.execute(select(GroupAdmin).where(GroupAdmin.chat_id==chat_id, GroupAdmin.tg_user_id==tg_user_id)).scalar_one_or_none()
    return bool(row)

def group_active(g: Group) -> bool:
    return bool(g.expires_at and g.expires_at > dt.datetime.utcnow())

async def require_active_or_warn(update: Update, context: ContextTypes.DEFAULT_TYPE, session, g: Group) -> bool:
    if group_active(g):
        return True
    btn = InlineKeyboardMarkup([[InlineKeyboardButton("ارتباط با مالک", url=f"tg://user?id={OWNER_ID}")]])
    try:
        await update.effective_chat.send_message(
            "⌛️ اعتبار ربات در این گروه تمام شده. «فضول شارژ» را استفاده کنید یا با مالک در ارتباط باشید.",
            reply_markup=btn
        )
    except: pass
    return False

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

def chunked(lst: List, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def mention_of(u: 'User') -> str:
    if u.username:
        return f"@{u.username}"
    name = u.first_name or "کاربر"
    return f'<a href="tg://user?id={u.tg_user_id}">{name}</a>'

# -------------------- STATE --------------------
PENDING_REL: Dict[Tuple[int,int], Dict] = {}   # key=(chat_id, actor_tg_id) -> {"target_tg_id": int}
TAG_DELAY_SECONDS = 0.8

# -------------------- PATTERNS --------------------
PAT_GROUP = {
    "help": re.compile(r"^(?:فضول کمک|راهنما|کمک)$"),
    "menu": re.compile(r"^(?:منو|فضول منو)$"),
    "config": re.compile(r"^(?:پیکربندی فضول|فضول پیکربندی|فضول تنظیمات|تنظیمات فضول)$"),
    "admin_add": re.compile(r"^فضول ادمین(?: @?(\w+))?$"),
    "admin_del": re.compile(r"^حذف فضول ادمین(?: @?(\w+))?$"),

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
    "extend": re.compile(r"^تمدید (\-?\d+)\s+(\d+)$"),
    "set_tz": re.compile(r"^تنظیم زمان (\-?\d+)\s+([\w\/]+)$"),
    "add_seller": re.compile(r"^افزودن فروشنده (\d+)(?:\s+(.+))?$"),
    "del_seller": re.compile(r"^حذف فروشنده (\d+)$"),
    "list_sellers": re.compile(r"^لیست فروشنده‌ها$"),
}

# -------------------- UI BUILDERS --------------------
def build_group_menu(is_group_admin_flag: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("👤 ثبت جنسیت", callback_data="ui:gset")],
        [InlineKeyboardButton("🎂 ثبت تولد", callback_data="ui:bd:start")],
        [InlineKeyboardButton("💘 ثبت کراش (روی ریپلای)", callback_data="ui:crush:add"),
         InlineKeyboardButton("🗑️ حذف کراش (روی ریپلای)", callback_data="ui:crush:del")],
        [InlineKeyboardButton("💞 ثبت رابطه (روی ریپلای)", callback_data="ui:rel:add"),
         InlineKeyboardButton("🗑️ حذف رابطه (روی ریپلای)", callback_data="ui:rel:del")],
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

def build_config_panel(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡️ شارژ گروه", callback_data=f"ui:charge:open")],
        [InlineKeyboardButton("👥 مدیران گروه", callback_data="ga:list")],
        [InlineKeyboardButton("⏱ تنظیم تایم‌زون", callback_data="tz:menu")],
        [InlineKeyboardButton("ℹ️ مشاهده انقضا", callback_data="ui:expiry"),
         InlineKeyboardButton("🧹 پاکسازی گروه", callback_data=f"wipe:{chat_id}")],
    ])

def build_owner_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 گروه‌ها", callback_data="adm:groups")],
        [InlineKeyboardButton("🛍️ لیست فروشنده‌ها", callback_data="adm:sellers")],
        [InlineKeyboardButton("ℹ️ راهنما", callback_data="adm:help")],
    ])

# -------------------- COMMANDS --------------------
async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return await update.message.reply_text("سلام! در گروه «فضول منو» یا «فضول کمک» بزن.")
    with SessionLocal() as s:
        if update.effective_user.id == OWNER_ID:
            return await update.message.reply_text("👑 پنل مالک", reply_markup=build_owner_panel())
        elif is_seller(s, update.effective_user.id):
            return await update.message.reply_text("🛍️ پنل فروشنده", reply_markup=build_owner_panel())
        else:
            return await update.message.reply_text("سلام! برای مدیریت باید مالک/فروشنده باشی. برای راهنما «کمک» بفرست.")

# -------------------- GROUP HANDLER --------------------
async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group","supergroup") or not update.message or not update.message.text:
        return
    text = clean_text(update.message.text)

    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)
        is_gadmin = is_group_admin(s, g.id, update.effective_user.id)

    if PAT_GROUP["help"].match(text) or PAT_GROUP["menu"].match(text):
        await update.message.reply_text("🕹 منوی فضول:", reply_markup=build_group_menu(is_gadmin))
        return

    # پیکربندی فضول (پنل)
    if PAT_GROUP["config"].match(text):
        with SessionLocal() as s:
            if not is_group_admin(s, update.effective_chat.id, update.effective_user.id):
                return await update.message.reply_text("فقط مدیر گروه/فروشنده/مالک می‌تواند.")
        return await update.message.reply_text("⚙️ پیکربندی فضول:", reply_markup=build_config_panel(update.effective_chat.id))

    # افزودن/حذف ادمین گروه (متنی)
    if PAT_GROUP["admin_add"].match(text):
        m = PAT_GROUP["admin_add"].match(text)
        with SessionLocal() as s:
            if not is_group_admin(s, update.effective_chat.id, update.effective_user.id):
                return await update.message.reply_text("فقط مدیر گروه/فروشنده/مالک می‌تواند.")
            target = None
            if update.message.reply_to_message:
                target = update.message.reply_to_message.from_user
            elif m and m.group(1):
                uname = m.group(1)
                urow = s.execute(select(User).where(User.chat_id==update.effective_chat.id, User.username==uname)).scalar_one_or_none()
                if urow:
                    class _Tmp: id=urow.tg_user_id; first_name=urow.first_name; username=urow.username
                    target = _Tmp()
            if not target:
                return await update.message.reply_text("برای افزودن ادمین: روی پیام کاربر ریپلای کن یا بنویس «فضول ادمین @username».")
            # اضافه به گروه
            try:
                s.add(GroupAdmin(chat_id=update.effective_chat.id, tg_user_id=target.id)); s.commit()
                return await update.message.reply_text("✅ به‌عنوان ادمین گروه اضافه شد.")
            except Exception as e:
                return await update.message.reply_text("قبلاً ادمین بوده یا خطا رخ داد.")
    if PAT_GROUP["admin_del"].match(text):
        m = PAT_GROUP["admin_del"].match(text)
        with SessionLocal() as s:
            if not is_group_admin(s, update.effective_chat.id, update.effective_user.id):
                return await update.message.reply_text("فقط مدیر گروه/فروشنده/مالک می‌تواند.")
            target_id = None
            if update.message.reply_to_message:
                target_id = update.message.reply_to_message.from_user.id
            elif m and m.group(1):
                uname = m.group(1)
                urow = s.execute(select(User).where(User.chat_id==update.effective_chat.id, User.username==uname)).scalar_one_or_none()
                if urow: target_id = urow.tg_user_id
            if not target_id:
                return await update.message.reply_text("برای حذف ادمین: ریپلای بزن یا «حذف فضول ادمین @username».")
            # جلوگیری از حذف مالک/فروشنده
            if target_id == OWNER_ID or is_seller(s, target_id):
                return await update.message.reply_text("نمی‌توان مالک/فروشنده را حذف کرد.")
            s.execute(GroupAdmin.__table__.delete().where(
                (GroupAdmin.chat_id==update.effective_chat.id) & (GroupAdmin.tg_user_id==target_id)
            )); s.commit()
            return await update.message.reply_text("🗑️ ادمین گروه حذف شد.")

    # — بقیهٔ دستورات (قدیمی + با دکمه) —
    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)

        if m := PAT_GROUP["gender"].match(text):
            if not await require_active_or_warn(update, context, s, g): return
            gender = "female" if m.group(1)=="دختر" else "male"
            u = upsert_user(s, g.id, update.effective_user)
            u.gender = gender; s.commit()
            return await update.message.reply_text("ثبت شد ✅")

        if m := PAT_GROUP["birthday_set"].match(text):
            if not await require_active_or_warn(update, context, s, g): return
            try: d = dt.date.fromisoformat(m.group(1))
            except ValueError: return await update.message.reply_text("تاریخ نامعتبر. فرمت YYYY-MM-DD")
            u = upsert_user(s, g.id, update.effective_user)
            u.birthday = d; s.commit()
            return await update.message.reply_text("تولد ثبت شد 🎂")

        if PAT_GROUP["birthday_del"].match(text):
            u = upsert_user(s, g.id, update.effective_user)
            u.birthday = None; s.commit()
            return await update.message.reply_text("تولد حذف شد 🗑️")

        if m := PAT_GROUP["relation_set"].match(text):
            if not await require_active_or_warn(update, context, s, g): return
            target_username, date_str = m.group(1), m.group(2)
            try: started = dt.date.fromisoformat(date_str)
            except ValueError: return await update.message.reply_text("تاریخ نامعتبر.")
            me = upsert_user(s, g.id, update.effective_user)
            to = s.execute(select(User).where(User.chat_id==g.id, User.username==target_username)).scalar_one_or_none()
            if not to: return await update.message.reply_text("کاربر هدف پیدا نشد.")
            s.execute(Relationship.__table__.delete().where(
                (Relationship.chat_id==g.id) & (
                    ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==to.id)) |
                    ((Relationship.user_a_id==to.id) & (Relationship.user_b_id==me.id))
                )
            ))
            s.add(Relationship(chat_id=g.id, user_a_id=min(me.id,to.id), user_b_id=max(me.id,to.id), started_at=started)); s.commit()
            return await update.message.reply_text("رابطه ثبت شد 💞")

        if m := PAT_GROUP["relation_del"].match(text):
            target_username = m.group(1)
            me = upsert_user(s, g.id, update.effective_user)
            to = s.execute(select(User).where(User.chat_id==g.id, User.username==target_username)).scalar_one_or_none()
            if not to: return await update.message.reply_text("کاربر هدف پیدا نشد.")
            s.execute(Relationship.__table__.delete().where(
                (Relationship.chat_id==g.id) & (
                    ((Relationship.user_a_id==me.id) & (Relationship.user_b_id==to.id)) |
                    ((Relationship.user_a_id==to.id) & (Relationship.user_b_id==me.id))
                )
            )); s.commit()
            return await update.message.reply_text("رابطه حذف شد 🗑️")

        if PAT_GROUP["crush_add"].match(text):
            if not update.message.reply_to_message:
                return await update.message.reply_text("روی پیام طرف ریپلای کن یا از دکمه منو استفاده کن.")
            if not await require_active_or_warn(update, context, s, g): return
            me = upsert_user(s, g.id, update.effective_user)
            to = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            if me.id == to.id: return await update.message.reply_text("روی خودت نمی‌شه 😅")
            try:
                s.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=to.id)); s.commit()
                return await update.message.reply_text("کراش ثبت شد 💘")
            except Exception:
                return await update.message.reply_text("از قبل ثبت شده بود.")

        if PAT_GROUP["crush_del"].match(text):
            if not update.message.reply_to_message:
                return await update.message.reply_text("روی پیام طرف ریپلای کن یا از دکمه منو استفاده کن.")
            me = upsert_user(s, g.id, update.effective_user)
            to = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            s.execute(Crush.__table__.delete().where(
                (Crush.chat_id==g.id) & (Crush.from_user_id==me.id) & (Crush.to_user_id==to.id)
            )); s.commit()
            return await update.message.reply_text("کراش حذف شد 🗑️")

        if PAT_GROUP["popular_today"].match(text):
            tz = get_tz(g); today = dt.datetime.now(tz).date()
            rows = s.execute(select(ReplyStatDaily).where(
                (ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.date==today)
            ).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if not rows: return await update.message.reply_text("امروز هنوز آماری نداریم.")
            lines=[]
            for i,r in enumerate(rows, start=1):
                u = s.get(User, r.target_user_id)
                name = u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                lines.append(f"{i}) {name} — {r.reply_count} ریپلای")
            return await update.message.reply_text("👑 محبوب‌های امروز:\n" + "\n".join(lines))

        if PAT_GROUP["ship_tonight"].match(text):
            tz = get_tz(g); today = dt.datetime.now(tz).date()
            last = s.execute(select(ShipHistory).where(
                (ShipHistory.chat_id==g.id) & (ShipHistory.date==today)
            ).order_by(ShipHistory.id.desc())).scalar_one_or_none()
            if not last: return await update.message.reply_text("هنوز شیپ امشب ساخته نشده. آخر شب منتشر می‌شه 💫")
            m, f = s.get(User, last.male_user_id), s.get(User, last.female_user_id)
            return await update.message.reply_text(
                f"💘 شیپِ امشب: {(m.first_name or '@'+(m.username or ''))} × {(f.first_name or '@'+(f.username or ''))}"
            )

        if PAT_GROUP["expiry"].match(text):
            return await update.message.reply_text(f"⏳ اعتبار این گروه تا: {g.expires_at} UTC")

        if PAT_GROUP["charge"].match(text):
            with SessionLocal() as s2:
                if not is_group_admin(s2, g.id, update.effective_user.id):
                    return await update.message.reply_text("دسترسی نداری.")
            chat_id = update.effective_chat.id
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{chat_id}:30"),
                InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{chat_id}:90"),
                InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{chat_id}:180"),
            ]])
            return await update.message.reply_text("⌁ پنل شارژ گروه:", reply_markup=kb)

        if PAT_GROUP["tag_girls"].match(text) or PAT_GROUP["tag_boys"].match(text) or PAT_GROUP["tag_all"].match(text):
            if not update.message.reply_to_message:
                return await update.message.reply_text("برای تگ کردن، روی یک پیام ریپلای کن.")
            reply_to = update.message.reply_to_message.message_id
            with SessionLocal() as s2:
                if PAT_GROUP["tag_girls"].match(text):
                    users = s2.query(User).filter_by(chat_id=g.id, gender="female").all()
                    header = "تگ دخترها:"
                elif PAT_GROUP["tag_boys"].match(text):
                    users = s2.query(User).filter_by(chat_id=g.id, gender="male").all()
                    header = "تگ پسرها:"
                else:
                    users = s2.query(User).filter_by(chat_id=g.id).all()
                    header = "تگ همه:"
            if not users: return await update.message.reply_text("کسی برای تگ پیدا نشد.")
            await update.message.reply_text(header, reply_to_message_id=reply_to)
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
                if not u: return await update.message.reply_text("اطلاعاتی از شما نداریم.")
                s2.execute(Crush.__table__.delete().where((Crush.chat_id==g.id) & ((Crush.from_user_id==u.id) | (Crush.to_user_id==u.id))))
                s2.execute(Relationship.__table__.delete().where((Relationship.chat_id==g.id) & ((Relationship.user_a_id==u.id) | (Relationship.user_b_id==u.id))))
                s2.execute(ReplyStatDaily.__table__.delete().where((ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.target_user_id==u.id)))
                s2.execute(User.__table__.delete().where((User.chat_id==g.id) & (User.id==u.id)))
                s2.commit()
            return await update.message.reply_text("✅ تمام داده‌های شما در این گروه حذف شد.")

        if PAT_GROUP["privacy_info"].match(text):
            me_id = update.effective_user.id
            with SessionLocal() as s2:
                u = s2.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==me_id)).scalar_one_or_none()
                if not u: return await update.message.reply_text("چیزی از شما ذخیره نشده.")
                info = f"👤 نام: {u.first_name or ''} @{u.username or ''}\nجنسیت: {u.gender}\nتولد: {u.birthday or '-'}"
            return await update.message.reply_text(info)

        if PAT_GROUP["wipe_group"].match(text):
            with SessionLocal() as s2:
                if not is_group_admin(s2, g.id, update.effective_user.id):
                    return await update.message.reply_text("فقط مدیر گروه/فروشنده/مالک.")
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🧹 تایید پاکسازی", callback_data=f"wipe:{g.id}"),
                                        InlineKeyboardButton("انصراف", callback_data="noop")]])
            return await update.message.reply_text("⚠️ مطمئنی کل داده‌های گروه حذف شود؟", reply_markup=kb)

    # شمارش ریپلای‌ها برای آمار (بی‌صدا)
    if update.message.reply_to_message:
        with SessionLocal() as s:
            g = ensure_group(s, update.effective_chat)
            tz = get_tz(g); today = dt.datetime.now(tz).date()
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

# -------------------- PRIVATE (OWNER/SELLER) --------------------
async def on_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private" or not update.message or not update.message.text:
        return
    text = clean_text(update.message.text)
    with SessionLocal() as s:
        uid = update.effective_user.id
        if uid != OWNER_ID and not is_seller(s, uid):
            return await update.message.reply_text("برای مدیریت باید مالک/فروشنده باشی. «/start» یا «کمک» بزن.")

        if PAT_DM["panel"].match(text):
            return await update.message.reply_text("🛠 پنل مدیریت:", reply_markup=build_owner_panel())

        if PAT_DM["groups"].match(text):
            groups = s.query(Group).order_by(Group.id.asc()).all()
            if not groups: return await update.message.reply_text("گروهی ثبت نشده.")
            lines = []
            now = dt.datetime.utcnow()
            for g in groups[:100]:
                status = "فعال ✅" if g.expires_at and g.expires_at > now else "منقضی ⛔️"
                lines.append(f"{g.title} | chat_id: {g.id} | تا: {g.expires_at} UTC | {status} | TZ: {g.timezone or '-'}")
            return await update.message.reply_text("\n".join(lines))

        if m := PAT_DM["extend"].match(text):
            chat_id = int(m.group(1)); days = int(m.group(2))
            g = s.get(Group, chat_id)
            if not g: return await update.message.reply_text("گروه پیدا نشد.")
            base = g.expires_at if g.expires_at and g.expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
            g.expires_at = base + dt.timedelta(days=days)
            s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=uid, action="extend", amount_days=days)); s.commit()
            return await update.message.reply_text(f"✅ تمدید شد تا {g.expires_at} UTC")

        if m := PAT_DM["set_tz"].match(text):
            chat_id = int(m.group(1)); tzname = m.group(2)
            g = s.get(Group, chat_id)
            if not g: return await update.message.reply_text("گروه پیدا نشد.")
            try:
                ZoneInfo(tzname)
            except Exception:
                return await update.message.reply_text("نام منطقه زمانی نامعتبر است. مثال: Asia/Tehran")
            g.timezone = tzname; s.commit()
            return await update.message.reply_text(f"⏱ تایم‌زون گروه تنظیم شد: {tzname}")

        if PAT_DM["list_sellers"].match(text):
            sellers = s.query(Seller).order_by(Seller.id.asc()).all()
            if not sellers: return await update.message.reply_text("هیچ فروشنده‌ای ثبت نشده.")
            lines = [f"{x.id}) {x.tg_user_id} | {'فعال' if x.is_active else 'غیرفعال'} | {x.note or ''}" for x in sellers]
            return await update.message.reply_text("\n".join(lines))

        if m := PAT_DM["add_seller"].match(text):
            if uid != OWNER_ID:
                return await update.message.reply_text("فقط مالک می‌تواند فروشنده اضافه کند.")
            seller_id = int(m.group(1)); note = m.group(2)
            ex = s.execute(select(Seller).where(Seller.tg_user_id==seller_id)).scalar_one_or_none()
            if ex:
                ex.is_active = True
                if note: ex.note = note
            else:
                s.add(Seller(tg_user_id=seller_id, note=note, is_active=True))
            s.commit()
            return await update.message.reply_text("✅ فروشنده اضافه/فعال شد.")

        if m := PAT_DM["del_seller"].match(text):
            if uid != OWNER_ID:
                return await update.message.reply_text("فقط مالک می‌تواند فروشنده حذف کند.")
            seller_id = int(m.group(1))
            ex = s.execute(select(Seller).where(Seller.tg_user_id==seller_id)).scalar_one_or_none()
            if not ex: return await update.message.reply_text("فروشنده پیدا نشد.")
            ex.is_active = False; s.commit()
            return await update.message.reply_text("🗑️ فروشنده غیرفعال شد.")

# -------------------- CALLBACKS --------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.data: return
    await q.answer(cache_time=0)

    # --- Config open ---
    if q.data == "cfg:open":
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                return await q.answer("فقط مدیر گروه/فروشنده/مالک.", show_alert=True)
        return await q.edit_message_reply_markup(reply_markup=build_config_panel(q.message.chat.id))

    # --- Charge ---
    if q.data.startswith("chg:"):
        _, chat_id_str, days_str = q.data.split(":")
        target_chat_id = int(chat_id_str); days = int(days_str)
        with SessionLocal() as s:
            if not is_group_admin(s, target_chat_id, q.from_user.id):
                return await q.answer("دسترسی نداری.", show_alert=True)
            g = s.get(Group, target_chat_id) or ensure_group(s, q.message.chat)
            base = g.expires_at if g.expires_at and g.expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
            g.expires_at = base + dt.timedelta(days=days)
            s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=q.from_user.id, action="extend", amount_days=days))
            s.commit()
        try:
            await q.edit_message_text(f"✅ تمدید شد تا {g.expires_at} UTC")
        except:
            await q.answer(f"تمدید شد تا {g.expires_at} UTC", show_alert=True)
        return

    if q.data == "ui:charge:open":
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                return await q.answer("فقط مدیر گروه/فروشنده/مالک.", show_alert=True)
        chat_id = q.message.chat.id
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("۳۰ روز", callback_data=f"chg:{chat_id}:30"),
            InlineKeyboardButton("۹۰ روز", callback_data=f"chg:{chat_id}:90"),
            InlineKeyboardButton("۱۸۰ روز", callback_data=f"chg:{chat_id}:180"),
        ]])
        return await q.edit_message_reply_markup(reply_markup=kb)

    # --- Owner panel quicks ---
    if q.data == "adm:groups":
        with SessionLocal() as s:
            if q.from_user.id != OWNER_ID and not is_seller(s, q.from_user.id):
                return await q.answer("دسترسی نداری.", show_alert=True)
            groups = s.query(Group).order_by(Group.id.asc()).all()
        if not groups:
            return await q.message.reply_text("گروهی ثبت نشده.")
        now = dt.datetime.utcnow()
        lines = [f"{g.title} | {g.id} | {'فعال ✅' if g.expires_at and g.expires_at>now else 'منقضی ⛔️'} | {g.expires_at} UTC" for g in groups[:100]]
        return await q.message.reply_text("\n".join(lines))

    if q.data == "adm:sellers":
        with SessionLocal() as s:
            if q.from_user.id != OWNER_ID and not is_seller(s, q.from_user.id):
                return await q.answer("دسترسی نداری.", show_alert=True)
            sellers = s.query(Seller).order_by(Seller.id.asc()).all()
        if not sellers:
            return await q.message.reply_text("هیچ فروشنده‌ای ثبت نشده.")
        lines = [f"{x.id}) {x.tg_user_id} | {'فعال' if x.is_active else 'غیرفعال'} | {x.note or ''}" for x in sellers]
        return await q.message.reply_text("\n".join(lines))

    if q.data == "adm:help":
        return await q.message.reply_text("دستورات: گروه‌ها | تمدید <chat_id> <days> | تنظیم زمان <chat_id> <Area/City> | لیست فروشنده‌ها | افزودن/حذف فروشنده")

    # --- Group menu actions ---
    if q.data == "ui:gset":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("👧 دختر", callback_data="gset:female"),
             InlineKeyboardButton("👦 پسر", callback_data="gset:male")]
        ])
        return await q.edit_message_reply_markup(reply_markup=kb)

    if q.data.startswith("gset:"):
        gender = q.data.split(":")[1]
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            u = upsert_user(s, g.id, q.from_user)
            u.gender = "female" if gender=="female" else "male"
            s.commit()
        return await q.answer("ثبت شد ✅", show_alert=False)

    # --- Birthday wizard ---
    def _year_page(start_year: int) -> InlineKeyboardMarkup:
        years = [start_year+i for i in range(-8, 9)]
        rows = []
        for i in range(0, len(years), 3):
            chunk = years[i:i+3]
            rows.append([InlineKeyboardButton(str(y), callback_data=f"bd:y:{y}") for y in chunk])
        rows.append([InlineKeyboardButton("⏪", callback_data=f"bd:yp:{years[0]-17}"),
                     InlineKeyboardButton("انصراف", callback_data="bd:cancel"),
                     InlineKeyboardButton("⏩", callback_data=f"bd:yn:{years[-1]+17}")])
        return InlineKeyboardMarkup(rows)

    def _month_kb(year: int) -> InlineKeyboardMarkup:
        rows = []
        for r in (1,4,7,10):
            rows.append([InlineKeyboardButton(f"{m:02d}", callback_data=f"bd:m:{year}:{m}") for m in range(r, r+3)])
        rows.append([InlineKeyboardButton("↩️ سال", callback_data="ui:bd:start")])
        return InlineKeyboardMarkup(rows)

    def _days_kb(year:int, month:int) -> InlineKeyboardMarkup:
        import calendar
        nd = calendar.monthrange(year, month)[1]
        rows = []
        row=[]
        for d in range(1, nd+1):
            row.append(InlineKeyboardButton(f"{d:02d}", callback_data=f"bd:d:{year}:{month}:{d}"))
            if len(row)==7: rows.append(row); row=[]
        if row: rows.append(row)
        rows.append([InlineKeyboardButton("↩️ ماه", callback_data=f"bd:m:{year}:{month}")])
        return InlineKeyboardMarkup(rows)

    if q.data in ("ui:bd:start","bd:start"):
        return await q.edit_message_reply_markup(reply_markup=_year_page(dt.datetime.utcnow().year-5))

    if q.data.startswith("bd:yp:") or q.data.startswith("bd:yn:"):
        base = int(q.data.split(":")[2])
        return await q.edit_message_reply_markup(reply_markup=_year_page(base))

    if q.data.startswith("bd:y:"):
        y = int(q.data.split(":")[2])
        return await q.edit_message_reply_markup(reply_markup=_month_kb(y))

    if q.data.startswith("bd:m:"):
        parts = q.data.split(":")
        if len(parts)==4:
            y = int(parts[2]); m = int(parts[3])
            return await q.edit_message_reply_markup(reply_markup=_days_kb(y,m))
        return await q.edit_message_reply_markup(reply_markup=_month_kb(int(parts[2])))

    if q.data.startswith("bd:d:"):
        _,_, y,m,d = q.data.split(":")
        Y,M,D = int(y), int(m), int(d)
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            u = upsert_user(s, g.id, q.from_user)
            u.birthday = dt.date(Y,M,D); s.commit()
        await q.answer("تولد ثبت شد 🎂", show_alert=True)
        try:
            await q.edit_message_text(f"🎂 تاریخ تولد شما: {Y:04d}-{M:02d}-{D:02d}")
        except: pass
        return

    if q.data == "bd:cancel":
        return await q.answer("لغو شد", show_alert=False)

    # --- Crush via button (requires reply) ---
    if q.data == "ui:crush:add":
        if not q.message or not q.message.reply_to_message:
            return await q.answer("روی پیام طرف ریپلای کن و دوباره بزن.", show_alert=True)
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            if not group_active(g):
                return await q.answer("اعتبار گروه تمام شده.", show_alert=True)
            me = upsert_user(s, g.id, q.from_user)
            to = upsert_user(s, g.id, q.message.reply_to_message.from_user)
            if me.id == to.id: return await q.answer("روی خودت نمی‌شه 😅", show_alert=True)
            try:
                s.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=to.id)); s.commit()
                return await q.answer("کراش ثبت شد 💘", show_alert=True)
            except Exception:
                return await q.answer("قبلاً ثبت شده.", show_alert=True)

    if q.data == "ui:crush:del":
        if not q.message or not q.message.reply_to_message:
            return await q.answer("روی پیام طرف ریپلای کن و دوباره بزن.", show_alert=True)
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            me = upsert_user(s, g.id, q.from_user)
            to = upsert_user(s, g.id, q.message.reply_to_message.from_user)
            s.execute(Crush.__table__.delete().where(
                (Crush.chat_id==g.id) & (Crush.from_user_id==me.id) & (Crush.to_user_id==to.id)
            )); s.commit()
        return await q.answer("کراش حذف شد 🗑️", show_alert=True)

    # --- Relationship via button (requires reply + date picker) ---
    if q.data == "ui:rel:add":
        if not q.message or not q.message.reply_to_message:
            return await q.answer("روی پیام طرف ریپلای کن و دوباره بزن.", show_alert=True)
        target_tg_id = q.message.reply_to_message.from_user.id
        PENDING_REL[(q.message.chat.id, q.from_user.id)] = {"target_tg_id": target_tg_id}
        # reuse birthday wizard for date selection:
        return await on_callback(type("obj",(object,),{"data":"ui:bd:start","message":q.message,"from_user":q.from_user,"answer":q.answer,"edit_message_reply_markup":q.edit_message_reply_markup})(), context)

    if q.data == "ui:rel:del":
        if not q.message or not q.message.reply_to_message:
            return await q.answer("روی پیام طرف ریپلای کن و دوباره بزن.", show_alert=True)
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
        return await q.answer("رابطه حذف شد 🗑️", show_alert=True)

    # complete relation after date chosen
    if q.data.startswith("bd:d:") and (q.message.chat.id, q.from_user.id) in PENDING_REL:
        _,_, y,m,d = q.data.split(":")
        Y,M,D = int(y), int(m), int(d)
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
            await q.edit_message_text(f"💞 رابطه ثبت شد از تاریخ {Y:04d}-{M:02d}-{D:02d}")
        except: pass
        return

    # --- Popular & Ship shortcuts ---
    if q.data == "ui:pop":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            tz = get_tz(g); today = dt.datetime.now(tz).date()
            rows = s.execute(select(ReplyStatDaily).where(
                (ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.date==today)
            ).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if not rows: return await q.answer("امروز آماری نداریم.", show_alert=True)
            lines=[]
            for i,r in enumerate(rows, start=1):
                u = s.get(User, r.target_user_id)
                name = u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                lines.append(f"{i}) {name} — {r.reply_count} ریپلای")
        try:
            await q.message.reply_text("👑 محبوب‌های امروز:\n" + "\n".join(lines))
        except: pass
        return

    if q.data == "ui:ship":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            tz = get_tz(g); today = dt.datetime.now(tz).date()
            last = s.execute(select(ShipHistory).where(
                (ShipHistory.chat_id==g.id) & (ShipHistory.date==today)
            ).order_by(ShipHistory.id.desc())).scalar_one_or_none()
        if not last:
            return await q.answer("هنوز شیپ امشب ساخته نشده.", show_alert=True)
        with SessionLocal() as s:
            m, f = s.get(User, last.male_user_id), s.get(User, last.female_user_id)
        try:
            await q.message.reply_text(f"💘 شیپ امشب: {(m.first_name or '@'+(m.username or ''))} × {(f.first_name or '@'+(f.username or ''))}")
        except: pass
        return

    # --- Tags via button (requires reply) ---
    if q.data.startswith("ui:tag:"):
        kind = q.data.split(":")[2]
        if not q.message or not q.message.reply_to_message:
            return await q.answer("برای تگ، روی پیام هدف ریپلای کن و دوباره بزن.", show_alert=True)
        reply_to = q.message.reply_to_message.message_id
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            if kind=="girls":
                users = s.query(User).filter_by(chat_id=g.id, gender="female").all()
            elif kind=="boys":
                users = s.query(User).filter_by(chat_id=g.id, gender="male").all()
            else:
                users = s.query(User).filter_by(chat_id=g.id).all()
        if not users: return await q.answer("کسی برای تگ موجود نیست.", show_alert=True)
        mentions = [mention_of(u) for u in users]
        await q.answer("در حال تگ…", show_alert=False)
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

    # --- Privacy buttons ---
    if q.data == "ui:privacy:me":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            u = s.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==q.from_user.id)).scalar_one_or_none()
        if not u: return await q.answer("چیزی ذخیره نشده.", show_alert=True)
        txt = f"👤 نام: {u.first_name or ''} @{u.username or ''}\nجنسیت: {u.gender}\nتولد: {u.birthday or '-'}"
        return await q.message.reply_text(txt)

    if q.data == "ui:privacy:delme":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
            u = s.execute(select(User).where(User.chat_id==g.id, User.tg_user_id==q.from_user.id)).scalar_one_or_none()
            if not u: return await q.answer("چیزی برای حذف نیست.", show_alert=True)
            s.execute(Crush.__table__.delete().where((Crush.chat_id==g.id) & ((Crush.from_user_id==u.id) | (Crush.to_user_id==u.id))))
            s.execute(Relationship.__table__.delete().where((Relationship.chat_id==g.id) & ((Relationship.user_a_id==u.id) | (Relationship.user_b_id==u.id))))
            s.execute(ReplyStatDaily.__table__.delete().where((ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.target_user_id==u.id)))
            s.execute(User.__table__.delete().where((User.chat_id==g.id) & (User.id==u.id)))
            s.commit()
        return await q.answer("✅ حذف شد.", show_alert=True)

    # --- Group Admin management via buttons ---
    if q.data == "ga:list":
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                return await q.answer("فقط مدیر گروه/فروشنده/مالک.", show_alert=True)
            gas = s.query(GroupAdmin).filter_by(chat_id=q.message.chat.id).all()
            # متن لیست
            if not gas:
                txt = "هیچ ادمینی ثبت نشده.\nبرای افزودن: روی پیام کاربر ریپلای کن و از منو «افزودن مدیر» را بزن."
            else:
                txt = "👥 مدیران گروه:\n" + "\n".join([f"• {ga.tg_user_id}" for ga in gas])
        # دکمه‌ها
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ افزودن مدیر (روی ریپلای)", callback_data="ga:add")],
            [InlineKeyboardButton("🔄 بروزرسانی لیست", callback_data="ga:list")]
        ])
        try:
            await q.message.reply_text(txt, reply_markup=kb)
        except: pass
        return

    if q.data == "ga:add":
        if not q.message or not q.message.reply_to_message:
            return await q.answer("روی پیام کاربر ریپلای کن و دوباره بزن.", show_alert=True)
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                return await q.answer("دسترسی نداری.", show_alert=True)
            target = q.message.reply_to_message.from_user
            try:
                s.add(GroupAdmin(chat_id=q.message.chat.id, tg_user_id=target.id)); s.commit()
                return await q.answer("✅ به‌عنوان ادمین گروه اضافه شد.", show_alert=True)
            except Exception:
                return await q.answer("قبلاً ادمین بوده یا خطا رخ داد.", show_alert=True)

    # --- Timezone quick menu ---
    if q.data == "tz:menu":
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                return await q.answer("دسترسی نداری.", show_alert=True)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Asia/Tehran", callback_data="tz:set:Asia/Tehran"),
             InlineKeyboardButton("Europe/Istanbul", callback_data="tz:set:Europe/Istanbul")],
            [InlineKeyboardButton("UTC", callback_data="tz:set:UTC")]
        ])
        return await q.edit_message_reply_markup(reply_markup=kb)

    if q.data.startswith("tz:set:"):
        tzname = q.data.split(":",2)[2]
        try: ZoneInfo(tzname)
        except Exception: return await q.answer("TZ نامعتبر.", show_alert=True)
        with SessionLocal() as s:
            if not is_group_admin(s, q.message.chat.id, q.from_user.id):
                return await q.answer("دسترسی نداری.", show_alert=True)
            g = ensure_group(s, q.message.chat)
            g.timezone = tzname; s.commit()
        return await q.answer(f"تایم‌زون = {tzname}", show_alert=True)

    # --- expiry quick ---
    if q.data == "ui:expiry":
        with SessionLocal() as s:
            g = ensure_group(s, q.message.chat)
        return await q.answer(f"انقضا: {g.expires_at} UTC", show_alert=True)

    # --- Wipe group ---
    if q.data.startswith("wipe:"):
        chat_id = int(q.data.split(":")[1])
        with SessionLocal() as s:
            if not is_group_admin(s, chat_id, q.from_user.id):
                return await q.answer("فقط مدیر گروه/فروشنده/مالک.", show_alert=True)
            s.execute(Crush.__table__.delete().where(Crush.chat_id==chat_id))
            s.execute(Relationship.__table__.delete().where(Relationship.chat_id==chat_id))
            s.execute(ReplyStatDaily.__table__.delete().where(ReplyStatDaily.chat_id==chat_id))
            s.execute(ShipHistory.__table__.delete().where(ShipHistory.chat_id==chat_id))
            s.execute(GroupAdmin.__table__.delete().where(GroupAdmin.chat_id==chat_id))
            s.execute(User.__table__.delete().where(User.chat_id==chat_id))
            s.commit()
        try:
            await q.edit_message_text("🧹 کل داده‌های این گروه پاک شد.")
        except: pass
        return

    if q.data == "noop":
        return await q.answer("لغو شد", show_alert=False)

# -------------------- INSTALL/UNINSTALL REPORTS --------------------
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

# -------------------- SCHEDULED JOBS --------------------
async def job_midnight(context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as s:
        groups = s.query(Group).all()
        for g in groups:
            if not group_active(g): continue
            tz = get_tz(g); today = dt.datetime.now(tz).date()
            top = s.execute(select(ReplyStatDaily).where(
                (ReplyStatDaily.chat_id==g.id) & (ReplyStatDaily.date==today)
            ).order_by(ReplyStatDaily.reply_count.desc()).limit(3)).scalars().all()
            if top:
                lines=[]
                for i,r in enumerate(top, start=1):
                    u = s.get(User, r.target_user_id)
                    name = u.first_name or (u.username and f"@{u.username}") or str(u.tg_user_id)
                    lines.append(f"{i}) {name} — {r.reply_count} ریپلای")
                try:
                    await context.bot.send_message(g.id, "🌙 گزارش آخر شب — محبوب‌های امروز:\n" + "\n".join(lines))
                except: pass
            # ship nightly
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
                    await context.bot.send_message(
                        g.id, f"💘 شیپِ امشب: {(m.first_name or '@'+(m.username or ''))} × {(f.first_name or '@'+(f.username or ''))}"
                    )
                except: pass

async def job_morning(context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as s:
        groups = s.query(Group).all()
        for g in groups:
            if not group_active(g): continue
            tz = get_tz(g); today = dt.datetime.now(tz).date()
            bdays = s.query(User).filter_by(chat_id=g.id).filter(User.birthday.isnot(None)).all()
            for u in bdays:
                if u.birthday.month==today.month and u.birthday.day==today.day:
                    try:
                        await context.bot.send_message(g.id, f"🎉🎂 تولدت مبارک {(u.first_name or '@'+(u.username or ''))}!")
                    except: pass
            rels = s.query(Relationship).filter_by(chat_id=g.id).all()
            for r in rels:
                if r.started_at and r.started_at.day==today.day:
                    ua, ub = s.get(User, r.user_a_id), s.get(User, r.user_b_id)
                    try:
                        await context.bot.send_message(
                            g.id, f"💞 ماهگرد {(ua.first_name or '@'+(ua.username or ''))} و {(ub.first_name or '@'+(ub.username or ''))} مبارک!"
                        )
                    except: pass

# -------------------- BOOT --------------------
def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN env var is required.")
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_group_text))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_private_text))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    jq = app.job_queue
    if jq is None:
        logging.warning('JobQueue فعال نیست. نصب کن: pip install "python-telegram-bot[job-queue]==21.6"')
    else:
        jq.run_daily(job_morning, time=dt.time(6,0,0))
        jq.run_daily(job_midnight, time=dt.time(21,0,0))

    logging.info("FazolBot FULL (single-file) is running…")
    app.run_polling()

if __name__ == "__main__":
    main()
