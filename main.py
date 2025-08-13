import os, logging, re, random, datetime as dt
from typing import Optional, List
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
from sqlalchemy import create_engine, Integer, BigInteger, String, DateTime, Date, Boolean, JSON, ForeignKey

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application, MessageHandler, CallbackQueryHandler, ChatMemberHandler,
    filters, ContextTypes
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

# 1) Read DATABASE_URL or build from PG* vars
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

# 2) driver & sslmode
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
elif db_url.startswith("postgresql://"):
    db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
if "sslmode=" not in db_url:
    sep = "&" if "?" in db_url else "?"
    db_url = f"{db_url}{sep}sslmode=require"

# 3) validate host
try:
    parsed = _up.urlsplit(db_url)
    host_ok = bool(parsed.hostname)
    logging.info(f"DB host: {parsed.hostname}, port: {parsed.port}, db: {parsed.path}")
except Exception:
    host_ok = False
    logging.info("DB URL parsed = <error>")
if not host_ok:
    masked = _mask_url(raw_db_url)
    raise RuntimeError(
        "DATABASE_URL نامعتبر است (هاست ندارد). مقدار فعلی (بدون پسورد): "
        f"{masked}\n"
        "در Railway از Postgres → Connect → External Connection String کپی کن و با کلید DATABASE_URL در سرویس ربات ست کن."
    )

# 4) engine
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
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(128))
    last_name: Mapped[Optional[str]] = mapped_column(String(128))
    username: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    gender: Mapped[str] = mapped_column(String(8), default="unknown")  # male/female/unknown
    birthday: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)

class Relationship(Base):
    __tablename__ = "relationships"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    user_a_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user_b_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    started_at: Mapped[Optional[dt.date]] = mapped_column(Date, nullable=True)

class Crush(Base):
    __tablename__ = "crushes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    from_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    to_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

class ReplyStatDaily(Base):
    __tablename__ = "reply_stat_daily"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    date: Mapped[dt.date] = mapped_column(Date, index=True)
    target_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    reply_count: Mapped[int] = mapped_column(Integer, default=0)

class ShipHistory(Base):
    __tablename__ = "ship_history"
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

def is_admin(session, tg_user_id: int) -> bool:
    return tg_user_id == OWNER_ID or is_seller(session, tg_user_id)

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

# -------------------- PATTERNS --------------------
PAT_GROUP = {
    "help": re.compile(r"^(?:فضول کمک|راهنما|کمک)$"),
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
}

PAT_DM = {
    "panel": re.compile(r"^(?:پنل|مدیریت)$"),
    "groups": re.compile(r"^گروه‌ها$"),
    "extend": re.compile(r"^تمدید (\-?\d+)\s+(\d+)$"),  # تمدید <chat_id> <days>
    "set_tz": re.compile(r"^تنظیم زمان (\-?\d+)\s+([\w\/]+)$"),  # تنظیم زمان <chat_id> <Area/City>
    "add_seller": re.compile(r"^افزودن فروشنده (\d+)(?:\s+(.+))?$"),
    "del_seller": re.compile(r"^حذف فروشنده (\d+)$"),
    "list_sellers": re.compile(r"^لیست فروشنده‌ها$"),
    "help": re.compile(r"^کمک$"),
}

# -------------------- GROUP HANDLER --------------------
async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group","supergroup") or not update.message or not update.message.text:
        return
    text = clean_text(update.message.text)

    if PAT_GROUP["help"].match(text):
        return await update.message.reply_text(
            "🕵️‍♂️ دستورات گروه (بدون /):\n"
            "• ثبت جنسیت دختر|پسر\n"
            "• ثبت تولد YYYY-MM-DD | حذف تولد\n"
            "• ثبت رابطه @username YYYY-MM-DD | حذف رابطه @username\n"
            "• ثبت کراش (با ریپلای) | حذف کراش (با ریپلای)\n"
            "• محبوب امروز | شیپ امشب | فضول انقضا | فضول شارژ\n"
            "• تگ دخترها | تگ پسرها | تگ همه  (با ریپلای روی یک پیام)\n"
            "ℹ️ فقط به همین دستورات پاسخ می‌دم."
        )

    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)

        # ثبت جنسیت
        if m := PAT_GROUP["gender"].match(text):
            if not await require_active_or_warn(update, context, s, g): return
            gender = "female" if m.group(1)=="دختر" else "male"
            u = upsert_user(s, g.id, update.effective_user)
            u.gender = gender; s.commit()
            return await update.message.reply_text("ثبت شد ✅")

        # تولد
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

        # رابطه
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
            s.add(Relationship(chat_id=g.id, user_a_id=me.id, user_b_id=to.id, started_at=started)); s.commit()
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

        # کراش
        if PAT_GROUP["crush_add"].match(text):
            if not update.message.reply_to_message:
                return await update.message.reply_text("برای ثبت کراش، روی پیام طرف ریپلای کن و بنویس: «ثبت کراش»")
            if not await require_active_or_warn(update, context, s, g): return
            me = upsert_user(s, g.id, update.effective_user)
            to = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            if me.id == to.id: return await update.message.reply_text("روی خودت نمی‌شه 😅")
            s.add(Crush(chat_id=g.id, from_user_id=me.id, to_user_id=to.id)); s.commit()
            return await update.message.reply_text("کراش ثبت شد 💘")

        if PAT_GROUP["crush_del"].match(text):
            if not update.message.reply_to_message:
                return await update.message.reply_text("برای حذف، روی پیام طرف ریپلای کن و بنویس: «حذف کراش»")
            me = upsert_user(s, g.id, update.effective_user)
            to = upsert_user(s, g.id, update.message.reply_to_message.from_user)
            s.execute(Crush.__table__.delete().where(
                (Crush.chat_id==g.id) & (Crush.from_user_id==me.id) & (Crush.to_user_id==to.id)
            )); s.commit()
            return await update.message.reply_text("کراش حذف شد 🗑️")

        # محبوب امروز
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

        # شیپ امشب (نمایش آخرین)
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

        # انقضا
        if PAT_GROUP["expiry"].match(text):
            return await update.message.reply_text(f"⏳ اعتبار این گروه تا: {g.expires_at} UTC")

        # شارژ (مالک یا فروشنده)
        if PAT_GROUP["charge"].match(text):
            if not is_admin(s, update.effective_user.id):
                return await update.message.reply_text("دسترسی نداری.")
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("۳۰ روز", callback_data="chg:30"),
                InlineKeyboardButton("۹۰ روز", callback_data="chg:90"),
                InlineKeyboardButton("۱۸۰ روز", callback_data="chg:180"),
            ]])
            return await update.message.reply_text("⌁ پنل شارژ گروه:", reply_markup=kb)

        # تگ‌ها (با ریپلای، ۴تایی، ریپلای روی همان پیام)
        if PAT_GROUP["tag_girls"].match(text) or PAT_GROUP["tag_boys"].match(text) or PAT_GROUP["tag_all"].match(text):
            if not update.message.reply_to_message:
                return await update.message.reply_text("برای تگ کردن، باید روی یک پیام ریپلای کنی.")
            if PAT_GROUP["tag_girls"].match(text):
                users = s.query(User).filter_by(chat_id=g.id, gender="female").all()
                header = "تگ دخترها:"
            elif PAT_GROUP["tag_boys"].match(text):
                users = s.query(User).filter_by(chat_id=g.id, gender="male").all()
                header = "تگ پسرها:"
            else:
                users = s.query(User).filter_by(chat_id=g.id).all()
                header = "تگ همه:"
            if not users: return await update.message.reply_text("کسی برای تگ پیدا نشد.")
            reply_to = update.message.reply_to_message.message_id
            await update.message.reply_text(header, reply_to_message_id=reply_to)
            mentions = [mention_of(u) for u in users]
            for pack in chunked(mentions, 4):
                try:
                    await context.bot.send_message(
                        chat_id=g.id,
                        text=" ".join(pack),
                        reply_to_message_id=reply_to,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                except Exception as e:
                    logging.info(f"Tag batch send failed: {e}")
            return

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

# -------------------- OWNER/SELLER DM PANEL --------------------
async def on_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private" or not update.message or not update.message.text:
        return
    text = clean_text(update.message.text)
    with SessionLocal() as s:
        uid = update.effective_user.id
        if uid != OWNER_ID and not is_seller(s, uid):
            return await update.message.reply_text("سلام! برای استفاده مدیریتی باید مالک یا فروشنده باشی.")

        if PAT_DM["panel"].match(text) or PAT_DM["help"].match(text):
            return await update.message.reply_text(
                "🛠 پنل مدیریت:\n"
                "• گروه‌ها → لیست گروه‌ها\n"
                "• تمدید <chat_id> <days>\n"
                "• تنظیم زمان <chat_id> <Area/City>\n"
                "• لیست فروشنده‌ها\n"
                "• افزودن فروشنده <user_id> [یادداشت]  (فقط مالک)\n"
                "• حذف فروشنده <user_id>  (فقط مالک)"
            )

        if PAT_DM["groups"].match(text):
            groups = s.query(Group).order_by(Group.id.asc()).all()
            if not groups: return await update.message.reply_text("گروهی ثبت نشده.")
            lines = []
            now = dt.datetime.utcnow()
            for g in groups[:100]:
                status = "فعال ✅" if g.expires_at and g.expires_at > now else "منقضی ⛔️"
                lines.append(f"{g.title}  | chat_id: {g.id} | تا: {g.expires_at} UTC | {status} | TZ: {g.timezone or '-'}")
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

# -------------------- CALLBACKS (Charge buttons) --------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.data: return
    await q.answer()
    if q.data.startswith("chg:"):
        days = int(q.data.split(":")[1])
        with SessionLocal() as s:
            if not is_admin(s, q.from_user.id):
                return await q.edit_message_text("دسترسی نداری.")
            g = ensure_group(s, q.message.chat)
            base = g.expires_at if g.expires_at and g.expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
            g.expires_at = base + dt.timedelta(days=days)
            s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=q.from_user.id, action="extend", amount_days=days))
            s.commit()
        await q.edit_message_text(f"✅ تمدید شد تا {g.expires_at} UTC")

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
    """هر شب: محبوب‌های امروز + شیپ شبانه برای گروه‌های فعال"""
    with SessionLocal() as s:
        groups = s.query(Group).all()
        for g in groups:
            if not group_active(g): continue
            tz = get_tz(g); today = dt.datetime.now(tz).date()

            # محبوب‌های امروز
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

            # شیپ شبانه
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
    """صبح‌ها: تبریک تولد و ماهگرد روابط"""
    with SessionLocal() as s:
        groups = s.query(Group).all()
        for g in groups:
            if not group_active(g): continue
            tz = get_tz(g); today = dt.datetime.now(tz).date()
            # تولد
            bdays = s.query(User).filter_by(chat_id=g.id).filter(User.birthday.isnot(None)).all()
            for u in bdays:
                if u.birthday.month==today.month and u.birthday.day==today.day:
                    try:
                        await context.bot.send_message(g.id, f"🎉🎂 تولدت مبارک {(u.first_name or '@'+(u.username or ''))}!")
                    except: pass
            # ماهگرد
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

    # گروه (فقط متن، بدون /)
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_group_text))
    # پی‌وی مالک/فروشنده
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_private_text))
    # شارژ
    app.add_handler(CallbackQueryHandler(on_callback))
    # گزارش نصب/خروج
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # زمان‌بندی‌ها (UTC سراسری)
    app.job_queue.run_daily(job_morning, time=dt.time(6,0,0))
    app.job_queue.run_daily(job_midnight, time=dt.time(21,0,0))

    logging.info("FazolBot FULL (single-file) is running…")
    app.run_polling()

if __name__ == "__main__":
    main()
