# -*- coding: utf-8 -*-
import os, re, html, random, logging, sys
import datetime as dt
from typing import Optional, Tuple, List, Dict
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Date, DateTime, Boolean, ForeignKey, UniqueConstraint, func, text
from sqlalchemy.orm import sessionmaker, declarative_base

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMemberUpdated, CallbackQuery, Message, Chat, User as TGUser, constants
from telegram.ext import Application, ApplicationBuilder, ContextTypes, MessageHandler, filters, CallbackQueryHandler, ChatMemberHandler

# ------------ Config ------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_CONTACT_USERNAME = os.getenv("OWNER_CONTACT_USERNAME", "soulsownerbot")
OWNER_NOTIFY_TG_ID = int(os.getenv("OWNER_NOTIFY_TG_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql+psycopg2://" + DATABASE_URL[len("postgres://"):]
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///bot.db"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("fazol")
TZ_TEHRAN = ZoneInfo("Asia/Tehran")

# ------------ DB ------------
Base = declarative_base()

class Group(Base):
    __tablename__ = "groups"
    id = Column(BigInteger, primary_key=True)  # chat_id
    title = Column(String)
    username = Column(String)
    owner_user_id = Column(BigInteger, nullable=True)
    trial_started_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now())

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(BigInteger, index=True)
    tg_user_id = Column(BigInteger, index=True)
    username = Column(String, nullable=True)
    first_name = Column(String, nullable=True)
    gender = Column(String, nullable=True)  # male|female
    birthday = Column(Date, nullable=True)  # stored Gregorian
    created_at = Column(DateTime, default=func.now())
    __table_args__ = (UniqueConstraint("chat_id","tg_user_id", name="uq_user_chat_member"),)

class Relationship(Base):
    __tablename__ = "relationships"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(BigInteger, index=True)
    user_a_id = Column(Integer, index=True)
    user_b_id = Column(Integer, index=True)
    started_at = Column(Date, nullable=True)  # Gregorian
    created_at = Column(DateTime, default=func.now())
    __table_args__ = (UniqueConstraint("chat_id","user_a_id","user_b_id", name="uq_rel_pair"),)

class Crush(Base):
    __tablename__ = "crushes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(BigInteger, index=True)
    from_user_id = Column(Integer, index=True)
    to_user_id = Column(Integer, index=True)
    created_at = Column(DateTime, default=func.now())
    __table_args__ = (UniqueConstraint("chat_id","from_user_id","to_user_id", name="uq_crush_pair"),)

class ReplyStatDaily(Base):
    __tablename__ = "reply_stat_daily"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(BigInteger, index=True)
    date = Column(Date, index=True)  # Gregorian day
    target_user_id = Column(Integer, index=True)
    reply_count = Column(Integer, default=0)
    __table_args__ = (UniqueConstraint("chat_id","date","target_user_id", name="uq_reply_daily"),)

class SubscriptionLog(Base):
    __tablename__ = "subscription_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(BigInteger, index=True)
    actor_tg_user_id = Column(BigInteger, index=True)
    action = Column(String)  # extend|zero
    amount_days = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=func.now())

from sqlalchemy.pool import QueuePool
engine = create_engine(DATABASE_URL, future=True, echo=False, poolclass=QueuePool, pool_size=int(os.getenv("DB_POOL_SIZE","5")), max_overflow=int(os.getenv("DB_MAX_OVERFLOW","5")), pool_pre_ping=True)

# ------------ Light Auto-Migrations (safe) ------------
def run_light_migrations(engine):
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE IF NOT EXISTS "groups"(id BIGINT PRIMARY KEY)'))
        conn.execute(text('ALTER TABLE "groups" ADD COLUMN IF NOT EXISTS title TEXT'))
        conn.execute(text('ALTER TABLE "groups" ADD COLUMN IF NOT EXISTS username TEXT'))
        conn.execute(text('ALTER TABLE "groups" ADD COLUMN IF NOT EXISTS owner_user_id BIGINT'))
        conn.execute(text('ALTER TABLE "groups" ADD COLUMN IF NOT EXISTS trial_started_at TIMESTAMPTZ'))
        conn.execute(text('ALTER TABLE "groups" ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ'))
        conn.execute(text('ALTER TABLE "groups" ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()'))

        conn.execute(text('CREATE TABLE IF NOT EXISTS "users"(id SERIAL PRIMARY KEY)'))
        conn.execute(text('ALTER TABLE "users" ADD COLUMN IF NOT EXISTS chat_id BIGINT'))
        conn.execute(text('ALTER TABLE "users" ADD COLUMN IF NOT EXISTS tg_user_id BIGINT'))
        conn.execute(text('ALTER TABLE "users" ADD COLUMN IF NOT EXISTS username TEXT'))
        conn.execute(text('ALTER TABLE "users" ADD COLUMN IF NOT EXISTS first_name TEXT'))
        conn.execute(text('ALTER TABLE "users" ADD COLUMN IF NOT EXISTS gender TEXT'))
        conn.execute(text('ALTER TABLE "users" ADD COLUMN IF NOT EXISTS birthday DATE'))
        conn.execute(text('ALTER TABLE "users" ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()'))

        conn.execute(text('CREATE TABLE IF NOT EXISTS "relationships"(id SERIAL PRIMARY KEY)'))
        conn.execute(text('ALTER TABLE "relationships" ADD COLUMN IF NOT EXISTS chat_id BIGINT'))
        conn.execute(text('ALTER TABLE "relationships" ADD COLUMN IF NOT EXISTS user_a_id INTEGER'))
        conn.execute(text('ALTER TABLE "relationships" ADD COLUMN IF NOT EXISTS user_b_id INTEGER'))
        conn.execute(text('ALTER TABLE "relationships" ADD COLUMN IF NOT EXISTS started_at DATE'))
        conn.execute(text('ALTER TABLE "relationships" ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()'))

        conn.execute(text('CREATE TABLE IF NOT EXISTS "crushes"(id SERIAL PRIMARY KEY)'))
        conn.execute(text('ALTER TABLE "crushes" ADD COLUMN IF NOT EXISTS chat_id BIGINT'))
        conn.execute(text('ALTER TABLE "crushes" ADD COLUMN IF NOT EXISTS from_user_id INTEGER'))
        conn.execute(text('ALTER TABLE "crushes" ADD COLUMN IF NOT EXISTS to_user_id INTEGER'))
        conn.execute(text('ALTER TABLE "crushes" ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()'))

        conn.execute(text('CREATE TABLE IF NOT EXISTS "reply_stat_daily"(id SERIAL PRIMARY KEY)'))
        conn.execute(text('ALTER TABLE "reply_stat_daily" ADD COLUMN IF NOT EXISTS chat_id BIGINT'))
        conn.execute(text('ALTER TABLE "reply_stat_daily" ADD COLUMN IF NOT EXISTS date DATE'))
        conn.execute(text('ALTER TABLE "reply_stat_daily" ADD COLUMN IF NOT EXISTS target_user_id INTEGER'))
        conn.execute(text('ALTER TABLE "reply_stat_daily" ADD COLUMN IF NOT EXISTS reply_count INTEGER DEFAULT 0'))

run_light_migrations(engine)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

# ------------ Digits/mentions ------------
fa_digits_map = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")
ar_digits = "٠١٢٣٤٥٦٧٨٩"
ar_to_en_map = {ord(ar_digits[i]): str(i) for i in range(10)}
fa_to_en_map = {ord("۰۱۲۳۴۵۶۷۸۹"[i]): str(i) for i in range(10)}

def fa_digits(n) -> str:
    try: return str(n).translate(fa_digits_map)
    except Exception: return str(n)

def fa_to_en_digits(s: str) -> str:
    if not isinstance(s, str): s=str(s)
    return s.translate(ar_to_en_map).translate(fa_to_en_map)

def mention_of(u: 'User') -> str:
    if u.username: return f"@{u.username}"
    n = u.first_name or "کاربر"
    return f"{n}({u.tg_user_id})"

def owner_mention_html(uid: Optional[int]) -> str:
    return f'<a href="tg://user?id={uid}">مالک</a> ' if uid else ""

# ------------ Jalali <-> Gregorian (pure) ------------
def _div(a,b): return a//b

def g2j(gy,gm,gd):
    gy2 = gy-1600; gm2 = gm-1; gd2 = gd-1
    g_day_no = 365*gy2 + _div(gy2+3,4) - _div(gy2+99,100) + _div(gy2+399,400)
    g_month_days = [31,28,31,30,31,30,31,31,30,31,30,31]
    for i in range(gm2): g_day_no += g_month_days[i]
    if gm2>1 and (((gy%4==0) and (gy%100!=0)) or (gy%400==0)): g_day_no+=1
    g_day_no += gd2
    j_day_no = g_day_no - 79
    j_np = _div(j_day_no,12053); j_day_no %= 12053
    jy = 979 + 33*j_np + 4*_div(j_day_no,1461)
    j_day_no %= 1461
    if j_day_no >= 366:
        jy += _div(j_day_no-366,365)
        j_day_no = (j_day_no-366)%365
    for i,md in enumerate([31,31,31,31,31,31,30,30,30,30,30,29]):
        if j_day_no < md: jm=i+1; jd=j_day_no+1; break
        j_day_no -= md
    return jy, jm, jd

def j2g(jy,jm,jd):
    jy2 = jy-979; jm2 = jm-1; jd2 = jd-1
    j_day_no = 365*jy2 + _div(jy2,33)*8 + _div((jy2%33)+3,4)
    for md in [31,31,31,31,31,31,30,30,30,30,30,29][:jm2]: j_day_no += md
    j_day_no += jd2
    g_day_no = j_day_no + 79
    gy = 1600 + 400*_div(g_day_no,146097); g_day_no %= 146097
    leap=True
    if g_day_no >= 36525:
        g_day_no -= 1
        gy += 100*_div(g_day_no,36524); g_day_no %= 36524
        if g_day_no >= 365: g_day_no += 1
        else: leap=False
    gy += 4*_div(g_day_no,1461); g_day_no %= 1461
    if g_day_no >= 366:
        leap=False; g_day_no -= 1
        gy += _div(g_day_no,365); g_day_no %= 365
    g_month_days=[31,28,31,30,31,30,31,31,30,31,30,31]
    if leap: g_month_days[1]=29
    for i,md in enumerate(g_month_days):
        if g_day_no < md: gm=i+1; gd=g_day_no+1; break
        g_day_no -= md
    return gy,gm,gd

def parse_jalali_to_gregorian(date_text: str) -> Optional[dt.date]:
    s = fa_to_en_digits(date_text).strip().replace("-", "/")
    m = re.match(r"^(\d{3,4})/(\d{1,2})/(\d{1,2})$", s)
    if not m: return None
    jy,jm,jd = map(int, m.groups())
    try:
        gy,gm,gd = j2g(jy,jm,jd); return dt.date(gy,gm,gd)
    except ValueError:
        return None

def gregorian_to_jalali(d: dt.date) -> Tuple[int,int,int]:
    return g2j(d.year, d.month, d.day)

def fmt_date_fa_from_greg(d: Optional[dt.date]) -> str:
    if not d: return "-"
    jy,jm,jd = gregorian_to_jalali(d)
    return fa_digits(f"{jy:04d}/{jm:02d}/{jd:02d}")

def fmt_dt_fa(d: Optional[dt.datetime]) -> str:
    if not d: return "-"
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.UTC)
    dl = d.astimezone(TZ_TEHRAN)
    jy,jm,jd = gregorian_to_jalali(dl.date())
    return fa_digits(f"{jy:04d}/{jm:02d}/{jd:02d} {dl.strftime('%H:%M')}")

def jalali_month_length(jy:int,jm:int)->int:
    g1 = j2g(jy,jm,1); g2 = j2g(jy+(1 if jm==12 else 0), 1 if jm==12 else jm+1, 1)
    d1 = dt.date(*g1); d2 = dt.date(*g2)
    return (d2-d1).days

# ------------ Cache & limits ------------
ADMIN_CACHE: Dict[int, Tuple[dt.datetime, set]] = {}
ADMIN_TTL = dt.timedelta(minutes=5)
TAG_RATE: Dict[int, dt.datetime] = {}
TAG_COOLDOWN = dt.timedelta(seconds=120)

async def get_admins_cached(context: ContextTypes.DEFAULT_TYPE, chat_id:int)->set:
    now = dt.datetime.now(dt.UTC)
    c = ADMIN_CACHE.get(chat_id)
    if c and now - c[0] < ADMIN_TTL: return c[1]
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        ids = {a.user.id for a in admins}
        ADMIN_CACHE[chat_id] = (now, ids)
        return ids
    except Exception:
        return set()

def ensure_group(s, chat: Chat) -> Group:
    g = s.get(Group, chat.id)
    if not g:
        g = Group(id=chat.id, title=chat.title or "", username=chat.username)
        s.add(g); s.commit()
    else:
        ch=False
        if (chat.title or "") != (g.title or ""): g.title = chat.title or ""; ch=True
        if (chat.username or "") != (g.username or ""): g.username = chat.username or ""; ch=True
        if ch: s.commit()
    return g

def upsert_user(s, chat_id:int, tg_user: TGUser) -> User:
    row = s.query(User).filter_by(chat_id=chat_id, tg_user_id=tg_user.id).one_or_none()
    if not row:
        row = User(chat_id=chat_id, tg_user_id=tg_user.id, username=tg_user.username, first_name=tg_user.first_name)
        s.add(row); s.commit()
    else:
        ch=False
        if (tg_user.username or "") != (row.username or ""): row.username = tg_user.username or None; ch=True
        if (tg_user.first_name or "") != (row.first_name or ""): row.first_name = tg_user.first_name or None; ch=True
        if ch: s.commit()
    return row

def group_active(g: Group) -> bool:
    if g.expires_at is None:
        return True
    exp = g.expires_at
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=dt.UTC)
    return exp > dt.datetime.now(dt.UTC)

async def create_join_button(context: ContextTypes.DEFAULT_TYPE, g: Group) -> Optional[InlineKeyboardMarkup]:
    if g.username:
        return InlineKeyboardMarkup([[InlineKeyboardButton("ورود به گروه", url=f"https://t.me/{g.username}")]])
    try:
        link = await context.bot.create_chat_invite_link(g.id, name="OwnerEntry")
        return InlineKeyboardMarkup([[InlineKeyboardButton("ورود به گروه", url=link.invite_link)]])
    except Exception:
        return None

async def notify_owner(context: ContextTypes.DEFAULT_TYPE, text_msg: str, group: Optional[Group]=None):
    if not OWNER_NOTIFY_TG_ID: return
    kb = await create_join_button(context, group) if group else None
    try:
        await context.bot.send_message(OWNER_NOTIFY_TG_ID, text_msg, reply_markup=kb, parse_mode=constants.ParseMode.HTML)
    except Exception as e:
        log.warning("notify_owner failed: %s", e)

FAZOL_REPLIES = ["جانم؟","ها؟","چیه؟","چی می‌خوای؟","آمادم 😎","بگو!","هستیم!","چی شد؟","صدام کردی؟","گوش می‌دم.","بزن بریم!","من اینجام.","صدام واضح میاد؟","فضول حاضره!","سلاممم","یاالله","بوس بهت","دستور بده عشقم","جون فضول","جانز","خب؟","بله؟","جوووووونم قلبم","ول کن ناموسا","باشه اومدم آخراشه","خبریه؟","فضول عمته","دوسم داری؟","اصن نمیقام","زن جاذاب میقام","زن میقام","جااااااان چه انسان مناسبی","کراش زدم","هعی","سازنده گفته اگه داف صدات کرد فرار کن.تو دافی؟ فرار","مغز داری؟ دو گرم بده","خاک تو سرت بای","ولم کن قهرم","میزنم تو دهنتا","جووون","عجبااااا","با من حرف نزن","قهرم","کات بای","دارم دنبال کراش میگردم","چی میخوای؟","رل پی","زهرمار","کوفت","مرض","نمیقااااااام","تویی","بووووووج","نبینم صدام کنی دیگه","سازنده میگه داف پی","نمیقام قهرم","بدو بینم","خودتییییی","خودتی","بریم پشت گپ","اگه سینگلی سلام عشقم اگه نه که تفم دهنت","بااااااع","اصن اوف تو فقط بگو فضول","جون دلم عشق من","هااااااااع","بستنی میقام","چاکرم","میمی میقام","میدونم عاشقمی","سرم تو چیز مردمه همش","نکن دارم فضولی میکنم","تو از من فضول تری","عاح گوشم","بوج","داد نزن خب","بیخیال شو عه","تف خب تف","با ولم کووووو","تا صب صدا کن","باع","چته؟","خب چییییز!"]

# ------------ Panels ------------
def help_kb(priv: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("ثبت جنسیت", callback_data="help:gender"),
         InlineKeyboardButton("ثبت تولد", callback_data="help:bd")],
        [InlineKeyboardButton("کراش‌ها", callback_data="help:crush"),
         InlineKeyboardButton("شیپم کن", callback_data="help:ship")],
        [InlineKeyboardButton("آیدی/ایدی", callback_data="help:id"),
         InlineKeyboardButton("شروع رابطه/عشق", callback_data="help:love")]
    ]
    if priv:
        rows += [[InlineKeyboardButton("مدیریت رابطه (ادمین)", callback_data="help:reladmin")],
                 [InlineKeyboardButton("فضول شارژ / اعتبار / خروج", callback_data="help:credit")]]
    rows.append([InlineKeyboardButton("❌ بستن", callback_data="close")])
    return InlineKeyboardMarkup(rows)

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE, priv: bool):
    await update.effective_message.reply_text("📘 راهنمای فضول — دکمه‌ها را بزن:", reply_markup=help_kb(priv))

def owner_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="adm:groups:0")],
        [InlineKeyboardButton("🛍️ فروشنده‌ها", callback_data="adm:sellers")],
        [InlineKeyboardButton("📊 آمار کلی", callback_data="adm:stats")],
        [InlineKeyboardButton("❌ بستن", callback_data="close")]
    ])

def seller_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 آمار من", callback_data="seller:mystats")],
        [InlineKeyboardButton("📜 گروه‌های من", callback_data="seller:mygroups")],
        [InlineKeyboardButton("❌ بستن", callback_data="close")]
    ])

async def open_owner_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("پنل مالک:", reply_markup=owner_main_kb())

async def open_seller_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("پنل فروشنده:", reply_markup=seller_main_kb())

async def open_group_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = [
        [InlineKeyboardButton("👥 ادمین‌ها و مالک", callback_data="ga:admins")],
        [InlineKeyboardButton("🧹 پاکسازی داده‌های گروه", callback_data="ga:wipe")],
        [InlineKeyboardButton("⏳ اعتبار", callback_data="ga:credit")],
        [InlineKeyboardButton("❌ بستن", callback_data="close")]
    ]
    await update.effective_message.reply_text("⚙️ پیکربندی فضول", reply_markup=InlineKeyboardMarkup(rows))

# ------------ Handlers ------------
async def on_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.effective_message
    txt = (m.text or "").strip()
    if txt in ("/start","start","شروع"):
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ افزودن به گروه", url=f"https://t.me/{(await context.bot.get_me()).username}?startgroup=true")],
            [InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]
        ])
        return await m.reply_text("سلام! من فضول‌ام 🤖\nمن رو به گروهت اضافه کن و ۷ روز رایگان تست کن.", reply_markup=kb)
    if txt in ("پنل","پنل مالک"):
        return await open_owner_panel(update, context)
    if txt in ("پنل فروشنده","فروشنده","seller"):
        return await open_seller_panel(update, context)

async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upd: ChatMemberUpdated = update.my_chat_member
    chat = upd.chat
    if chat.type in ("group","supergroup") and upd.new_chat_member and upd.new_chat_member.user and upd.new_chat_member.user.is_bot:
        with SessionLocal() as s:
            g = ensure_group(s, chat)
            if (g.trial_started_at is None) and (g.expires_at is None):
                now = dt.datetime.now(dt.UTC)
                g.trial_started_at = now; g.expires_at = now + dt.timedelta(days=7); s.commit()
                try: await context.bot.send_message(chat.id, "🎁 شروع تست رایگان ۷ روزه!")
                except Exception: pass

async def _track_reply_stat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not (msg and msg.reply_to_message and (msg.text or msg.caption)):
        return
    chat = update.effective_chat
    with SessionLocal() as s:
        g = ensure_group(s, chat)
        target = upsert_user(s, g.id, msg.reply_to_message.from_user)
        today = dt.datetime.now(TZ_TEHRAN).date()
        row = s.query(ReplyStatDaily).filter_by(chat_id=g.id, date=today, target_user_id=target.id).one_or_none()
        if not row:
            row = ReplyStatDaily(chat_id=g.id, date=today, target_user_id=target.id, reply_count=0)
            s.add(row)
        row.reply_count = (row.reply_count or 0) + 1
        s.commit()

def _is_seller_for_group(s, seller_tg_id:int, gid:int)->bool:
    q = s.execute(text("SELECT 1 FROM subscription_log WHERE actor_tg_user_id=:sid AND chat_id=:gid LIMIT 1"), {"sid": seller_tg_id, "gid": gid}).fetchone()
    return bool(q)

async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m: Message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    text = (m.text or "").strip()
    if not text: return
    if m.reply_to_message and m.reply_to_message.from_user and m.reply_to_message.from_user.is_bot:
        return await m.reply_text("گمشو دارم فضولی می‌کنم، مزاحم نشو! بیا با دستورام بازی کن 😎")
    with SessionLocal() as s:
        g = ensure_group(s, chat)
        u = upsert_user(s, g.id, user)
        admin_ids = await get_admins_cached(context, g.id)
        is_admin = user.id in admin_ids
        is_operator = (user.id == OWNER_NOTIFY_TG_ID and OWNER_NOTIFY_TG_ID != 0)
        is_owner_of_group = (g.owner_user_id == user.id)

        allow_prefixes = ("فضول شارژ","صفر کردن اعتبار","خروج فضول","اعتبار فضول","پیکربندی","پیکربندی فضول", "فضول", "فضول پاکسازی")
        if not group_active(g) and not any(text.startswith(a) for a in allow_prefixes):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("📨 تماس با مالک", url=f"https://t.me/{OWNER_CONTACT_USERNAME}")]])
            return await m.reply_text("اعتبار ربات تموم شده. لطفاً با تیم سازنده تماس بگیرید.", reply_markup=kb)

        # 1) Gender
        mg = re.match(r"^ثبت\s*جنسیت\s*(پسر|دختر)$", text)
        if mg:
            target = u
            if m.reply_to_message and is_admin:
                target = upsert_user(s, g.id, m.reply_to_message.from_user)
            target.gender = "male" if mg.group(1)=="پسر" else "female"; s.commit()
            return await m.reply_text("جنسیت ثبت شد.")

        # 2) Birthday (Jalali-only input)
        mbd = re.match(r"^ثبت\s*تولد\s+(.+)$", text)
        if mbd:
            d_g = parse_jalali_to_gregorian(mbd.group(1))
            if not d_g: return await m.reply_text("فرمت تاریخ درست نیست (شمسی). مثل 1402/01/01")
            target = u
            if m.reply_to_message and is_admin:
                target = upsert_user(s, g.id, m.reply_to_message.from_user)
            target.birthday = d_g; s.commit()
            return await m.reply_text(f"تولد ثبت شد: {fmt_date_fa_from_greg(d_g)}")

        # 3) ID (admins only)
        if text in ("آیدی","ایدی"):
            if not is_admin: return await m.reply_text("این دستور مخصوص ادمین‌های گروه است.")
            target = u
            if m.reply_to_message:
                target = upsert_user(s, g.id, m.reply_to_message.from_user)
            crush_count = s.query(Crush).filter_by(chat_id=g.id, from_user_id=target.id).count()
            rel = s.query(Relationship).filter_by(chat_id=g.id).filter((Relationship.user_a_id==target.id)|(Relationship.user_b_id==target.id)).one_or_none()
            rel_txt = "-"
            if rel:
                other_id = rel.user_b_id if rel.user_a_id==target.id else rel.user_a_id
                other = s.get(User, other_id)
                rel_txt = f"{mention_of(other)} — از {fmt_date_fa_from_greg(rel.started_at)}" if other else "-"
            today = dt.datetime.now(TZ_TEHRAN).date()
            my_row = s.query(ReplyStatDaily).filter_by(chat_id=g.id, date=today, target_user_id=target.id).one_or_none()
            max_row = s.query(ReplyStatDaily).filter_by(chat_id=g.id, date=today).order_by(ReplyStatDaily.reply_count.desc()).first()
            score=0
            if my_row and max_row and (max_row.reply_count or 0)>0:
                score = round(100*my_row.reply_count/max_row.reply_count)
            lines = [
                f"👤 نام: {target.first_name or ''} @{target.username or ''}",
                f"جنسیت: {'دختر' if target.gender=='female' else ('پسر' if target.gender=='male' else 'نامشخص')}",
                f"تولد: {fmt_date_fa_from_greg(target.birthday)}",
                f"کراش‌ها: {fa_digits(crush_count)}",
                f"رابطه/پارتنر: {rel_txt}",
                f"محبوبیت امروز: {score}%"
            ]
            if crush_count > 10: lines.append("رتبه: هول")
            return await m.reply_text("\n".join(lines))

        # 4) Relationship admin add/del by @ or id
        m_rel = re.match(r"^(@\S+|\d{6,})\s+(?:رل|پارتنر|عشق)\s+(@\S+|\d{6,})$", text)
        if m_rel and is_admin:
            def resolve(sel:str)->Optional[User]:
                if sel.startswith("@"):
                    return s.query(User).filter(User.chat_id==g.id, func.lower(User.username)==sel[1:].lower()).one_or_none()
                try: tid = int(fa_to_en_digits(sel))
                except Exception: return None
                return s.query(User).filter_by(chat_id=g.id, tg_user_id=tid).one_or_none()
            u1 = resolve(m_rel.group(1)); u2 = resolve(m_rel.group(2))
            if not u1 or not u2 or u1.id==u2.id:
                return await m.reply_text("کاربرها یافت نشدند یا یکسان‌اند.")
            s.query(Relationship).filter(Relationship.chat_id==g.id).filter((Relationship.user_a_id.in_([u1.id,u2.id]))|(Relationship.user_b_id.in_([u1.id,u2.id]))).delete(synchronize_session=False)
            ua,ub = (u1.id,u2.id) if u1.id<u2.id else (u2.id,u1.id)
            s.add(Relationship(chat_id=g.id, user_a_id=ua, user_b_id=ub, started_at=dt.date.today())); s.commit()
            await m.reply_text(f"✅ رابطه ثبت شد: {mention_of(u1)} × {mention_of(u2)}")
            await notify_owner(context, f"[گزارش] کاربر <a href=\"tg://user?id={u1.tg_user_id}\">{u1.tg_user_id}</a> و <a href=\"tg://user?id={u2.tg_user_id}\">{u2.tg_user_id}</a> در گروه <b>{html.escape(g.title or str(g.id))}</b> وارد رابطه شدند.", g)
            return
        m_rel_del = re.match(r"^(@\S+|\d{6,})\s+(?:کات|حذف\s*(?:رل|عشق|پارتنر))\s+(@\S+|\d{6,})$", text)
        if m_rel_del and is_admin:
            def resolve(sel:str)->Optional[User]:
                if sel.startswith("@"):
                    return s.query(User).filter(User.chat_id==g.id, func.lower(User.username)==sel[1:].lower()).one_or_none()
                try: tid = int(fa_to_en_digits(sel))
                except Exception: return None
                return s.query(User).filter_by(chat_id=g.id, tg_user_id=tid).one_or_none()
            u1 = resolve(m_rel_del.group(1)); u2 = resolve(m_rel_del.group(2))
            if not u1 or not u2 or u1.id==u2.id:
                return await m.reply_text("کاربرها یافت نشدند یا یکسان‌اند.")
            s.query(Relationship).filter(Relationship.chat_id==g.id).filter(((Relationship.user_a_id==u1.id)&(Relationship.user_b_id==u2.id))|((Relationship.user_a_id==u2.id)&(Relationship.user_b_id==u1.id))).delete(synchronize_session=False)
            s.commit()
            await m.reply_text("✂️ رابطه حذف شد.")
            await notify_owner(context, f"[گزارش] رابطه بین <a href=\"tg://user?id={u1.tg_user_id}\">{u1.tg_user_id}</a> و <a href=\"tg://user?id={u2.tg_user_id}\">{u2.tg_user_id}</a> در گروه <b>{html.escape(g.title or str(g.id))}</b> حذف شد.", g)
            return

        # 5) Start love / relation (Jalali date or keyboard)
        m_start = re.match(r"^(?:شروع\s*رابطه|شروع\s*عشق)(?:\s+(امروز|[\d\/\-]+))?$", text)
        if m_start:
            arg = m_start.group(1)
            target_user = u
            if m.reply_to_message and is_admin:
                target_user = upsert_user(s, g.id, m.reply_to_message.from_user)
            rel = s.query(Relationship).filter_by(chat_id=g.id).filter((Relationship.user_a_id==target_user.id)|(Relationship.user_b_id==target_user.id)).one_or_none()
            if not rel: return await m.reply_text("اول باید رابطه/پارتنر ثبت شده باشد.")
            if arg:
                date_val = dt.date.today() if arg=="امروز" else parse_jalali_to_gregorian(arg)
                if not date_val: return await m.reply_text("فرمت تاریخ درست نیست (شمسی). مثل 1402/01/01")
                rel.started_at = date_val; s.commit()
                return await m.reply_text(f"⏱ شروع رابطه ثبت شد: {fmt_date_fa_from_greg(date_val)}")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("امروز", callback_data=f"startlove:today:{target_user.id}")],
                [InlineKeyboardButton("انتخاب دستی", callback_data=f"startlove:manual:{target_user.id}")],
                [InlineKeyboardButton("لغو", callback_data="startlove:cancel")]
            ])
            return await m.reply_text("تاریخ شروع را انتخاب کن:", reply_markup=kb)

        # 6) Crush add/remove via reply
        if text in ("کراشم","ثبت کراش"):
            if not m.reply_to_message: return await m.reply_text("روی پیام طرف ریپلای کن بعد بنویس «کراشم».")
            target = upsert_user(s, g.id, m.reply_to_message.from_user)
            if target.id == u.id: return await m.reply_text("به خودت نمی‌تونی کراش بزنی 😅")
            try:
                s.add(Crush(chat_id=g.id, from_user_id=u.id, to_user_id=target.id)); s.commit()
                await m.reply_text("💘 کراش ثبت شد.")
                await notify_owner(context, f"[گزارش] کاربر <a href=\"tg://user?id={u.tg_user_id}\">{u.tg_user_id}</a> روی <a href=\"tg://user?id={target.tg_user_id}\">{target.tg_user_id}</a> در گروه <b>{html.escape(g.title or str(g.id))}</b> کراش زد.", g)
            except Exception:
                await m.reply_text("از قبل کراش ثبت شده بود.")
            return
        if text in ("حذف کراش","کراش حذف"):
            if not m.reply_to_message: return await m.reply_text("روی پیام طرف ریپلای کن بعد بنویس «حذف کراش».")
            target = upsert_user(s, g.id, m.reply_to_message.from_user)
            s.query(Crush).filter_by(chat_id=g.id, from_user_id=u.id, to_user_id=target.id).delete(synchronize_session=False); s.commit()
            return await m.reply_text("❌ کراش حذف شد.")

        # 7) Partner suggestion + shipm kon
        if text == "شیپم کن":
            males = s.query(User).filter_by(chat_id=g.id, gender="male").all()
            females = s.query(User).filter_by(chat_id=g.id, gender="female").all()
            if not males or not females:
                return await m.reply_text("کافیه دخترا و پسرا «ثبت جنسیت» بزنن تا شیپ کنیم!")
            mm,ff = random.choice(males), random.choice(females)
            return await m.reply_text(f"💘 شیپ: {mention_of(mm)} × {mention_of(ff)}")
        if "رل" in text:
            rels = s.query(Relationship).filter_by(chat_id=g.id).all()
            in_rel = set([r.user_a_id for r in rels]+[r.user_b_id for r in rels])
            if u.id not in in_rel and u.gender in ("male","female"):
                opp = "female" if u.gender=="male" else "male"
                cands = s.query(User).filter_by(chat_id=g.id, gender=opp).all()
                cands = [x for x in cands if x.id not in in_rel and x.id != u.id]
                if cands:
                    cand = random.choice(cands)
                    await m.reply_text(f"❤️ پارتنر پیشنهادی: {mention_of(cand)}")

        # 8) Crush list
        if text in ("کراشام","کراش های من","لیست کراشام"):
            rows = s.query(Crush).filter_by(chat_id=g.id, from_user_id=u.id).all()
            if not rows: return await m.reply_text("هیچ کراشی نداری.")
            names=[]; 
            for r in rows[:50]:
                to = s.get(User, r.to_user_id)
                if to: names.append(mention_of(to))
            return await m.reply_text(f"💘 کراش‌های {mention_of(u)}:\n" + "\n".join(f"- {n}" for n in names) + f"\n— مجموع: {fa_digits(len(rows))}")
        if text in ("کراشاش","کراش هاش","کراشاشو"):
            if not m.reply_to_message: return await m.reply_text("روی پیامش ریپلای کن و بنویس «کراشاش».")
            target = upsert_user(s, g.id, m.reply_to_message.from_user)
            rows = s.query(Crush).filter_by(chat_id=g.id, from_user_id=target.id).all()
            if not rows: return await m.reply_text("کراشی ندارد.")
            names=[]; 
            for r in rows[:50]:
                to = s.get(User, r.to_user_id)
                if to: names.append(mention_of(to))
            return await m.reply_text(f"💘 کراش‌های {mention_of(target)}:\n" + "\n".join(f"- {n}" for n in names) + f"\n— مجموع: {fa_digits(len(rows))}")

        # 9) Tagging (rate-limited)
        if text in ("تگ پسرها","تگ پسر ها","تگ دخترها","تگ دختر ها","تگ همه"):
            if not m.reply_to_message: return await m.reply_text("روی یک پیام ریپلای کن بعد بنویس «تگ ...».")
            last = TAG_RATE.get(g.id); now = dt.datetime.now(dt.UTC)
            if last and now - last < TAG_COOLDOWN:
                remain = TAG_COOLDOWN - (now - last)
                return await m.reply_text(f"⏱ لطفاً {fa_digits(remain.seconds)} ثانیه صبر کن.")
            q = s.query(User).filter_by(chat_id=g.id)
            if text in ("تگ پسرها","تگ پسر ها"): q=q.filter(User.gender=="male")
            elif text in ("تگ دخترها","تگ دختر ها"): q=q.filter(User.gender=="female")
            tags = [mention_of(x) for x in q.limit(50).all()]
            if not tags: return await m.reply_text("کسی برای تگ نیست.")
            TAG_RATE[g.id] = now
            return await m.reply_to_message.reply_text(" ".join(tags))

        
# لیست ادمین‌ها (متنی) — فقط ادمین/مالک/اپراتور
if text in ("ادمین‌ها","ادمین ها","لیست ادمین‌ها","ادمین های گروه"):
    if not (is_admin or is_owner_of_group or is_operator):
        return await m.reply_text("این دستور مخصوص ادمین‌ها و مالک گروه است.")
    admin_lines = []
    try:
        admins = await context.bot.get_chat_administrators(g.id)
        for a in admins:
            uu = a.user
            role = "سازنده" if getattr(a, "status", "") == "creator" else "ادمین"
            nm = html.escape(uu.first_name or str(uu.id))
            admin_lines.append(f"- {role}: <a href=\"tg://user?id={uu.id}\">{nm}</a> @{uu.username or ''}")
    except Exception:
        admin_lines.append("⚠️ برای دیدن فهرست ادمین‌ها، ربات باید ادمین گروه باشد.")
    owner_line = "— مالک فعلی (DB): نامشخص"
    if g.owner_user_id:
        owner_line = f"— مالک فعلی (DB): <a href=\"tg://user?id={g.owner_user_id}\">{g.owner_user_id}</a>"
    admin_text = "👥 ادمین‌ها و مالک:\n" + "\n".join(admin_lines) + "\n" + owner_line
    return await m.reply_html(admin_text)

        # 18) Config
        if text in ("پیکربندی","پیکربندی فضول"):
            if not (is_admin or is_owner_of_group):
                return await m.reply_text("این دستور مخصوص ادمین‌ها و مالک گروه است.")
            return await open_group_admin_panel(update, context)

        # 20) Help
        if text in ("فضول راهنما","راهنما","کمک","فضول کمک"):
            privileged = is_admin or is_operator or is_owner_of_group
            return await show_help(update, context, privileged)

        # 21) Owner/Seller/Admin ops
        if text.startswith("فضول شارژ"):
            if not (is_operator or is_admin or is_owner_of_group):
                return await m.reply_text("اجازه نداری.")
            mchg = re.match(r"^فضول\s*شارژ\s+(\d+)$", fa_to_en_digits(text))
            if not mchg: return await m.reply_text("مثال: فضول شارژ 1")
            days = int(mchg.group(1))
            now = dt.datetime.now(dt.UTC)
            exp = g.expires_at.replace(tzinfo=dt.UTC) if (g.expires_at and g.expires_at.tzinfo is None) else g.expires_at
            base = exp if (exp and exp>now) else now
            g.expires_at = base + dt.timedelta(days=days)
            s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=user.id, action="extend", amount_days=days)); s.commit()
            await m.reply_text(f"✅ تمدید شد تا {fmt_dt_fa(g.expires_at)}")
            await notify_owner(context, f"[گزارش] فروشنده/ادمین <a href=\"tg://user?id={user.id}\">{user.id}</a> گروه <b>{html.escape(g.title or str(g.id))}</b> را به مقدار {fa_digits(days)} روز شارژ کرد.", g)
            return

        if text in ("صفر کردن اعتبار","صفرکردن اعتبار"):
            if not (is_operator or is_admin or is_owner_of_group):
                return await m.reply_text("اجازه نداری.")
            g.expires_at = dt.datetime.now(dt.UTC)
            s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=user.id, action="zero")); s.commit()
            await m.reply_text("⏱ اعتبار صفر شد.")
            await notify_owner(context, f"[گزارش] اعتبار گروه <b>{html.escape(g.title or str(g.id))}</b> توسط <a href=\"tg://user?id={user.id}\">{user.id}</a> صفر شد.", g)
            return

        if text == "خروج فضول":
            if not (is_operator or is_admin or is_owner_of_group):
                return await m.reply_text("اجازه نداری.")
            await m.reply_text("خدافظ فضولا 👋")
            try: await context.bot.leave_chat(g.id)
            except Exception: pass
            try: s.query(Group).filter_by(id=g.id).delete(synchronize_session=False); s.commit()
            except Exception: pass
            return

        if text == "اعتبار فضول":
            if not (is_operator or is_admin or is_owner_of_group):
                return await m.reply_text("اجازه نداری.")
            return await m.reply_text(f"⏳ اعتبار فعلی: {fmt_dt_fa(g.expires_at)}")

        if text == "فضول":
            return await m.reply_text(random.choice(FAZOL_REPLIES))

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q:
if data == "ga:admins":
    g = None
    with SessionLocal() as s:
        g = s.get(Group, q.message.chat.id) if q.message and q.message.chat else None
    admin_lines = []
    try:
        admins = await context.bot.get_chat_administrators(q.message.chat.id)
        for a in admins:
            u = a.user
            role = "سازنده" if getattr(a, "status", "") == "creator" else "ادمین"
            nm = html.escape(u.first_name or str(u.id))
            admin_lines.append(f"- {role}: <a href=\"tg://user?id={u.id}\">{nm}</a> @{u.username or ''}")
    except Exception:
        admin_lines.append("⚠️ برای دیدن فهرست ادمین‌ها، ربات باید ادمین گروه باشد.")
    owner_line = "— مالک فعلی (DB): نامشخص"
    if g and g.owner_user_id:
        owner_line = f"— مالک فعلی (DB): <a href=\"tg://user?id={g.owner_user_id}\">{g.owner_user_id}</a>"
    txt = "👥 ادمین‌ها و مالک:\n" + "\n".join(admin_lines) + "\n" + owner_line
    try:
        await q.message.edit_text(txt, parse_mode=constants.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ بازگشت", callback_data="help:home")],[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
    except Exception:
        pass
    return
 CallbackQuery = update.callback_query
    await q.answer()
    data = q.data or ""
    if data == "close":
        try: await q.message.delete()
        except Exception: pass
        return
    if data.startswith("help:"):
        m = data.split(":")[1]
        txt = ""
        if m=="gender": txt = "ثبت جنسیت پسر|دختر — با ریپلای ادمین برای دیگران."
        elif m=="bd": txt = "ثبت تولد 1377/06/08 — اعداد فارسی/عربی قابل قبول. تبریک ۹ صبح به تقویم شمسی."
        elif m=="crush": txt = "کراشم/ثبت کراش (با ریپلای) — حذف کراش نیز با ریپلای."
        elif m=="ship": txt = "شیپم کن — زوج تصادفی (پسر×دختر)."
        elif m=="id": txt = "آیدی/ایدی — فقط ادمین؛ نام، یوزرنیم، تولد، پارتنر، کراش‌ها، شروع رابطه، محبوبیت امروز."
        elif m=="love": txt = "شروع رابطه|عشق [امروز/تاریخ] — بدون ریپلای؛ ادمین با ریپلای می‌تواند برای دیگران ثبت کند."
        elif m=="reladmin": txt = "@u رل @u | @id رل @id — و «کات/حذف رل/عشق/پارتنر»"
        elif m=="credit": txt = "فضول شارژ N / صفر کردن اعتبار / خروج فضول / اعتبار فضول — برای مالک/فروشنده/ادمین."
        else: txt = "—"
        try: await q.message.edit_text(txt, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ بازگشت", callback_data="help:home")],[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
        except Exception: pass
        return
    if data == "help:home":
        try: await q.message.edit_text("📘 راهنمای فضول — دکمه‌ها را بزن:", reply_markup=help_kb(True))
        except Exception: pass
        return

    if data == "adm:stats":
        with SessionLocal() as s:
            g_cnt = s.execute(text("SELECT COUNT(*) FROM groups")).scalar() or 0
            u_cnt = s.execute(text("SELECT COUNT(*) FROM users")).scalar() or 0
            r_cnt = s.execute(text("SELECT COUNT(*) FROM relationships")).scalar() or 0
            c_cnt = s.execute(text("SELECT COUNT(*) FROM crushes")).scalar() or 0
        return await q.message.edit_text(f"📊 آمار کلی:\nگروه‌ها: {fa_digits(g_cnt)}\nکاربران: {fa_digits(u_cnt)}\nرابطه‌ها: {fa_digits(r_cnt)}\nکراش‌ها: {fa_digits(c_cnt)}",
                                         reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ بازگشت", callback_data="owner:main")],[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
    if data == "owner:main":
        return await q.message.edit_text("پنل مالک:", reply_markup=owner_main_kb())
    if data == "adm:groups:0":
        with SessionLocal() as s:
            rows = s.query(Group).order_by(Group.created_at.desc()).limit(50).all()
        btns = [[InlineKeyboardButton((g.title or str(g.id))[:28], callback_data=f"adm:g:{g.id}")] for g in rows] or [[InlineKeyboardButton("— خالی —", callback_data="owner:main")]]
        btns.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="owner:main")])
        return await q.message.edit_text("📋 لیست گروه‌ها:", reply_markup=InlineKeyboardMarkup(btns))

    if data == "adm:sellers":
        with SessionLocal() as s:
            rows = s.execute(text("SELECT DISTINCT actor_tg_user_id FROM subscription_log WHERE actor_tg_user_id IS NOT NULL ORDER BY actor_tg_user_id DESC LIMIT 50")).fetchall()
        btns = [[InlineKeyboardButton(str(r[0]), callback_data=f"adm:seller:{r[0]}")] for r in rows] or [[InlineKeyboardButton("— خالی —", callback_data="owner:main")]]
        btns.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="owner:main")])
        return await q.message.edit_text("🛍️ فروشنده‌ها:", reply_markup=InlineKeyboardMarkup(btns))

    if data.startswith("adm:seller:"):
        sid = int(data.split(":")[2])
        with SessionLocal() as s:
            total_ext = s.execute(text("SELECT COUNT(*) FROM subscription_log WHERE actor_tg_user_id=:sid AND action='extend'"), {"sid": sid}).scalar() or 0
            groups_touched = s.execute(text("SELECT COUNT(DISTINCT chat_id) FROM subscription_log WHERE actor_tg_user_id=:sid AND action='extend'"), {"sid": sid}).scalar() or 0
            groups_rows = s.execute(text("""
                SELECT DISTINCT g.id, COALESCE(g.title,'-') AS title, COALESCE(g.username,'') AS username
                FROM groups g JOIN subscription_log sl ON sl.chat_id=g.id
                WHERE sl.actor_tg_user_id=:sid AND sl.action='extend'
                ORDER BY g.id DESC LIMIT 50
            """), {"sid": sid}).fetchall()
        btns = [[InlineKeyboardButton(f"{(gr.title or '-')[:26]} ({gr.id})", callback_data=f"adm:g:{gr.id}")] for gr in groups_rows] or [[InlineKeyboardButton("— گروهی ندارد —", callback_data="adm:sellers")]]
        btns.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:sellers")])
        return await q.message.edit_text(f"📈 آمار فروشنده {sid}:\nتمدیدها: {fa_digits(total_ext)}\nگروه‌های تمدیدشده: {fa_digits(groups_touched)}\n— گروه‌ها پایین:",
                                         reply_markup=InlineKeyboardMarkup(btns))

    if data.startswith("adm:g:"):
        gid = int(data.split(":")[2])
        rows = [
            [InlineKeyboardButton("⏳ اعتبار", callback_data=f"g:{gid}:credit"),
             InlineKeyboardButton("➕ تمدید 1 روز", callback_data=f"g:{gid}:ext:1")],
            [InlineKeyboardButton("➕ تمدید 7 روز", callback_data=f"g:{gid}:ext:7"),
             InlineKeyboardButton("⏱ صفر کردن", callback_data=f"g:{gid}:zero")],
            [InlineKeyboardButton("🧹 پاکسازی", callback_data=f"g:{gid}:wipe")],
            [InlineKeyboardButton("🚪 خروج ربات", callback_data=f"g:{gid}:leave")],
            [InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:groups:0")]
        ]
        return await q.message.edit_text(f"تنظیمات گروه {gid}:", reply_markup=InlineKeyboardMarkup(rows))

    if data.startswith("g:"):
        _, gid, action, *rest = data.split(":")
        gid = int(gid)
        with SessionLocal() as s:
            g = s.get(Group, gid)
            if not g:
                return await q.message.edit_text("گروه پیدا نشد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:groups:0")],[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
            actor_id = update.effective_user.id
            actor_is_owner = (actor_id == OWNER_NOTIFY_TG_ID and OWNER_NOTIFY_TG_ID != 0)
            actor_is_seller_for_group = _is_seller_for_group(s, actor_id, gid)
            if not (actor_is_owner or actor_is_seller_for_group):
                return await q.message.edit_text("اجازه نداری.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ بازگشت", callback_data="adm:groups:0")],[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
            if action == "credit":
                return await q.message.edit_text(f"اعتبار: {fmt_dt_fa(g.expires_at)}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ بازگشت", callback_data=f"adm:g:{gid}")],[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
            if action == "ext":
                days = int(rest[0])
                now = dt.datetime.now(dt.UTC)
                exp = g.expires_at.replace(tzinfo=dt.UTC) if (g.expires_at and g.expires_at.tzinfo is None) else g.expires_at
                base = exp if (exp and exp>now) else now
                g.expires_at = base + dt.timedelta(days=days)
                s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=actor_id, action="extend", amount_days=days)); s.commit()
                await q.message.edit_text(f"✅ تمدید شد تا {fmt_dt_fa(g.expires_at)}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ بازگشت", callback_data=f"adm:g:{gid}")],[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
                await notify_owner(context, f"[گزارش] گروه <b>{html.escape(g.title or str(g.id))}</b> {fa_digits(days)} روز تمدید شد.", g)
                return
            if action == "zero":
                g.expires_at = dt.datetime.now(dt.UTC)
                s.add(SubscriptionLog(chat_id=g.id, actor_tg_user_id=actor_id, action="zero")); s.commit()
                await q.message.edit_text("⏱ اعتبار صفر شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ بازگشت", callback_data=f"adm:g:{gid}")],[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
                await notify_owner(context, f"[گزارش] اعتبار گروه <b>{html.escape(g.title or str(g.id))}</b> صفر شد.", g)
                return
            if action == "wipe":
                s.query(Relationship).filter_by(chat_id=g.id).delete(synchronize_session=False)
                s.query(Crush).filter_by(chat_id=g.id).delete(synchronize_session=False)
                s.commit()
                return await q.message.edit_text("🧹 پاکسازی انجام شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ بازگشت", callback_data=f"adm:g:{gid}")],[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
            if action == "leave":
                try: await q.message.edit_text("خروج از گروه درخواست شد.")
                except Exception: pass
                try: await context.bot.leave_chat(g.id)
                except Exception: pass
                try: s.query(Group).filter_by(id=g.id).delete(synchronize_session=False); s.commit()
                except Exception: pass
                return
    
if data == "ga:wipe":
    with SessionLocal() as s:
        g = s.get(Group, q.message.chat.id) if q.message and q.message.chat else None
        actor_id = update.effective_user.id
        actor_is_owner = (actor_id == OWNER_NOTIFY_TG_ID and OWNER_NOTIFY_TG_ID != 0)
        actor_is_seller_for_group = _is_seller_for_group(s, actor_id, g.id) if g else False
        if not (actor_is_owner or actor_is_seller_for_group):
            return await q.message.edit_text("اجازه نداری.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
        if g:
            s.query(Relationship).filter_by(chat_id=g.id).delete(synchronize_session=False)
            s.query(Crush).filter_by(chat_id=g.id).delete(synchronize_session=False)
            s.commit()
    return await q.message.edit_text("🧹 پاکسازی گروه انجام شد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ بستن", callback_data="close")]]))
    if data == "ga:credit":
        with SessionLocal() as s:
            g = s.get(Group, q.message.chat.id) if q.message.chat else None
        return await q.message.edit_text(f"⏳ اعتبار گروه: {fmt_dt_fa(g.expires_at if g else None)}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ بستن", callback_data="close")]]))

    if data.startswith("startlove:"):
        parts = data.split(":"); action = parts[1]
        if action == "cancel":
            try: await q.message.edit_text("لغو شد.")
            except Exception: pass
            return
        uid = int(parts[2]) if len(parts)>2 else None
        with SessionLocal() as s:
            g = s.get(Group, q.message.chat.id)
            if not g or not uid:
                return await q.message.edit_text("منقضی شد.")
            rel = s.query(Relationship).filter_by(chat_id=g.id).filter((Relationship.user_a_id==uid)|(Relationship.user_b_id==uid)).one_or_none()
            if not rel: return await q.message.edit_text("اول باید رابطه/پارتنر ثبت شده باشد.")
            if action == "today":
                rel.started_at = dt.date.today(); s.commit()
                return await q.message.edit_text("⏱ شروع رابطه: امروز ثبت شد.")
            if action == "manual":
                context.user_data["awaiting_startlove_date_for"] = uid
                try: await q.message.edit_text("تاریخ را مثل 1402/01/01 بفرست.")
                except Exception: pass
                return

async def capture_manual_startlove_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "awaiting_startlove_date_for" not in context.user_data: return
    uid = context.user_data.get("awaiting_startlove_date_for")
    msg = update.effective_message
    d = parse_jalali_to_gregorian((msg.text or "").strip())
    if not d: return await msg.reply_text("فرمت تاریخ درست نیست (شمسی)؛ مثل 1402/01/01.")
    with SessionLocal() as s:
        g = ensure_group(s, update.effective_chat)
        rel = s.query(Relationship).filter_by(chat_id=g.id).filter((Relationship.user_a_id==uid)|(Relationship.user_b_id==uid)).one_or_none()
        if not rel:
            context.user_data.pop("awaiting_startlove_date_for", None)
            return await msg.reply_text("اول باید رابطه/پارتنر ثبت شده باشد.")
        rel.started_at = d; s.commit()
    context.user_data.pop("awaiting_startlove_date_for", None)
    await msg.reply_text(f"⏱ شروع رابطه ثبت شد: {fmt_date_fa_from_greg(d)}")

# ------------ Jobs ------------
async def job_morning(context: ContextTypes.DEFAULT_TYPE):
    """09:00 Tehran — birthdays (Jalali), monthiversaries (Jalali clip), low credit alerts"""
    with SessionLocal() as s:
        groups = s.query(Group).all()
        today_g = dt.datetime.now(TZ_TEHRAN).date()
        jY,jM,jD = g2j(today_g.year, today_g.month, today_g.day)
        soon = dt.datetime.now(dt.UTC) + dt.timedelta(days=3)
        for g in groups:
            if not group_active(g):
                continue
            # low credit
            if g.expires_at:
                exp = g.expires_at.replace(tzinfo=dt.UTC) if g.expires_at.tzinfo is None else g.expires_at
                if exp <= soon:
                    try:
                        await context.bot.send_message(g.id, f"{owner_mention_html(g.owner_user_id)}⏳ اعتبار گروه کمتر از ۳ روزه. اعتبار: {fmt_dt_fa(g.expires_at)}", parse_mode=constants.ParseMode.HTML)
                    except Exception:
                        pass
            # birthdays
            users = s.query(User).filter_by(chat_id=g.id).all()
            for u in users:
                if not u.birthday: continue
                jy,jm,jd = g2j(u.birthday.year, u.birthday.month, u.birthday.day)
                if jm==jM and jd==jD:
                    try: await context.bot.send_message(g.id, f"🎂 تولدت مبارک {mention_of(u)}! 🌟")
                    except Exception: pass
            # monthiversaries (by Jalali, clip)
            rels = s.query(Relationship).filter_by(chat_id=g.id).all()
            for r in rels:
                if not r.started_at: continue
                sjy,sjm,sjd = g2j(r.started_at.year, r.started_at.month, r.started_at.day)
                ml = jalali_month_length(jY, jM)
                target = sjd if sjd<=ml else ml
                if target == jD:
                    try:
                        u1 = s.get(User, r.user_a_id); u2 = s.get(User, r.user_b_id)
                        await context.bot.send_message(g.id, f"💞 ماهگرد {mention_of(u1)} و {mention_of(u2)} مبارک!")
                    except Exception: pass

async def job_ship_evening(context: ContextTypes.DEFAULT_TYPE):
    """19:00 Tehran — auto ship between singles"""
    with SessionLocal() as s:
        groups = s.query(Group).all()
        for g in groups:
            if not group_active(g): continue
            males = s.query(User).filter_by(chat_id=g.id, gender="male").all()
            females = s.query(User).filter_by(chat_id=g.id, gender="female").all()
            rels = s.query(Relationship).filter_by(chat_id=g.id).all()
            in_rel = set([r.user_a_id for r in rels] + [r.user_b_id for r in rels])
            males = [u for u in males if u.id not in in_rel]
            females = [u for u in females if u.id not in in_rel]
            if males and females:
                muser = random.choice(males); fuser = random.choice(females)
                try: await context.bot.send_message(g.id, f"💘 شیپ امروز: {mention_of(muser)} × {mention_of(fuser)}")
                except Exception: pass

# ------------ Boot ------------
def build_app()->Application:
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.REPLY & (filters.TEXT | filters.CAPTION), _track_reply_stat), group=1)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT, on_private_text))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT, on_group_text))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT, capture_manual_startlove_date), group=2)
    jq = app.job_queue
    if jq:
        jq.run_daily(job_morning, time=dt.time(9,0,0, tzinfo=TZ_TEHRAN))
        jq.run_daily(job_ship_evening, time=dt.time(19,0,0, tzinfo=TZ_TEHRAN))
    return app

def main():
    if not BOT_TOKEN:
        log.error("Please set BOT_TOKEN."); sys.exit(1)
    log.info("DB: %s", DATABASE_URL)
    app = build_app()
    log.info("Starting Fazol bot ...")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
