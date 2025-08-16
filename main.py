
# -*- coding: utf-8 -*-
"""
Final replacement bot file: main_final_deploy.py
Framework: python-telegram-bot (v20+)
Storage: SQLite via SQLAlchemy
Timezone: Asia/Tehran
All commands are TEXT (no slash). Persian triggers as specified.

ENV:
- BOT_TOKEN: Telegram bot token
- OWNER_ID: Telegram numeric ID of the bot owner (int)

Run:
    pip install python-telegram-bot==20.7 SQLAlchemy==2.0.29 persiantools
    python main_final_deploy.py
"""

import logging
import os
import random
import re
from datetime import datetime, date, time
from typing import Optional, List

from zoneinfo import ZoneInfo

from sqlalchemy import (
    create_engine,
    select,
    func,
    ForeignKey,
    UniqueConstraint,
    Date,
    String,
    Integer,
    Boolean,
    DateTime,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

from telegram import (
    Update,
    ChatMemberOwner,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode, ChatType
from telegram.ext import (
    Application,
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)
from telegram.error import BadRequest

# Optional Persian date parsing
try:
    from persiantools.jdatetime import JalaliDate
    HAS_PTOOLS = True
except Exception:
    HAS_PTOOLS = False

# -------------------- Config & Logging --------------------
logging.basicConfig(
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("relbot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
if not BOT_TOKEN:
    logger.error("You must set BOT_TOKEN environment variable.")
    raise SystemExit(1)

TZ = ZoneInfo("Asia/Tehran")

# -------------------- Database --------------------
class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    tg_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    username: Mapped[Optional[str]] = mapped_column(String, index=True, nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    gender: Mapped[str] = mapped_column(String, default="unknown")  # male/female/unknown
    birthday: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    avatar_file_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    popularity_cache: Mapped[int] = mapped_column(Integer, default=0)
    popularity_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    snoop_credits: Mapped[int] = mapped_column(Integer, default=0)
    is_seller: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(TZ))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(TZ), onupdate=lambda: datetime.now(TZ))

class Group(Base):
    __tablename__ = "groups"
    id: Mapped[int] = mapped_column(primary_key=True)
    chat_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    auto_ship_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(TZ))

class GroupMember(Base):
    __tablename__ = "group_members"
    id: Mapped[int] = mapped_column(primary_key=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("groups.id"))
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    UniqueConstraint("group_id", "user_id")

class GroupAdmin(Base):
    __tablename__ = "group_admins"
    id: Mapped[int] = mapped_column(primary_key=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("groups.id"))
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    role: Mapped[str] = mapped_column(String)  # creator / administrator
    UniqueConstraint("group_id", "user_id")

class Crush(Base):
    __tablename__ = "crushes"
    id: Mapped[int] = mapped_column(primary_key=True)
    from_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    to_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(TZ))
    __table_args__ = (
        UniqueConstraint("from_user_id", "to_user_id", name="uq_crush_pair"),
    )

class Relationship(Base):
    __tablename__ = "relationships"
    id: Mapped[int] = mapped_column(primary_key=True)
    user1_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user2_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    __table_args__ = (
        UniqueConstraint("user1_id", "user2_id", name="uq_rel_pair"),
    )

DB_PATH = os.getenv("DB_PATH", "bot.db")
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)
Base.metadata.create_all(engine)

# -------------------- Text Normalization --------------------
_ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
_PERSIAN_DIGITS = "۰۱۲۳۴۵۶۷۸۹"
_WESTERN_DIGITS = "0123456789"
DIACRITICS = "".join([
    "\u064B", "\u064C", "\u064D", "\u064E", "\u064F", "\u0650", "\u0651", "\u0652", "\u0670"
])

def normalize_fa(s: str) -> str:
    if not s:
        return s
    # unify spaces
    s = s.replace("\u200c", " ")  # ZWNJ -> space
    s = s.replace("\u00A0", " ")  # NBSP
    s = re.sub(r"\s+", " ", s)

    # Arabic to Persian forms
    s = s.replace("ي", "ی").replace("ك", "ک")

    # Remove diacritics
    s = s.translate({ord(d): None for d in DIACRITICS})

    # Convert digits to western
    trans = {}
    for i, ch in enumerate(_ARABIC_DIGITS):
        trans[ord(ch)] = ord(_WESTERN_DIGITS[i])
    for i, ch in enumerate(_PERSIAN_DIGITS):
        trans[ord(ch)] = ord(_WESTERN_DIGITS[i])
    s = s.translate(trans)

    return s.strip()

# -------------------- Utilities --------------------
def now_teh() -> datetime:
    return datetime.now(TZ)

def parse_date_fa_or_en(s: str) -> Optional[date]:
    s = normalize_fa(s or "")
    try:
        if "/" in s:
            parts = s.split("/")
        else:
            parts = s.split("-")
        y, m, d = map(int, parts)
        if HAS_PTOOLS and y < 1700:
            g = JalaliDate(y, m, d).to_gregorian()
            return date(g.year, g.month, g.day)
        else:
            return date(y, m, d)
    except Exception:
        return None

def fmt_date_fa(dt: Optional[date]) -> str:
    if not dt:
        return "—"
    if HAS_PTOOLS:
        jd = JalaliDate.fromgregorian(date=dt)
        return f"{jd.year:04d}/{jd.month:02d}/{jd.day:02d}"
    return dt.strftime("%Y-%m-%d")

def get_or_create_user(session: Session, tg_user) -> User:
    u = session.scalar(select(User).where(User.tg_id == tg_user.id))
    if not u:
        u = User(
            tg_id=tg_user.id,
            username=tg_user.username,
            first_name=tg_user.first_name,
            last_name=tg_user.last_name,
            gender="unknown",
        )
        session.add(u)
        session.commit()
    else:
        changed = False
        if u.username != tg_user.username:
            u.username = tg_user.username; changed = True
        if u.first_name != tg_user.first_name:
            u.first_name = tg_user.first_name; changed = True
        if u.last_name != tg_user.last_name:
            u.last_name = tg_user.last_name; changed = True
        if changed:
            session.commit()
    if OWNER_ID and u.tg_id == OWNER_ID and not u.is_seller:
        u.is_seller = True  # owner can act as seller
        session.commit()
    return u

def get_or_create_group(session: Session, chat) -> Group:
    g = session.scalar(select(Group).where(Group.chat_id == chat.id))
    if not g:
        g = Group(chat_id=chat.id, title=getattr(chat, "title", None), auto_ship_enabled=True)
        session.add(g)
        session.commit()
    else:
        if g.title != getattr(chat, "title", g.title):
            g.title = getattr(chat, "title", g.title)
            session.commit()
    return g

def ensure_group_member(session: Session, group: Group, user: User):
    gm = session.scalar(select(GroupMember).where(GroupMember.group_id == group.id, GroupMember.user_id == user.id))
    if not gm:
        gm = GroupMember(group_id=group.id, user_id=user.id, message_count=0)
        session.add(gm)
        session.commit()
    return gm

def increment_message_count(session: Session, chat, from_user):
    if chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return
    group = get_or_create_group(session, chat)
    user = get_or_create_user(session, from_user)
    gm = ensure_group_member(session, group, user)
    gm.message_count += 1
    group.last_seen_at = now_teh()
    session.commit()

def hlink_for(user: User) -> str:
    if user.username:
        return f"@{user.username}"
    name = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
    name = name.strip() or "کاربر"
    return f'<a href="tg://user?id={user.tg_id}">{name}</a>'

def is_owner(user_id: int) -> bool:
    return OWNER_ID and user_id == OWNER_ID

async def is_group_admin(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    if is_owner(user_id):
        return True
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except BadRequest:
        return False

def resolve_token_to_user(session: Session, token: str) -> Optional[User]:
    token = normalize_fa(token or "")
    if token.startswith("@"):
        uname = token[1:].lower()
        return session.scalar(select(User).where(func.lower(User.username) == uname))
    else:
        try:
            tid = int(token)
        except ValueError:
            return None
        return session.scalar(select(User).where(User.tg_id == tid))

async def cache_avatar_file_id(context: ContextTypes.DEFAULT_TYPE, u: User):
    if u.avatar_file_id:
        return
    try:
        photos = await context.bot.get_user_profile_photos(u.tg_id, limit=1)
        if photos.total_count and photos.photos and photos.photos[0]:
            u.avatar_file_id = photos.photos[0][0].file_id
            with Session(engine) as s:
                dbu = s.scalar(select(User).where(User.id == u.id))
                if dbu:
                    dbu.avatar_file_id = u.avatar_file_id
                    s.commit()
    except Exception as e:
        logger.warning(f"avatar cache failed for {u.tg_id}: {e}")

def popularity_percent(session: Session, user: User) -> int:
    cnt = session.scalar(select(func.count(Crush.id)).where(Crush.to_user_id == user.id)) or 0
    val = min(100, round(10 * (cnt ** 0.5)))
    return val

async def notify_owner(context: ContextTypes.DEFAULT_TYPE, text: str, html: bool = True):
    if not OWNER_ID:
        return
    try:
        if html:
            await context.bot.send_message(chat_id=OWNER_ID, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        else:
            await context.bot.send_message(chat_id=OWNER_ID, text=text, disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"notify_owner failed: {e}")

# -------------------- Triggers --------------------
PAT_GENDER = re.compile(r"^ثبت\s+جنسیت\s+(پسر|دختر)$")
PAT_BDAY = re.compile(r"^ثبت\s+تولد\s+(\d{4}[-/]\d{1,2}[-/]\d{1,2})$")  # relaxed
PAT_PROFILE = re.compile(r"^(نمایش\s+اطلاعات|نمایش\s+پروفایل)(?:\s+@[\w_]+)?$")
PAT_IDONLY = re.compile(r"^آیدی(?:\s+آیدی)?(?:\s+(@[\w_]+|\d+))?$")  # id or id id
PAT_REL_SET = re.compile(r"^(@[\w_]+|\d+)\s+رل\s+(@[\w_]+|\d+)$")
PAT_REL_DEL = re.compile(r"^(@[\w_]+|\d+)\s+حذف\s+رل\s+(@[\w_]+|\d+)$")
PAT_START_REL = re.compile(r"^شروع\s+رابطه(?:\s+(@[\w_]+|\d+))?(?:\s+(\d{4}[-/]\d{1,2}[-/]\d{1,2}))?$")  # username optional
PAT_CRUSH = re.compile(r"^(ثبت\s+کراش|حذف\s+کراش)$")
PAT_SHIPME = re.compile(r"^شیپم\s+کن$")
PAT_TAGS = re.compile(r"^تگ\s+(پسرها|دخترها|همه)$")
PAT_MYCRUSHES = re.compile(r"^کراشام$")
PAT_THEIR = re.compile(r"^(کراشاش|کراشرهاش)$")
PAT_CHARGE = re.compile(r"^شارژ(?:\s+@[\w_]+|\s+\d+)?\s+(\d+)$")
PAT_PANEL = re.compile(r"^(پنل\s+مدیریت|پنل\s+اینجا)$")
PAT_OWNER_PANEL = re.compile(r"^پنل\s+مالک$")
PAT_HELP = re.compile(r"^راهنما$")
PAT_CFG = re.compile(r"^(پیکربندی\s+فضول|به‌روزرسانی\s+مدیران)$")
PAT_AUTOSHIP = re.compile(r"^شیپ\s+خودکار\s+(روشن|خاموش)$")
# Owner PV tools (also via inline)
PAT_GROUP_LIST = re.compile(r"^لیست\s+گروه‌ها$")
PAT_GROUP_AUTOSHIP_SET = re.compile(r"^گروه\s+(-?\d+)\s+شیپ\s+خودکار\s+(روشن|خاموش)$")
PAT_GROUP_REPORT = re.compile(r"^گروه\s+(-?\d+)\s+گزارش$")
PAT_SEND_TO_GROUP = re.compile(r"^ارسال\s+گروه\s+(-?\d+)\s+(.+)$")

# -------------------- Handlers --------------------
async def on_any_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    raw = (update.message.text or "")
    text = normalize_fa(raw)
    if not text:
        return

    logger.info(f"RX chat={update.effective_chat.id} type={update.effective_chat.type} from={update.effective_user.id} text_raw={raw!r} norm={text!r}")

    with Session(engine) as session:
        # Track users/groups
        user = get_or_create_user(session, update.effective_user)
        chat = update.effective_chat
        if chat.type in (ChatType.SUPERGROUP, ChatType.GROUP):
            group = get_or_create_group(session, chat)
            ensure_group_member(session, group, user)
            increment_message_count(session, chat, update.effective_user)

        # Owner send-to-group typing mode
        if is_owner(user.tg_id) and context.user_data.get("send_to_chat_id"):
            target_chat = int(context.user_data.pop("send_to_chat_id"))
            try:
                await context.bot.send_message(chat_id=target_chat, text=raw)
                await update.message.reply_text("ارسال شد.")
                await notify_owner(context, f"LOG: پیام به گروه {target_chat} ارسال شد (از پنل مالک).")
            except Exception as e:
                await update.message.reply_text(f"ارسال ناموفق: {e}")
            return

        # Owner panel & PV tools first (now allowed anywhere for owner)
        if PAT_OWNER_PANEL.match(text):
            await handle_owner_panel(update, context, session, user)
            return
        if PAT_GROUP_LIST.match(text):
            await handle_owner_group_list(update, context, session, user)
            return
        if PAT_GROUP_AUTOSHIP_SET.match(text):
            await handle_owner_group_autoship(update, context, session, user, text)
            return
        if PAT_GROUP_REPORT.match(text):
            await handle_owner_group_report(update, context, session, user, text)
            return
        if PAT_SEND_TO_GROUP.match(text):
            await handle_owner_sendto_group(update, context, session, user, text)
            return

        # Dispatch by patterns (normalized)
        if PAT_GENDER.match(text):
            await handle_gender(update, context, session, user, text)
        elif PAT_BDAY.match(text):
            await handle_birthday(update, context, session, user, text)
        elif PAT_IDONLY.match(text):
            await handle_id_only(update, context, session, user, text)
        elif PAT_PROFILE.match(text):
            await handle_profile(update, context, session, user, text)
        elif PAT_REL_SET.match(text):
            await handle_rel_set(update, context, session, user, text)
        elif PAT_REL_DEL.match(text):
            await handle_rel_del(update, context, session, user, text)
        elif PAT_START_REL.match(text):
            await handle_start_rel(update, context, session, user, text)
        elif PAT_CRUSH.match(text):
            await handle_crush(update, context, session, user, text)
        elif PAT_SHIPME.match(text):
            await handle_shipme(update, context, session, user, text)
        elif PAT_TAGS.match(text):
            await handle_tags(update, context, session, user, text)
        elif PAT_MYCRUSHES.match(text) or PAT_THEIR.match(text):
            await handle_crush_lists(update, context, session, user, text)
        elif PAT_CHARGE.match(text):
            await handle_charge(update, context, session, user, text)
        elif PAT_PANEL.match(text):
            await handle_panels(update, context, session, user, text)
        elif PAT_HELP.match(text):
            await send_help(update, context)
        elif PAT_CFG.match(text):
            await handle_configure(update, context, session, user, text)
        elif PAT_AUTOSHIP.match(text):
            await handle_autoship(update, context, session, user, text)
        else:
            return

# -------------------- Specific feature handlers --------------------
async def handle_gender(update, context, session, actor, text):
    m = PAT_GENDER.match(text)
    val = m.group(1)
    gender = "male" if val == "پسر" else "female"
    target_user = actor
    if update.message.reply_to_message:
        if not await is_group_admin(context, update.effective_chat.id, actor.tg_id) and not is_owner(actor.tg_id):
            return await update.message.reply_text("فقط ادمین‌ها می‌تونن برای دیگری ثبت کنند.")
        r = update.message.reply_to_message.from_user
        target_user = get_or_create_user(session, r)
    target_user.gender = gender
    session.commit()
    await update.message.reply_html(f"جنسیت برای {hlink_for(target_user)} ثبت شد: <b>{val}</b>")
    await notify_owner(context, f"LOG: {hlink_for(actor)} جنسیت {hlink_for(target_user)} را «{val}» کرد.")

async def handle_birthday(update, context, session, actor, text):
    m = PAT_BDAY.match(text)
    datestr = m.group(1)
    d = parse_date_fa_or_en(datestr)
    if not d:
        return await update.message.reply_text("فرمت تاریخ نامعتبر است. نمونه: 2001-07-23 یا 1380/01/01")
    target_user = actor
    if update.message.reply_to_message:
        if not await is_group_admin(context, update.effective_chat.id, actor.tg_id) and not is_owner(actor.tg_id):
            return await update.message.reply_text("فقط ادمین‌ها می‌تونن برای دیگری ثبت کنند.")
        r = update.message.reply_to_message.from_user
        target_user = get_or_create_user(session, r)
    target_user.birthday = d
    session.commit()
    await update.message.reply_html(f"تاریخ تولد برای {hlink_for(target_user)} ثبت شد: <b>{fmt_date_fa(d)}</b>")
    await notify_owner(context, f"LOG: {hlink_for(actor)} تولد {hlink_for(target_user)} را {fmt_date_fa(d)} ثبت کرد.")

async def handle_id_only(update, context, session, actor, text):
    target = actor
    m = PAT_IDONLY.match(text)
    if update.message.reply_to_message:
        target = get_or_create_user(session, update.message.reply_to_message.from_user)
    elif m and m.group(1):
        cand = resolve_token_to_user(session, m.group(1))
        if cand:
            target = cand
    await update.message.reply_html(f"آیدی عددی {hlink_for(target)}: <code>{target.tg_id}</code>")

async def handle_profile(update, context, session, actor, text):
    target_user = actor
    if update.message.reply_to_message:
        r = update.message.reply_to_message.from_user
        target_user = get_or_create_user(session, r)
    else:
        m = re.search(r"@([\w_]+)$", text)
        if m:
            cand = session.scalar(select(User).where(func.lower(User.username) == m.group(1).lower()))
            if cand:
                target_user = cand
    await cache_avatar_file_id(context, target_user)
    pop = popularity_percent(session, target_user)
    info = [
        f"پروفایل {hlink_for(target_user)}",
        f"آیدی عددی: <code>{target_user.tg_id}</code>",
        f"نام: {(target_user.first_name or '')} {(target_user.last_name or '')}".strip(),
        f"یوزرنیم: @{target_user.username}" if target_user.username else "یوزرنیم: —",
        f"جنسیت: {'پسر' if target_user.gender=='male' else ('دختر' if target_user.gender=='female' else 'نامشخص')}",
        f"تولد: {fmt_date_fa(target_user.birthday)}",
        f"محبوبیت: <b>{pop}%</b>",
    ]
    caption = "\n".join(info)
    if target_user.avatar_file_id:
        try:
            await update.message.reply_photo(photo=target_user.avatar_file_id, caption=caption, parse_mode=ParseMode.HTML)
            return
        except Exception:
            pass
    await update.message.reply_html(caption)

async def handle_rel_set(update, context, session, actor, text):
    if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌ها می‌تونن رل تعیین کنند.")
    m = PAT_REL_SET.match(text)
    tok1, tok2 = m.group(1), m.group(2)
    u1 = resolve_token_to_user(session, tok1)
    u2 = resolve_token_to_user(session, tok2)
    if not u1 or not u2:
        return await update.message.reply_text("هر دو طرف باید قبلاً توسط ربات دیده شده باشند (یوزرنیم/آیدی معتبر).")
    if u1.id == u2.id:
        return await update.message.reply_text("طرفین نمی‌تونن یک نفر باشند.")
    a, b = (u1, u2) if u1.id < u2.id else (u2, u1)
    rel = session.scalar(select(Relationship).where(Relationship.user1_id==a.id, Relationship.user2_id==b.id))
    if rel and rel.active:
        return await update.message.reply_html(f"بین {hlink_for(u1)} و {hlink_for(u2)} از قبل رِل فعاله.")
    if not rel:
        rel = Relationship(user1_id=a.id, user2_id=b.id, start_date=date.today(), active=True)
        session.add(rel)
    else:
        rel.active = True
        if not rel.start_date:
            rel.start_date = date.today()
    session.commit()
    await update.message.reply_html(f"رِل ثبت شد بین {hlink_for(u1)} و {hlink_for(u2)} ✨")
    await notify_owner(context, f"LOG: {hlink_for(actor)} رِل بین {hlink_for(u1)} و {hlink_for(u2)} را ست کرد.")

async def handle_rel_del(update, context, session, actor, text):
    if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌ها می‌تونن رل رو حذف کنند.")
    m = PAT_REL_DEL.match(text)
    tok1, tok2 = m.group(1), m.group(2)
    u1 = resolve_token_to_user(session, tok1)
    u2 = resolve_token_to_user(session, tok2)
    if not u1 or not u2:
        return await update.message.reply_text("هر دو طرف باید قبلاً توسط ربات دیده شده باشند (یوزرنیم/آیدی معتبر).")
    a, b = (u1, u2) if u1.id < u2.id else (u2, u1)
    rel = session.scalar(select(Relationship).where(Relationship.user1_id==a.id, Relationship.user2_id==b.id))
    if not rel or not rel.active:
        return await update.message.reply_html(f"بین {hlink_for(u1)} و {hlink_for(u2)} رِلی یافت نشد.")
    rel.active = False
    session.commit()
    await update.message.reply_html(f"رِل بین {hlink_for(u1)} و {hlink_for(u2)} حذف شد.")
    await notify_owner(context, f"LOG: {hlink_for(actor)} رِل بین {hlink_for(u1)} و {hlink_for(u2)} را حذف کرد.")

async def handle_start_rel(update, context, session, actor, text):
    m = PAT_START_REL.match(text)
    tok = m.group(1)
    d = parse_date_fa_or_en(m.group(2)) if m.group(2) else date.today()
    if tok:
        partner = resolve_token_to_user(session, tok)
        if not partner:
            return await update.message.reply_text("طرف مقابل باید قبلاً توسط ربات دیده شده باشد (یوزرنیم/آیدی معتبر).")
    else:
        if not update.message.reply_to_message:
            return await update.message.reply_text("برای شروع رابطه بدون یوزرنیم، باید روی پیام طرف مقابل ریپلای کنی.")
        partner = get_or_create_user(session, update.message.reply_to_message.from_user)
    u_self = actor if not update.message.reply_to_message else get_or_create_user(session, update.message.reply_to_message.from_user)
    if u_self.id == partner.id:
        return await update.message.reply_text("با خودت نمی‌تونی رابطه بزنی :)")
    a, b = (u_self, partner) if u_self.id < partner.id else (partner, u_self)
    rel = session.scalar(select(Relationship).where(Relationship.user1_id==a.id, Relationship.user2_id==b.id))
    if not rel:
        rel = Relationship(user1_id=a.id, user2_id=b.id, start_date=d, active=True)
        session.add(rel)
    else:
        rel.active = True
        rel.start_date = d
    session.commit()
    await update.message.reply_html(f"شروع رابطه ثبت شد بین {hlink_for(u_self)} و {hlink_for(partner)} در تاریخ <b>{fmt_date_fa(d)}</b> 💞")
    await notify_owner(context, f"LOG: {hlink_for(actor)} شروع رابطه بین {hlink_for(u_self)} و {hlink_for(partner)} در {fmt_date_fa(d)} را ثبت کرد.")

async def handle_crush(update, context, session, actor, text):
    if not update.message.reply_to_message:
        return await update.message.reply_text("باید روی پیام شخص ریپلای کنی.")
    target = get_or_create_user(session, update.message.reply_to_message.from_user)
    if target.id == actor.id:
        return await update.message.reply_text("روی خودت کراش ثبت نمی‌شه :)")
    is_set = "ثبت کراش" in text
    if is_set:
        ex = session.scalar(select(Crush).where(Crush.from_user_id==actor.id, Crush.to_user_id==target.id))
        if ex:
            return await update.message.reply_html(f"قبلاً روی {hlink_for(target)} کراش ثبت کردی.")
        cr = Crush(from_user_id=actor.id, to_user_id=target.id)
        session.add(cr)
        session.commit()
        await update.message.reply_html(f"کراش ثبت شد روی {hlink_for(target)} 💘")
        await notify_owner(context, f"LOG: {hlink_for(actor)} روی {hlink_for(target)} کراش ثبت کرد.")
    else:
        cr = session.scalar(select(Crush).where(Crush.from_user_id==actor.id, Crush.to_user_id==target.id))
        if not cr:
            return await update.message.reply_html(f"کراشی روی {hlink_for(target)} ثبت نشده.")
        session.delete(cr)
        session.commit()
        await update.message.reply_html(f"کراش روی {hlink_for(target)} حذف شد.")
        await notify_owner(context, f"LOG: {hlink_for(actor)} کراش روی {hlink_for(target)} را حذف کرد.")

async def handle_shipme(update, context, session, actor, text):
    chat = update.effective_chat
    if chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return await update.message.reply_text("این دستور فقط در گروه کار می‌کند.")
    group = get_or_create_group(session, chat)
    if actor.gender not in ("male", "female"):
        return await update.message.reply_text("اول جنسیتت رو ثبت کن: «ثبت جنسیت پسر/دختر».")
    opposite = "female" if actor.gender == "male" else "male"
    from sqlalchemy import join
    j = join(GroupMember, User, GroupMember.user_id == User.id)
    rows = session.execute(
        select(User).select_from(j).where(GroupMember.group_id==group.id, User.gender==opposite, User.id != actor.id)
    ).scalars().all()
    if not rows:
        return await update.message.reply_text("کسی با جنسیت مناسب در این گروه پیدا نشد.")
    partner = random.choice(rows)
    await update.message.reply_html(f"شیپ شدین: {hlink_for(actor)} ❤️ {hlink_for(partner)}")
    await notify_owner(context, f"LOG: شیپ در گروه {chat.title or chat.id}: {hlink_for(actor)} و {hlink_for(partner)}.")

async def handle_tags(update, context, session, actor, text):
    if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌ها اجازهٔ تگ دارند.")
    if not update.message.reply_to_message:
        return await update.message.reply_text("باید روی یک پیام ریپلای کنی تا تگ ارسال بشه.")
    group = get_or_create_group(session, update.effective_chat)
    which = PAT_TAGS.match(text).group(1)
    gender_filter = None
    if which == "پسرها":
        gender_filter = "male"
    elif which == "دخترها":
        gender_filter = "female"
    from sqlalchemy import join
    j = join(GroupMember, User, GroupMember.user_id == User.id)
    q = select(User).select_from(j).where(GroupMember.group_id==group.id)
    if gender_filter:
        q = q.where(User.gender == gender_filter)
    users = session.execute(q).scalars().all()
    if not users:
        return await update.message.reply_text("کسی پیدا نشد.")
    CHUNK = 6
    mentions = [hlink_for(u) for u in users]
    total = 0
    for i in range(0, len(mentions), CHUNK):
        part = " ".join(mentions[i:i+CHUNK])
        await update.message.reply_html(part, disable_web_page_preview=True)
        total += len(mentions[i:i+CHUNK])
    await notify_owner(context, f"LOG: {hlink_for(actor)} تگ «{which}» را در گروه {group.title or group.chat_id} ارسال کرد ({total} نفر).")

async def handle_crush_lists(update, context, session, actor, text):
    if PAT_MYCRUSHES.match(text):
        rows = session.execute(
            select(User).join(Crush, User.id==Crush.to_user_id).where(Crush.from_user_id==actor.id)
        ).scalars().all()
        if not rows:
            return await update.message.reply_text("هیچ کراشی ثبت نکردی.")
        msg = "کراش‌هات:\n" + "\n".join([f"• {hlink_for(u)}" for u in rows])
        return await update.message.reply_html(msg)
    if not update.message.reply_to_message:
        return await update.message.reply_text("برای دیدن لیست دیگری باید روی پیامش ریپلای کنی.")
    target = get_or_create_user(session, update.message.reply_to_message.from_user)
    if "کراشاش" in text:
        rows = session.execute(
            select(User).join(Crush, User.id==Crush.to_user_id).where(Crush.from_user_id==target.id)
        ).scalars().all()
        if not rows:
            return await update.message.reply_html(f"{hlink_for(target)} هیچ کراشی ثبت نکرده.")
        msg = f"کراش‌های {hlink_for(target)}:\n" + "\n".join([f"• {hlink_for(u)}" for u in rows])
        return await update.message.reply_html(msg)
    else:
        rows = session.execute(
            select(User).join(Crush, User.id==Crush.from_user_id).where(Crush.to_user_id==target.id)
        ).scalars().all()
        if not rows:
            return await update.message.reply_html(f"کسی روی {hlink_for(target)} کراش نداره.")
        msg = f"کراشرهای {hlink_for(target)}:\n" + "\n".join([f"• {hlink_for(u)}" for u in rows])
        return await update.message.reply_html(msg)

async def handle_charge(update, context, session, actor, text):
    if not (is_owner(actor.tg_id) or actor.is_seller):
        return await update.message.reply_text("فقط مالک یا فروشنده می‌تواند شارژ کند.")
    m = PAT_CHARGE.match(text)
    amount = int(m.group(1))
    target = None
    if update.message.reply_to_message:
        target = get_or_create_user(session, update.message.reply_to_message.from_user)
    else:
        m2 = re.search(r"شارژ\s+(@[\w_]+|\d+)\s+\d+$", text)
        if m2:
            target = resolve_token_to_user(session, m2.group(1))
    if not target:
        target = actor
    target.snoop_credits += amount
    session.commit()
    await update.message.reply_html(f"برای {hlink_for(target)} شارژ انجام شد: +{amount}")
    await notify_owner(context, f"LOG: {hlink_for(actor)} برای {hlink_for(target)} {amount} واحد شارژ ثبت کرد.")

async def handle_panels(update, context, session, actor, text):
    is_owner_or_seller = is_owner(actor.tg_id) or actor.is_seller
    if "پنل مدیریت" in text:
        if not is_owner_or_seller:
            return await update.message.reply_text("دسترسی نداری.")
        total_users = session.scalar(select(func.count(User.id))) or 0
        total_groups = session.scalar(select(func.count(Group.id))) or 0
        total_crushes = session.scalar(select(func.count(Crush.id))) or 0
        total_rel = session.scalar(select(func.count(Relationship.id)).where(Relationship.active==True)) or 0
        return await update.message.reply_html(
            f"پنل مدیریت\n"
            f"• کاربران: <b>{total_users}</b>\n"
            f"• گروه‌ها: <b>{total_groups}</b>\n"
            f"• کراش‌ها: <b>{total_crushes}</b>\n"
            f"• رِل‌های فعال: <b>{total_rel}</b>\n"
        )
    else:
        if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return await update.message.reply_text("فقط در گروه.")
        if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner_or_seller):
            return await update.message.reply_text("دسترسی نداری.")
        group = get_or_create_group(session, update.effective_chat)
        members = session.scalar(select(func.count(GroupMember.id)).where(GroupMember.group_id==group.id)) or 0
        return await update.message.reply_html(
            f"پنل اینجا ({group.title or group.chat_id})\n"
            f"• اعضای ثبت‌شده: <b>{members}</b>\n"
            f"• شیپ خودکار: <b>{'روشن' if group.auto_ship_enabled else 'خاموش'}</b>\n"
        )

async def send_help(update, context):
    msg = (
        "راهنما (دستورات متنی):\n"
        "• ثبت جنسیت پسر|دختر\n"
        "• ثبت تولد YYYY-MM-DD یا YYYY/M/D (شمسی/میلادی)\n"
        "• نمایش اطلاعات | نمایش پروفایل | آیدی | آیدی آیدی\n"
        "• شروع رابطه [@partner] [تاریخ]  ← بدون یوزرنیم با ریپلای\n"
        "• ثبت کراش / حذف کراش (فقط با ریپلای)\n"
        "• شیپم کن (گروه)\n"
        "• کراشام | (با ریپلای) کراشاش / کراشرهاش\n"
        "• (ادمین/مالک) @a رل @b | @a حذف رل @b\n"
        "• (ادمین/مالک) تگ پسرها | تگ دخترها | تگ همه (با ریپلای)\n"
        "• (مالک/فروشنده) شارژ [@user] N\n"
        "• پنل مدیریت | پنل اینجا | پنل مالک\n"
        "• پیکربندی فضول | به‌روزرسانی مدیران\n"
    )
    await update.message.reply_text(msg)

async def handle_configure(update, context, session, actor, text):
    chat = update.effective_chat
    if chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return await update.message.reply_text("فقط در گروه.")
    if not (await is_group_admin(context, chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌های گروه اجازهٔ پیکربندی دارند.")
    group = get_or_create_group(session, chat)
    try:
        admins = await context.bot.get_chat_administrators(chat.id)
    except Exception as e:
        return await update.message.reply_text(f"دریافت مدیران ناموفق: {e}")
    session.query(GroupAdmin).filter(GroupAdmin.group_id==group.id).delete()
    session.commit()
    stored = []
    for adm in admins:
        tu = adm.user
        u = get_or_create_user(session, tu)
        role = "creator" if isinstance(adm, ChatMemberOwner) or getattr(adm, "status", "")=="creator" else "administrator"
        ga = GroupAdmin(group_id=group.id, user_id=u.id, role=role)
        session.add(ga); session.commit()
        stored.append(u)
    if not stored:
        return await update.message.reply_text("ادمینی یافت نشد.")
    txt = "مدیران به‌روزرسانی شد:\n" + "\n".join([f"• {hlink_for(u)}" for u in stored])
    await update.message.reply_html(txt)
    await notify_owner(context, f"LOG: پیکربندی مدیران گروه {group.title or group.chat_id} به‌روزرسانی شد ({len(stored)} مدیر).")

async def handle_autoship(update, context, session, actor, text):
    if update.effective_chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return await update.message.reply_text("فقط در گروه.")
    if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌های گروه.")
    group = get_or_create_group(session, update.effective_chat)
    onoff = PAT_AUTOSHIP.match(text).group(1) == "روشن"
    group.auto_ship_enabled = onoff
    session.commit()
    await update.message.reply_html(f"شیپ خودکار: <b>{'روشن' if onoff else 'خاموش'}</b>")
    await notify_owner(context, f"LOG: {hlink_for(actor)} شیپ خودکار گروه {group.title or group.chat_id} را «{'روشن' if onoff else 'خاموش'}» کرد.")

# -------------------- OWNER PANEL (Inline Keyboard) --------------------
def owner_menu_markup(page: int = 0, session: Optional[Session] = None) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("↻ تازه‌سازی", callback_data="op:home")],
        [InlineKeyboardButton("📋 لیست گروه‌ها", callback_data=f"op:gl:{page}")],
    ]
    return InlineKeyboardMarkup(buttons)

async def handle_owner_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    if not is_owner(actor.tg_id):
        return await update.message.reply_text("فقط مالک.")
    text = "پنل مالک — دکمه‌ها را انتخاب کنید."
    await update.message.reply_text(text, reply_markup=owner_menu_markup(0, session))

async def render_group_list(update_or_query, context: ContextTypes.DEFAULT_TYPE, session: Session, page: int):
    PER_PAGE = 5
    groups = session.execute(select(Group).order_by(Group.last_seen_at.desc())).scalars().all()
    total = len(groups)
    start = page * PER_PAGE
    end = min(start + PER_PAGE, total)
    page_groups = groups[start:end]
    lines = [f"📋 لیست گروه‌ها (صفحه {page+1}/{max(1,(total+PER_PAGE-1)//PER_PAGE)}):"]
    kb: List[List[InlineKeyboardButton]] = []
    for g in page_groups:
        status = "روشن" if g.auto_ship_enabled else "خاموش"
        lines.append(f"• {g.title or '—'} | <code>{g.chat_id}</code> | شیپ: <b>{status}</b>")
        kb.append([
            InlineKeyboardButton("گزارش", callback_data=f"op:gr:{g.chat_id}:{page}"),
            InlineKeyboardButton(f"شیپ:{'خاموش' if g.auto_ship_enabled else 'روشن'}", callback_data=f"op:gtoggle:{g.chat_id}:{page}"),
        ])
        kb.append([
            InlineKeyboardButton("به‌روزرسانی مدیران", callback_data=f"op:syncadmins:{g.chat_id}:{page}"),
            InlineKeyboardButton("ارسال پیام…", callback_data=f"op:asksend:{g.chat_id}:{page}"),
        ])
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton("◀️ قبلی", callback_data=f"op:gl:{page-1}"))
    nav.append(InlineKeyboardButton("خانه", callback_data="op:home"))
    if end < total:
        nav.append(InlineKeyboardButton("بعدی ▶️", callback_data=f"op:gl:{page+1}"))
    kb.append(nav)
    text = "\n".join(lines)
    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_html(text, reply_markup=InlineKeyboardMarkup(kb))
    else:
        await update_or_query.edit_message_text(text=text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb), disable_web_page_preview=True)

async def handle_owner_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    with Session(engine) as session:
        user = get_or_create_user(session, query.from_user)
        if not is_owner(user.tg_id):
            return await query.edit_message_text("فقط مالک.")
        parts = data.split(":")
        op = parts[1] if len(parts) > 1 else ""
        if op == "home":
            return await query.edit_message_text("پنل مالک — دکمه‌ها را انتخاب کنید.", reply_markup=owner_menu_markup(0, session))
        if op == "gl":
            page = int(parts[2]) if len(parts) > 2 else 0
            return await render_group_list(query, context, session, page)
        if op == "gr":
            chat_id = int(parts[2]); page = int(parts[3]) if len(parts) > 3 else 0
            g = session.scalar(select(Group).where(Group.chat_id==chat_id))
            if not g:
                return await query.edit_message_text("گروه یافت نشد.")
            members = session.scalar(select(func.count(GroupMember.id)).where(GroupMember.group_id==g.id)) or 0
            male = session.scalar(select(func.count(GroupMember.id)).join(User, User.id==GroupMember.user_id).where(GroupMember.group_id==g.id, User.gender=="male")) or 0
            female = session.scalar(select(func.count(GroupMember.id)).join(User, User.id==GroupMember.user_id).where(GroupMember.group_id==g.id, User.gender=="female")) or 0
            text = (
                f"گزارش گروه {g.title or chat_id}\n"
                f"• اعضای ثبت‌شده: <b>{members}</b>\n"
                f"• پسر: <b>{male}</b> | دختر: <b>{female}</b>\n"
                f"• شیپ خودکار: <b>{'روشن' if g.auto_ship_enabled else 'خاموش'}</b>\n"
                f"• آخرین فعالیت: <code>{g.last_seen_at}</code>"
            )
            kb = [[
                InlineKeyboardButton("بازگشت به لیست", callback_data=f"op:gl:{page}"),
                InlineKeyboardButton(f"شیپ:{'خاموش' if g.auto_ship_enabled else 'روشن'}", callback_data=f"op:gtoggle:{chat_id}:{page}")
            ]]
            return await query.edit_message_text(text=text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        if op == "gtoggle":
            chat_id = int(parts[2]); page = int(parts[3]) if len(parts) > 3 else 0
            g = session.scalar(select(Group).where(Group.chat_id==chat_id))
            if not g:
                return await query.edit_message_text("گروه یافت نشد.")
            g.auto_ship_enabled = not g.auto_ship_enabled
            session.commit()
            await notify_owner(context, f"LOG: شیپ خودکار گروه {g.title or chat_id} {'روشن' if g.auto_ship_enabled else 'خاموش'} شد.")
            return await render_group_list(query, context, session, page)
        if op == "syncadmins":
            chat_id = int(parts[2]); page = int(parts[3]) if len(parts) > 3 else 0
            try:
                admins = await context.bot.get_chat_administrators(chat_id)
            except Exception as e:
                return await query.edit_message_text(f"دریافت مدیران ناموفق: {e}")
            group = session.scalar(select(Group).where(Group.chat_id==chat_id))
            if not group:
                return await query.edit_message_text("گروه ثبت نشده.")
            session.query(GroupAdmin).filter(GroupAdmin.group_id==group.id).delete()
            session.commit()
            stored = []
            for adm in admins:
                tu = adm.user
                u = get_or_create_user(session, tu)
                role = "creator" if getattr(adm, 'status', '') == 'creator' else 'administrator'
                session.add(GroupAdmin(group_id=group.id, user_id=u.id, role=role))
                session.commit()
                stored.append(u)
            await notify_owner(context, f"LOG: پیکربندی مدیران گروه {group.title or chat_id} به‌روزرسانی شد ({len(stored)} مدیر).")
            return await render_group_list(query, context, session, page)
        if op == "asksend":
            chat_id = int(parts[2]); page = int(parts[3]) if len(parts) > 3 else 0
            context.user_data["send_to_chat_id"] = chat_id
            kb = [[InlineKeyboardButton("بازگشت", callback_data=f"op:gl:{page}")]]
            return await query.edit_message_text(f"پیام خود را بفرست تا به گروه <code>{chat_id}</code> ارسال شود.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

# Owner textual tools
async def handle_owner_group_list(update, context, session, actor):
    if not is_owner(actor.tg_id):
        return await update.message.reply_text("فقط مالک.")
    return await render_group_list(update, context, session, 0)

async def handle_owner_group_autoship(update, context, session, actor, text):
    if not is_owner(actor.tg_id):
        return await update.message.reply_text("فقط مالک.")
    m = PAT_GROUP_AUTOSHIP_SET.match(text)
    chat_id = int(m.group(1))
    onoff = m.group(2) == "روشن"
    g = session.scalar(select(Group).where(Group.chat_id == chat_id))
    if not g:
        return await update.message.reply_text("گروه یافت نشد.")
    g.auto_ship_enabled = onoff
    session.commit()
    await update.message.reply_html(f"شیپ خودکار برای <code>{chat_id}</code>: <b>{'روشن' if onoff else 'خاموش'}</b>")
    await notify_owner(context, f"LOG: شیپ خودکار گروه {g.title or chat_id} در پنل مالک «{'روشن' if onoff else 'خاموش'}» شد.", html=True)

async def handle_owner_group_report(update, context, session, actor, text):
    if not is_owner(actor.tg_id):
        return await update.message.reply_text("فقط مالک.")
    m = PAT_GROUP_REPORT.match(text)
    chat_id = int(m.group(1))
    g = session.scalar(select(Group).where(Group.chat_id == chat_id))
    if not g:
        return await update.message.reply_text("گروه یافت نشد.")
    members = session.scalar(select(func.count(GroupMember.id)).where(GroupMember.group_id==g.id)) or 0
    male = session.scalar(select(func.count(GroupMember.id)).join(User, User.id==GroupMember.user_id).where(GroupMember.group_id==g.id, User.gender=="male")) or 0
    female = session.scalar(select(func.count(GroupMember.id)).join(User, User.id==GroupMember.user_id).where(GroupMember.group_id==g.id, User.gender=="female")) or 0
    await update.message.reply_html(
        f"گزارش گروه {g.title or chat_id}\n"
        f"• اعضای ثبت‌شده: <b>{members}</b>\n"
        f"• پسر: <b>{male}</b> | دختر: <b>{female}</b>\n"
        f"• شیپ خودکار: <b>{'روشن' if g.auto_ship_enabled else 'خاموش'}</b>\n"
        f"• آخرین فعالیت: <code>{g.last_seen_at}</code>"
    )

async def handle_owner_sendto_group(update, context, session, actor, text):
    if not is_owner(actor.tg_id):
        return await update.message.reply_text("فقط مالک.")
    m = PAT_SEND_TO_GROUP.match(text)
    chat_id = int(m.group(1))
    message = m.group(2)
    try:
        await context.bot.send_message(chat_id=chat_id, text=message)
        await update.message.reply_text("ارسال شد.")
        await notify_owner(context, f"LOG: پیام به گروه {chat_id} ارسال شد.")
    except Exception as e:
        await update.message.reply_text(f"ارسال ناموفق: {e}")

# -------------------- Scheduled Jobs --------------------
async def job_daily_ship(context: ContextTypes.DEFAULT_TYPE):
    with Session(engine) as session:
        groups = session.execute(select(Group).where(Group.auto_ship_enabled==True)).scalars().all()
        for g in groups:
            try:
                from sqlalchemy import join
                j = join(GroupMember, User, GroupMember.user_id == User.id)
                males = session.execute(select(User).select_from(j).where(GroupMember.group_id==g.id, User.gender=="male")).scalars().all()
                females = session.execute(select(User).select_from(j).where(GroupMember.group_id==g.id, User.gender=="female")).scalars().all()
                if not males or not females:
                    continue
                m = random.choice(males)
                f = random.choice(females)
                text = f"شیپ روز:\n{hlink_for(m)} ❤️ {hlink_for(f)}"
                await context.bot.send_message(chat_id=g.chat_id, text=text, parse_mode=ParseMode.HTML)
                await notify_owner(context, f"LOG: شیپ خودکار در {g.title or g.chat_id}: {hlink_for(m)} ❤️ {hlink_for(f)}")
            except Exception as e:
                logger.warning(f"auto ship failed for {g.chat_id}: {e}")

async def job_daily_birthdays(context: ContextTypes.DEFAULT_TYPE):
    today = now_teh().date()
    with Session(engine) as session:
        users = session.execute(select(User).where(User.birthday != None)).scalars().all()
        for u in users:
            b = u.birthday
            if not b:
                continue
            if b.month == today.month and b.day == today.day:
                gm = session.execute(
                    select(GroupMember, Group).join(Group, GroupMember.group_id==Group.id).where(GroupMember.user_id==u.id).order_by(GroupMember.message_count.desc())
                ).first()
                target_chat_id = gm[1].chat_id if gm else None
                try:
                    msg = f"تولدت مبارک {hlink_for(u)} 🎉🎂"
                    if target_chat_id:
                        await context.bot.send_message(chat_id=target_chat_id, text=msg, parse_mode=ParseMode.HTML)
                    else:
                        await context.bot.send_message(chat_id=u.tg_id, text=msg, parse_mode=ParseMode.HTML)
                    await notify_owner(context, f"LOG: تبریک تولد برای {hlink_for(u)} ارسال شد.")
                except Exception as e:
                    logger.warning(f"birthday congratulate failed for {u.tg_id}: {e}")

# -------------------- Application Setup --------------------
def build_application() -> Application:
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), on_any_message))
    app.add_handler(CallbackQueryHandler(handle_owner_callback, pattern=r"^op:"))
    app.job_queue.run_daily(job_daily_ship, time=time(18, 0, tzinfo=TZ))
    app.job_queue.run_daily(job_daily_birthdays, time=time(9, 0, tzinfo=TZ))
    return app

def main():
    app = build_application()
    logger.info("Bot starting with Tehran timezone scheduling.")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
