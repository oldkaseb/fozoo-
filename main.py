
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
    pip install python-telegram-bot==20.7 SQLAlchemy==2.0.29 pytz persiantools
    python main_final_deploy.py
"""

import asyncio
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, date, time
from typing import Optional, Tuple, List

from zoneinfo import ZoneInfo

from sqlalchemy import (
    create_engine,
    select,
    func,
    ForeignKey,
    UniqueConstraint,
    and_,
    or_,
    Date,
    String,
    Integer,
    Boolean,
    DateTime,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session

from telegram import (
    Update,
    ChatMember,
    ChatMemberAdministrator,
    ChatMemberOwner,
    InputMediaPhoto,
)
from telegram.constants import ParseMode, ChatType
from telegram.ext import (
    Application,
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
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

# -------------------- Utilities --------------------
def now_teh() -> datetime:
    return datetime.now(TZ)

def parse_date_fa_or_en(s: str) -> Optional[date]:
    """Accept YYYY-MM-DD or YYYY/MM/DD (Gregorian). If persian (Jalali) provided, convert if persiantools installed."""
    s = s.strip()
    try:
        # Detect delimiter
        if "/" in s:
            parts = s.split("/")
        else:
            parts = s.split("-")
        y, m, d = map(int, parts)
        if HAS_PTOOLS and y < 1700:
            # Assume Jalali
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
        # Update basic fields
        changed = False
        if u.username != tg_user.username:
            u.username = tg_user.username; changed = True
        if u.first_name != tg_user.first_name:
            u.first_name = tg_user.first_name; changed = True
        if u.last_name != tg_user.last_name:
            u.last_name = tg_user.last_name; changed = True
        if changed:
            session.commit()
    # Owner autoclaim
    if OWNER_ID and u.tg_id == OWNER_ID and not u.is_seller:
        # owner can act as seller too
        u.is_seller = True
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
    # Escape names for HTML
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
    """token = '@username' or '123456' (id). Must have been seen before by the bot."""
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
    # Based on number of people who crushed on this user
    cnt = session.scalar(select(func.count(Crush.id)).where(Crush.to_user_id == user.id)) or 0
    # Smooth function
    val = min(100, round(10 * (cnt ** 0.5)))
    return val

# -------------------- Triggers --------------------
# Regex patterns (Persian)
PAT_GENDER = re.compile(r"^ثبت\s+جنسیت\s+(پسر|دختر)$")
PAT_BDAY = re.compile(r"^ثبت\s+تولد\s+(\d{4}[-/]\d{2}[-/]\d{2})$")
PAT_PROFILE = re.compile(r"^(نمایش\s+اطلاعات|آیدی|نمایش\s+پروفایل)(?:\s+@[\w_]+)?$")
PAT_REL_SET = re.compile(r"^(@[\w_]+|\d+)\s+رل\s+(@[\w_]+|\d+)$")
PAT_REL_DEL = re.compile(r"^(@[\w_]+|\d+)\s+حذف\s+رل\s+(@[\w_]+|\d+)$")
PAT_START_REL = re.compile(r"^شروع\s+رابطه\s+(@[\w_]+|\d+)(?:\s+(\d{4}[-/]\d{2}[-/]\d{2}))?$")
PAT_CRUSH = re.compile(r"^(ثبت\s+کراش|حذف\s+کراش)$")
PAT_SHIPME = re.compile(r"^شیپم\s+کن$")
PAT_TAGS = re.compile(r"^تگ\s+(پسرها|دخترها|همه)$")
PAT_MYCRUSHES = re.compile(r"^کراشام$")
PAT_THEIR = re.compile(r"^(کراشاش|کراشرهاش)$")
PAT_CHARGE = re.compile(r"^شارژ(?:\s+@[\w_]+|\s+\d+)?\s+(\d+)$")
PAT_PANEL = re.compile(r"^(پنل\s+مدیریت|پنل\s+اینجا)$")
PAT_HELP = re.compile(r"^راهنما$")
PAT_CFG = re.compile(r"^(پیکربندی\s+فضول|به‌روزرسانی\s+مدیران)$")
PAT_AUTOSHIP = re.compile(r"^شیپ\s+خودکار\s+(روشن|خاموش)$")

# -------------------- Handlers --------------------
async def on_any_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    text = (update.message.text or "").strip()
    if not text:
        return

    with Session(engine) as session:
        # Track users/groups
        user = get_or_create_user(session, update.effective_user)
        chat = update.effective_chat
        if chat.type in (ChatType.SUPERGROUP, ChatType.GROUP):
            group = get_or_create_group(session, chat)
            ensure_group_member(session, group, user)
            increment_message_count(session, chat, update.effective_user)

        # Dispatch by patterns
        if PAT_GENDER.match(text):
            await handle_gender(update, context, session, user)
        elif PAT_BDAY.match(text):
            await handle_birthday(update, context, session, user)
        elif PAT_PROFILE.match(text):
            await handle_profile(update, context, session, user)
        elif PAT_REL_SET.match(text):
            await handle_rel_set(update, context, session, user)
        elif PAT_REL_DEL.match(text):
            await handle_rel_del(update, context, session, user)
        elif PAT_START_REL.match(text):
            await handle_start_rel(update, context, session, user)
        elif PAT_CRUSH.match(text):
            await handle_crush(update, context, session, user)
        elif PAT_SHIPME.match(text):
            await handle_shipme(update, context, session, user)
        elif PAT_TAGS.match(text):
            await handle_tags(update, context, session, user)
        elif PAT_MYCRUSHES.match(text) or PAT_THEIR.match(text):
            await handle_crush_lists(update, context, session, user)
        elif PAT_CHARGE.match(text):
            await handle_charge(update, context, session, user)
        elif PAT_PANEL.match(text):
            await handle_panels(update, context, session, user)
        elif PAT_HELP.match(text):
            await send_help(update, context)
        elif PAT_CFG.match(text):
            await handle_configure(update, context, session, user)
        elif PAT_AUTOSHIP.match(text):
            await handle_autoship(update, context, session, user)
        else:
            # Not a command we care about
            return

# -------------------- Specific feature handlers --------------------
async def handle_gender(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    text = update.message.text.strip()
    m = PAT_GENDER.match(text)
    val = m.group(1)
    gender = "male" if val == "پسر" else "female"

    target_user = actor
    if update.message.reply_to_message:
        # admin-only when setting for someone else
        if not await is_group_admin(context, update.effective_chat.id, actor.tg_id) and not is_owner(actor.tg_id):
            return await update.message.reply_text("فقط ادمین‌ها می‌تونن برای دیگری ثبت کنند.")
        r = update.message.reply_to_message.from_user
        target_user = get_or_create_user(session, r)

    target_user.gender = gender
    session.commit()
    await update.message.reply_html(f"جنسیت برای {hlink_for(target_user)} ثبت شد: <b>{val}</b>")

async def handle_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    text = update.message.text.strip()
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

async def handle_profile(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    text = update.message.text.strip()
    target_user = actor
    # If username provided explicitly (rare) or reply
    if update.message.reply_to_message:
        # admin can view others; everyone can view reply target too
        r = update.message.reply_to_message.from_user
        target_user = get_or_create_user(session, r)
    else:
        m = re.search(r"@([\w_]+)$", text)
        if m:
            cand = session.scalar(select(User).where(func.lower(User.username) == m.group(1).lower()))
            if cand:
                target_user = cand

    # refresh avatar cache
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
            await update.message.reply_photo(
                photo=target_user.avatar_file_id,
                caption=caption,
                parse_mode=ParseMode.HTML
            )
            return
        except Exception:
            pass
    await update.message.reply_html(caption)

async def handle_rel_set(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌ها می‌تونن رل تعیین کنند.")
    m = PAT_REL_SET.match(update.message.text.strip())
    tok1, tok2 = m.group(1), m.group(2)
    u1 = resolve_token_to_user(session, tok1)
    u2 = resolve_token_to_user(session, tok2)
    if not u1 or not u2:
        return await update.message.reply_text("هر دو طرف باید قبلاً توسط ربات دیده شده باشند (یوزرنیم/آیدی معتبر).")
    if u1.id == u2.id:
        return await update.message.reply_text("طرفین نمی‌تونن یک نفر باشند.")
    # Ensure order (smaller id first) to respect uniqueness
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

async def handle_rel_del(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌ها می‌تونن رل رو حذف کنند.")
    m = PAT_REL_DEL.match(update.message.text.strip())
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

async def handle_start_rel(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    # user for himself (or admin via reply)
    m = PAT_START_REL.match(update.message.text.strip())
    tok = m.group(1)
    d = parse_date_fa_or_en(m.group(2)) if m.group(2) else date.today()
    if update.message.reply_to_message and not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌ها می‌تونن برای دیگری شروع رابطه بزنند.")

    partner = resolve_token_to_user(session, tok)
    if not partner:
        return await update.message.reply_text("طرف مقابل باید قبلاً توسط ربات دیده شده باشد (یوزرنیم/آیدی معتبر).")
    # self must be actor unless admin+reply
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

async def handle_crush(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    if not update.message.reply_to_message:
        return await update.message.reply_text("باید روی پیام شخص ریپلای کنی.")
    target = get_or_create_user(session, update.message.reply_to_message.from_user)
    if target.id == actor.id:
        return await update.message.reply_text("روی خودت کراش ثبت نمی‌شه :)")

    is_set = "ثبت کراش" in update.message.text
    if is_set:
        ex = session.scalar(select(Crush).where(Crush.from_user_id==actor.id, Crush.to_user_id==target.id))
        if ex:
            return await update.message.reply_html(f"قبلاً روی {hlink_for(target)} کراش ثبت کردی.")
        cr = Crush(from_user_id=actor.id, to_user_id=target.id)
        session.add(cr)
        session.commit()
        await update.message.reply_html(f"کراش ثبت شد روی {hlink_for(target)} 💘")
    else:
        cr = session.scalar(select(Crush).where(Crush.from_user_id==actor.id, Crush.to_user_id==target.id))
        if not cr:
            return await update.message.reply_html(f"کراشی روی {hlink_for(target)} ثبت نشده.")
        session.delete(cr)
        session.commit()
        await update.message.reply_html(f"کراش روی {hlink_for(target)} حذف شد.")

async def handle_shipme(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    chat = update.effective_chat
    if chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return await update.message.reply_text("این دستور فقط در گروه کار می‌کند.")
    group = get_or_create_group(session, chat)
    # find opposite gender
    if actor.gender not in ("male", "female"):
        return await update.message.reply_text("اول جنسیتت رو ثبت کن: «ثبت جنسیت پسر/دختر».")
    opposite = "female" if actor.gender == "male" else "male"

    # members of this group with opposite gender
    # join GroupMember -> User
    from sqlalchemy import join
    j = join(GroupMember, User, GroupMember.user_id == User.id)
    rows = session.execute(
        select(User).select_from(j).where(GroupMember.group_id==group.id, User.gender==opposite, User.id != actor.id)
    ).scalars().all()
    if not rows:
        return await update.message.reply_text("کسی با جنسیت مناسب در این گروه پیدا نشد.")
    partner = random.choice(rows)
    await update.message.reply_html(f"شیپ شدین: {hlink_for(actor)} ❤️ {hlink_for(partner)}")

async def handle_tags(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌ها اجازهٔ تگ دارند.")
    if not update.message.reply_to_message:
        return await update.message.reply_text("باید روی یک پیام ریپلای کنی تا تگ ارسال بشه.")
    group = get_or_create_group(session, update.effective_chat)
    which = PAT_TAGS.match(update.message.text.strip()).group(1)
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
    # Chunk mentions to avoid spam
    CHUNK = 6
    mentions = [hlink_for(u) for u in users]
    for i in range(0, len(mentions), CHUNK):
        part = " ".join(mentions[i:i+CHUNK])
        await update.message.reply_html(part, disable_web_page_preview=True)

async def handle_crush_lists(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    text = update.message.text.strip()
    # my list
    if PAT_MYCRUSHES.match(text):
        rows = session.execute(
            select(User).join(Crush, User.id==Crush.to_user_id).where(Crush.from_user_id==actor.id)
        ).scalars().all()
        if not rows:
            return await update.message.reply_text("هیچ کراشی ثبت نکردی.")
        msg = "کراش‌هات:\n" + "\n".join([f"• {hlink_for(u)}" for u in rows])
        return await update.message.reply_html(msg)
    # other's (reply)
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
    else:  # کراشرهاش
        rows = session.execute(
            select(User).join(Crush, User.id==Crush.from_user_id).where(Crush.to_user_id==target.id)
        ).scalars().all()
        if not rows:
            return await update.message.reply_html(f"کسی روی {hlink_for(target)} کراش نداره.")
        msg = f"کراشرهای {hlink_for(target)}:\n" + "\n".join([f"• {hlink_for(u)}" for u in rows])
        return await update.message.reply_html(msg)

async def handle_charge(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    text = update.message.text.strip()
    if not (is_owner(actor.tg_id) or actor.is_seller):
        return await update.message.reply_text("فقط مالک یا فروشنده می‌تواند شارژ کند.")
    m = PAT_CHARGE.match(text)
    amount = int(m.group(1))
    target = None
    if update.message.reply_to_message:
        target = get_or_create_user(session, update.message.reply_to_message.from_user)
    else:
        # optional @username before amount
        m2 = re.search(r"شارژ\s+(@[\w_]+|\d+)\s+\d+$", text)
        if m2:
            target = resolve_token_to_user(session, m2.group(1))
    if not target:
        target = actor
    target.snoop_credits += amount
    session.commit()
    await update.message.reply_html(f"برای {hlink_for(target)} شارژ انجام شد: +{amount}")

async def handle_panels(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    text = update.message.text.strip()
    is_owner_or_seller = is_owner(actor.tg_id) or actor.is_seller
    if "پنل مدیریت" in text:
        if not is_owner_or_seller:
            return await update.message.reply_text("دسترسی نداری.")
        # simple stats
        total_users = session.scalar(select(func.count(User.id))) or 0
        total_groups = session.scalar(select(func.count(Group.id))) or 0
        total_crushes = session.scalar(select(func.count(Crush.id))) or 0
        total_rel = session.scalar(select(func.count(Relationship.id)).where(Relationship.active==True)) or 0
        await update.message.reply_html(
            f"پنل مدیریت\n"
            f"• کاربران: <b>{total_users}</b>\n"
            f"• گروه‌ها: <b>{total_groups}</b>\n"
            f"• کراش‌ها: <b>{total_crushes}</b>\n"
            f"• رِل‌های فعال: <b>{total_rel}</b>\n"
        )
    else:
        # پنل اینجا : group-specific
        if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return await update.message.reply_text("فقط در گروه.")
        if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner_or_seller):
            return await update.message.reply_text("دسترسی نداری.")
        group = get_or_create_group(session, update.effective_chat)
        members = session.scalar(select(func.count(GroupMember.id)).where(GroupMember.group_id==group.id)) or 0
        await update.message.reply_html(
            f"پنل اینجا ({group.title or group.chat_id})\n"
            f"• اعضای ثبت‌شده: <b>{members}</b>\n"
            f"• شیپ خودکار: <b>{'روشن' if group.auto_ship_enabled else 'خاموش'}</b>\n"
        )

async def send_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "راهنما (دستورات متنی):\n"
        "• ثبت جنسیت پسر|دختر\n"
        "• ثبت تولد YYYY-MM-DD یا YYYY/MM/DD (شمسی/میلادی)\n"
        "• نمایش اطلاعات | آیدی | نمایش پروفایل\n"
        "• شروع رابطه @partner [تاریخ]\n"
        "• ثبت کراش / حذف کراش (فقط با ریپلای)\n"
        "• شیپم کن (گروه)\n"
        "• کراشام | (با ریپلای) کراشاش / کراشرهاش\n"
        "• (ادمین/مالک) @a رل @b | @a حذف رل @b\n"
        "• (ادمین/مالک) تگ پسرها | تگ دخترها | تگ همه (با ریپلای)\n"
        "• (مالک/فروشنده) شارژ [@user] N\n"
        "• پنل مدیریت | پنل اینجا\n"
        "• پیکربندی فضول | به‌روزرسانی مدیران\n"
    )
    await update.message.reply_text(msg)

async def handle_configure(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    chat = update.effective_chat
    if chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return await update.message.reply_text("فقط در گروه.")
    # Only admins of the group can run (owner bypass)
    if not (await is_group_admin(context, chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌های گروه اجازهٔ پیکربندی دارند.")

    group = get_or_create_group(session, chat)
    try:
        admins = await context.bot.get_chat_administrators(chat.id)
    except Exception as e:
        return await update.message.reply_text(f"دریافت مدیران ناموفق: {e}")

    # Reset and store
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

async def handle_autoship(update: Update, context: ContextTypes.DEFAULT_TYPE, session: Session, actor: User):
    if update.effective_chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return await update.message.reply_text("فقط در گروه.")
    if not (await is_group_admin(context, update.effective_chat.id, actor.tg_id) or is_owner(actor.tg_id)):
        return await update.message.reply_text("فقط ادمین‌های گروه.")
    group = get_or_create_group(session, update.effective_chat)
    onoff = PAT_AUTOSHIP.match(update.message.text.strip()).group(1) == "روشن"
    group.auto_ship_enabled = onoff
    session.commit()
    await update.message.reply_html(f"شیپ خودکار: <b>{'روشن' if onoff else 'خاموش'}</b>")

# -------------------- Scheduled Jobs --------------------
async def job_daily_ship(context: ContextTypes.DEFAULT_TYPE):
    """18:00 Tehran: for each group with auto_ship_enabled, pick a male+female pair random and announce."""
    with Session(engine) as session:
        groups = session.execute(select(Group).where(Group.auto_ship_enabled==True)).scalars().all()
        for g in groups:
            try:
                # Pick random pair
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
            except Exception as e:
                logger.warning(f"auto ship failed for {g.chat_id}: {e}")

async def job_daily_birthdays(context: ContextTypes.DEFAULT_TYPE):
    """09:00 Tehran: Congratulate users whose birthday is today (by Jalali/Gregorian match of month/day)."""
    today = now_teh().date()
    with Session(engine) as session:
        users = session.execute(select(User).where(User.birthday != None)).scalars().all()
        for u in users:
            b = u.birthday
            if not b:
                continue
            if b.month == today.month and b.day == today.day:
                # Find most active group for this user
                gm = session.execute(
                    select(GroupMember, Group).join(Group, GroupMember.group_id==Group.id).where(GroupMember.user_id==u.id).order_by(GroupMember.message_count.desc())
                ).first()
                target_chat_id = None
                if gm:
                    target_chat_id = gm[1].chat_id
                try:
                    msg = f"تولدت مبارک {hlink_for(u)} 🎉🎂"
                    if target_chat_id:
                        await context.bot.send_message(chat_id=target_chat_id, text=msg, parse_mode=ParseMode.HTML)
                    else:
                        # Try private
                        await context.bot.send_message(chat_id=u.tg_id, text=msg, parse_mode=ParseMode.HTML)
                except Exception as e:
                    logger.warning(f"birthday congratulate failed for {u.tg_id}: {e}")

# -------------------- Application Setup --------------------
def build_application() -> Application:
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # single message handler (text only)
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), on_any_message))

    # Jobs (JobQueue runs inside run_polling)
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
