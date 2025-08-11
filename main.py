# main.py
# -*- coding: utf-8 -*-
import os
import re
import asyncio
import random
import datetime as dt
from typing import Optional, List, Tuple

import asyncpg
import jdatetime
from pytz import timezone

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChatMemberUpdated,
    ChatMemberAdministrator,
)
from telegram.constants import ParseMode, ChatType
from telegram.ext import (
    ApplicationBuilder,
    AIORateLimiter,
    MessageHandler,
    ChatMemberHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ----------------------- Config -----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

DEFAULT_TZ = "Asia/Tehran"  # قابل تغییر از تنظیمات گروه
ACTIVE_PING_INTERVAL_MIN = 30  # هر 30 دقیقه

PLANS = {
    "WEEK": {"days": 7, "price": 1000},
    "MONTH": {"days": 30, "price": 3000},
    "QUARTER": {"days": 90, "price": 8000},
}

# ----------------------- Globals -----------------------
DB: Optional[asyncpg.Pool] = None
APP = None

# ----------------------- Utils -----------------------
def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()

def mention_user(user) -> str:
    name = (user.first_name or "") + " " + (user.last_name or "")
    name = name.strip() or (user.username and f"@{user.username}") or "کاربر"
    return f"[{name}](tg://user?id={user.id})"

def now_in_tz(tz: str) -> dt.datetime:
    return dt.datetime.now(timezone(tz))

def jalali_to_gregorian(jalali_str: str) -> dt.date:
    # jalali_str: "YYYY/MM/DD" یا "YYYY-MM-DD"
    s = jalali_str.replace("-", "/").strip()
    y, m, d = [int(x) for x in s.split("/")]
    jd = jdatetime.date(y, m, d)
    gd = jd.togregorian()
    return dt.date(gd.year, gd.month, gd.day)

def gregorian_to_jalali(gdate: dt.date) -> str:
    jd = jdatetime.date.fromgregorian(date=gdate)
    return f"{jd.year:04d}/{jd.month:02d}/{jd.day:02d}"

async def is_group_admin(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        admins = await ctx.bot.get_chat_administrators(chat_id)
        return any(a.user.id == user_id for a in admins)
    except Exception:
        return False

async def get_creator_mention(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int) -> str:
    try:
        admins = await ctx.bot.get_chat_administrators(chat_id)
        creator = next((a for a in admins if isinstance(a, ChatMemberAdministrator) and a.can_manage_chat and a.status == "creator"), None)
        if not creator:
            creator = next((a for a in admins if getattr(a, "status", "") == "creator"), None)
        return mention_user(creator.user) if creator else "ادمین‌ها"
    except Exception:
        return "ادمین‌ها"

# ----------------------- DB: migrations -----------------------
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS users (
  id BIGINT PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  role TEXT CHECK (role IN ('OWNER','SELLER','NONE')) DEFAULT 'NONE',
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS groups (
  chat_id BIGINT PRIMARY KEY,
  title TEXT,
  seller_id BIGINT REFERENCES users(id),
  added_by BIGINT REFERENCES users(id),
  expires_at TIMESTAMPTZ,
  status TEXT CHECK (status IN ('TRIAL','ACTIVE','EXPIRED','LEFT')) DEFAULT 'TRIAL',
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS group_settings (
  chat_id BIGINT PRIMARY KEY REFERENCES groups(chat_id) ON DELETE CASCADE,
  timezone TEXT DEFAULT 'Asia/Tehran',
  greetings BOOLEAN DEFAULT TRUE,
  night_silence BOOLEAN DEFAULT TRUE,
  active_ping_interval_minutes INT DEFAULT 30
);

CREATE TABLE IF NOT EXISTS group_members (
  chat_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  gender TEXT CHECK (gender IN ('MALE','FEMALE')) NULL,
  relation_status TEXT CHECK (relation_status IN ('SINGLE','IN_RELATION')) DEFAULT 'SINGLE',
  partner_user_id BIGINT NULL,
  relation_since DATE NULL,
  relation_since_jalali TEXT NULL,
  birthday DATE NULL,
  birthday_jalali TEXT NULL,
  is_active BOOLEAN DEFAULT TRUE,
  last_seen_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS message_stats (
  chat_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  day DATE NOT NULL,
  messages INT DEFAULT 0,
  replies INT DEFAULT 0,
  PRIMARY KEY (chat_id, user_id, day)
);

CREATE TABLE IF NOT EXISTS reply_edges (
  chat_id BIGINT NOT NULL,
  from_user BIGINT NOT NULL,
  to_user BIGINT NOT NULL,
  weight INT DEFAULT 0,
  last_interaction TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (chat_id, from_user, to_user)
);

CREATE TABLE IF NOT EXISTS picks_log (
  id BIGSERIAL PRIMARY KEY,
  chat_id BIGINT NOT NULL,
  type TEXT CHECK (type IN ('ACTIVE_PING','SHIP','BROS_BOYS','BROS_GIRLS','BIRTHDAY','ANNIVERSARY')),
  user_ids BIGINT[] NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- ایندکس‌های مفید
CREATE INDEX IF NOT EXISTS idx_message_stats_day ON message_stats (chat_id, day);
CREATE INDEX IF NOT EXISTS idx_reply_edges_w ON reply_edges (chat_id, weight DESC);
CREATE INDEX IF NOT EXISTS idx_picks_log_time ON picks_log (chat_id, type, created_at DESC);
"""

async def db_init_pool():
    global DB
    DB = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

async def db_migrate():
    async with DB.acquire() as conn:
        await conn.execute(CREATE_TABLES_SQL)
        # ثبت مالک
        if OWNER_ID:
            await conn.execute(
                """
                INSERT INTO users (id, role) VALUES ($1,'OWNER')
                ON CONFLICT (id) DO UPDATE SET role='OWNER'
                """,
                OWNER_ID,
            )

# ----------------------- DB: helpers -----------------------
async def db_upsert_group(chat_id: int, title: str, added_by: Optional[int]):
    async with DB.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO groups (chat_id, title, added_by, status, created_at, updated_at)
            VALUES ($1,$2,$3,'TRIAL',now(),now())
            ON CONFLICT (chat_id) DO UPDATE SET title=EXCLUDED.title, updated_at=now()
            """,
            chat_id, title, added_by,
        )
        await conn.execute(
            """
            INSERT INTO group_settings (chat_id) VALUES ($1)
            ON CONFLICT (chat_id) DO NOTHING
            """,
            chat_id,
        )

async def db_get_all_groups() -> List[asyncpg.Record]:
    async with DB.acquire() as conn:
        rows = await conn.fetch("SELECT g.chat_id, s.timezone, s.active_ping_interval_minutes FROM groups g JOIN group_settings s ON s.chat_id=g.chat_id WHERE COALESCE(g.status,'ACTIVE')!='LEFT'")
        return rows

async def db_get_group_tz(chat_id: int) -> str:
    async with DB.acquire() as conn:
        row = await conn.fetchrow("SELECT timezone FROM group_settings WHERE chat_id=$1", chat_id)
        return row["timezone"] if row else DEFAULT_TZ

async def db_upsert_member(chat_id: int, u):
    async with DB.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO group_members (chat_id,user_id,username,first_name,last_name,is_active,last_seen_at)
            VALUES ($1,$2,$3,$4,$5,TRUE,now())
            ON CONFLICT (chat_id,user_id) DO UPDATE SET
              username=EXCLUDED.username, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, is_active=TRUE, last_seen_at=now()
            """,
            chat_id, u.id, u.username, u.first_name, u.last_name
        )

async def db_add_message_stat(chat_id: int, user_id: int, is_reply: bool, reply_to_user: Optional[int]):
    day = dt.date.today()
    async with DB.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO message_stats (chat_id,user_id,day,messages,replies)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (chat_id,user_id,day) DO UPDATE SET
              messages=message_stats.messages + EXCLUDED.messages,
              replies=message_stats.replies + EXCLUDED.replies
            """,
            chat_id, user_id, day, 1, 1 if is_reply else 0
        )
        if is_reply and reply_to_user and reply_to_user != user_id:
            await conn.execute(
                """
                INSERT INTO reply_edges (chat_id,from_user,to_user,weight,last_interaction)
                VALUES ($1,$2,$3,1,now())
                ON CONFLICT (chat_id,from_user,to_user) DO UPDATE SET
                  weight=reply_edges.weight+1, last_interaction=now()
                """,
                chat_id, user_id, reply_to_user
            )

async def db_set_gender(chat_id: int, user_id: int, gender: str):
    async with DB.acquire() as conn:
        await conn.execute(
            "UPDATE group_members SET gender=$1 WHERE chat_id=$2 AND user_id=$3",
            gender, chat_id, user_id
        )

async def db_set_birthday(chat_id: int, user_id: int, gdate: dt.date, jstr: str):
    async with DB.acquire() as conn:
        await conn.execute(
            """
            UPDATE group_members SET birthday=$1, birthday_jalali=$2 WHERE chat_id=$3 AND user_id=$4
            """,
            gdate, jstr, chat_id, user_id
        )

async def db_set_relation_single(chat_id: int, user_id: int):
    async with DB.acquire() as conn:
        await conn.execute(
            """
            UPDATE group_members SET relation_status='SINGLE', partner_user_id=NULL, relation_since=NULL, relation_since_jalali=NULL
            WHERE chat_id=$1 AND user_id=$2
            """,
            chat_id, user_id
        )

async def db_set_relation_pair(chat_id: int, a: int, b: int, since_g: dt.date, since_j: str):
    async with DB.acquire() as conn:
        async with conn.transaction():
            for uid in (a, b):
                await conn.execute(
                    """
                    UPDATE group_members
                    SET relation_status='IN_RELATION', partner_user_id=$1, relation_since=$2, relation_since_jalali=$3
                    WHERE chat_id=$4 AND user_id=$5
                    """,
                    b if uid == a else a, since_g, since_j, chat_id, uid
                )

async def db_pick_active_member(chat_id: int) -> Optional[int]:
    async with DB.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH recent AS (
              SELECT user_id, SUM(messages) + 2*SUM(replies) AS score
              FROM message_stats
              WHERE chat_id=$1 AND day >= (CURRENT_DATE - INTERVAL '1 day')
              GROUP BY user_id
            ),
            excluded AS (
              SELECT unnest(user_ids) AS uid
              FROM picks_log
              WHERE chat_id=$1 AND type='ACTIVE_PING' AND created_at > now() - INTERVAL '6 hours'
            )
            SELECT r.user_id, r.score
            FROM recent r
            WHERE r.score > 0 AND r.user_id NOT IN (SELECT uid FROM excluded)
            ORDER BY r.score DESC
            LIMIT 50
            """,
            chat_id
        )
        if not rows:
            return None
        # weighted choice
        users = [r["user_id"] for r in rows]
        weights = [max(1, r["score"]) for r in rows]
        pick = random.choices(users, weights=weights, k=1)[0]
        await conn.execute(
            "INSERT INTO picks_log (chat_id, type, user_ids) VALUES ($1,'ACTIVE_PING',ARRAY[$2])",
            chat_id, pick
        )
        return pick

async def db_get_registered_by_gender(chat_id: int, gender: str) -> List[int]:
    async with DB.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id FROM group_members WHERE chat_id=$1 AND gender=$2",
            chat_id, gender
        )
        return [r["user_id"] for r in rows]

async def db_get_birthdays_today(chat_id: int, j_today: jdatetime.date) -> List[int]:
    j_mm_dd = f"{j_today.month:02d}/{j_today.day:02d}"
    async with DB.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT user_id FROM group_members
            WHERE chat_id=$1 AND birthday_jalali IS NOT NULL
              AND substring(birthday_jalali from 6 for 5) = $2
            """,
            chat_id, j_mm_dd
        )
        return [r["user_id"] for r in rows]

async def db_get_rel_anniversaries(chat_id: int, j_today: jdatetime.date) -> List[Tuple[int,int,str]]:
    j_mm_dd = f"{j_today.month:02d}/{j_today.day:02d}"
    async with DB.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT a.user_id AS a_id, b.user_id AS b_id, a.relation_since_jalali AS since_j
            FROM group_members a
            JOIN group_members b
              ON b.chat_id=a.chat_id AND b.user_id=a.partner_user_id
            WHERE a.chat_id=$1
              AND a.relation_status='IN_RELATION'
              AND b.relation_status='IN_RELATION'
              AND a.user_id < b.user_id  -- هر رابطه یکبار
              AND a.relation_since_jalali IS NOT NULL
              AND substring(a.relation_since_jalali from 6 for 5) = $2
            """,
            chat_id, j_mm_dd
        )
        return [(r["a_id"], r["b_id"], r["since_j"]) for r in rows]

async def db_extend_group(chat_id: int, days: int):
    async with DB.acquire() as conn:
        await conn.execute(
            """
            UPDATE groups
            SET expires_at = COALESCE(expires_at, now()) + ($2 || ' days')::interval,
                status = 'ACTIVE',
                updated_at = now()
            WHERE chat_id=$1
            """,
            chat_id, days
        )

async def db_get_group_title(chat_id: int) -> str:
    async with DB.acquire() as conn:
        row = await conn.fetchrow("SELECT title FROM groups WHERE chat_id=$1", chat_id)
        return row["title"] if row and row["title"] else f"{chat_id}"

# ----------------------- Keyboards -----------------------
def kb_gender():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👩 دختر", callback_data="gender:FEMALE"),
         InlineKeyboardButton("👨 پسر", callback_data="gender:MALE")]
    ])

def kb_relation_state():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("سینگل", callback_data="rel:SINGLE"),
         InlineKeyboardButton("در رابطه‌ام", callback_data="rel:IN")]
    ])

def kb_birthday_hint():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("نمونه: 1403/05/20", callback_data="noop")]
    ])

def kb_charge_plans():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("ماهانه", callback_data="charge:MONTH"),
         InlineKeyboardButton("سه‌ماهه", callback_data="charge:QUARTER")],
        [InlineKeyboardButton("هفتگی", callback_data="charge:WEEK")]
    ])

def kb_lists():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎂 تولدها", callback_data="list:birthdays"),
         InlineKeyboardButton("❤️ رابطه‌ها", callback_data="list:relations")],
        [InlineKeyboardButton("🚶 سینگل‌ها", callback_data="list:singles")],
        [InlineKeyboardButton("👩 دخترها", callback_data="list:girls"),
         InlineKeyboardButton("👨 پسرها", callback_data="list:boys")]
    ])

# ----------------------- Jobs -----------------------
async def job_active_ping(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    tz = await db_get_group_tz(chat_id)
    local_now = now_in_tz(tz)
    # سکوت شبانه: ارسال فقط بین 10:00 تا 01:00
    if not (local_now.time() >= dt.time(10, 0) or local_now.time() <= dt.time(1, 0)):
        return
    user_id = await db_pick_active_member(chat_id)
    if not user_id:
        return
    try:
        cm = await context.bot.get_chat_member(chat_id, user_id)
        lines = [
            "یه چیزی بگو ببینیم چه خبره! 🎯",
            "خیلی ساکت شدی... حرف بزن ببینیم! 🙃",
            "حواسم بهت هست 😏 چی تو چنته داری امروز؟",
        ]
        await context.bot.send_message(
            chat_id, f"{mention_user(cm.user)} {random.choice(lines)}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        pass

async def job_ship(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    girls = await db_get_registered_by_gender(chat_id, "FEMALE")
    boys = await db_get_registered_by_gender(chat_id, "MALE")
    if not girls or not boys:
        return
    g = random.choice(girls)
    b = random.choice(boys)
    gu = (await context.bot.get_chat_member(chat_id, g)).user
    bu = (await context.bot.get_chat_member(chat_id, b)).user
    await context.bot.send_message(
        chat_id, f"شیپ امشب: {mention_user(gu)} ❤️ {mention_user(bu)}\nکامنت بذارید ببینیم به هم میاین یا نه! 😍",
        parse_mode=ParseMode.MARKDOWN
    )

async def job_bros_boys(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    boys = await db_get_registered_by_gender(chat_id, "MALE")
    if len(boys) < 2:
        return
    a, b = random.sample(boys, 2)
    au = (await context.bot.get_chat_member(chat_id, a)).user
    bu = (await context.bot.get_chat_member(chat_id, b)).user
    await context.bot.send_message(
        chat_id, f"رفقای امروز (پسرا): {mention_user(au)} 🤝 {mention_user(bu)}",
        parse_mode=ParseMode.MARKDOWN
    )

async def job_bros_girls(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    girls = await db_get_registered_by_gender(chat_id, "FEMALE")
    if len(girls) < 2:
        return
    a, b = random.sample(girls, 2)
    au = (await context.bot.get_chat_member(chat_id, a)).user
    bu = (await context.bot.get_chat_member(chat_id, b)).user
    await context.bot.send_message(
        chat_id, f"رفقای امروز (دخترا): {mention_user(au)} 🤝 {mention_user(bu)}",
        parse_mode=ParseMode.MARKDOWN
    )

async def job_birthdays(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    tz = await db_get_group_tz(chat_id)
    j_today = jdatetime.date.fromgregorian(date=now_in_tz(tz).date())
    uids = await db_get_birthdays_today(chat_id, j_today)
    if not uids:
        return
    mentions = []
    for uid in uids:
        u = (await context.bot.get_chat_member(chat_id, uid)).user
        mentions.append(mention_user(u))
    creator = await get_creator_mention(context, chat_id)
    await context.bot.send_message(
        chat_id,
        f"🎂 تولد مبارک {', '.join(mentions)}!\n{creator} یه جشنی بگیر براشون! 🥳",
        parse_mode=ParseMode.MARKDOWN
    )

async def job_anniversaries(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    tz = await db_get_group_tz(chat_id)
    j_today = jdatetime.date.fromgregorian(date=now_in_tz(tz).date())
    rels = await db_get_rel_anniversaries(chat_id, j_today)
    for a_id, b_id, since_j in rels:
        sj = since_j.replace("-", "/")
        y, m, d = [int(x) for x in sj.split("/")]
        rs = jdatetime.date(y, m, d)
        months = (j_today.year - rs.year) * 12 + (j_today.month - rs.month)
        au = (await context.bot.get_chat_member(chat_id, a_id)).user
        bu = (await context.bot.get_chat_member(chat_id, b_id)).user
        text = f"💞 ماهگرد {months} ماهه مبارک {mention_user(au)} و {mention_user(bu)}!"
        if j_today.month == rs.month and j_today.day == rs.day and (j_today.year - rs.year) >= 1:
            years = j_today.year - rs.year
            text += f"\n✨ سالگرد {years} ساله هم مبارک!"
        await context.bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN)

async def job_greeting(context: ContextTypes.DEFAULT_TYPE, which: str):
    chat_id = context.job.chat_id
    text = "صبح بخیر 🌅" if which == "morning" else ("ظهر بخیر ☀️" if which == "noon" else "عصر بخیر 🌇")
    await context.bot.send_message(chat_id, text)

# ----------------------- Handlers -----------------------
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmu: ChatMemberUpdated = update.my_chat_member
    if cmu.new_chat_member.user.id != context.bot.id:
        return
    if cmu.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    if cmu.new_chat_member.status in ("member", "administrator"):
        await db_upsert_group(cmu.chat.id, cmu.chat.title or str(cmu.chat.id), cmu.from_user.id if cmu.from_user else None)
        await context.bot.send_message(
            cmu.chat.id,
            "سلام! من فضول گروهم 😎\nبرای شروع بگو: «ثبت جنسیت»، «ثبت تولد»، «ثبت رابطه»"
        )
        # ثبت جاب‌ها برای این گروه
        await schedule_group_jobs(context.application, cmu.chat.id)

async def on_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    if not update.message or not update.message.text:
        return

    chat_id = update.effective_chat.id
    user = update.effective_user
    msg = norm(update.message.text)

    # ثبت گروه/عضو/آمار
    await db_upsert_group(chat_id, update.effective_chat.title or str(chat_id), None)
    await db_upsert_member(chat_id, user)
    is_reply = bool(update.message.reply_to_message and getattr(update.message.reply_to_message, "from_user", None))
    reply_to_user = update.message.reply_to_message.from_user.id if is_reply else None
    await db_add_message_stat(chat_id, user.id, is_reply, reply_to_user)

    # سه دستور مجاز با / هم پشتیبانی می‌شوند، ولی همه‌چیز پیام‌محور هم کار می‌کند
    # ثبت جنسیت
    if "ثبت جنسیت" in msg or msg.startswith("/gender"):
        await update.message.reply_text("جنسیتت رو انتخاب کن:", reply_markup=kb_gender())
        return

    # ثبت تولد
    if "ثبت تولد" in msg or msg.startswith("/birthday"):
        await update.message.reply_text("تاریخ تولدت رو به شمسی بفرست مثل 1400/01/31", reply_markup=kb_birthday_hint())
        context.user_data["await_birthday"] = True
        return

    if context.user_data.get("await_birthday"):
        try:
            gdate = jalali_to_gregorian(update.message.text)
            jstr = update.message.text.replace("-", "/").strip()
            await db_set_birthday(chat_id, user.id, gdate, jstr)
            await update.message.reply_text(f"ثبت شد! تولد: {jstr} (شمسی) | {gdate.isoformat()} (میلادی)")
            context.user_data["await_birthday"] = False
        except Exception:
            await update.message.reply_text("فرمت تاریخ درست نیست. نمونه: 1403/05/20")
        return

    # ثبت رابطه
    if "ثبت رابطه" in msg or msg.startswith("/relation"):
        await update.message.reply_text("وضعیتتو انتخاب کن:", reply_markup=kb_relation_state())
        context.user_data["relation_flow"] = {"state": "choose"}
        return

    # ادامه فلو رابطه: انتظار پارتنر
    if context.user_data.get("relation_flow", {}).get("state") == "await_partner":
        ents = update.message.entities or []
        mentioned = [e.user.id for e in ents if getattr(e, "user", None)]
        if update.message.reply_to_message and mentioned:
            context.user_data["relation_flow"]["partner_id"] = mentioned[0]
            await update.message.reply_text("تاریخ شروع رابطه به شمسی؟ مثل 1402/08/15")
            context.user_data["relation_flow"]["state"] = "await_since"
        else:
            await update.message.reply_text("باید روی پیام من ریپلای کنی و پارتنرت رو منشن کنی.")
        return

    if context.user_data.get("relation_flow", {}).get("state") == "await_since":
        try:
            since_g = jalali_to_gregorian(update.message.text)
            since_j = update.message.text.replace("-", "/").strip()
            partner_id = context.user_data["relation_flow"]["partner_id"]
            await db_set_relation_pair(chat_id, user.id, partner_id, since_g, since_j)
            await update.message.reply_text("ثبت شد! خوشبخت باشین ❤️")
            context.user_data["relation_flow"] = {}
        except Exception:
            await update.message.reply_text("فرمت تاریخ درست نیست. نمونه: 1403/05/20")
        return

    # فضول پنل (فقط مالک/فروشنده/ادمین)
    if "فضول پنل" in msg:
        is_admin = await is_group_admin(context, chat_id, user.id)
        role = "OWNER" if user.id == OWNER_ID else "SELLER"  # MVP: تشخیص فروشنده واقعی را بعداً از جدول users.role بخوان
        if not (is_admin or role in ("OWNER", "SELLER")):
            await update.message.reply_text("دسترسی پنل فقط برای ادمین/مالک/فروشنده است.")
            return
        await update.message.reply_text(
            "پنل فضول:\n- شارژ\n- لیست‌ها\n- تنظیمات (به‌زودی)",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("شارژ", callback_data="panel:charge"),
                 InlineKeyboardButton("لیست‌ها", callback_data="panel:lists")],
                [InlineKeyboardButton("تنظیمات", callback_data="panel:settings")]
            ])
        )
        return

    # فضول شارژ +عدد
    m = re.search(r"فضول\s*شارژ\s*\+?(\d+)", msg)
    if m:
        days = int(m.group(1))
        if not await is_group_admin(context, chat_id, user.id) and user.id != OWNER_ID:
            await update.message.reply_text("فقط ادمین‌ها می‌تونن شارژ کنن.")
            return
        await db_extend_group(chat_id, days)
        title = await db_get_group_title(chat_id)
        await update.message.reply_text(f"✅ گروه «{title}» {days} روز تمدید شد.")
        return

    if "فضول شارژ" in msg:
        await update.message.reply_text("یکی از پلن‌ها رو انتخاب کن:", reply_markup=kb_charge_plans())
        return

    if "فضول لیست" in msg:
        await update.message.reply_text("کدوم لیست رو می‌خوای؟", reply_markup=kb_lists())
        return

    # تگ‌ها
    if "تگ دختر" in msg:
        girls = await db_get_registered_by_gender(chat_id, "FEMALE")
        await tag_in_batches(context, chat_id, girls, "👩 تگ دخترها:")
        return

    if "تگ پسر" in msg:
        boys = await db_get_registered_by_gender(chat_id, "MALE")
        await tag_in_batches(context, chat_id, boys, "👨 تگ پسرها:")
        return

    if "تگ همه" in msg:
        if not await is_group_admin(context, chat_id, user.id) and user.id != OWNER_ID:
            await update.message.reply_text("فقط ادمین‌ها اجازه «تگ همه» دارن.")
            return
        async with DB.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM group_members WHERE chat_id=$1 AND is_active=TRUE", chat_id)
        everyone = [r["user_id"] for r in rows]
        await tag_in_batches(context, chat_id, everyone, "📣 تگ همه:")
        return

async def tag_in_batches(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_ids: List[int], header: str, batch: int = 10):
    if not user_ids:
        await context.bot.send_message(chat_id, "لیستی برای تگ کردن پیدا نشد.")
        return
    await context.bot.send_message(chat_id, header)
    for i in range(0, len(user_ids), batch):
        chunk = user_ids[i:i+batch]
        mentions = []
        for uid in chunk:
            try:
                u = (await context.bot.get_chat_member(chat_id, uid)).user
                mentions.append(mention_user(u))
            except Exception:
                pass
        if mentions:
            await context.bot.send_message(chat_id, " ".join(mentions), parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(2.5)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    chat_id = q.message.chat.id
    user_id = q.from_user.id

    if data.startswith("gender:"):
        g = data.split(":")[1]
        await db_set_gender(chat_id, user_id, g)
        await q.edit_message_text(f"ثبت شد: {'دختر' if g=='FEMALE' else 'پسر'}")

    elif data.startswith("rel:"):
        val = data.split(":")[1]
        if val == "SINGLE":
            await db_set_relation_single(chat_id, user_id)
            await q.edit_message_text("ثبت شد: سینگل ✅")
            context.user_data["relation_flow"] = {}
        else:
            await q.edit_message_text("روی همین پیام من ریپلای کن و پارتنرت رو منشن کن.")
            context.user_data["relation_flow"] = {"state": "await_partner"}

    elif data.startswith("charge:"):
        plan = data.split(":")[1]
        days = PLANS[plan]["days"]
        # دسترسی
        if not await is_group_admin(context, chat_id, user_id) and user_id != OWNER_ID:
            await q.edit_message_text("فقط ادمین‌ها می‌تونن شارژ کنن.")
            return
        await db_extend_group(chat_id, days)
        await q.edit_message_text(f"✅ پلن {plan} ({days} روز) اعمال شد.")

    elif data == "panel:charge":
        await q.edit_message_text("یکی از پلن‌ها رو انتخاب کن:", reply_markup=kb_charge_plans())

    elif data == "panel:lists":
        await q.edit_message_text("کدوم لیست؟", reply_markup=kb_lists())

    elif data == "panel:settings":
        await q.edit_message_text("تنظیمات به‌زودی اضافه می‌شه.")

    elif data.startswith("list:"):
        kind = data.split(":")[1]
        await show_list(context, chat_id, kind, q)

    elif data == "noop":
        pass

async def show_list(context: ContextTypes.DEFAULT_TYPE, chat_id: int, kind: str, q):
    if kind == "girls":
        ids = await db_get_registered_by_gender(chat_id, "FEMALE")
        title = "👩 لیست دخترها:"
    elif kind == "boys":
        ids = await db_get_registered_by_gender(chat_id, "MALE")
        title = "👨 لیست پسرها:"
    elif kind == "singles":
        async with DB.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM group_members WHERE chat_id=$1 AND relation_status='SINGLE'", chat_id)
        ids = [r["user_id"] for r in rows]
        title = "🚶 لیست سینگل‌ها:"
    elif kind == "relations":
        async with DB.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT a.user_id a_id, b.user_id b_id
                FROM group_members a
                JOIN group_members b ON b.chat_id=a.chat_id AND b.user_id=a.partner_user_id
                WHERE a.chat_id=$1 AND a.relation_status='IN_RELATION' AND a.user_id < b.user_id
                """,
                chat_id
            )
        if not rows:
            await q.edit_message_text("هیچ رابطه‌ای ثبت نشده.")
            return
        lines = []
        for r in rows:
            au = (await context.bot.get_chat_member(chat_id, r["a_id"])).user
            bu = (await context.bot.get_chat_member(chat_id, r["b_id"])).user
            lines.append(f"{mention_user(au)} ❤️ {mention_user(bu)}")
        await q.edit_message_text("❤️ لیست رابطه‌ها:\n" + "\n".join(lines), parse_mode=ParseMode.MARKDOWN)
        return
    elif kind == "birthdays":
        tz = await db_get_group_tz(chat_id)
        j_today = jdatetime.date.fromgregorian(date=now_in_tz(tz).date())
        ids = await db_get_birthdays_today(chat_id, j_today)
        title = f"🎂 تولدهای امروز ({j_today.year:04d}/{j_today.month:02d}/{j_today.day:02d}):"
    else:
        await q.edit_message_text("ناشناخته.")
        return

    if not ids:
        await q.edit_message_text("لیستی پیدا نشد.")
        return
    parts = []
    for uid in ids:
        u = (await context.bot.get_chat_member(chat_id, uid)).user
        parts.append(mention_user(u))
    await q.edit_message_text(f"{title}\n" + "، ".join(parts), parse_mode=ParseMode.MARKDOWN)

# ----------------------- Scheduling -----------------------
async def schedule_group_jobs(app, chat_id: int):
    # حذف جاب‌های قبلی این گروه
    for job in app.job_queue.get_jobs_by_name(f"active:{chat_id}"):
        job.schedule_removal()
    for name in ("ship", "bros_b", "bros_g", "bday", "anni", "gm", "gn", "ge"):
        for job in app.job_queue.get_jobs_by_name(f"{name}:{chat_id}"):
            job.schedule_removal()

    tz = await db_get_group_tz(chat_id)
    # Active ping
    app.job_queue.run_repeating(
        job_active_ping,
        interval=dt.timedelta(minutes=ACTIVE_PING_INTERVAL_MIN),
        first=10,
        name=f"active:{chat_id}",
        chat_id=chat_id
    )
    # Ship nightly 23:00
    app.job_queue.run_daily(job_ship, time=dt.time(23, 0, tzinfo=timezone(tz)), name=f"ship:{chat_id}", chat_id=chat_id)
    # Bros boys 17:00, girls 17:05
    app.job_queue.run_daily(job_bros_boys, time=dt.time(17, 0, tzinfo=timezone(tz)), name=f"bros_b:{chat_id}", chat_id=chat_id)
    app.job_queue.run_daily(job_bros_girls, time=dt.time(17, 5, tzinfo=timezone(tz)), name=f"bros_g:{chat_id}", chat_id=chat_id)
    # Birthdays 09:00, Anniversaries 10:00
    app.job_queue.run_daily(job_birthdays, time=dt.time(9, 0, tzinfo=timezone(tz)), name=f"bday:{chat_id}", chat_id=chat_id)
    app.job_queue.run_daily(job_anniversaries, time=dt.time(10, 0, tzinfo=timezone(tz)), name=f"anni:{chat_id}", chat_id=chat_id)
    # Greetings
    app.job_queue.run_daily(lambda c: job_greeting(c, "morning"), time=dt.time(8, 30, tzinfo=timezone(tz)), name=f"gm:{chat_id}", chat_id=chat_id)
    app.job_queue.run_daily(lambda c: job_greeting(c, "noon"), time=dt.time(12, 30, tzinfo=timezone(tz)), name=f"gn:{chat_id}", chat_id=chat_id)
    app.job_queue.run_daily(lambda c: job_greeting(c, "evening"), time=dt.time(18, 30, tzinfo=timezone(tz)), name=f"ge:{chat_id}", chat_id=chat_id)

async def post_init(app):
    await db_init_pool()
   
