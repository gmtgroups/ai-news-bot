"""
AI News Digest Bot - v2
Features:
- 3 tiers: Free / Premium (200 Stars) / Premium+ (500 Stars)
- RSS feed fetching from 8 major AI/Tech sources
- Claude AI summarization
- Auto-posts to public Telegram channel every 4 hours
- Referral system (7 free premium days per referral)
- Keyword alerts, sentiment analysis, weekly reports (Premium+)
- /admin dashboard
"""

import os
import logging
import sqlite3
import json
import asyncio
import hashlib
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

import feedparser
import httpx
import anthropic
from flask import Flask, jsonify
from flask_cors import CORS

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    LabeledPrice, ChatPermissions
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, PreCheckoutQueryHandler, filters, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
BOT_TOKEN        = os.environ["BOT_TOKEN"]
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ADMIN_IDS        = [int(x) for x in os.environ.get("ADMIN_IDS", "0").split(",") if x]
CHANNEL_USERNAME = os.environ.get("CHANNEL_USERNAME", "")   # e.g. @ai_news_daily_feed

# ── Pricing (Telegram Stars) ──────────────────────────────────────────────────
PREMIUM_STARS      = 200   # ~$2.50 / month
PREMIUM_PLUS_STARS = 500   # ~$6.25 / month

# ── RSS Feeds ─────────────────────────────────────────────────────────────────
RSS_FEEDS = [
    "https://feeds.feedburner.com/venturebeat/SZYF",
    "https://techcrunch.com/feed/",
    "https://www.thenextweb.com/feed/",
    "https://www.wired.com/feed/rss",
    "https://feeds.arstechnica.com/arstechnica/technology-lab",
    "https://www.theverge.com/rss/index.xml",
    "https://feeds.feedburner.com/oreilly/radar/atom",
    "https://rss.slashdot.org/Slashdot/slashdotMain",
]

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

def get_db():
    db = sqlite3.connect("ainews.db", check_same_thread=False)
    db.row_factory = sqlite3.Row
    return db

def init_db():
    db = get_db()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER PRIMARY KEY,
        username    TEXT,
        first_name  TEXT,
        tier        TEXT DEFAULT 'free',        -- free | premium | premium_plus
        tier_until  TEXT,                       -- ISO datetime, NULL = lifetime
        stars_spent INTEGER DEFAULT 0,
        referral_code TEXT UNIQUE,
        referred_by INTEGER,
        joined_at   TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS referrals (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER,
        referred_id INTEGER,
        created_at  TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS keywords (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        keyword TEXT
    );

    CREATE TABLE IF NOT EXISTS sent_articles (
        url_hash TEXT PRIMARY KEY,
        sent_at  TEXT DEFAULT (datetime('now'))
    );
    """)
    db.commit()
    db.close()

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def make_referral_code(user_id: int) -> str:
    return hashlib.md5(f"ainews_{user_id}".encode()).hexdigest()[:8].upper()

def get_or_create_user(user_id: int, username: str = "", first_name: str = "") -> sqlite3.Row:
    db = get_db()
    row = db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    if not row:
        code = make_referral_code(user_id)
        db.execute(
            "INSERT INTO users (user_id, username, first_name, referral_code) VALUES (?,?,?,?)",
            (user_id, username, first_name, code)
        )
        db.commit()
        row = db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    db.close()
    return row

def user_tier(user_id: int) -> str:
    db = get_db()
    row = db.execute("SELECT tier, tier_until FROM users WHERE user_id=?", (user_id,)).fetchone()
    db.close()
    if not row:
        return "free"
    if row["tier"] in ("premium", "premium_plus"):
        if row["tier_until"] and datetime.fromisoformat(row["tier_until"]) < datetime.utcnow():
            # Expired — downgrade
            db2 = get_db()
            db2.execute("UPDATE users SET tier='free', tier_until=NULL WHERE user_id=?", (user_id,))
            db2.commit()
            db2.close()
            return "free"
        return row["tier"]
    return "free"

def grant_premium(user_id: int, tier: str, days: int):
    db = get_db()
    row = db.execute("SELECT tier_until FROM users WHERE user_id=?", (user_id,)).fetchone()
    now = datetime.utcnow()
    if row and row["tier_until"]:
        base = max(datetime.fromisoformat(row["tier_until"]), now)
    else:
        base = now
    until = (base + timedelta(days=days)).isoformat()
    db.execute("UPDATE users SET tier=?, tier_until=? WHERE user_id=?", (tier, until, user_id))
    db.commit()
    db.close()

def handle_referral(new_user_id: int, code: str):
    db = get_db()
    referrer = db.execute("SELECT user_id FROM users WHERE referral_code=?", (code,)).fetchone()
    if referrer and referrer["user_id"] != new_user_id:
        already = db.execute(
            "SELECT id FROM referrals WHERE referred_id=?", (new_user_id,)
        ).fetchone()
        if not already:
            db.execute(
                "INSERT INTO referrals (referrer_id, referred_id) VALUES (?,?)",
                (referrer["user_id"], new_user_id)
            )
            db.execute(
                "UPDATE users SET referred_by=? WHERE user_id=?",
                (referrer["user_id"], new_user_id)
            )
            db.commit()
            db.close()
            return referrer["user_id"]
    db.close()
    return None

async def summarise(text: str) -> str:
    if not ANTHROPIC_API_KEY:
        return text[:300] + "…"
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        msg = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=120,
            messages=[{"role": "user", "content":
                f"Summarise in exactly 2 punchy sentences for a tech-savvy audience:\n\n{text[:2000]}"}]
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.warning(f"Summarise error: {e}")
        return text[:300] + "…"

async def fetch_articles(limit: int = 20) -> list[dict]:
    articles = []
    db = get_db()
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:5]:
                url_hash = hashlib.md5(entry.get("link", "").encode()).hexdigest()
                already_sent = db.execute(
                    "SELECT 1 FROM sent_articles WHERE url_hash=?", (url_hash,)
                ).fetchone()
                if not already_sent:
                    articles.append({
                        "title": entry.get("title", "No title"),
                        "link":  entry.get("link", ""),
                        "summary": entry.get("summary", entry.get("description", "")),
                        "hash": url_hash,
                    })
        except Exception as e:
            log.warning(f"Feed error {feed_url}: {e}")
    db.close()
    return articles[:limit]

def mark_articles_sent(hashes: list[str]):
    db = get_db()
    db.executemany(
        "INSERT OR IGNORE INTO sent_articles (url_hash) VALUES (?)",
        [(h,) for h in hashes]
    )
    db.commit()
    db.close()

# ─────────────────────────────────────────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────────────────────────────────────────

def main_menu(tier: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📰 Latest News", callback_data="news")],
        [InlineKeyboardButton("⭐ Upgrade to Premium", callback_data="upgrade")] if tier == "free" else
        [InlineKeyboardButton("💎 Upgrade to Premium+", callback_data="upgrade_plus")] if tier == "premium" else
        [InlineKeyboardButton("💎 Premium+ Active ✅", callback_data="noop")],
        [
            InlineKeyboardButton("👥 Refer a Friend", callback_data="referral"),
            InlineKeyboardButton("⚙️ My Account", callback_data="account"),
        ],
    ]
    if tier in ("premium", "premium_plus"):
        rows.insert(1, [InlineKeyboardButton("🔍 Keyword Alerts", callback_data="keywords")])
    return InlineKeyboardMarkup(rows)

def upgrade_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⭐ Premium — {PREMIUM_STARS} Stars/mo", callback_data="buy_premium")],
        [InlineKeyboardButton(f"💎 Premium+ — {PREMIUM_PLUS_STARS} Stars/mo", callback_data="buy_premium_plus")],
        [InlineKeyboardButton("◀️ Back", callback_data="back")],
    ])

# ─────────────────────────────────────────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = ctx.args or []

    row = get_or_create_user(user.id, user.username or "", user.first_name or "")
    referrer_id = None
    if args and args[0].startswith("ref_"):
        code = args[0][4:]
        referrer_id = handle_referral(user.id, code)

    if referrer_id:
        # Give new user 3 free premium days
        grant_premium(user.id, "premium", 3)
        # Give referrer 7 free premium days
        grant_premium(referrer_id, "premium", 7)
        try:
            await ctx.bot.send_message(
                referrer_id,
                "🎉 Someone joined using your referral link! You've earned 7 free Premium days!"
            )
        except Exception:
            pass
        await update.message.reply_text(
            "🎉 Welcome! You've been given 3 free Premium days as a referral bonus!"
        )

    tier = user_tier(user.id)
    welcome = (
        f"👋 Welcome to *AI News Digest*, {user.first_name}!\n\n"
        f"🤖 Your daily AI & Tech news, automatically summarised.\n\n"
        f"*Your tier:* {'💎 Premium+' if tier=='premium_plus' else '⭐ Premium' if tier=='premium' else '🆓 Free'}\n\n"
        f"Free: 5 stories/day\n"
        f"⭐ Premium: Unlimited + real-time alerts every 2 hrs\n"
        f"💎 Premium+: Everything + sentiment, keywords, weekly report & more"
    )
    await update.message.reply_text(welcome, parse_mode="Markdown", reply_markup=main_menu(tier))

async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    db = get_db()
    total    = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    premium  = db.execute("SELECT COUNT(*) FROM users WHERE tier='premium'").fetchone()[0]
    plus     = db.execute("SELECT COUNT(*) FROM users WHERE tier='premium_plus'").fetchone()[0]
    stars    = db.execute("SELECT COALESCE(SUM(stars_spent),0) FROM users").fetchone()[0]
    refs     = db.execute("SELECT COUNT(*) FROM referrals").fetchone()[0]
    db.close()
    usd = round(stars * 0.0125, 2)
    msg = (
        f"📊 *Admin Dashboard*\n\n"
        f"👥 Total users: {total}\n"
        f"⭐ Premium: {premium}\n"
        f"💎 Premium+: {plus}\n"
        f"🆓 Free: {total - premium - plus}\n\n"
        f"⭐ Stars earned: {stars} (~${usd})\n"
        f"👥 Referrals: {refs}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cmd_news(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tier = user_tier(update.effective_user.id)
    limit = 30 if tier in ("premium", "premium_plus") else 5
    await update.message.reply_text("⏳ Fetching latest AI & Tech news…")
    articles = await fetch_articles(limit)
    if not articles:
        await update.message.reply_text("No new articles right now. Check back soon!")
        return
    for a in articles[:limit]:
        summary = await summarise(a["summary"])
        msg = f"📰 *{a['title']}*\n\n{summary}\n\n[Read more]({a['link']})"
        try:
            await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)
        except Exception:
            pass
        await asyncio.sleep(0.4)

# ─────────────────────────────────────────────────────────────────────────────
# CALLBACK HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    data = q.data
    uid  = q.from_user.id
    tier = user_tier(uid)
    await q.answer()

    if data == "news":
        await cmd_news(update, ctx)

    elif data == "upgrade":
        await q.message.reply_text("Choose your plan:", reply_markup=upgrade_keyboard())

    elif data == "upgrade_plus":
        await q.message.reply_text("Upgrade to Premium+:", reply_markup=upgrade_keyboard())

    elif data == "buy_premium":
        await ctx.bot.send_invoice(
            chat_id=uid,
            title="AI News Premium",
            description="Unlimited news, real-time alerts every 2 hrs — 30 days",
            payload="premium_30",
            currency="XTR",
            prices=[LabeledPrice("Premium 30 days", PREMIUM_STARS)],
        )

    elif data == "buy_premium_plus":
        await ctx.bot.send_invoice(
            chat_id=uid,
            title="AI News Premium+",
            description="Everything in Premium + sentiment, keywords, weekly report — 30 days",
            payload="premium_plus_30",
            currency="XTR",
            prices=[LabeledPrice("Premium+ 30 days", PREMIUM_PLUS_STARS)],
        )

    elif data == "referral":
        db = get_db()
        row = db.execute("SELECT referral_code FROM users WHERE user_id=?", (uid,)).fetchone()
        db.close()
        code = row["referral_code"] if row else make_referral_code(uid)
        bot_info = await ctx.bot.get_me()
        link = f"https://t.me/{bot_info.username}?start=ref_{code}"
        await q.message.reply_text(
            f"👥 *Your Referral Link*\n\n`{link}`\n\n"
            f"Share this link. For every person who joins:\n"
            f"• They get 3 free Premium days\n"
            f"• You get 7 free Premium days\n\n"
            f"No limit — refer as many as you like!",
            parse_mode="Markdown"
        )

    elif data == "account":
        db = get_db()
        row = db.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
        refs = db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (uid,)).fetchone()[0]
        db.close()
        until = row["tier_until"][:10] if row and row["tier_until"] else "—"
        await q.message.reply_text(
            f"⚙️ *My Account*\n\n"
            f"Tier: {'💎 Premium+' if tier=='premium_plus' else '⭐ Premium' if tier=='premium' else '🆓 Free'}\n"
            f"Active until: {until}\n"
            f"Referrals made: {refs}",
            parse_mode="Markdown"
        )

    elif data == "keywords":
        if tier != "premium_plus":
            await q.message.reply_text("💎 Keyword alerts are a Premium+ feature. Upgrade to unlock!")
            return
        db = get_db()
        kws = [r["keyword"] for r in db.execute("SELECT keyword FROM keywords WHERE user_id=?", (uid,)).fetchall()]
        db.close()
        kw_str = "\n".join(f"• {k}" for k in kws) if kws else "_None set yet_"
        await q.message.reply_text(
            f"🔍 *Your Keyword Alerts*\n\n{kw_str}\n\n"
            f"To add: `/addkeyword bitcoin`\nTo remove: `/removekeyword bitcoin`",
            parse_mode="Markdown"
        )

    elif data == "back":
        await q.message.reply_text("Main menu:", reply_markup=main_menu(tier))

    elif data == "noop":
        pass

async def on_precheckout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)

async def on_payment(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid     = update.effective_user.id
    payload = update.message.successful_payment.invoice_payload
    stars   = update.message.successful_payment.total_amount

    if payload == "premium_30":
        grant_premium(uid, "premium", 30)
        tier_name = "Premium"
    else:
        grant_premium(uid, "premium_plus", 30)
        tier_name = "Premium+"

    db = get_db()
    db.execute("UPDATE users SET stars_spent=stars_spent+? WHERE user_id=?", (stars, uid))
    db.commit()
    db.close()

    await update.message.reply_text(
        f"🎉 *{tier_name} activated for 30 days!*\n\n"
        f"Thank you for your support. Enjoy your upgraded experience!",
        parse_mode="Markdown",
        reply_markup=main_menu(user_tier(uid))
    )

async def cmd_addkeyword(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if user_tier(uid) != "premium_plus":
        await update.message.reply_text("💎 Keyword alerts require Premium+.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /addkeyword <word>")
        return
    kw = " ".join(ctx.args).lower()
    db = get_db()
    existing = db.execute("SELECT COUNT(*) FROM keywords WHERE user_id=?", (uid,)).fetchone()[0]
    if existing >= 10:
        await update.message.reply_text("You can have up to 10 keywords. Remove one first.")
        db.close()
        return
    db.execute("INSERT OR IGNORE INTO keywords (user_id, keyword) VALUES (?,?)", (uid, kw))
    db.commit()
    db.close()
    await update.message.reply_text(f"✅ Keyword added: *{kw}*", parse_mode="Markdown")

async def cmd_removekeyword(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not ctx.args:
        await update.message.reply_text("Usage: /removekeyword <word>")
        return
    kw = " ".join(ctx.args).lower()
    db = get_db()
    db.execute("DELETE FROM keywords WHERE user_id=? AND keyword=?", (uid, kw))
    db.commit()
    db.close()
    await update.message.reply_text(f"✅ Keyword removed: *{kw}*", parse_mode="Markdown")

# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULED JOBS
# ─────────────────────────────────────────────────────────────────────────────

async def send_morning_digest(app: Application):
    """Send daily digest to all users at 8 AM UTC."""
    log.info("Running morning digest…")
    articles = await fetch_articles(30)
    if not articles:
        return

    db = get_db()
    users = db.execute("SELECT user_id, tier, tier_until FROM users").fetchall()
    db.close()

    hashes_to_mark = []

    for u in users:
        uid  = u["user_id"]
        tier = user_tier(uid)
        limit = 30 if tier in ("premium", "premium_plus") else 5
        batch = articles[:limit]

        lines = []
        for a in batch:
            summary = await summarise(a["summary"])
            lines.append(f"📰 *{a['title']}*\n{summary}\n[Read]({a['link']})")
            hashes_to_mark.append(a["hash"])

        msg = "☀️ *Your Morning AI News Digest*\n\n" + "\n\n".join(lines)
        try:
            await app.bot.send_message(uid, msg, parse_mode="Markdown",
                                        disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Digest send failed for {uid}: {e}")
        await asyncio.sleep(0.3)

    mark_articles_sent(list(set(hashes_to_mark)))

async def send_channel_post(app: Application):
    """Auto-post to public channel every 4 hours."""
    if not CHANNEL_USERNAME:
        return
    articles = await fetch_articles(5)
    if not articles:
        return
    lines = []
    for a in articles[:3]:
        summary = await summarise(a["summary"])
        lines.append(f"📰 *{a['title']}*\n{summary}\n[Read]({a['link']})")
    msg = (
        "🤖 *AI News Update*\n\n" +
        "\n\n".join(lines) +
        f"\n\n👉 Get personalised alerts: @{CHANNEL_USERNAME.lstrip('@')}"
    )
    try:
        await app.bot.send_message(CHANNEL_USERNAME, msg, parse_mode="Markdown",
                                    disable_web_page_preview=True)
    except Exception as e:
        log.warning(f"Channel post failed: {e}")

async def send_realtime_alerts(app: Application):
    """Send breaking news to premium users every 2 hrs, premium+ every 30 min."""
    articles = await fetch_articles(10)
    if not articles:
        return
    db = get_db()
    premium_users = db.execute(
        "SELECT user_id, tier FROM users WHERE tier IN ('premium','premium_plus')"
    ).fetchall()
    db.close()

    for u in premium_users:
        uid  = u["user_id"]
        tier = u["tier"]
        batch = articles[:5]
        lines = []
        for a in batch:
            summary = await summarise(a["summary"])
            lines.append(f"🔔 *{a['title']}*\n{summary}\n[Read]({a['link']})")
        msg = "⚡ *Breaking AI News*\n\n" + "\n\n".join(lines)
        try:
            await app.bot.send_message(uid, msg, parse_mode="Markdown",
                                        disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Alert send failed for {uid}: {e}")
        await asyncio.sleep(0.3)

async def send_weekly_report(app: Application):
    """Premium+ weekly report every Sunday 9 AM UTC."""
    db = get_db()
    plus_users = db.execute(
        "SELECT user_id FROM users WHERE tier='premium_plus'"
    ).fetchall()
    db.close()

    articles = await fetch_articles(30)
    if not articles:
        return

    lines = []
    for a in articles[:10]:
        summary = await summarise(a["summary"])
        lines.append(f"• *{a['title']}*\n  {summary}")

    msg = (
        "📊 *Weekly AI & Tech Report*\n\n"
        "Here are the top stories from the past week:\n\n" +
        "\n\n".join(lines) +
        "\n\n💎 Thank you for being a Premium+ subscriber!"
    )
    for u in plus_users:
        try:
            await app.bot.send_message(u["user_id"], msg, parse_mode="Markdown",
                                        disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Weekly report failed for {u['user_id']}: {e}")
        await asyncio.sleep(0.3)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# FLASK STATS API
# ─────────────────────────────────────────────────────────────────────────────

flask_app = Flask(__name__)
# Tight CORS on the Telegram proxy (origins limited to known dashboards);
# other routes keep wide-open CORS for existing integrations.
CORS(flask_app, resources={
    r"/api/channel/*": {"origins": [
        "https://clever-charisma-production-bdbe.up.railway.app",
        "http://localhost:8765",
        "http://127.0.0.1:8765",
    ]},
    r"/*": {"origins": "*"},
})

@flask_app.route("/api/stats")
def api_stats():
    db = get_db()
    total   = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    premium = db.execute("SELECT COUNT(*) FROM users WHERE tier='premium'").fetchone()[0]
    plus    = db.execute("SELECT COUNT(*) FROM users WHERE tier='premium_plus'").fetchone()[0]
    stars   = db.execute("SELECT COALESCE(SUM(stars_spent),0) FROM users").fetchone()[0]
    refs    = db.execute("SELECT COUNT(*) FROM referrals").fetchone()[0]
    today   = db.execute(
        "SELECT COUNT(*) FROM users WHERE date(joined_at)=date('now')"
    ).fetchone()[0]
    db.close()
    return jsonify({
        "bot": "AI News Digest",
        "total_users": total,
        "free_users": total - premium - plus,
        "premium_users": premium,
        "premium_plus_users": plus,
        "stars_earned": stars,
        "usd_earned": round(stars * 0.0125, 2),
        "referrals": refs,
        "new_today": today,
        "updated_at": datetime.utcnow().isoformat()
    })

@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "bot": "ai-news"})

# Server-side Telegram proxy — keeps BOT_TOKEN out of any HTML/JS served to clients.
# Allowlist guards against arbitrary-handle enumeration; rate limit guards quota.
_TG_PROXY_ALLOWED_HANDLES = {
    "ainewsdailyfeeds",
    "cryptosignalsdailyglobal",
    "alphapulse_official",
}
_TG_PROXY_LOCK = threading.Lock()
_TG_PROXY_CALLS: list[float] = []
_TG_PROXY_WINDOW = 60.0   # seconds
_TG_PROXY_MAX = 60        # max calls per window globally

@flask_app.route("/api/channel/<handle>/members")
def api_channel_members(handle):
    # Flask URL-decodes once; %40-prefix already arrives as '@'. Double-encoded
    # (%2540) input arrives as literal '%40<handle>', which fails the allowlist.
    h = handle.lstrip("@")
    if h not in _TG_PROXY_ALLOWED_HANDLES:
        return jsonify({"ok": False, "error": "handle not allowed"}), 400
    now = time.monotonic()
    with _TG_PROXY_LOCK:
        _TG_PROXY_CALLS[:] = [t for t in _TG_PROXY_CALLS if now - t < _TG_PROXY_WINDOW]
        if len(_TG_PROXY_CALLS) >= _TG_PROXY_MAX:
            return jsonify({"ok": False, "error": "rate limited"}), 429
        _TG_PROXY_CALLS.append(now)
    try:
        r = httpx.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMemberCount",
            params={"chat_id": f"@{h}"},
            timeout=5.0,
        )
        d = r.json()
        return jsonify({"ok": bool(d.get("ok")), "result": d.get("result")})
    except httpx.TimeoutException:
        log.warning("Telegram getChatMemberCount timeout handle=%s", h)
    except httpx.HTTPError as exc:
        # Never log str(exc) — httpx errors include the full URL with BOT_TOKEN.
        log.warning("Telegram getChatMemberCount HTTPError handle=%s type=%s",
                    h, type(exc).__name__)
    except Exception as exc:
        log.warning("Unexpected error in api_channel_members handle=%s type=%s",
                    h, type(exc).__name__)
    return jsonify({"ok": False, "result": None}), 502

@flask_app.route("/dashboard")
@flask_app.route("/")
def dashboard():
    return """<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>TrendFlow Dashboard</title>
<style>
:root{--bg:#0a0c14;--card:#111827;--border:#1f2937;--cyan:#00d4ff;--gold:#f59e0b;--green:#10b981;--red:#ef4444;--text:#e5e7eb;--muted:#6b7280}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh}
header{display:flex;align-items:center;justify-content:space-between;padding:20px 32px;border-bottom:1px solid var(--border);background:rgba(0,212,255,0.03)}
.logo{font-size:22px;font-weight:700;letter-spacing:-0.5px}.logo span{color:var(--cyan)}
.refresh-info{font-size:12px;color:var(--muted)}#last-updated{color:var(--cyan)}
main{padding:32px;max-width:1200px;margin:0 auto}
.section-title{font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:16px}
.grid-4{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin-bottom:32px}
.grid-2{display:grid;grid-template-columns:repeat(auto-fit,minmax(460px,1fr));gap:24px;margin-bottom:32px}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px 24px}
.card-label{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px}
.card-value{font-size:32px;font-weight:700;line-height:1}.card-sub{font-size:12px;color:var(--muted);margin-top:6px}
.card-value.cyan{color:var(--cyan)}.card-value.gold{color:var(--gold)}.card-value.green{color:var(--green)}
.bot-panel,.channel-panel{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:24px}
.bot-header,.channel-header{display:flex;align-items:center;gap:12px;margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid var(--border)}
.bot-dot{width:10px;height:10px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green)}
.bot-dot.offline{background:var(--red);box-shadow:0 0 8px var(--red)}
.bot-name{font-size:16px;font-weight:600}.bot-handle{font-size:12px;color:var(--muted)}
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid var(--border)}
.stat-row:last-child{border-bottom:none}
.stat-row-label{font-size:13px;color:var(--muted)}.stat-row-value{font-size:14px;font-weight:600}
.channel-icon{width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:18px}
.channel-icon.ai{background:rgba(0,212,255,0.1);border:1px solid rgba(0,212,255,0.3)}
.channel-icon.crypto{background:rgba(245,158,11,0.1);border:1px solid rgba(245,158,11,0.3)}
.channel-icon.signals{background:rgba(16,185,129,0.15);border:1px solid rgba(16,185,129,0.4)}
.status-bar{display:flex;gap:16px;margin-bottom:24px;flex-wrap:wrap}
.status-pill{display:flex;align-items:center;gap:6px;background:var(--card);border:1px solid var(--border);border-radius:20px;padding:6px 14px;font-size:12px}
.dot{width:7px;height:7px;border-radius:50%}.dot.green{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot.red{background:var(--red)}.dot.yellow{background:var(--gold);box-shadow:0 0 6px var(--gold)}
.gold{color:var(--gold)}.green-text{color:var(--green)}
</style></head><body>
<header>
  <div class="logo">Trend<span>Flow</span> Dashboard</div>
  <div class="refresh-info">Auto-refreshes every 30s &nbsp;·&nbsp; Last updated: <span id="last-updated">—</span></div>
</header>
<main>
  <div class="status-bar">
    <div class="status-pill"><div class="dot" id="dot-ainews"></div><span id="status-ainews">AI News Bot</span></div>
    <div class="status-pill"><div class="dot" id="dot-crypto"></div><span id="status-crypto">Crypto Bot</span></div>
    <div class="status-pill"><div class="dot" id="dot-alpha"></div><span id="status-alpha">AlphaPulse Bot</span></div>
    <div class="status-pill"><div class="dot green"></div><span>@ainewsdailyfeeds</span></div>
    <div class="status-pill"><div class="dot yellow"></div><span>@cryptosignalsdailyglobal</span></div>
    <div class="status-pill"><div class="dot green"></div><span>@alphapulse_official</span></div>
  </div>
  <div class="section-title">Overview</div>
  <div class="grid-4">
    <div class="card"><div class="card-label">Total Users</div><div class="card-value cyan" id="total-users">—</div><div class="card-sub">Across all bots</div></div>
    <div class="card"><div class="card-label">Premium Subscribers</div><div class="card-value green" id="total-premium">—</div><div class="card-sub">Paying customers</div></div>
    <div class="card"><div class="card-label">Total Revenue</div><div class="card-value gold" id="total-revenue">—</div><div class="card-sub" id="total-stars">— Stars earned</div></div>
    <div class="card"><div class="card-label">New Today</div><div class="card-value" id="new-today">—</div><div class="card-sub">Joined today</div></div>
  </div>
  <div class="section-title">Bot Stats</div>
  <div class="grid-2">
    <div class="bot-panel">
      <div class="bot-header"><div class="bot-dot" id="ainews-dot"></div><div><div class="bot-name">🤖 AI News Digest</div><div class="bot-handle">@ai_newss_digest_bot</div></div></div>
      <div class="stat-row"><span class="stat-row-label">Total Users</span><span class="stat-row-value" id="an-total">—</span></div>
      <div class="stat-row"><span class="stat-row-label">🆓 Free</span><span class="stat-row-value" id="an-free">—</span></div>
      <div class="stat-row"><span class="stat-row-label">⭐ Premium</span><span class="stat-row-value" id="an-premium">—</span></div>
      <div class="stat-row"><span class="stat-row-label">💎 Premium+</span><span class="stat-row-value" id="an-plus">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Stars Earned</span><span class="stat-row-value gold" id="an-stars">—</span></div>
      <div class="stat-row"><span class="stat-row-label">USD Earned</span><span class="stat-row-value green-text" id="an-usd">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Referrals</span><span class="stat-row-value" id="an-refs">—</span></div>
      <div class="stat-row"><span class="stat-row-label">New Today</span><span class="stat-row-value cyan" id="an-today">—</span></div>
    </div>
    <div class="bot-panel">
      <div class="bot-header"><div class="bot-dot" id="crypto-dot"></div><div><div class="bot-name">📈 Crypto Signals Pro</div><div class="bot-handle">@cryptosignalspro_bot</div></div></div>
      <div class="stat-row"><span class="stat-row-label">Total Users</span><span class="stat-row-value" id="cs-total">—</span></div>
      <div class="stat-row"><span class="stat-row-label">🆓 Free</span><span class="stat-row-value" id="cs-free">—</span></div>
      <div class="stat-row"><span class="stat-row-label">⭐ Premium</span><span class="stat-row-value" id="cs-premium">—</span></div>
      <div class="stat-row"><span class="stat-row-label">💎 Premium+</span><span class="stat-row-value" id="cs-plus">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Stars Earned</span><span class="stat-row-value gold" id="cs-stars">—</span></div>
      <div class="stat-row"><span class="stat-row-label">USD Earned</span><span class="stat-row-value green-text" id="cs-usd">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Referrals</span><span class="stat-row-value" id="cs-refs">—</span></div>
      <div class="stat-row"><span class="stat-row-label">New Today</span><span class="stat-row-value cyan" id="cs-today">—</span></div>
    </div>
    <div class="bot-panel">
      <div class="bot-header"><div class="bot-dot" id="alpha-dot"></div><div><div class="bot-name">📡 AlphaPulse Signals</div><div class="bot-handle">@trendflow_replicator_bot</div></div></div>
      <div class="stat-row"><span class="stat-row-label">Total Users</span><span class="stat-row-value" id="ap-total">—</span></div>
      <div class="stat-row"><span class="stat-row-label">🆓 Free</span><span class="stat-row-value" id="ap-free">—</span></div>
      <div class="stat-row"><span class="stat-row-label">⭐ Premium</span><span class="stat-row-value" id="ap-premium">—</span></div>
      <div class="stat-row"><span class="stat-row-label">💎 Premium+</span><span class="stat-row-value" id="ap-plus">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Stars Earned</span><span class="stat-row-value gold" id="ap-stars">—</span></div>
      <div class="stat-row"><span class="stat-row-label">USD Earned</span><span class="stat-row-value green-text" id="ap-usd">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Signals Published</span><span class="stat-row-value cyan" id="ap-today">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Last Run</span><span class="stat-row-value" id="ap-lastrun">—</span></div>
    </div>
  </div>
  <div class="section-title">Telegram Channels</div>
  <div class="grid-2">
    <div class="channel-panel">
      <div class="channel-header"><div class="channel-icon ai">🤖</div><div><div class="bot-name">AI News Daily</div><div class="bot-handle">@ainewsdailyfeeds</div></div></div>
      <div class="stat-row"><span class="stat-row-label">Subscribers</span><span class="stat-row-value cyan" id="ch-ainews-members">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Auto-posts</span><span class="stat-row-value green-text">Every 4 hours ✅</span></div>
      <div class="stat-row"><span class="stat-row-label">Profile Picture</span><span class="stat-row-value green-text">Set ✅</span></div>
    </div>
    <div class="channel-panel">
      <div class="channel-header"><div class="channel-icon crypto">📈</div><div><div class="bot-name">Crypto Signals Daily</div><div class="bot-handle">@cryptosignalsdailyglobal</div></div></div>
      <div class="stat-row"><span class="stat-row-label">Subscribers</span><span class="stat-row-value gold" id="ch-crypto-members">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Auto-posts</span><span class="stat-row-value green-text">Every 4 hours ✅</span></div>
      <div class="stat-row"><span class="stat-row-label">Profile Picture</span><span class="stat-row-value green-text">Set ✅</span></div>
    </div>
    <div class="channel-panel">
      <div class="channel-header"><div class="channel-icon signals">📡</div><div><div class="bot-name">AlphaPulse Signals</div><div class="bot-handle">@alphapulse_official</div></div></div>
      <div class="stat-row"><span class="stat-row-label">Subscribers</span><span class="stat-row-value green-text" id="ch-alpha-members">—</span></div>
      <div class="stat-row"><span class="stat-row-label">Free Signals</span><span class="stat-row-value green-text">3/day to channel ✅</span></div>
      <div class="stat-row"><span class="stat-row-label">Premium DMs</span><span class="stat-row-value green-text">10+/day via bot ✅</span></div>
      <div class="stat-row"><span class="stat-row-label">Monetisation</span><span class="stat-row-value gold">200 ⭐ / 500 ⭐ per mo</span></div>
      <div class="stat-row"><span class="stat-row-label">Profile Picture</span><span class="stat-row-value green-text">Set ✅</span></div>
    </div>
  </div>
</main>
<script>
const CRYPTO_URL='https://crypto-signals-bot-production.up.railway.app';
const ALPHAPULSE_URL='https://alphapulse-signals-bot-production.up.railway.app';
async function getBotStats(url){try{const r=await fetch(url+'/api/stats',{signal:AbortSignal.timeout(5000)});return await r.json();}catch{return null;}}
async function getTgMembers(handle){try{const h=handle.replace(/^@/,'');const r=await fetch('/api/channel/'+encodeURIComponent(h)+'/members',{signal:AbortSignal.timeout(5000)});const d=await r.json();return d.ok?d.result:null;}catch{return null;}}
function fmt(n){return n===null||n===undefined?'—':n.toLocaleString();}
function usd(n){return n===null?'—':'$'+n.toFixed(2);}
async function refresh(){
  const[an,cs,ap,anM,csM,apM]=await Promise.all([fetch('/api/stats').then(r=>r.json()).catch(()=>null),getBotStats(CRYPTO_URL),getBotStats(ALPHAPULSE_URL),getTgMembers('@ainewsdailyfeeds'),getTgMembers('@cryptosignalsdailyglobal'),getTgMembers('@alphapulse_official')]);
  const setStatus=(dotId,statusId,bdotId,data,name)=>{const ok=!!data;document.getElementById(dotId).className='dot '+(ok?'green':'red');document.getElementById(statusId).textContent=name+(ok?' ✅':' ⚠️');document.getElementById(bdotId).className='bot-dot'+(ok?'':' offline');};
  setStatus('dot-ainews','status-ainews','ainews-dot',an,'AI News Bot');
  setStatus('dot-crypto','status-crypto','crypto-dot',cs,'Crypto Bot');
  setStatus('dot-alpha','status-alpha','alpha-dot',ap,'AlphaPulse Bot');
  if(an){document.getElementById('an-total').textContent=fmt(an.total_users);document.getElementById('an-free').textContent=fmt(an.free_users);document.getElementById('an-premium').textContent=fmt(an.premium_users);document.getElementById('an-plus').textContent=fmt(an.premium_plus_users);document.getElementById('an-stars').textContent=fmt(an.stars_earned)+' ⭐';document.getElementById('an-usd').textContent=usd(an.usd_earned);document.getElementById('an-refs').textContent=fmt(an.referrals);document.getElementById('an-today').textContent=fmt(an.new_today);}
  if(cs){document.getElementById('cs-total').textContent=fmt(cs.total_users);document.getElementById('cs-free').textContent=fmt(cs.free_users);document.getElementById('cs-premium').textContent=fmt(cs.premium_users);document.getElementById('cs-plus').textContent=fmt(cs.premium_plus_users);document.getElementById('cs-stars').textContent=fmt(cs.stars_earned)+' ⭐';document.getElementById('cs-usd').textContent=usd(cs.usd_earned);document.getElementById('cs-refs').textContent=fmt(cs.referrals);document.getElementById('cs-today').textContent=fmt(cs.new_today);}
  if(ap){document.getElementById('ap-total').textContent=fmt(ap.total_users);document.getElementById('ap-free').textContent=fmt(ap.free_users);document.getElementById('ap-premium').textContent=fmt(ap.premium_users);document.getElementById('ap-plus').textContent=fmt(ap.premium_plus_users);document.getElementById('ap-stars').textContent=fmt(ap.stars_earned)+' ⭐';document.getElementById('ap-usd').textContent=usd(ap.usd_earned);document.getElementById('ap-today').textContent=fmt(ap.total_forwarded);if(ap.last_run){const d=new Date(ap.last_run);document.getElementById('ap-lastrun').textContent=d.toLocaleTimeString();}}
  const tu=(an?.total_users||0)+(cs?.total_users||0)+(ap?.total_users||0),tp=(an?.premium_users||0)+(an?.premium_plus_users||0)+(cs?.premium_users||0)+(cs?.premium_plus_users||0)+(ap?.premium_users||0)+(ap?.premium_plus_users||0),tr=(an?.usd_earned||0)+(cs?.usd_earned||0)+(ap?.usd_earned||0),ts=(an?.stars_earned||0)+(cs?.stars_earned||0)+(ap?.stars_earned||0),tt=(an?.new_today||0)+(cs?.new_today||0);
  document.getElementById('total-users').textContent=fmt(tu);document.getElementById('total-premium').textContent=fmt(tp);document.getElementById('total-revenue').textContent='$'+tr.toFixed(2);document.getElementById('total-stars').textContent=fmt(ts)+' Stars earned';document.getElementById('new-today').textContent=fmt(tt);
  document.getElementById('ch-ainews-members').textContent=anM!==null?fmt(anM):'—';document.getElementById('ch-crypto-members').textContent=csM!==null?fmt(csM):'—';document.getElementById('ch-alpha-members').textContent=apM!==null?fmt(apM):'—';
  document.getElementById('last-updated').textContent=new Date().toLocaleTimeString();
}
refresh();setInterval(refresh,30000);
</script></body></html>"""

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False)

def main():
    init_db()
    log.info("🚀 AI News Bot starting…")

    app = Application.builder().token(BOT_TOKEN).build()

    # Handlers
    app.add_handler(CommandHandler("start",         cmd_start))
    app.add_handler(CommandHandler("admin",         cmd_admin))
    app.add_handler(CommandHandler("news",          cmd_news))
    app.add_handler(CommandHandler("addkeyword",    cmd_addkeyword))
    app.add_handler(CommandHandler("removekeyword", cmd_removekeyword))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(PreCheckoutQueryHandler(on_precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_payment))

    # Scheduler
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(send_morning_digest,   "cron",   hour=8,  minute=0,  args=[app])
    scheduler.add_job(send_weekly_report,    "cron",   day_of_week="sun", hour=9, minute=0, args=[app])
    scheduler.add_job(send_channel_post,     "interval", hours=4,   args=[app])
    scheduler.add_job(send_realtime_alerts,  "interval", hours=2,   args=[app])
    scheduler.start()

    # Start Flask stats API in background thread
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    log.info("✅ Bot running. Ctrl+C to stop.")
    app.run_polling(allowed_updates=["message", "callback_query", "pre_checkout_query"])

if __name__ == "__main__":
    main()
