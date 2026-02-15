import os
import csv
import sqlite3
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Tuple

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

import matplotlib
matplotlib.use("Agg")  # no GUI needed
import matplotlib.pyplot as plt

load_dotenv()

TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
OWNER_USER_IDS_RAW = (os.getenv("OWNER_USER_IDS", "") or "").strip()

DB_PATH = os.getenv("DB_PATH", "/data/gymbot.db").strip()

# Default schedule: chest -> back -> shoulders -> legs
SPLIT_ORDER = ["CHEST_TRICEPS", "BACK_BICEPS", "SHOULDERS", "LEGS"]

# Your warmup rule
WARMUP_TEXT = "Warm up: 5 min treadmill run 🏃‍♂️"

# Exercise library (flexible options)
EXERCISES = {
    "CHEST_TRICEPS": [
        "Bench Press (Barbell)",
        "Incline Dumbbell Press",
        "Chest Press Machine",
        "Cable Fly",
        "Dips (Assisted if needed)",
        "Triceps Pushdown (Cable)",
        "Overhead Triceps Extension",
        "Skull Crushers",
    ],
    "BACK_BICEPS": [
        "Lat Pulldown",
        "Seated Cable Row",
        "Barbell Row",
        "Dumbbell Row",
        "Pull-ups (Assisted if needed)",
        "Face Pull",
        "Biceps Curl (Dumbbells)",
        "Cable Curl",
        "Hammer Curl",
    ],
    "SHOULDERS": [
        "Overhead Press (Barbell or Dumbbells)",
        "Shoulder Press Machine",
        "Lateral Raise",
        "Rear Delt Fly",
        "Cable Lateral Raise",
        "Upright Row (Light)",
        "Shrugs",
    ],
    "LEGS": [
        "Squat (Barbell)",
        "Leg Press",
        "Romanian Deadlift",
        "Leg Extension",
        "Leg Curl",
        "Calf Raise (Machine)",
        "Walking Lunges",
    ],
}

# -------------------------
# AUTH
# -------------------------
def parse_id_set(raw: str) -> set[int]:
    ids = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return ids

OWNER_USER_IDS = parse_id_set(OWNER_USER_IDS_RAW)

def is_authorized(update: Update) -> bool:
    # Authorize by USER ID (most stable)
    uid = update.effective_user.id if update.effective_user else None
    if not OWNER_USER_IDS:
        # If you forgot to set env, fail closed (private)
        return False
    return uid in OWNER_USER_IDS

# -------------------------
# DB
# -------------------------
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    return con

def init_db():
    with db() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS state (
            user_id INTEGER PRIMARY KEY,
            split_index INTEGER NOT NULL DEFAULT 0,
            last_workout_date TEXT,
            pending_day TEXT,
            pending_exercise TEXT,
            pending_set_index INTEGER,
            pending_reps REAL,
            pending_weight REAL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS sets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            day TEXT NOT NULL,
            exercise TEXT NOT NULL,
            performed_at TEXT NOT NULL,
            set_no INTEGER NOT NULL,
            reps REAL NOT NULL,
            weight REAL NOT NULL
        )
        """)
        con.execute("""
        CREATE INDEX IF NOT EXISTS idx_sets_user_date ON sets(user_id, performed_at);
        """)
        con.execute("""
        CREATE INDEX IF NOT EXISTS idx_sets_user_day_ex ON sets(user_id, day, exercise);
        """)

def get_state(user_id: int) -> dict:
    with db() as con:
        row = con.execute("SELECT * FROM state WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            con.execute("INSERT INTO state(user_id, split_index) VALUES(?, 0)", (user_id,))
            row = con.execute("SELECT * FROM state WHERE user_id=?", (user_id,)).fetchone()
    cols = ["user_id","split_index","last_workout_date","pending_day","pending_exercise",
            "pending_set_index","pending_reps","pending_weight"]
    return dict(zip(cols, row))

def set_state(user_id: int, **kwargs):
    if not kwargs:
        return
    keys = list(kwargs.keys())
    vals = [kwargs[k] for k in keys]
    sets_sql = ", ".join([f"{k}=?" for k in keys])
    with db() as con:
        con.execute(f"UPDATE state SET {sets_sql} WHERE user_id=?", (*vals, user_id))

def add_set(user_id: int, day: str, exercise: str, set_no: int, reps: float, weight: float):
    with db() as con:
        con.execute("""
        INSERT INTO sets(user_id, day, exercise, performed_at, set_no, reps, weight)
        VALUES(?,?,?,?,?,?,?)
        """, (user_id, day, exercise, datetime.utcnow().isoformat(), set_no, reps, weight))

def get_last_set(user_id: int, exercise: str) -> Optional[Tuple[float,float]]:
    # returns (reps, weight) of the last logged set for this exercise
    with db() as con:
        row = con.execute("""
        SELECT reps, weight FROM sets
        WHERE user_id=? AND exercise=?
        ORDER BY id DESC LIMIT 1
        """, (user_id, exercise)).fetchone()
    return (row[0], row[1]) if row else None

def get_pr(user_id: int, exercise: str) -> Optional[Tuple[float,float,str]]:
    # PR = highest weight. If tie, higher reps wins.
    with db() as con:
        row = con.execute("""
        SELECT weight, reps, performed_at FROM sets
        WHERE user_id=? AND exercise=?
        ORDER BY weight DESC, reps DESC
        LIMIT 1
        """, (user_id, exercise)).fetchone()
    return (row[0], row[1], row[2]) if row else None

# -------------------------
# LOGIC: Next day, skip, replace, suggestions
# -------------------------
def current_day_name(state: dict) -> str:
    idx = state["split_index"] % len(SPLIT_ORDER)
    return SPLIT_ORDER[idx]

def advance_day_index(user_id: int):
    st = get_state(user_id)
    set_state(user_id, split_index=(st["split_index"] + 1) % len(SPLIT_ORDER), last_workout_date=date.today().isoformat())

def suggest_next_weight(user_id: int, exercise: str) -> Optional[float]:
    """
    Very simple progressive overload:
    - If last set >= 10 reps: +2.5kg
    - If last set 6-9 reps: +1.25kg
    - If last set < 6 reps: keep same
    """
    last = get_last_set(user_id, exercise)
    if not last:
        return None
    reps, weight = last
    if reps >= 10:
        return round(weight + 2.5, 2)
    if reps >= 6:
        return round(weight + 1.25, 2)
    return round(weight, 2)

def week_start(d: date) -> date:
    # Monday
    return d - timedelta(days=d.weekday())

def weekly_volume(user_id: int, start: date, end: date) -> Dict[str, float]:
    # volume = sum(reps * weight) per day
    with db() as con:
        rows = con.execute("""
        SELECT day, reps, weight, performed_at FROM sets
        WHERE user_id=?
        """, (user_id,)).fetchall()

    vol = {}
    for day_name, reps, weight, ts in rows:
        dt = datetime.fromisoformat(ts.replace("Z","")).date()
        if start <= dt <= end:
            vol[day_name] = vol.get(day_name, 0.0) + float(reps) * float(weight)
    return vol

def export_csv(user_id: int, filepath: str):
    with db() as con:
        rows = con.execute("""
        SELECT performed_at, day, exercise, set_no, reps, weight
        FROM sets
        WHERE user_id=?
        ORDER BY id ASC
        """, (user_id,)).fetchall()

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["performed_at","day","exercise","set_no","reps","weight"])
        w.writerows(rows)

# -------------------------
# UI helpers (Telegram)
# -------------------------
def day_label(day_key: str) -> str:
    return {
        "CHEST_TRICEPS": "Chest + Triceps",
        "BACK_BICEPS": "Back + Biceps",
        "SHOULDERS": "Shoulders",
        "LEGS": "Legs",
    }.get(day_key, day_key)

def kb_for_workout(day_key: str) -> InlineKeyboardMarkup:
    buttons = []
    for ex in EXERCISES.get(day_key, []):
        buttons.append([InlineKeyboardButton(f"➕ {ex}", callback_data=f"EX|{day_key}|{ex}")])
    buttons.append([InlineKeyboardButton("✅ Finish workout", callback_data=f"FINISH|{day_key}")])
    buttons.append([InlineKeyboardButton("⏭ Skip this day", callback_data="SKIP")])
    return InlineKeyboardMarkup(buttons)

def kb_for_set_input(day_key: str, exercise: str) -> InlineKeyboardMarkup:
    last = None
    # show quick presets: reps and weight entry still needed, but we can suggest weight
    buttons = [
        [InlineKeyboardButton("↩ Replace exercise", callback_data=f"REPLACE|{day_key}")],
        [InlineKeyboardButton("⬅ Back to workout", callback_data=f"BACK|{day_key}")],
    ]
    return InlineKeyboardMarkup(buttons)

def kb_replace(day_key: str) -> InlineKeyboardMarkup:
    buttons = []
    for ex in EXERCISES.get(day_key, []):
        buttons.append([InlineKeyboardButton(f"🔁 {ex}", callback_data=f"EX|{day_key}|{ex}")])
    buttons.append([InlineKeyboardButton("⬅ Back", callback_data=f"BACK|{day_key}")])
    return InlineKeyboardMarkup(buttons)

# -------------------------
# COMMANDS
# -------------------------
async def whoami_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    await update.message.reply_text(f"user_id: {uid}\nchat_id: {cid}")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("This bot is private. Add your user_id to OWNER_USER_IDS in .env")
        return
    msg = (
        "/workout  Start today’s workout\n"
        "/status   Show next training day\n"
        "/pr <exercise>  Show PR for exercise\n"
        "/week     Weekly volume summary\n"
        "/chart    Progress chart (volume)\n"
        "/export   Export history to CSV\n"
        "/whoami   Show user_id/chat_id\n"
    )
    await update.message.reply_text(msg)

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("This bot is private.")
        return
    st = get_state(update.effective_user.id)
    day_key = current_day_name(st)
    await update.message.reply_text(f"Next workout: {day_label(day_key)}")

async def workout_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("This bot is private.")
        return
    user_id = update.effective_user.id
    st = get_state(user_id)
    day_key = current_day_name(st)

    text = (
        f"🏋️ *Workout: {day_label(day_key)}*\n"
        f"{WARMUP_TEXT}\n\n"
        "Pick an exercise to log.\n"
        "Tip: After you pick, send reps + weight like: `8 42.5`\n"
    )
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb_for_workout(day_key))

async def week_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("This bot is private.")
        return
    user_id = update.effective_user.id
    today = date.today()
    ws = week_start(today)
    we = ws + timedelta(days=6)
    vol = weekly_volume(user_id, ws, we)
    lines = [f"📦 Weekly volume ({ws.isoformat()} → {we.isoformat()})"]
    for k in SPLIT_ORDER:
        lines.append(f"- {day_label(k)}: {vol.get(k, 0.0):.0f}")
    await update.message.reply_text("\n".join(lines))

async def chart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("This bot is private.")
        return
    user_id = update.effective_user.id

    # Build last 8 weeks volume totals
    today = date.today()
    weeks = []
    totals = []
    for i in range(7, -1, -1):
        ws = week_start(today - timedelta(days=i*7))
        we = ws + timedelta(days=6)
        vol = weekly_volume(user_id, ws, we)
        weeks.append(ws.strftime("%d-%m"))
        totals.append(sum(vol.values()))

    fig = plt.figure()
    plt.plot(weeks, totals, marker="o")
    plt.title("Weekly Training Volume")
    plt.xlabel("Week start")
    plt.ylabel("Total volume (reps * kg)")
    plt.xticks(rotation=45)

    path = "weekly_volume.png"
    plt.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)

    await update.message.reply_photo(photo=open(path, "rb"), caption="📊 Weekly volume chart")

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("This bot is private.")
        return
    user_id = update.effective_user.id
    path = "gym_history.csv"
    export_csv(user_id, path)
    await update.message.reply_document(document=open(path, "rb"), filename="gym_history.csv")

async def pr_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("This bot is private.")
        return
    if not context.args:
        await update.message.reply_text("Use: /pr <exercise name contains...>  e.g. /pr bench")
        return

    query = " ".join(context.args).lower().strip()
    # Find best matching exercise in library
    all_ex = [e for day in EXERCISES.values() for e in day]
    matches = [e for e in all_ex if query in e.lower()]
    if not matches:
        await update.message.reply_text("No matching exercise found.")
        return
    ex = matches[0]

    p = get_pr(update.effective_user.id, ex)
    if not p:
        await update.message.reply_text(f"No PR yet for: {ex}")
        return
    w, r, ts = p
    dt = datetime.fromisoformat(ts).date().strftime("%d-%m-%y")
    await update.message.reply_text(f"🏆 PR for {ex}\n{w} kg x {r} reps\n({dt})")

# -------------------------
# CALLBACKS + MESSAGE INPUT
# -------------------------
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.callback_query.answer("Private bot.")
        return

    q = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    data = (q.data or "")
    parts = data.split("|")
    action = parts[0]

    if action == "EX":
        _, day_key, exercise = parts
        # set pending input state: waiting for reps+weight (set_no increments automatically)
        st = get_state(user_id)

        # determine next set number for this exercise today (count existing sets for day/exercise today)
        today = date.today()
        with db() as con:
            rows = con.execute("""
                SELECT COUNT(*) FROM sets
                WHERE user_id=? AND day=? AND exercise=?
            """, (user_id, day_key, exercise)).fetchone()
        next_set_no = int(rows[0]) + 1

        set_state(
            user_id,
            pending_day=day_key,
            pending_exercise=exercise,
            pending_set_index=next_set_no
        )

        sug = suggest_next_weight(user_id, exercise)
        sug_txt = f"\nSuggested next weight: *{sug} kg*" if sug is not None else ""
        pr = get_pr(user_id, exercise)
        pr_txt = ""
        if pr:
            pr_txt = f"\nPR: *{pr[0]} kg x {pr[1]} reps*"

        await q.message.reply_text(
            f"📝 *{day_label(day_key)}*\n"
            f"Exercise: *{exercise}*\n"
            f"Set #{next_set_no}\n"
            f"Send: `reps weight`\n"
            f"Example: `8 42.5`{sug_txt}{pr_txt}",
            parse_mode="Markdown",
            reply_markup=kb_for_set_input(day_key, exercise)
        )
        return

    if action == "FINISH":
        _, day_key = parts
        advance_day_index(user_id)
        await q.message.reply_text("✅ Workout saved. Next day scheduled.")
        return

    if action == "SKIP":
        # skip today in schedule (advance day)
        advance_day_index(user_id)
        await q.message.reply_text("⏭ Skipped. Schedule moved to next day.")
        return

    if action == "REPLACE":
        _, day_key = parts
        await q.message.reply_text("Pick a replacement exercise:", reply_markup=kb_replace(day_key))
        return

    if action == "BACK":
        _, day_key = parts
        await q.message.reply_text(
            f"🏋️ *Workout: {day_label(day_key)}*\n{WARMUP_TEXT}\n\nPick an exercise:",
            parse_mode="Markdown",
            reply_markup=kb_for_workout(day_key),
        )
        return

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("This bot is private.")
        return

    user_id = update.effective_user.id
    st = get_state(user_id)

    pending_day = st.get("pending_day")
    pending_ex = st.get("pending_exercise")
    set_no = st.get("pending_set_index")

    if not pending_day or not pending_ex or not set_no:
        return  # ignore random text

    txt = (update.message.text or "").strip().replace(",", ".")
    parts = txt.split()
    if len(parts) != 2:
        await update.message.reply_text("Send reps and weight like: 8 42.5")
        return

    try:
        reps = float(parts[0])
        weight = float(parts[1])
    except ValueError:
        await update.message.reply_text("Numbers only. Example: 8 42.5")
        return

    add_set(user_id, pending_day, pending_ex, int(set_no), reps, weight)

    # prepare next set number
    next_set_no = int(set_no) + 1
    set_state(user_id, pending_set_index=next_set_no)

    sug = suggest_next_weight(user_id, pending_ex)
    sug_txt = f" Suggested next weight: {sug} kg" if sug is not None else ""

    await update.message.reply_text(
        f"✅ Logged: {pending_ex} | set #{set_no} | {reps} reps x {weight} kg\n"
        f"Next set will be #{next_set_no}.{sug_txt}\n"
        f"Pick another exercise or finish workout in the buttons."
    )

# -------------------------
# ERRORS
# -------------------------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    # keeps bot alive
    try:
        print("ERROR:", context.error)
    except Exception:
        pass

# -------------------------
# SIMPLE PHONE UI APP (Streamlit)
# -------------------------
# This is separate: gym_app.py (below)

def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN missing")
    if not OWNER_USER_IDS:
        raise RuntimeError("OWNER_USER_IDS missing (put your Telegram user_id)")

    init_db()

    app = Application.builder().token(TOKEN).build()
    app.add_error_handler(error_handler)
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("whoami", whoami_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("workout", workout_cmd))
    app.add_handler(CommandHandler("week", week_cmd))
    app.add_handler(CommandHandler("chart", chart_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("pr", pr_cmd))

    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(CommandHandler("start", workout_cmd))

    # Text input for reps+weight
    app.add_handler(telegram.ext.MessageHandler(telegram.ext.filters.TEXT & ~telegram.ext.filters.COMMAND, message_handler))

    print("GymBot is running.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

    if __name__ == "__main__":
     main()

