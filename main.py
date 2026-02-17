import csv
import io
import logging
import os
import sqlite3
from contextlib import closing
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("GymBot")

ROTATION = ["Chest", "Back", "Shoulders", "Legs"]
MUSCLE_OPTIONS = ["Chest", "Back", "Legs", "Shoulders"]
EXERCISES_BY_GROUP: Dict[str, List[str]] = {
    "Chest": [
        "Bench Press",
        "Incline Dumbbell Press",
        "Cable Fly",
        "Chest Press Machine",
        "Dips",
        "Push Ups",
        "Decline Press",
        "Pec Deck",
    ],
    "Back": [
        "Pull Ups",
        "Lat Pulldown",
        "Seated Cable Row",
        "Barbell Row",
        "One Arm Dumbbell Row",
        "T-Bar Row",
        "Face Pull",
        "Straight Arm Pulldown",
    ],
    "Shoulders": [
        "Overhead Press",
        "Arnold Press",
        "Lateral Raise",
        "Cable Lateral Raise",
        "Rear Delt Fly",
        "Front Raise",
        "Upright Row",
        "Shrugs",
    ],
    "Legs": [
        "Back Squat",
        "Leg Press",
        "Romanian Deadlift",
        "Walking Lunges",
        "Leg Extension",
        "Leg Curl",
        "Calf Raise",
        "Bulgarian Split Squat",
    ],
}

SELECT_MUSCLE, WARMUP_CHOICE, WARMUP_INPUT, SELECT_EXERCISE, EX_SETS, EX_REPS, EX_WEIGHT, POST_ACTION = range(8)

CB_GROUP_PREFIX = "group:"
CB_EX_PREFIX = "ex:"
CB_SKIP_DAY = "skip_day"
CB_END_WORKOUT = "end_workout"
CB_NEXT_EXERCISE = "next_exercise"
CB_REPLACE_EXERCISE = "replace_exercise"
CB_FINISH_SESSION = "finish_session"
CB_WARMUP_YES = "warmup_yes"
CB_WARMUP_NO = "warmup_no"
CB_SETS_PREFIX = "sets:"
CB_REP_PREFIX = "rep:"
CB_WADJ_PREFIX = "wadj:"
CB_WCONFIRM = "wconfirm"
CB_WCOPY = "wcopy"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso() -> str:
    return to_iso(now_utc())


def start_of_today_utc() -> datetime:
    n = now_utc()
    return n.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_weight(text: str) -> Optional[float]:
    try:
        value = Decimal(text.strip().replace(",", "."))
    except (InvalidOperation, AttributeError):
        return None
    if value < 0:
        return None
    return float(value)


def parse_warmup_input(text: str) -> Optional[Tuple[float, float]]:
    parts = text.replace(",", ".").split()
    if len(parts) != 2:
        return None

    try:
        minutes = float(parts[0])
        distance = float(parts[1])
    except ValueError:
        return None

    if minutes <= 0 or distance < 0:
        return None
    return round(minutes, 2), round(distance, 3)


def clamp_weight_kg(value: float) -> float:
    return round(min(500.0, max(1.0, value)), 2)


class GymDB:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    def init_schema(self) -> None:
        with closing(self.connect()) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    chat_id INTEGER,
                    username TEXT,
                    first_name TEXT,
                    registered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    rotation_index INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS workout_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    muscle_group TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    warmup_done INTEGER NOT NULL DEFAULT 0,
                    warmup_minutes REAL,
                    warmup_distance_km REAL,
                    status TEXT NOT NULL CHECK(status IN ('active', 'completed', 'skipped', 'cancelled')),
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS exercises (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    muscle_group TEXT NOT NULL,
                    name TEXT NOT NULL,
                    sets INTEGER NOT NULL CHECK(sets > 0),
                    reps INTEGER NOT NULL CHECK(reps > 0),
                    reps_sequence TEXT,
                    weight REAL NOT NULL CHECK(weight >= 0),
                    weight_sequence TEXT,
                    volume REAL NOT NULL CHECK(volume >= 0),
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(session_id) REFERENCES workout_sessions(id),
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );

                CREATE INDEX IF NOT EXISTS idx_sessions_user_status ON workout_sessions(user_id, status);
                CREATE INDEX IF NOT EXISTS idx_exercises_user_time ON exercises(user_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_exercises_user_name ON exercises(user_id, name);
                """
            )

            ex_cols = {row["name"] for row in conn.execute("PRAGMA table_info(exercises)")}
            if "reps_sequence" not in ex_cols:
                conn.execute("ALTER TABLE exercises ADD COLUMN reps_sequence TEXT")
            if "weight_sequence" not in ex_cols:
                conn.execute("ALTER TABLE exercises ADD COLUMN weight_sequence TEXT")

            session_cols = {row["name"] for row in conn.execute("PRAGMA table_info(workout_sessions)")}
            if "warmup_done" not in session_cols:
                conn.execute("ALTER TABLE workout_sessions ADD COLUMN warmup_done INTEGER NOT NULL DEFAULT 0")
            if "warmup_minutes" not in session_cols:
                conn.execute("ALTER TABLE workout_sessions ADD COLUMN warmup_minutes REAL")
            if "warmup_distance_km" not in session_cols:
                conn.execute("ALTER TABLE workout_sessions ADD COLUMN warmup_distance_km REAL")

    def register_user(self, user_id: int, chat_id: int, username: str, first_name: str) -> None:
        ts = now_iso()
        with closing(self.connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO users (user_id, chat_id, username, first_name, registered_at, updated_at, rotation_index)
                VALUES (?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(user_id) DO UPDATE SET
                    chat_id = excluded.chat_id,
                    username = excluded.username,
                    first_name = excluded.first_name,
                    updated_at = excluded.updated_at
                """,
                (user_id, chat_id, username, first_name, ts, ts),
            )

    def list_users_for_reminders(self) -> List[Tuple[int, int]]:
        with closing(self.connect()) as conn:
            rows = conn.execute(
                "SELECT user_id, chat_id FROM users WHERE chat_id IS NOT NULL"
            ).fetchall()
        return [(int(r["user_id"]), int(r["chat_id"])) for r in rows]

    def get_next_group(self, user_id: int) -> str:
        with closing(self.connect()) as conn:
            row = conn.execute(
                "SELECT rotation_index FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        idx = int(row["rotation_index"]) % len(ROTATION) if row else 0
        return ROTATION[idx]

    def set_next_group_after(self, user_id: int, trained_group: str) -> None:
        if trained_group not in ROTATION:
            return
        next_idx = (ROTATION.index(trained_group) + 1) % len(ROTATION)
        with closing(self.connect()) as conn, conn:
            conn.execute(
                "UPDATE users SET rotation_index = ?, updated_at = ? WHERE user_id = ?",
                (next_idx, now_iso(), user_id),
            )

    def create_session(self, user_id: int, muscle_group: str, status: str = "active") -> int:
        started_at = now_iso()
        ended_at = None if status == "active" else now_iso()
        with closing(self.connect()) as conn, conn:
            cur = conn.execute(
                """
                INSERT INTO workout_sessions (user_id, muscle_group, started_at, ended_at, status)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, muscle_group, started_at, ended_at, status),
            )
            return int(cur.lastrowid)

    def get_active_session(self, user_id: int) -> Optional[sqlite3.Row]:
        with closing(self.connect()) as conn:
            row = conn.execute(
                """
                SELECT * FROM workout_sessions
                WHERE user_id = ? AND status = 'active'
                ORDER BY id DESC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
        return row

    def close_session(self, session_id: int, status: str) -> None:
        with closing(self.connect()) as conn, conn:
            conn.execute(
                """
                UPDATE workout_sessions
                SET status = ?, ended_at = ?
                WHERE id = ?
                """,
                (status, now_iso(), session_id),
            )

    def set_session_warmup(
        self,
        session_id: int,
        done: bool,
        minutes: Optional[float] = None,
        distance_km: Optional[float] = None,
    ) -> None:
        with closing(self.connect()) as conn, conn:
            conn.execute(
                """
                UPDATE workout_sessions
                SET warmup_done = ?, warmup_minutes = ?, warmup_distance_km = ?
                WHERE id = ?
                """,
                (1 if done else 0, minutes, distance_km, session_id),
            )

    def get_session(self, session_id: int) -> Optional[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT *
                FROM workout_sessions
                WHERE id = ?
                """,
                (session_id,),
            ).fetchone()

    def skip_day(self, user_id: int) -> Tuple[str, str]:
        current_group = self.get_next_group(user_id)
        self.create_session(user_id=user_id, muscle_group=current_group, status="skipped")
        self.set_next_group_after(user_id, current_group)
        return current_group, self.get_next_group(user_id)

    def add_exercise(
        self,
        session_id: int,
        user_id: int,
        muscle_group: str,
        name: str,
        sets: int,
        reps: int,
        weight: float,
        reps_sequence: str = "",
        weight_sequence: str = "",
    ) -> Tuple[int, float]:
        if reps_sequence and weight_sequence:
            reps_parts = [int(x) for x in reps_sequence.split()]
            weight_parts = [float(x) for x in weight_sequence.split()]
            if len(reps_parts) == len(weight_parts):
                volume = float(sum(r * w for r, w in zip(reps_parts, weight_parts)))
            else:
                total_reps = sum(reps_parts)
                volume = float(total_reps * weight)
        elif reps_sequence:
            total_reps = sum(int(x) for x in reps_sequence.split())
            volume = float(total_reps * weight)
        else:
            volume = float(sets * reps * weight)
        with closing(self.connect()) as conn, conn:
            cur = conn.execute(
                """
                INSERT INTO exercises (
                    session_id, user_id, muscle_group, name, sets, reps, reps_sequence, weight, weight_sequence, volume, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    user_id,
                    muscle_group,
                    name,
                    sets,
                    reps,
                    reps_sequence,
                    weight,
                    weight_sequence,
                    volume,
                    now_iso(),
                ),
            )
            return int(cur.lastrowid), volume

    def delete_exercise(self, exercise_id: int, user_id: int) -> bool:
        with closing(self.connect()) as conn, conn:
            cur = conn.execute(
                "DELETE FROM exercises WHERE id = ? AND user_id = ?",
                (exercise_id, user_id),
            )
            return cur.rowcount > 0

    def get_session_totals(self, session_id: int) -> Tuple[int, float]:
        with closing(self.connect()) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c, COALESCE(SUM(volume), 0) AS v
                FROM exercises
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
        return int(row["c"]), float(row["v"])

    def get_history_rows(self, user_id: int) -> List[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT
                    e.created_at,
                    e.muscle_group,
                    e.name,
                    e.sets,
                    e.reps,
                    e.reps_sequence,
                    e.weight,
                    e.weight_sequence,
                    e.volume,
                    e.session_id,
                    ws.warmup_done,
                    ws.warmup_minutes,
                    ws.warmup_distance_km
                FROM exercises e
                JOIN workout_sessions ws ON ws.id = e.session_id
                WHERE e.user_id = ?
                ORDER BY created_at ASC
                """,
                (user_id,),
            ).fetchall()

    def get_summary(self, user_id: int, start_dt: datetime, end_dt: datetime) -> Dict[str, object]:
        start_iso = to_iso(start_dt)
        end_iso = to_iso(end_dt)
        with closing(self.connect()) as conn:
            totals = conn.execute(
                """
                SELECT COUNT(*) AS exercise_count, COALESCE(SUM(volume), 0) AS total_volume
                FROM exercises
                WHERE user_id = ? AND created_at >= ? AND created_at < ?
                """,
                (user_id, start_iso, end_iso),
            ).fetchone()

            session_count = conn.execute(
                """
                SELECT COUNT(*) AS session_count
                FROM workout_sessions
                WHERE user_id = ? AND status = 'completed' AND ended_at >= ? AND ended_at < ?
                """,
                (user_id, start_iso, end_iso),
            ).fetchone()

            group_rows = conn.execute(
                """
                SELECT muscle_group, COALESCE(SUM(volume), 0) AS group_volume
                FROM exercises
                WHERE user_id = ? AND created_at >= ? AND created_at < ?
                GROUP BY muscle_group
                """,
                (user_id, start_iso, end_iso),
            ).fetchall()

            warmup_totals = conn.execute(
                """
                SELECT
                    COUNT(*) AS warmup_count,
                    COALESCE(SUM(warmup_minutes), 0) AS warmup_minutes_total,
                    COALESCE(SUM(warmup_distance_km), 0) AS warmup_distance_total
                FROM workout_sessions
                WHERE user_id = ?
                  AND warmup_done = 1
                  AND started_at >= ?
                  AND started_at < ?
                  AND status != 'cancelled'
                """,
                (user_id, start_iso, end_iso),
            ).fetchone()

        group_volumes: Dict[str, float] = {group: 0.0 for group in ROTATION}
        for row in group_rows:
            group_volumes[row["muscle_group"]] = float(row["group_volume"])

        return {
            "exercise_count": int(totals["exercise_count"]),
            "total_volume": float(totals["total_volume"]),
            "session_count": int(session_count["session_count"]),
            "group_volumes": group_volumes,
            "warmup_count": int(warmup_totals["warmup_count"]),
            "warmup_minutes_total": float(warmup_totals["warmup_minutes_total"]),
            "warmup_distance_total": float(warmup_totals["warmup_distance_total"]),
        }

    def get_personal_records(self, user_id: int) -> List[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT name, MAX(weight) AS max_weight
                FROM exercises
                WHERE user_id = ?
                GROUP BY name
                ORDER BY max_weight DESC, name COLLATE NOCASE
                """,
                (user_id,),
            ).fetchall()

    def get_last_weight(self, user_id: int, exercise_name: str) -> Optional[float]:
        with closing(self.connect()) as conn:
            row = conn.execute(
                """
                SELECT weight
                FROM exercises
                WHERE user_id = ? AND name = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user_id, exercise_name),
            ).fetchone()
        if row is None:
            return None
        return float(row["weight"])

def get_db(context: ContextTypes.DEFAULT_TYPE) -> GymDB:
    return context.application.bot_data["db"]


def ensure_registered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return None
    db = get_db(context)
    db.register_user(
        user_id=user.id,
        chat_id=chat.id,
        username=user.username or "",
        first_name=user.first_name or "",
    )
    return user.id


def group_keyboard(next_group: str) -> InlineKeyboardMarkup:
    rows = []
    for group in MUSCLE_OPTIONS:
        label = f"{group} (Next)" if group == next_group else group
        rows.append([InlineKeyboardButton(label, callback_data=f"{CB_GROUP_PREFIX}{group}")])
    rows.append(
        [
            InlineKeyboardButton("Skip day", callback_data=CB_SKIP_DAY),
            InlineKeyboardButton("End workout", callback_data=CB_END_WORKOUT),
        ]
    )
    return InlineKeyboardMarkup(rows)


def end_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("End workout", callback_data=CB_FINISH_SESSION)]]
    )


def exercise_keyboard(muscle_group: str) -> InlineKeyboardMarkup:
    exercises = EXERCISES_BY_GROUP.get(muscle_group, [])
    rows = [
        [InlineKeyboardButton(name, callback_data=f"{CB_EX_PREFIX}{idx}")]
        for idx, name in enumerate(exercises)
    ]
    rows.append([InlineKeyboardButton("End workout", callback_data=CB_FINISH_SESSION)])
    return InlineKeyboardMarkup(rows)


def warmup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Yes, I did warm-up run", callback_data=CB_WARMUP_YES),
                InlineKeyboardButton("No warm-up", callback_data=CB_WARMUP_NO),
            ],
            [InlineKeyboardButton("End workout", callback_data=CB_FINISH_SESSION)],
        ]
    )


def sets_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1", callback_data=f"{CB_SETS_PREFIX}1"),
                InlineKeyboardButton("2", callback_data=f"{CB_SETS_PREFIX}2"),
                InlineKeyboardButton("3", callback_data=f"{CB_SETS_PREFIX}3"),
            ],
            [
                InlineKeyboardButton("4", callback_data=f"{CB_SETS_PREFIX}4"),
                InlineKeyboardButton("5", callback_data=f"{CB_SETS_PREFIX}5"),
                InlineKeyboardButton("6", callback_data=f"{CB_SETS_PREFIX}6"),
            ],
            [InlineKeyboardButton("End workout", callback_data=CB_FINISH_SESSION)],
        ]
    )


def reps_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for start in (1, 6, 11, 16):
        row = [
            InlineKeyboardButton(str(rep), callback_data=f"{CB_REP_PREFIX}{rep}")
            for rep in range(start, start + 5)
        ]
        rows.append(row)
    rows.append([InlineKeyboardButton("End workout", callback_data=CB_FINISH_SESSION)])
    return InlineKeyboardMarkup(rows)


def weight_adjust_keyboard(can_copy_prev: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("-10", callback_data=f"{CB_WADJ_PREFIX}-10"),
            InlineKeyboardButton("-2.5", callback_data=f"{CB_WADJ_PREFIX}-2.5"),
            InlineKeyboardButton("-1", callback_data=f"{CB_WADJ_PREFIX}-1"),
        ],
        [
            InlineKeyboardButton("+1", callback_data=f"{CB_WADJ_PREFIX}1"),
            InlineKeyboardButton("+2.5", callback_data=f"{CB_WADJ_PREFIX}2.5"),
            InlineKeyboardButton("+10", callback_data=f"{CB_WADJ_PREFIX}10"),
        ],
    ]
    if can_copy_prev:
        rows.append([InlineKeyboardButton("Use previous set weight", callback_data=CB_WCOPY)])
    rows.append([InlineKeyboardButton("Confirm weight", callback_data=CB_WCONFIRM)])
    rows.append([InlineKeyboardButton("End workout", callback_data=CB_FINISH_SESSION)])
    return InlineKeyboardMarkup(rows)


def weight_prompt_text(set_no: int, total_sets: int, current_weight: float) -> str:
    return (
        f"Set {set_no}/{total_sets} weight\n"
        f"Current: {current_weight:.2f} kg\n"
        "Adjust with buttons, then tap Confirm weight."
    )


def post_exercise_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Add another exercise", callback_data=CB_NEXT_EXERCISE)],
            [InlineKeyboardButton("Replace exercise", callback_data=CB_REPLACE_EXERCISE)],
            [InlineKeyboardButton("End workout", callback_data=CB_FINISH_SESSION)],
        ]
    )


def reminder_job_name(user_id: int) -> str:
    return f"gymbot_reminder_{user_id}"


def schedule_user_reminder(application: Application, user_id: int, chat_id: int) -> None:
    if application.job_queue is None:
        return

    hour = int(os.getenv("GYMBOT_REMINDER_HOUR_UTC", "18"))
    minute = int(os.getenv("GYMBOT_REMINDER_MINUTE_UTC", "0"))
    hour = min(max(hour, 0), 23)
    minute = min(max(minute, 0), 59)

    name = reminder_job_name(user_id)
    for job in application.job_queue.get_jobs_by_name(name):
        job.schedule_removal()

    application.job_queue.run_daily(
        callback=daily_reminder_job,
        time=time(hour=hour, minute=minute, tzinfo=timezone.utc),
        days=(0, 1, 2, 3, 4, 5, 6),
        name=name,
        data={"user_id": user_id, "chat_id": chat_id},
    )


async def daily_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data or {}
    user_id = data.get("user_id")
    chat_id = data.get("chat_id")
    if not user_id or not chat_id:
        return

    db = get_db(context)
    next_group = db.get_next_group(int(user_id))
    text = (
        "GymBot reminder:\n"
        f"Next scheduled muscle group: {next_group}\n"
        "Use /workout to log your session."
    )
    try:
        await context.bot.send_message(chat_id=int(chat_id), text=text)
    except Exception:
        logger.exception("Failed to send daily reminder to user_id=%s", user_id)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return

    db = get_db(context)
    next_group = db.get_next_group(user_id)
    schedule_user_reminder(context.application, user_id, update.effective_chat.id)

    msg = (
        "Welcome to GymBot.\n"
        "Use /workout to log a workout session.\n"
        f"Next in your 4-day rotation: {next_group}\n\n"
        "Commands:\n"
        "/workout, /history, /today, /thisweek, /pr, /help"
    )
    await update.effective_message.reply_text(msg)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "/start - Register and initialize reminders\n"
        "/workout - Log a workout\n"
        "/history - Export workout history CSV\n"
        "/today - Today summary stats\n"
        "/thisweek - Weekly volume by muscle group\n"
        "/pr - Personal records (max weight by exercise)\n"
        "/cancel - Cancel active workout conversation"
    )


async def workout_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return ConversationHandler.END

    db = get_db(context)
    active = db.get_active_session(user_id)
    if active:
        db.close_session(int(active["id"]), "cancelled")
        await update.effective_message.reply_text(
            "Closed a previously unfinished workout session."
        )

    next_group = db.get_next_group(user_id)
    context.user_data.pop("workout", None)

    await update.effective_message.reply_text(
        "Choose the muscle group you're training today:",
        reply_markup=group_keyboard(next_group),
    )
    return SELECT_MUSCLE


async def select_muscle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    user = update.effective_user
    if not user:
        return ConversationHandler.END

    db = get_db(context)
    data = query.data or ""

    if data == CB_SKIP_DAY:
        skipped, next_group = db.skip_day(user.id)
        context.user_data.pop("workout", None)
        await query.edit_message_text(
            f"Skipped day recorded for {skipped}.\nNext scheduled group: {next_group}"
        )
        return ConversationHandler.END

    if data == CB_END_WORKOUT:
        context.user_data.pop("workout", None)
        await query.edit_message_text("Workout ended.")
        return ConversationHandler.END

    if not data.startswith(CB_GROUP_PREFIX):
        await query.edit_message_text("Invalid selection. Use /workout to start again.")
        return ConversationHandler.END

    group = data.split(":", 1)[1]
    if group not in MUSCLE_OPTIONS:
        await query.edit_message_text("Unknown muscle group. Use /workout to start again.")
        return ConversationHandler.END

    session_id = db.create_session(user_id=user.id, muscle_group=group, status="active")
    context.user_data["workout"] = {
        "session_id": session_id,
        "muscle_group": group,
        "last_exercise_id": None,
    }

    await query.edit_message_text(f"{group} workout started.")
    await query.message.reply_text(
        "Did you do your warm-up run?",
        reply_markup=warmup_keyboard(),
    )
    return WARMUP_CHOICE


async def warmup_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text("No active session. Use /workout.")
        return ConversationHandler.END

    db = get_db(context)
    data = query.data or ""
    session_id = int(workout["session_id"])
    group = str(workout["muscle_group"])

    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)

    if data == CB_WARMUP_NO:
        db.set_session_warmup(session_id, done=False, minutes=None, distance_km=None)
        await query.edit_message_text("Warm-up skipped.")
        await query.message.reply_text(
            f"Pick an exercise for {group}:",
            reply_markup=exercise_keyboard(group),
        )
        return SELECT_EXERCISE

    if data == CB_WARMUP_YES:
        await query.edit_message_text(
            "Send warm-up as: minutes distance_km\nExample: 5 1"
        )
        return WARMUP_INPUT

    await query.edit_message_text("Invalid option. Use /workout to restart.")
    return ConversationHandler.END


async def warmup_input_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    workout = context.user_data.get("workout")
    if not workout:
        await update.effective_message.reply_text("No active session. Use /workout.")
        return ConversationHandler.END

    parsed = parse_warmup_input(update.effective_message.text or "")
    if parsed is None:
        await update.effective_message.reply_text(
            "Please send warm-up like: 5 1 (minutes distance_km)"
        )
        return WARMUP_INPUT

    minutes, distance = parsed
    db = get_db(context)
    db.set_session_warmup(int(workout["session_id"]), done=True, minutes=minutes, distance_km=distance)

    group = str(workout["muscle_group"])
    await update.effective_message.reply_text(
        f"Warm-up saved: {minutes:.2f} min, {distance:.2f} km."
    )
    await update.effective_message.reply_text(
        f"Pick an exercise for {group}:",
        reply_markup=exercise_keyboard(group),
    )
    return SELECT_EXERCISE


async def select_exercise_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text("No active session. Use /workout.")
        return ConversationHandler.END

    data = query.data or ""
    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)
    if not data.startswith(CB_EX_PREFIX):
        await query.edit_message_text("Invalid exercise selection. Use /workout to restart.")
        return ConversationHandler.END

    try:
        ex_index = int(data.split(":", 1)[1])
    except ValueError:
        await query.edit_message_text("Invalid exercise selection. Use /workout to restart.")
        return ConversationHandler.END

    group = str(workout["muscle_group"])
    exercises = EXERCISES_BY_GROUP.get(group, [])
    if ex_index < 0 or ex_index >= len(exercises):
        await query.edit_message_text("Exercise not found. Use /workout to restart.")
        return ConversationHandler.END

    exercise_name = exercises[ex_index]
    workout["exercise_name"] = exercise_name
    workout.pop("sets_target", None)
    workout.pop("reps_list", None)
    workout.pop("weights_list", None)
    workout.pop("current_weight", None)
    await query.edit_message_text(f"Exercise selected: {exercise_name}")
    await query.message.reply_text(
        "Choose number of sets (1-6):",
        reply_markup=sets_keyboard(),
    )
    return EX_SETS


async def sets_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text("No active session. Use /workout.")
        return ConversationHandler.END

    data = query.data or ""
    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)
    if not data.startswith(CB_SETS_PREFIX):
        await query.edit_message_text("Invalid sets selection. Use /workout to restart.")
        return ConversationHandler.END

    try:
        sets_count = int(data.split(":", 1)[1])
    except ValueError:
        await query.edit_message_text("Invalid sets selection. Use /workout to restart.")
        return ConversationHandler.END

    if sets_count < 1 or sets_count > 6:
        await query.edit_message_text("Sets must be between 1 and 6.")
        return EX_SETS

    workout["sets_target"] = sets_count
    workout["reps_list"] = []
    workout["weights_list"] = []
    workout.pop("current_weight", None)

    await query.edit_message_text(f"Sets selected: {sets_count}")
    await query.message.reply_text(
        f"Set 1/{sets_count}: choose reps (1-20)",
        reply_markup=reps_keyboard(),
    )
    return EX_REPS


async def reps_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text("No active session. Use /workout.")
        return ConversationHandler.END

    data = query.data or ""
    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)
    if not data.startswith(CB_REP_PREFIX):
        await query.edit_message_text("Invalid reps selection. Use /workout to restart.")
        return ConversationHandler.END

    try:
        rep = int(data.split(":", 1)[1])
    except ValueError:
        await query.edit_message_text("Invalid reps selection. Use /workout to restart.")
        return ConversationHandler.END

    if rep < 1 or rep > 20:
        await query.edit_message_text("Reps must be between 1 and 20.")
        return EX_REPS

    sets_target = int(workout.get("sets_target", 0))
    reps_list: List[int] = list(workout.get("reps_list", []))
    if sets_target <= 0:
        await query.edit_message_text("Sets are missing. Use /workout to restart.")
        return ConversationHandler.END
    if len(reps_list) >= sets_target:
        await query.edit_message_text("All sets already entered. Use /workout to restart.")
        return ConversationHandler.END

    reps_list.append(rep)
    workout["reps_list"] = reps_list

    set_no = len(reps_list)
    prev_weights: List[float] = list(workout.get("weights_list", []))
    if prev_weights:
        current_weight = clamp_weight_kg(prev_weights[-1])
    else:
        db = get_db(context)
        last_weight = db.get_last_weight(update.effective_user.id, str(workout.get("exercise_name", "")))
        current_weight = clamp_weight_kg(last_weight if last_weight is not None else 20.0)

    workout["current_weight"] = current_weight

    await query.edit_message_text(f"Set {set_no}/{sets_target} reps selected: {rep}")
    await query.message.reply_text(
        weight_prompt_text(set_no, sets_target, current_weight),
        reply_markup=weight_adjust_keyboard(can_copy_prev=len(prev_weights) > 0),
    )
    return EX_WEIGHT


async def weight_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text("No active session. Use /workout.")
        return ConversationHandler.END

    data = query.data or ""
    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)

    sets_target = int(workout.get("sets_target", 0))
    reps_list: List[int] = list(workout.get("reps_list", []))
    weights_list: List[float] = list(workout.get("weights_list", []))
    current_weight = clamp_weight_kg(float(workout.get("current_weight", 20.0)))
    set_no = len(reps_list)

    if sets_target <= 0 or set_no <= 0:
        await query.edit_message_text("Set context missing. Use /workout to restart.")
        return ConversationHandler.END

    if data.startswith(CB_WADJ_PREFIX):
        try:
            delta = float(data.split(":", 1)[1])
        except ValueError:
            await query.edit_message_text("Invalid weight adjustment.")
            return EX_WEIGHT
        current_weight = clamp_weight_kg(current_weight + delta)
        workout["current_weight"] = current_weight
        await query.edit_message_text(
            weight_prompt_text(set_no, sets_target, current_weight),
            reply_markup=weight_adjust_keyboard(can_copy_prev=len(weights_list) > 0),
        )
        return EX_WEIGHT

    if data == CB_WCOPY:
        if not weights_list:
            await query.answer("No previous set weight yet.", show_alert=True)
            return EX_WEIGHT
        current_weight = clamp_weight_kg(weights_list[-1])
        workout["current_weight"] = current_weight
        await query.edit_message_text(
            weight_prompt_text(set_no, sets_target, current_weight),
            reply_markup=weight_adjust_keyboard(can_copy_prev=True),
        )
        return EX_WEIGHT

    if data == CB_WCONFIRM:
        weights_list.append(current_weight)
        workout["weights_list"] = weights_list
        await query.edit_message_text(
            f"Set {set_no}/{sets_target} weight saved: {current_weight:.2f} kg"
        )

        if len(weights_list) < sets_target:
            next_set = len(weights_list) + 1
            await query.message.reply_text(
                f"Set {next_set}/{sets_target}: choose reps (1-20)",
                reply_markup=reps_keyboard(),
            )
            return EX_REPS

        workout["sets"] = sets_target
        workout["reps"] = max(1, round(sum(reps_list) / len(reps_list)))
        workout["reps_sequence"] = " ".join(str(x) for x in reps_list)
        weight_sequence = " ".join(f"{w:.2f}" for w in weights_list)
        primary_weight = max(weights_list)
        return await save_current_exercise(update, context, primary_weight, weight_sequence)

    await query.edit_message_text("Unknown action. Use /workout.")
    return ConversationHandler.END


async def save_current_exercise(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    primary_weight: float,
    weight_sequence: str,
) -> int:
    workout = context.user_data.get("workout")
    if not workout:
        await update.effective_message.reply_text("No active session. Use /workout.")
        return ConversationHandler.END

    required_keys = ("session_id", "muscle_group", "exercise_name", "sets", "reps", "reps_sequence")
    if any(k not in workout for k in required_keys):
        await update.effective_message.reply_text("Session data was incomplete. Use /workout to restart.")
        context.user_data.pop("workout", None)
        return ConversationHandler.END

    db = get_db(context)
    ex_id, volume = db.add_exercise(
        session_id=int(workout["session_id"]),
        user_id=update.effective_user.id,
        muscle_group=str(workout["muscle_group"]),
        name=str(workout["exercise_name"]),
        sets=int(workout["sets"]),
        reps=int(workout["reps"]),
        weight=float(primary_weight),
        reps_sequence=str(workout["reps_sequence"]),
        weight_sequence=weight_sequence,
    )
    workout["last_exercise_id"] = ex_id
    saved_name = str(workout["exercise_name"])

    workout.pop("exercise_name", None)
    workout.pop("sets", None)
    workout.pop("reps", None)
    workout.pop("reps_sequence", None)
    workout.pop("sets_target", None)
    workout.pop("reps_list", None)
    workout.pop("weights_list", None)
    workout.pop("current_weight", None)

    await update.effective_message.reply_text(
        f"Saved: {saved_name} | volume {volume:.2f}\nWhat next?",
        reply_markup=post_exercise_keyboard(),
    )
    return POST_ACTION


async def post_action_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text("No active workout. Use /workout.")
        return ConversationHandler.END

    data = query.data or ""
    db = get_db(context)
    user_id = update.effective_user.id

    if data == CB_NEXT_EXERCISE:
        group = str(workout["muscle_group"])
        await query.edit_message_text("Add the next exercise:")
        await query.message.reply_text(
            f"Pick an exercise for {group}:",
            reply_markup=exercise_keyboard(group),
        )
        return SELECT_EXERCISE

    if data == CB_REPLACE_EXERCISE:
        last_id = workout.get("last_exercise_id")
        if not last_id:
            await query.answer("No saved exercise to replace yet.", show_alert=True)
            return POST_ACTION

        deleted = db.delete_exercise(int(last_id), user_id)
        if not deleted:
            await query.answer("Could not replace the last exercise.", show_alert=True)
            return POST_ACTION

        workout["last_exercise_id"] = None
        group = str(workout["muscle_group"])
        await query.edit_message_text("Last exercise removed. Pick a replacement:")
        await query.message.reply_text(
            f"Pick an exercise for {group}:",
            reply_markup=exercise_keyboard(group),
        )
        return SELECT_EXERCISE

    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)

    await query.edit_message_text("Unknown action. Use /workout.")
    context.user_data.pop("workout", None)
    return ConversationHandler.END


async def finish_workout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    workout = context.user_data.get("workout")
    db = get_db(context)
    user = update.effective_user

    if not workout or not user:
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("No active workout. Use /workout.")
        else:
            await update.effective_message.reply_text("No active workout. Use /workout.")
        return ConversationHandler.END

    session_id = int(workout["session_id"])
    muscle_group = str(workout["muscle_group"])
    count, total_volume = db.get_session_totals(session_id)
    session = db.get_session(session_id)
    warmup_line = ""
    if session and int(session["warmup_done"] or 0) == 1:
        warmup_minutes = float(session["warmup_minutes"] or 0.0)
        warmup_distance = float(session["warmup_distance_km"] or 0.0)
        warmup_line = f"\nWarm-up: {warmup_minutes:.2f} min, {warmup_distance:.2f} km"

    if count > 0:
        db.close_session(session_id, "completed")
        db.set_next_group_after(user.id, muscle_group)
        next_group = db.get_next_group(user.id)
        text = (
            f"Workout ended.\n"
            f"Exercises saved: {count}\n"
            f"Total volume: {total_volume:.2f}\n"
            f"{warmup_line}"
            f"\n"
            f"Next scheduled group: {next_group}"
        )
    else:
        db.close_session(session_id, "cancelled")
        next_group = db.get_next_group(user.id)
        text = (
            "Workout ended with no exercises saved.\n"
            f"Next scheduled group remains: {next_group}"
        )

    context.user_data.pop("workout", None)

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text)
    else:
        await update.effective_message.reply_text(text)
    return ConversationHandler.END


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    workout = context.user_data.get("workout")
    if workout:
        db = get_db(context)
        db.close_session(int(workout["session_id"]), "cancelled")
    context.user_data.pop("workout", None)
    await update.effective_message.reply_text("Workout conversation cancelled.")
    return ConversationHandler.END


async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return

    db = get_db(context)
    rows = db.get_history_rows(user_id)
    if not rows:
        await update.effective_message.reply_text("No workout history found.")
        return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "timestamp_utc",
            "muscle_group",
            "exercise",
            "sets",
            "reps",
            "reps_sequence",
            "weight_kg",
            "weight_sequence",
            "warmup_done",
            "warmup_minutes",
            "warmup_distance_km",
            "volume",
            "session_id",
        ]
    )
    for r in rows:
        writer.writerow(
            [
                r["created_at"],
                r["muscle_group"],
                r["name"],
                r["sets"],
                r["reps"],
                r["reps_sequence"] or "",
                f"{float(r['weight']):.2f}",
                r["weight_sequence"] or "",
                int(r["warmup_done"] or 0),
                f"{float(r['warmup_minutes'] or 0.0):.2f}",
                f"{float(r['warmup_distance_km'] or 0.0):.2f}",
                f"{float(r['volume']):.2f}",
                r["session_id"],
            ]
        )

    csv_bytes = output.getvalue().encode("utf-8")
    filename = f"gymbot_history_{now_utc().strftime('%Y%m%d_%H%M%S')}.csv"
    await update.effective_message.reply_document(
        document=InputFile(io.BytesIO(csv_bytes), filename=filename),
        caption="Workout history export (CSV)",
    )


def render_group_volume_lines(group_volumes: Dict[str, float]) -> str:
    lines = []
    for group in ROTATION:
        lines.append(f"{group}: {group_volumes.get(group, 0.0):.2f}")
    return "\n".join(lines)


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return

    db = get_db(context)
    start = start_of_today_utc()
    end = start + timedelta(days=1)
    summary = db.get_summary(user_id, start, end)

    text = (
        "Today Summary (UTC)\n"
        f"Completed workouts: {summary['session_count']}\n"
        f"Exercises logged: {summary['exercise_count']}\n"
        f"Total volume: {summary['total_volume']:.2f}\n"
        f"Warm-up sessions: {summary['warmup_count']}\n"
        f"Warm-up total: {summary['warmup_minutes_total']:.2f} min, {summary['warmup_distance_total']:.2f} km\n"
        "Volume by muscle group:\n"
        f"{render_group_volume_lines(summary['group_volumes'])}"
    )
    await update.effective_message.reply_text(text)


async def thisweek_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return

    db = get_db(context)
    now = now_utc()
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end = week_start + timedelta(days=7)
    summary = db.get_summary(user_id, week_start, week_end)

    text = (
        "This Week Summary (UTC)\n"
        f"Week: {week_start.date()} to {(week_end - timedelta(days=1)).date()}\n"
        f"Completed workouts: {summary['session_count']}\n"
        f"Exercises logged: {summary['exercise_count']}\n"
        f"Total weekly volume: {summary['total_volume']:.2f}\n"
        f"Warm-up sessions: {summary['warmup_count']}\n"
        f"Warm-up total: {summary['warmup_minutes_total']:.2f} min, {summary['warmup_distance_total']:.2f} km\n"
        "Weekly volume by muscle group:\n"
        f"{render_group_volume_lines(summary['group_volumes'])}"
    )
    await update.effective_message.reply_text(text)


async def pr_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return

    db = get_db(context)
    records = db.get_personal_records(user_id)
    if not records:
        await update.effective_message.reply_text("No PRs yet. Log a workout with /workout.")
        return

    lines = ["Personal Records (max weight by exercise):"]
    for r in records:
        lines.append(f"{r['name']}: {float(r['max_weight']):.2f} kg")
    await update.effective_message.reply_text("\n".join(lines))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error while processing update", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="An unexpected error occurred. Please try again.",
            )
    except Exception:
        logger.exception("Failed to notify user about the error")


async def on_startup(application: Application) -> None:
    if application.job_queue is None:
        logger.warning("Job queue is unavailable. Install python-telegram-bot[job-queue].")
        return

    db: GymDB = application.bot_data["db"]
    users = db.list_users_for_reminders()
    for user_id, chat_id in users:
        schedule_user_reminder(application, user_id, chat_id)

    logger.info("Startup complete. Scheduled reminders for %d users.", len(users))


def print_deployment_instructions() -> None:
    print("\n=== GymBot Deployment Instructions ===")
    print("1) Install dependencies:")
    print("   pip install -r requirements.txt")
    print("2) Set environment variables:")
    print("   TELEGRAM_BOT_TOKEN=your_bot_token")
    print("   GYMBOT_DB_PATH=./gymbot.db")
    print("   GYMBOT_REMINDER_HOUR_UTC=18")
    print("   GYMBOT_REMINDER_MINUTE_UTC=0")
    print("3) Run in polling mode:")
    print("   python main.py")
    print("4) Optional webhook mode (server deployment):")
    print("   GYMBOT_USE_WEBHOOK=true")
    print("   GYMBOT_WEBHOOK_URL=https://your-domain.com")
    print("   PORT=8080")
    print("   python main.py")
    print("5) Ensure persistent disk for SQLite in production.")
    print("=== End Deployment Instructions ===\n")


def build_application() -> Application:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required.")

    db_path = (os.getenv("DB_PATH") or os.getenv("GYMBOT_DB_PATH") or "/data/gymbot.db").strip()
    db = GymDB(db_path)
    db.init_schema()

    app = Application.builder().token(token).post_init(on_startup).build()
    app.bot_data["db"] = db

    workout_conv = ConversationHandler(
        entry_points=[CommandHandler("workout", workout_cmd)],
        states={
            SELECT_MUSCLE: [
                CallbackQueryHandler(
                    select_muscle_cb,
                    pattern=r"^(group:.+|skip_day|end_workout)$",
                )
            ],
            WARMUP_CHOICE: [
                CallbackQueryHandler(
                    warmup_choice_cb,
                    pattern=r"^(warmup_yes|warmup_no|finish_session)$",
                )
            ],
            WARMUP_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, warmup_input_msg),
                CallbackQueryHandler(finish_workout, pattern=r"^finish_session$"),
            ],
            SELECT_EXERCISE: [
                CallbackQueryHandler(
                    select_exercise_cb,
                    pattern=r"^(ex:\d+|finish_session)$",
                ),
            ],
            EX_SETS: [
                CallbackQueryHandler(
                    sets_choice_cb,
                    pattern=r"^(sets:[1-6]|finish_session)$",
                ),
            ],
            EX_REPS: [
                CallbackQueryHandler(
                    reps_choice_cb,
                    pattern=r"^(rep:(?:[1-9]|1[0-9]|20)|finish_session)$",
                ),
            ],
            EX_WEIGHT: [
                CallbackQueryHandler(
                    weight_choice_cb,
                    pattern=r"^(wadj:[+-]?(?:\d+(?:\.\d+)?)|wconfirm|wcopy|finish_session)$",
                ),
            ],
            POST_ACTION: [
                CallbackQueryHandler(
                    post_action_cb,
                    pattern=r"^(next_exercise|replace_exercise|finish_session)$",
                )
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
        allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("today", today_cmd))
    app.add_handler(CommandHandler("thisweek", thisweek_cmd))
    app.add_handler(CommandHandler("pr", pr_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(workout_conv)

    app.add_error_handler(error_handler)
    return app


def main() -> None:
    app = build_application()
    print_deployment_instructions()

    use_webhook = os.getenv("GYMBOT_USE_WEBHOOK", "false").strip().lower() == "true"
    if use_webhook:
        webhook_url = os.getenv("GYMBOT_WEBHOOK_URL", "").strip()
        if not webhook_url:
            raise RuntimeError("GYMBOT_WEBHOOK_URL is required when GYMBOT_USE_WEBHOOK=true.")

        listen = os.getenv("GYMBOT_WEBHOOK_LISTEN", "0.0.0.0").strip()
        port = int(os.getenv("PORT", "8080"))
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        url_path = os.getenv("GYMBOT_WEBHOOK_PATH", token)

        app.run_webhook(
            listen=listen,
            port=port,
            url_path=url_path,
            webhook_url=f"{webhook_url.rstrip('/')}/{url_path}",
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)


if __name__ == "__main__":
    main()
