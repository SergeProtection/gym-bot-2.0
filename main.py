import csv
import io
import logging
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from telegram import BotCommand, BotCommandScopeChat, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
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
EXERCISE_ASSETS_DIR = Path((os.getenv("GYMBOT_EXERCISE_DIR") or "Exercise").strip())
BODYWEIGHT_EXERCISE_PDF = EXERCISE_ASSETS_DIR / "Exercises with body weight.pdf"
EXERCISE_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp"}
EXCLUDED_EXERCISE_IMAGE_STEMS = {
    "chest",
    "abs",
    "back",
    "biceps",
    "calves",
    "legs",
    "shoulders",
    "triceps",
}
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

SELECT_MUSCLE, BODYWEIGHT_INPUT, WARMUP_CHOICE, WARMUP_INPUT, SELECT_EXERCISE, EX_SETS, EX_REPS, EX_WEIGHT, POST_ACTION = range(9)

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
CB_WBODY = "wbody"
CB_BACK_EXERCISE = "back_exercise"
CB_LANG_PREFIX = "lang:"

SUPPORTED_LANGS = ("en", "id", "ru")
LANG_LABELS = {
    "en": "English",
    "id": "Bahasa Indonesia",
    "ru": "Русский",
}

LANG_COMMAND_SETS: Dict[str, List[Tuple[str, str]]] = {
    "en": [
        ("start", "Register and start"),
        ("workout", "Log a workout"),
        ("last", "Last 3 workouts"),
        ("history", "Export workout history"),
        ("today", "Today summary"),
        ("thisweek", "This week summary"),
        ("pr", "Personal records"),
        ("help", "Show help"),
        ("cancel", "Cancel current flow"),
    ],
    "id": [
        ("mulai", "Daftar dan mulai"),
        ("latihan", "Catat latihan"),
        ("terakhir", "3 latihan terakhir"),
        ("riwayat", "Ekspor riwayat"),
        ("hariini", "Ringkasan hari ini"),
        ("mingguini", "Ringkasan minggu ini"),
        ("rekor", "Rekor pribadi"),
        ("bantuan", "Tampilkan bantuan"),
        ("batal", "Batalkan alur saat ini"),
    ],
    "ru": [
        ("start", "Запуск и регистрация"),
        ("tren", "Записать тренировку"),
        ("poslednie", "Last 3 workouts"),
        ("istoriya", "Экспорт истории"),
        ("segodnya", "Итоги за сегодня"),
        ("nedelya", "Итоги за неделю"),
        ("rekord", "Личные рекорды"),
        ("pomosh", "Показать помощь"),
        ("otmena", "Отменить текущий ввод"),
    ],
}

TR: Dict[str, Dict[str, str]] = {
    "en": {
        "select_language": "Choose your language:",
        "language_saved": "Language saved.",
        "welcome": "Welcome to GymBot.\nUse /workout to log a workout session.\nNext in your 4-day rotation: {next_group}\n\nCommands:\n/workout, /last, /history, /today, /thisweek, /pr, /help",
        "welcome_free_plan": "Welcome to GymBot.\nUse /workout to log a workout session.\nAvailable muscle groups: {groups}\nRecent muscle groups: {recent}\n\nCommands:\n/workout, /last, /history, /today, /thisweek, /pr, /help",
        "help": "/start - Register and initialize reminders\n/workout - Log a workout\n/last - Last 3 completed workouts\n/history - Export workout history CSV\n/today - Today summary stats\n/thisweek - Weekly volume by muscle group\n/pr - Personal records (max weight by exercise)\n/cancel - Cancel active workout conversation",
        "none_yet": "None yet",
        "skip_day": "Skip day",
        "end_workout": "End workout",
        "yes_warmup": "Yes, I did warm-up run",
        "no_warmup": "No warm-up",
        "add_another": "Add another exercise",
        "replace_exercise": "Replace exercise",
        "back_exercise": "Back to exercises",
        "back_exercise_done": "Selection cleared. Pick an exercise again:",
        "use_prev_weight": "Use previous set weight",
        "use_body_weight": "My bodyweight",
        "confirm_weight": "Confirm weight",
        "closed_unfinished": "Closed a previously unfinished workout session.",
        "choose_muscle": "Choose the muscle group you're training today:\nRecent muscle groups: {recent}",
        "workout_ended": "Workout ended.",
        "invalid_selection_restart": "Invalid selection. Use /workout to start again.",
        "unknown_group_restart": "Unknown muscle group. Use /workout to start again.",
        "workout_started": "{group} workout started.",
        "ask_body_weight": "Enter your bodyweight in kg (example: 72.4):",
        "invalid_body_weight": "Please enter a valid bodyweight in kg (example: 72.4).",
        "body_weight_saved": "Bodyweight saved: {body_weight:.2f} kg",
        "did_warmup": "Did you do your warm-up run?",
        "no_active_session": "No active session. Use /workout.",
        "warmup_skipped": "Warm-up skipped.",
        "pick_exercise": "Pick an exercise for {group}:",
        "send_warmup": "Send warm-up as: minutes distance_km\nExample: 5 1",
        "invalid_option_restart": "Invalid option. Use /workout to restart.",
        "warmup_format_error": "Please send warm-up like: 5 1 (minutes distance_km)",
        "warmup_saved": "Warm-up saved: {minutes:.2f} min, {distance:.2f} km.",
        "invalid_exercise_restart": "Invalid exercise selection. Use /workout to restart.",
        "exercise_not_found": "Exercise not found. Use /workout to restart.",
        "exercise_selected": "Exercise selected: {exercise}",
        "choose_sets": "Choose number of sets (1-6):",
        "invalid_sets_restart": "Invalid sets selection. Use /workout to restart.",
        "sets_range": "Sets must be between 1 and 6.",
        "sets_selected": "Sets selected: {sets}",
        "choose_reps": "Set {set_no}/{sets}: choose reps (1-20)",
        "invalid_reps_restart": "Invalid reps selection. Use /workout to restart.",
        "reps_range": "Reps must be between 1 and 20.",
        "sets_missing_restart": "Sets are missing. Use /workout to restart.",
        "all_sets_entered_restart": "All sets already entered. Use /workout to restart.",
        "set_reps_selected": "Set {set_no}/{sets} reps selected: {rep}",
        "set_context_missing": "Set context missing. Use /workout to restart.",
        "invalid_weight_adjustment": "Invalid weight adjustment.",
        "no_prev_weight": "No previous set weight yet.",
        "set_weight_saved": "Set {set_no}/{sets} weight saved: {weight:.2f} kg",
        "unknown_action_restart": "Unknown action. Use /workout.",
        "session_incomplete_restart": "Session data was incomplete. Use /workout to restart.",
        "saved_line": "Saved: {name} | volume {volume:.2f}{pr_line}\nWhat next?",
        "first_pr": "\n🏆 First PR set for {name}: {weight:.2f} kg 💪",
        "new_pr": "\n🏆 New PR for {name}: {old:.2f} -> {new:.2f} kg 💪",
        "no_active_workout": "No active workout. Use /workout.",
        "add_next_exercise": "Add the next exercise:",
        "replace_pick": "Last exercise removed. Pick a replacement:",
        "replace_not_found": "Could not replace the last exercise.",
        "replace_none": "No saved exercise to replace yet.",
        "cancelled": "Workout conversation cancelled.",
        "no_history": "No workout history found.",
        "history_caption": "Workout history export (CSV)",
        "last_header": "Last 3 completed workouts (UTC):",
        "last_line": "{idx}. {ended} | {group} | exercises: {exercise_count} | volume: {total_volume:.2f} | bodyweight: {body_weight} ({delta})",
        "no_last_workouts": "No completed workouts found yet.",
        "no_body_weight_value": "No bodyweight recorded for this workout.",
        "body_weight_change_unknown": "not available",
        "body_weight_change_gain": "gaining +{delta:.2f} kg",
        "body_weight_change_loss": "losing {delta:.2f} kg",
        "body_weight_change_same": "no change",
        "body_weight_change_first": "first record",
        "today_summary": "Today Summary (UTC)\nCompleted workouts: {session_count}\nExercises logged: {exercise_count}\nTotal volume: {total_volume:.2f}\nWarm-up sessions: {warmup_count}\nWarm-up total: {warmup_minutes_total:.2f} min, {warmup_distance_total:.2f} km\nVolume by muscle group:\n{group_lines}",
        "week_summary": "This Week Summary (UTC)\nWeek: {start_date} to {end_date}\nCompleted workouts: {session_count}\nExercises logged: {exercise_count}\nTotal weekly volume: {total_volume:.2f}\nWarm-up sessions: {warmup_count}\nWarm-up total: {warmup_minutes_total:.2f} min, {warmup_distance_total:.2f} km\nWeekly volume by muscle group:\n{group_lines}",
        "no_prs": "No PRs yet. Log a workout with /workout.",
        "pr_header": "🏆 Personal Records (max weight by exercise):",
        "pr_line": "💪 {name}: {weight:.2f} kg",
        "error_text": "An unexpected error occurred. Please try again.",
        "workout_finish": "Workout ended.\nExercises saved: {count}\nTotal volume: {volume:.2f}{warmup_line}\nNext scheduled group: {next_group}",
        "workout_finish_empty": "Workout ended with no exercises saved.\nNext scheduled group remains: {next_group}",
        "workout_finish_free": "Workout ended.\nExercises saved: {count}\nTotal volume: {volume:.2f}{warmup_line}\nRecent muscle groups: {recent}",
        "workout_finish_empty_free": "Workout ended with no exercises saved.\nRecent muscle groups: {recent}",
        "warmup_line": "\nWarm-up: {minutes:.2f} min, {distance:.2f} km",
        "body_weight_line": "\nBodyweight: {body_weight} ({delta})",
        "skipped_day": "Skipped day recorded for {skipped}.\nNext scheduled group: {next_group}",
        "reminder_free": "GymBot reminder:\nTime to log your workout.\nRecent muscle groups: {recent}\nUse /workout to log your session.",
    },
    "id": {
        "select_language": "Pilih bahasa Anda:",
        "language_saved": "Bahasa disimpan.",
        "welcome": "Selamat datang di GymBot.\nGunakan /latihan untuk mencatat latihan.\nGiliran berikutnya dalam rotasi 4 hari: {next_group}\n\nPerintah:\n/latihan, /riwayat, /hariini, /mingguini, /rekor, /bantuan",
        "help": "/mulai - Daftar dan aktifkan pengingat\n/latihan - Catat latihan\n/terakhir - 3 latihan selesai terakhir\n/riwayat - Ekspor riwayat CSV\n/hariini - Ringkasan hari ini\n/mingguini - Volume mingguan per otot\n/rekor - Rekor pribadi (beban maksimum)\n/batal - Batalkan sesi latihan",
        "skip_day": "Lewati hari",
        "end_workout": "Selesai latihan",
        "yes_warmup": "Ya, saya pemanasan lari",
        "no_warmup": "Tanpa pemanasan",
        "add_another": "Tambah latihan lain",
        "replace_exercise": "Ganti latihan",
        "use_prev_weight": "Pakai beban set sebelumnya",
        "confirm_weight": "Konfirmasi beban",
    },
    "ru": {
        "select_language": "Выберите язык:",
        "language_saved": "Язык сохранен.",
        "welcome": "Добро пожаловать в GymBot.\nИспользуйте /tren для записи тренировки.\nСледующая группа в 4-дневной ротации: {next_group}\n\nКоманды:\n/tren, /istoriya, /segodnya, /nedelya, /rekord, /pomosh",
        "help": "/start - Регистрация и напоминания\n/tren - Записать тренировку\n/istoriya - Экспорт CSV\n/segodnya - Статистика за сегодня\n/nedelya - Недельный объем по мышцам\n/rekord - Личные рекорды (макс. вес)\n/otmena - Отменить текущую тренировку",
        "skip_day": "Пропустить день",
        "end_workout": "Завершить",
        "yes_warmup": "Да, сделал разминку",
        "no_warmup": "Без разминки",
        "add_another": "Добавить упражнение",
        "replace_exercise": "Заменить упражнение",
        "use_prev_weight": "Вес предыдущего подхода",
        "confirm_weight": "Подтвердить вес",
    },
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso() -> str:
    return to_iso(now_utc())


def start_of_today_utc() -> datetime:
    n = now_utc()
    return n.replace(hour=0, minute=0, second=0, microsecond=0)


def format_iso_utc(ts: str) -> str:
    try:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return ts
    return dt.strftime("%Y-%m-%d %H:%M")


def parse_weight(text: str) -> Optional[float]:
    try:
        value = Decimal(text.strip().replace(",", "."))
    except (InvalidOperation, AttributeError):
        return None
    if value < 0:
        return None
    return float(value)


def parse_body_weight(text: str) -> Optional[float]:
    value = parse_weight(text)
    if value is None:
        return None
    if value < 20 or value > 400:
        return None
    return round(value, 2)


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


ExerciseOption = Tuple[str, Optional[Path]]


def normalize_key(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def pretty_exercise_name(stem: str) -> str:
    cleaned = stem.replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", cleaned).strip()


def canonical_group_name(raw_name: str) -> str:
    cleaned = raw_name.replace("_", " ").replace("-", " ").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if cleaned.lower().endswith(" exercise"):
        cleaned = cleaned[: -len(" exercise")].strip()
    return " ".join(part.capitalize() for part in cleaned.split())


def load_exercise_catalog(base_dir: Path) -> Dict[str, List[ExerciseOption]]:
    catalog: Dict[str, List[ExerciseOption]] = {}
    if base_dir.exists() and base_dir.is_dir():
        for group_dir in sorted(base_dir.iterdir(), key=lambda p: p.name.lower()):
            if not group_dir.is_dir():
                continue

            group_name = canonical_group_name(group_dir.name)
            group_key = normalize_key(group_name)
            options: List[ExerciseOption] = []
            for image_path in sorted(group_dir.iterdir(), key=lambda p: p.name.lower()):
                if not image_path.is_file() or image_path.suffix.lower() not in EXERCISE_IMAGE_SUFFIXES:
                    continue
                stem_key = normalize_key(image_path.stem)
                if not stem_key or stem_key == group_key or stem_key in EXCLUDED_EXERCISE_IMAGE_STEMS:
                    continue
                options.append((pretty_exercise_name(image_path.stem), image_path))

            if options:
                catalog[group_name] = options

    if not catalog:
        for group in MUSCLE_OPTIONS:
            catalog[group] = [(name, None) for name in EXERCISES_BY_GROUP.get(group, [])]

    return catalog


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
                    language TEXT,
                    rotation_index INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS workout_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    muscle_group TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    body_weight_kg REAL,
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

            user_cols = {row["name"] for row in conn.execute("PRAGMA table_info(users)")}
            if "language" not in user_cols:
                conn.execute("ALTER TABLE users ADD COLUMN language TEXT")

            session_cols = {row["name"] for row in conn.execute("PRAGMA table_info(workout_sessions)")}
            if "body_weight_kg" not in session_cols:
                conn.execute("ALTER TABLE workout_sessions ADD COLUMN body_weight_kg REAL")
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

    def list_users_with_language(self) -> List[Tuple[int, int, str]]:
        with closing(self.connect()) as conn:
            rows = conn.execute(
                "SELECT user_id, chat_id, language FROM users WHERE chat_id IS NOT NULL"
            ).fetchall()
        result: List[Tuple[int, int, str]] = []
        for row in rows:
            lang = row["language"] if row["language"] in SUPPORTED_LANGS else "en"
            result.append((int(row["user_id"]), int(row["chat_id"]), str(lang)))
        return result

    def get_recent_trained_groups(self, user_id: int, limit: int = 3) -> List[str]:
        with closing(self.connect()) as conn:
            rows = conn.execute(
                """
                SELECT muscle_group
                FROM workout_sessions
                WHERE user_id = ?
                  AND status = 'completed'
                ORDER BY ended_at DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        return [str(row["muscle_group"]) for row in rows if row["muscle_group"]]

    def get_last_completed_workouts(self, user_id: int, limit: int = 3) -> List[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT
                    ws.id,
                    ws.muscle_group,
                    ws.ended_at,
                    ws.body_weight_kg,
                    COUNT(e.id) AS exercise_count,
                    COALESCE(SUM(e.volume), 0) AS total_volume
                FROM workout_sessions ws
                LEFT JOIN exercises e ON e.session_id = ws.id
                WHERE ws.user_id = ?
                  AND ws.status = 'completed'
                GROUP BY ws.id, ws.muscle_group, ws.ended_at, ws.body_weight_kg
                ORDER BY ws.ended_at DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()

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

    def set_session_body_weight(self, session_id: int, body_weight_kg: float) -> None:
        with closing(self.connect()) as conn, conn:
            conn.execute(
                """
                UPDATE workout_sessions
                SET body_weight_kg = ?
                WHERE id = ?
                """,
                (body_weight_kg, session_id),
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
                    ws.warmup_distance_km,
                    ws.body_weight_kg
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

        group_volumes: Dict[str, float] = {}
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

    def get_exercise_max_weight(self, user_id: int, exercise_name: str) -> Optional[float]:
        with closing(self.connect()) as conn:
            row = conn.execute(
                """
                SELECT MAX(weight) AS max_weight
                FROM exercises
                WHERE user_id = ? AND name = ?
                """,
                (user_id, exercise_name),
            ).fetchone()
        if row is None or row["max_weight"] is None:
            return None
        return float(row["max_weight"])

    def get_user_language(self, user_id: int) -> Optional[str]:
        with closing(self.connect()) as conn:
            row = conn.execute(
                "SELECT language FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        lang = row["language"]
        if lang in SUPPORTED_LANGS:
            return str(lang)
        return None

    def set_user_language(self, user_id: int, language: str) -> None:
        if language not in SUPPORTED_LANGS:
            return
        with closing(self.connect()) as conn, conn:
            conn.execute(
                "UPDATE users SET language = ?, updated_at = ? WHERE user_id = ?",
                (language, now_iso(), user_id),
            )

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


def tr(lang: str, key: str, **kwargs: object) -> str:
    lang_map = TR.get(lang, TR["en"])
    template = lang_map.get(key) or TR["en"].get(key, key)
    return template.format(**kwargs)


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(LANG_LABELS["en"], callback_data=f"{CB_LANG_PREFIX}en")],
            [InlineKeyboardButton(LANG_LABELS["id"], callback_data=f"{CB_LANG_PREFIX}id")],
            [InlineKeyboardButton(LANG_LABELS["ru"], callback_data=f"{CB_LANG_PREFIX}ru")],
        ]
    )


def user_lang(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> str:
    lang = get_db(context).get_user_language(user_id)
    return lang if lang in SUPPORTED_LANGS else "en"


def get_muscle_groups(context: ContextTypes.DEFAULT_TYPE) -> List[str]:
    catalog = context.application.bot_data.get("exercise_catalog", {})
    if isinstance(catalog, dict) and catalog:
        groups = [group for group, options in catalog.items() if isinstance(options, list) and options]
        if groups:
            return sorted(groups, key=str.lower)
    return list(MUSCLE_OPTIONS)


def recent_groups_text(db: GymDB, user_id: int, lang: str, limit: int = 3) -> str:
    recent = db.get_recent_trained_groups(user_id, limit=limit)
    return ", ".join(recent) if recent else tr(lang, "none_yet")


def body_weight_change_text(lang: str, current_bw: Optional[float], previous_bw: Optional[float]) -> str:
    if current_bw is None:
        return tr(lang, "body_weight_change_unknown")
    if previous_bw is None:
        return tr(lang, "body_weight_change_first")
    delta = round(current_bw - previous_bw, 2)
    if abs(delta) < 0.01:
        return tr(lang, "body_weight_change_same")
    if delta > 0:
        return tr(lang, "body_weight_change_gain", delta=delta)
    return tr(lang, "body_weight_change_loss", delta=delta)


def welcome_text(context: ContextTypes.DEFAULT_TYPE, user_id: int, lang: str) -> str:
    db = get_db(context)
    return tr(
        lang,
        "welcome_free_plan",
        groups=", ".join(get_muscle_groups(context)),
        recent=recent_groups_text(db, user_id, lang),
    )


async def ensure_language_selected(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int) -> Optional[str]:
    db = get_db(context)
    lang = db.get_user_language(user_id)
    if lang:
        return lang

    prompt = tr("en", "select_language")
    if update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(prompt, reply_markup=language_keyboard())
    elif update.effective_message:
        await update.effective_message.reply_text(prompt, reply_markup=language_keyboard())
    return None


async def set_chat_commands_for_language(bot, chat_id: int, lang: str) -> None:
    commands = LANG_COMMAND_SETS.get(lang) or LANG_COMMAND_SETS["en"]
    try:
        await bot.set_my_commands(
            commands=[BotCommand(command=name, description=desc) for name, desc in commands],
            scope=BotCommandScopeChat(chat_id=chat_id),
        )
    except Exception:
        logger.exception("Failed setting chat command menu for chat_id=%s lang=%s", chat_id, lang)


def group_keyboard(muscle_groups: List[str], lang: str) -> InlineKeyboardMarkup:
    rows = []
    for group in muscle_groups:
        rows.append([InlineKeyboardButton(group, callback_data=f"{CB_GROUP_PREFIX}{group}")])
    rows.append([InlineKeyboardButton(tr(lang, "end_workout"), callback_data=CB_END_WORKOUT)])
    return InlineKeyboardMarkup(rows)


def end_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(tr(lang, "end_workout"), callback_data=CB_FINISH_SESSION)]]
    )


def get_exercise_options(context: ContextTypes.DEFAULT_TYPE, muscle_group: str) -> List[ExerciseOption]:
    catalog = context.application.bot_data.get("exercise_catalog", {})
    if isinstance(catalog, dict):
        options = catalog.get(muscle_group)
        if isinstance(options, list) and options:
            return options
    return [(name, None) for name in EXERCISES_BY_GROUP.get(muscle_group, [])]


def clear_pending_exercise_input(workout: Dict[str, object]) -> None:
    workout.pop("exercise_name", None)
    workout.pop("sets", None)
    workout.pop("reps", None)
    workout.pop("reps_sequence", None)
    workout.pop("sets_target", None)
    workout.pop("reps_list", None)
    workout.pop("weights_list", None)
    workout.pop("current_weight", None)


async def back_to_exercise_list(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> int:
    query = update.callback_query
    workout = context.user_data.get("workout")
    if not query or not workout:
        return ConversationHandler.END

    clear_pending_exercise_input(workout)
    group = str(workout["muscle_group"])
    exercise_options = get_exercise_options(context, group)
    await query.edit_message_text(tr(lang, "back_exercise_done"))
    await query.message.reply_text(
        tr(lang, "pick_exercise", group=group),
        reply_markup=exercise_keyboard(exercise_options, lang),
    )
    return SELECT_EXERCISE


def exercise_keyboard(exercises: List[ExerciseOption], lang: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(name, callback_data=f"{CB_EX_PREFIX}{idx}")]
        for idx, (name, _) in enumerate(exercises)
    ]
    rows.append([InlineKeyboardButton(tr(lang, "end_workout"), callback_data=CB_FINISH_SESSION)])
    return InlineKeyboardMarkup(rows)


def warmup_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(tr(lang, "yes_warmup"), callback_data=CB_WARMUP_YES),
                InlineKeyboardButton(tr(lang, "no_warmup"), callback_data=CB_WARMUP_NO),
            ],
            [InlineKeyboardButton(tr(lang, "end_workout"), callback_data=CB_FINISH_SESSION)],
        ]
    )


def sets_keyboard(lang: str) -> InlineKeyboardMarkup:
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
            [InlineKeyboardButton(tr(lang, "back_exercise"), callback_data=CB_BACK_EXERCISE)],
            [InlineKeyboardButton(tr(lang, "end_workout"), callback_data=CB_FINISH_SESSION)],
        ]
    )


def reps_keyboard(lang: str) -> InlineKeyboardMarkup:
    rows = []
    for start in (1, 6, 11, 16):
        row = [
            InlineKeyboardButton(str(rep), callback_data=f"{CB_REP_PREFIX}{rep}")
            for rep in range(start, start + 5)
        ]
        rows.append(row)
    rows.append([InlineKeyboardButton(tr(lang, "back_exercise"), callback_data=CB_BACK_EXERCISE)])
    rows.append([InlineKeyboardButton(tr(lang, "end_workout"), callback_data=CB_FINISH_SESSION)])
    return InlineKeyboardMarkup(rows)


def weight_adjust_keyboard(
    can_copy_prev: bool,
    body_weight_kg: Optional[float],
    allow_bodyweight_button: bool,
    lang: str,
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("-20", callback_data=f"{CB_WADJ_PREFIX}-20"),
            InlineKeyboardButton("-10", callback_data=f"{CB_WADJ_PREFIX}-10"),
            InlineKeyboardButton("-2.5", callback_data=f"{CB_WADJ_PREFIX}-2.5"),
            InlineKeyboardButton("-1", callback_data=f"{CB_WADJ_PREFIX}-1"),
        ],
        [
            InlineKeyboardButton("+1", callback_data=f"{CB_WADJ_PREFIX}1"),
            InlineKeyboardButton("+2.5", callback_data=f"{CB_WADJ_PREFIX}2.5"),
            InlineKeyboardButton("+10", callback_data=f"{CB_WADJ_PREFIX}10"),
            InlineKeyboardButton("+20", callback_data=f"{CB_WADJ_PREFIX}20"),
            InlineKeyboardButton("+50", callback_data=f"{CB_WADJ_PREFIX}50"),
        ],
    ]
    if can_copy_prev:
        rows.append([InlineKeyboardButton(tr(lang, "use_prev_weight"), callback_data=CB_WCOPY)])
    if allow_bodyweight_button and body_weight_kg is not None:
        rows.append([InlineKeyboardButton(tr(lang, "use_body_weight"), callback_data=CB_WBODY)])
    rows.append([InlineKeyboardButton(tr(lang, "confirm_weight"), callback_data=CB_WCONFIRM)])
    rows.append([InlineKeyboardButton(tr(lang, "back_exercise"), callback_data=CB_BACK_EXERCISE)])
    rows.append([InlineKeyboardButton(tr(lang, "end_workout"), callback_data=CB_FINISH_SESSION)])
    return InlineKeyboardMarkup(rows)


def weight_prompt_text(set_no: int, total_sets: int, current_weight: float) -> str:
    return (
        f"Set {set_no}/{total_sets} weight\n"
        f"Current: {current_weight:.2f} kg\n"
        "Adjust with buttons, then tap Confirm weight."
    )


def post_exercise_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(tr(lang, "add_another"), callback_data=CB_NEXT_EXERCISE)],
            [InlineKeyboardButton(tr(lang, "replace_exercise"), callback_data=CB_REPLACE_EXERCISE)],
            [InlineKeyboardButton(tr(lang, "end_workout"), callback_data=CB_FINISH_SESSION)],
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
    uid = int(user_id)
    lang = db.get_user_language(uid) or "en"
    recent = recent_groups_text(db, uid, lang)
    text = tr(lang, "reminder_free", recent=recent)
    try:
        await context.bot.send_message(chat_id=int(chat_id), text=text)
    except Exception:
        logger.exception("Failed to send daily reminder to user_id=%s", user_id)


async def language_select_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user_id = ensure_registered(update, context)
    if user_id is None:
        return

    data = query.data or ""
    lang = data.split(":", 1)[1] if ":" in data else ""
    if lang not in SUPPORTED_LANGS:
        return

    db = get_db(context)
    db.set_user_language(user_id, lang)
    schedule_user_reminder(context.application, user_id, update.effective_chat.id)
    await set_chat_commands_for_language(context.bot, update.effective_chat.id, lang)

    await query.edit_message_text(tr(lang, "language_saved"))
    await query.message.reply_text(welcome_text(context, user_id, lang))


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return

    lang = await ensure_language_selected(update, context, user_id)
    if not lang:
        return

    schedule_user_reminder(context.application, user_id, update.effective_chat.id)
    await set_chat_commands_for_language(context.bot, update.effective_chat.id, lang)
    await update.effective_message.reply_text(welcome_text(context, user_id, lang))


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return
    lang = await ensure_language_selected(update, context, user_id)
    if not lang:
        return
    await update.effective_message.reply_text(tr(lang, "help"))


async def workout_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return ConversationHandler.END
    lang = await ensure_language_selected(update, context, user_id)
    if not lang:
        return ConversationHandler.END

    db = get_db(context)
    active = db.get_active_session(user_id)
    if active:
        db.close_session(int(active["id"]), "cancelled")
        await update.effective_message.reply_text(tr(lang, "closed_unfinished"))

    muscle_groups = get_muscle_groups(context)
    recent = recent_groups_text(db, user_id, lang)
    context.user_data.pop("workout", None)

    await update.effective_message.reply_text(
        tr(lang, "choose_muscle", recent=recent),
        reply_markup=group_keyboard(muscle_groups, lang),
    )
    return SELECT_MUSCLE


async def select_muscle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    user = update.effective_user
    if not user:
        return ConversationHandler.END
    lang = user_lang(context, user.id)

    db = get_db(context)
    data = query.data or ""

    if data == CB_END_WORKOUT:
        context.user_data.pop("workout", None)
        await query.edit_message_text(tr(lang, "workout_ended"))
        return ConversationHandler.END

    if not data.startswith(CB_GROUP_PREFIX):
        await query.edit_message_text(tr(lang, "invalid_selection_restart"))
        return ConversationHandler.END

    group = data.split(":", 1)[1]
    if group not in set(get_muscle_groups(context)):
        await query.edit_message_text(tr(lang, "unknown_group_restart"))
        return ConversationHandler.END

    session_id = db.create_session(user_id=user.id, muscle_group=group, status="active")
    context.user_data["workout"] = {
        "session_id": session_id,
        "muscle_group": group,
        "last_exercise_id": None,
    }

    await query.edit_message_text(tr(lang, "workout_started", group=group))
    await query.message.reply_text(
        tr(lang, "ask_body_weight"),
        reply_markup=end_keyboard(lang),
    )
    return BODYWEIGHT_INPUT


async def warmup_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text(tr("en", "no_active_session"))
        return ConversationHandler.END
    lang = user_lang(context, update.effective_user.id)

    db = get_db(context)
    data = query.data or ""
    session_id = int(workout["session_id"])
    group = str(workout["muscle_group"])

    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)

    if data == CB_WARMUP_NO:
        db.set_session_warmup(session_id, done=False, minutes=None, distance_km=None)
        exercise_options = get_exercise_options(context, group)
        await query.edit_message_text(tr(lang, "warmup_skipped"))
        await query.message.reply_text(
            tr(lang, "pick_exercise", group=group),
            reply_markup=exercise_keyboard(exercise_options, lang),
        )
        return SELECT_EXERCISE

    if data == CB_WARMUP_YES:
        await query.edit_message_text(tr(lang, "send_warmup"))
        return WARMUP_INPUT

    await query.edit_message_text(tr(lang, "invalid_option_restart"))
    return ConversationHandler.END


async def bodyweight_input_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    workout = context.user_data.get("workout")
    if not workout:
        await update.effective_message.reply_text(tr("en", "no_active_session"))
        return ConversationHandler.END

    lang = user_lang(context, update.effective_user.id)
    body_weight = parse_body_weight(update.effective_message.text or "")
    if body_weight is None:
        await update.effective_message.reply_text(tr(lang, "invalid_body_weight"))
        return BODYWEIGHT_INPUT

    db = get_db(context)
    db.set_session_body_weight(int(workout["session_id"]), body_weight)
    workout["body_weight_kg"] = body_weight

    await update.effective_message.reply_text(tr(lang, "body_weight_saved", body_weight=body_weight))
    await update.effective_message.reply_text(
        tr(lang, "did_warmup"),
        reply_markup=warmup_keyboard(lang),
    )
    return WARMUP_CHOICE


async def warmup_input_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    workout = context.user_data.get("workout")
    if not workout:
        await update.effective_message.reply_text(tr("en", "no_active_session"))
        return ConversationHandler.END
    lang = user_lang(context, update.effective_user.id)

    parsed = parse_warmup_input(update.effective_message.text or "")
    if parsed is None:
        await update.effective_message.reply_text(tr(lang, "warmup_format_error"))
        return WARMUP_INPUT

    minutes, distance = parsed
    db = get_db(context)
    db.set_session_warmup(int(workout["session_id"]), done=True, minutes=minutes, distance_km=distance)

    group = str(workout["muscle_group"])
    exercise_options = get_exercise_options(context, group)
    await update.effective_message.reply_text(tr(lang, "warmup_saved", minutes=minutes, distance=distance))
    await update.effective_message.reply_text(
        tr(lang, "pick_exercise", group=group),
        reply_markup=exercise_keyboard(exercise_options, lang),
    )
    return SELECT_EXERCISE


async def select_exercise_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text(tr("en", "no_active_session"))
        return ConversationHandler.END
    lang = user_lang(context, update.effective_user.id)

    data = query.data or ""
    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)
    if not data.startswith(CB_EX_PREFIX):
        await query.edit_message_text(tr(lang, "invalid_exercise_restart"))
        return ConversationHandler.END

    try:
        ex_index = int(data.split(":", 1)[1])
    except ValueError:
        await query.edit_message_text(tr(lang, "invalid_exercise_restart"))
        return ConversationHandler.END

    group = str(workout["muscle_group"])
    exercise_options = get_exercise_options(context, group)
    if ex_index < 0 or ex_index >= len(exercise_options):
        await query.edit_message_text(tr(lang, "exercise_not_found"))
        return ConversationHandler.END

    exercise_name, image_path = exercise_options[ex_index]
    workout["exercise_name"] = exercise_name
    workout.pop("sets_target", None)
    workout.pop("reps_list", None)
    workout.pop("weights_list", None)
    workout.pop("current_weight", None)
    await query.edit_message_text(tr(lang, "exercise_selected", exercise=exercise_name))
    if image_path and query.message:
        try:
            with image_path.open("rb") as image_file:
                await query.message.reply_photo(
                    photo=InputFile(image_file, filename=image_path.name),
                    caption=exercise_name,
                )
        except Exception:
            logger.exception("Failed to send exercise image: %s", image_path)
    await query.message.reply_text(
        tr(lang, "choose_sets"),
        reply_markup=sets_keyboard(lang),
    )
    return EX_SETS


async def sets_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text(tr("en", "no_active_session"))
        return ConversationHandler.END
    lang = user_lang(context, update.effective_user.id)

    data = query.data or ""
    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)
    if data == CB_BACK_EXERCISE:
        return await back_to_exercise_list(update, context, lang)
    if not data.startswith(CB_SETS_PREFIX):
        await query.edit_message_text(tr(lang, "invalid_sets_restart"))
        return ConversationHandler.END

    try:
        sets_count = int(data.split(":", 1)[1])
    except ValueError:
        await query.edit_message_text(tr(lang, "invalid_sets_restart"))
        return ConversationHandler.END

    if sets_count < 1 or sets_count > 6:
        await query.edit_message_text(tr(lang, "sets_range"))
        return EX_SETS

    workout["sets_target"] = sets_count
    workout["reps_list"] = []
    workout["weights_list"] = []
    workout.pop("current_weight", None)

    await query.edit_message_text(tr(lang, "sets_selected", sets=sets_count))
    await query.message.reply_text(
        tr(lang, "choose_reps", set_no=1, sets=sets_count),
        reply_markup=reps_keyboard(lang),
    )
    return EX_REPS


async def reps_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text(tr("en", "no_active_session"))
        return ConversationHandler.END
    lang = user_lang(context, update.effective_user.id)

    data = query.data or ""
    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)
    if data == CB_BACK_EXERCISE:
        return await back_to_exercise_list(update, context, lang)
    if not data.startswith(CB_REP_PREFIX):
        await query.edit_message_text(tr(lang, "invalid_reps_restart"))
        return ConversationHandler.END

    try:
        rep = int(data.split(":", 1)[1])
    except ValueError:
        await query.edit_message_text(tr(lang, "invalid_reps_restart"))
        return ConversationHandler.END

    if rep < 1 or rep > 20:
        await query.edit_message_text(tr(lang, "reps_range"))
        return EX_REPS

    sets_target = int(workout.get("sets_target", 0))
    reps_list: List[int] = list(workout.get("reps_list", []))
    if sets_target <= 0:
        await query.edit_message_text(tr(lang, "sets_missing_restart"))
        return ConversationHandler.END
    if len(reps_list) >= sets_target:
        await query.edit_message_text(tr(lang, "all_sets_entered_restart"))
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

    await query.edit_message_text(tr(lang, "set_reps_selected", set_no=set_no, sets=sets_target, rep=rep))
    await query.message.reply_text(
        weight_prompt_text(set_no, sets_target, current_weight),
        reply_markup=weight_adjust_keyboard(
            can_copy_prev=len(prev_weights) > 0,
            body_weight_kg=workout.get("body_weight_kg"),
            allow_bodyweight_button=bool(context.application.bot_data.get("has_bodyweight_pdf")),
            lang=lang,
        ),
    )
    return EX_WEIGHT


async def weight_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text(tr("en", "no_active_session"))
        return ConversationHandler.END
    lang = user_lang(context, update.effective_user.id)

    data = query.data or ""
    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)
    if data == CB_BACK_EXERCISE:
        return await back_to_exercise_list(update, context, lang)

    sets_target = int(workout.get("sets_target", 0))
    reps_list: List[int] = list(workout.get("reps_list", []))
    weights_list: List[float] = list(workout.get("weights_list", []))
    current_weight = clamp_weight_kg(float(workout.get("current_weight", 20.0)))
    set_no = len(reps_list)

    if sets_target <= 0 or set_no <= 0:
        await query.edit_message_text(tr(lang, "set_context_missing"))
        return ConversationHandler.END

    if data.startswith(CB_WADJ_PREFIX):
        try:
            delta = float(data.split(":", 1)[1])
        except ValueError:
            await query.edit_message_text(tr(lang, "invalid_weight_adjustment"))
            return EX_WEIGHT
        current_weight = clamp_weight_kg(current_weight + delta)
        workout["current_weight"] = current_weight
        await query.edit_message_text(
            weight_prompt_text(set_no, sets_target, current_weight),
            reply_markup=weight_adjust_keyboard(
                can_copy_prev=len(weights_list) > 0,
                body_weight_kg=workout.get("body_weight_kg"),
                allow_bodyweight_button=bool(context.application.bot_data.get("has_bodyweight_pdf")),
                lang=lang,
            ),
        )
        return EX_WEIGHT

    if data == CB_WCOPY:
        if not weights_list:
            await query.answer(tr(lang, "no_prev_weight"), show_alert=True)
            return EX_WEIGHT
        current_weight = clamp_weight_kg(weights_list[-1])
        workout["current_weight"] = current_weight
        await query.edit_message_text(
            weight_prompt_text(set_no, sets_target, current_weight),
            reply_markup=weight_adjust_keyboard(
                can_copy_prev=True,
                body_weight_kg=workout.get("body_weight_kg"),
                allow_bodyweight_button=bool(context.application.bot_data.get("has_bodyweight_pdf")),
                lang=lang,
            ),
        )
        return EX_WEIGHT

    if data == CB_WBODY:
        body_weight = workout.get("body_weight_kg")
        if body_weight is None:
            await query.answer(tr(lang, "no_body_weight_value"), show_alert=True)
            return EX_WEIGHT
        current_weight = clamp_weight_kg(float(body_weight))
        workout["current_weight"] = current_weight
        await query.edit_message_text(
            weight_prompt_text(set_no, sets_target, current_weight),
            reply_markup=weight_adjust_keyboard(
                can_copy_prev=len(weights_list) > 0,
                body_weight_kg=workout.get("body_weight_kg"),
                allow_bodyweight_button=bool(context.application.bot_data.get("has_bodyweight_pdf")),
                lang=lang,
            ),
        )
        return EX_WEIGHT

    if data == CB_WCONFIRM:
        weights_list.append(current_weight)
        workout["weights_list"] = weights_list
        await query.edit_message_text(
            tr(lang, "set_weight_saved", set_no=set_no, sets=sets_target, weight=current_weight)
        )

        if len(weights_list) < sets_target:
            next_set = len(weights_list) + 1
            await query.message.reply_text(
                tr(lang, "choose_reps", set_no=next_set, sets=sets_target),
                reply_markup=reps_keyboard(lang),
            )
            return EX_REPS

        workout["sets"] = sets_target
        workout["reps"] = max(1, round(sum(reps_list) / len(reps_list)))
        workout["reps_sequence"] = " ".join(str(x) for x in reps_list)
        weight_sequence = " ".join(f"{w:.2f}" for w in weights_list)
        primary_weight = max(weights_list)
        return await save_current_exercise(update, context, primary_weight, weight_sequence)

    await query.edit_message_text(tr(lang, "unknown_action_restart"))
    return ConversationHandler.END


async def save_current_exercise(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    primary_weight: float,
    weight_sequence: str,
) -> int:
    workout = context.user_data.get("workout")
    if not workout:
        await update.effective_message.reply_text(tr("en", "no_active_session"))
        return ConversationHandler.END
    lang = user_lang(context, update.effective_user.id)

    required_keys = ("session_id", "muscle_group", "exercise_name", "sets", "reps", "reps_sequence")
    if any(k not in workout for k in required_keys):
        await update.effective_message.reply_text(tr(lang, "session_incomplete_restart"))
        context.user_data.pop("workout", None)
        return ConversationHandler.END

    db = get_db(context)
    previous_pr = db.get_exercise_max_weight(
        user_id=update.effective_user.id,
        exercise_name=str(workout["exercise_name"]),
    )
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

    pr_line = ""
    if previous_pr is None:
        pr_line = tr(lang, "first_pr", name=saved_name, weight=primary_weight)
    elif float(primary_weight) > float(previous_pr):
        pr_line = tr(lang, "new_pr", name=saved_name, old=previous_pr, new=primary_weight)

    await update.effective_message.reply_text(
        tr(lang, "saved_line", name=saved_name, volume=volume, pr_line=pr_line),
        reply_markup=post_exercise_keyboard(lang),
    )
    return POST_ACTION


async def post_action_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    workout = context.user_data.get("workout")
    if not workout:
        await query.edit_message_text(tr("en", "no_active_workout"))
        return ConversationHandler.END
    lang = user_lang(context, update.effective_user.id)

    data = query.data or ""
    db = get_db(context)
    user_id = update.effective_user.id

    if data == CB_NEXT_EXERCISE:
        group = str(workout["muscle_group"])
        exercise_options = get_exercise_options(context, group)
        await query.edit_message_text(tr(lang, "add_next_exercise"))
        await query.message.reply_text(
            tr(lang, "pick_exercise", group=group),
            reply_markup=exercise_keyboard(exercise_options, lang),
        )
        return SELECT_EXERCISE

    if data == CB_REPLACE_EXERCISE:
        last_id = workout.get("last_exercise_id")
        if not last_id:
            await query.answer(tr(lang, "replace_none"), show_alert=True)
            return POST_ACTION

        deleted = db.delete_exercise(int(last_id), user_id)
        if not deleted:
            await query.answer(tr(lang, "replace_not_found"), show_alert=True)
            return POST_ACTION

        workout["last_exercise_id"] = None
        group = str(workout["muscle_group"])
        exercise_options = get_exercise_options(context, group)
        await query.edit_message_text(tr(lang, "replace_pick"))
        await query.message.reply_text(
            tr(lang, "pick_exercise", group=group),
            reply_markup=exercise_keyboard(exercise_options, lang),
        )
        return SELECT_EXERCISE

    if data == CB_FINISH_SESSION:
        return await finish_workout(update, context)

    await query.edit_message_text(tr(lang, "unknown_action_restart"))
    context.user_data.pop("workout", None)
    return ConversationHandler.END


async def finish_workout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    workout = context.user_data.get("workout")
    db = get_db(context)
    user = update.effective_user
    lang = user_lang(context, user.id) if user else "en"

    if not workout or not user:
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(tr(lang, "no_active_workout"))
        else:
            await update.effective_message.reply_text(tr(lang, "no_active_workout"))
        return ConversationHandler.END

    session_id = int(workout["session_id"])
    count, total_volume = db.get_session_totals(session_id)
    session = db.get_session(session_id)
    warmup_line = ""
    if session and int(session["warmup_done"] or 0) == 1:
        warmup_minutes = float(session["warmup_minutes"] or 0.0)
        warmup_distance = float(session["warmup_distance_km"] or 0.0)
        warmup_line = tr(lang, "warmup_line", minutes=warmup_minutes, distance=warmup_distance)

    if count > 0:
        db.close_session(session_id, "completed")
        completed_rows = db.get_last_completed_workouts(user_id=user.id, limit=2)
        body_weight_line = ""
        if completed_rows:
            current_bw = (
                float(completed_rows[0]["body_weight_kg"])
                if completed_rows[0]["body_weight_kg"] is not None
                else None
            )
            previous_bw = None
            if len(completed_rows) > 1 and completed_rows[1]["body_weight_kg"] is not None:
                previous_bw = float(completed_rows[1]["body_weight_kg"])
            body_weight_line = tr(
                lang,
                "body_weight_line",
                body_weight=(f"{current_bw:.2f} kg" if current_bw is not None else tr(lang, "no_body_weight_value")),
                delta=body_weight_change_text(lang, current_bw, previous_bw),
            )
        recent = recent_groups_text(db, user.id, lang)
        text = tr(lang, "workout_finish_free", count=count, volume=total_volume, warmup_line=warmup_line, recent=recent)
        text += body_weight_line
    else:
        db.close_session(session_id, "cancelled")
        recent = recent_groups_text(db, user.id, lang)
        text = tr(lang, "workout_finish_empty_free", recent=recent)

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
    uid = update.effective_user.id if update.effective_user else 0
    await update.effective_message.reply_text(tr(user_lang(context, uid), "cancelled"))
    return ConversationHandler.END


async def last_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return
    lang = await ensure_language_selected(update, context, user_id)
    if not lang:
        return

    db = get_db(context)
    rows = db.get_last_completed_workouts(user_id=user_id, limit=4)
    if not rows:
        await update.effective_message.reply_text(tr(lang, "no_last_workouts"))
        return

    lines = [tr(lang, "last_header")]
    for idx, row in enumerate(rows[:3], start=1):
        current_bw = float(row["body_weight_kg"]) if row["body_weight_kg"] is not None else None
        previous_bw: Optional[float] = None
        if idx < len(rows):
            next_row = rows[idx]
            if next_row["body_weight_kg"] is not None:
                previous_bw = float(next_row["body_weight_kg"])
        lines.append(
            tr(
                lang,
                "last_line",
                idx=idx,
                ended=format_iso_utc(str(row["ended_at"] or "")),
                group=str(row["muscle_group"]),
                exercise_count=int(row["exercise_count"] or 0),
                total_volume=float(row["total_volume"] or 0.0),
                body_weight=(f"{current_bw:.2f} kg" if current_bw is not None else tr(lang, "no_body_weight_value")),
                delta=body_weight_change_text(lang, current_bw, previous_bw),
            )
        )
    await update.effective_message.reply_text("\n".join(lines))


async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return
    lang = await ensure_language_selected(update, context, user_id)
    if not lang:
        return

    db = get_db(context)
    rows = db.get_history_rows(user_id)
    if not rows:
        await update.effective_message.reply_text(tr(lang, "no_history"))
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
            "body_weight_kg",
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
                (f"{float(r['body_weight_kg']):.2f}" if r["body_weight_kg"] is not None else ""),
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
        caption=tr(lang, "history_caption"),
    )


def render_group_volume_lines(group_volumes: Dict[str, float]) -> str:
    if not group_volumes:
        return "-"
    lines = []
    for group in sorted(group_volumes.keys(), key=str.lower):
        lines.append(f"{group}: {group_volumes.get(group, 0.0):.2f}")
    return "\n".join(lines)


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return
    lang = await ensure_language_selected(update, context, user_id)
    if not lang:
        return

    db = get_db(context)
    start = start_of_today_utc()
    end = start + timedelta(days=1)
    summary = db.get_summary(user_id, start, end)

    text = tr(
        lang,
        "today_summary",
        session_count=summary["session_count"],
        exercise_count=summary["exercise_count"],
        total_volume=summary["total_volume"],
        warmup_count=summary["warmup_count"],
        warmup_minutes_total=summary["warmup_minutes_total"],
        warmup_distance_total=summary["warmup_distance_total"],
        group_lines=render_group_volume_lines(summary["group_volumes"]),
    )
    await update.effective_message.reply_text(text)


async def thisweek_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return
    lang = await ensure_language_selected(update, context, user_id)
    if not lang:
        return

    db = get_db(context)
    now = now_utc()
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end = week_start + timedelta(days=7)
    summary = db.get_summary(user_id, week_start, week_end)

    text = tr(
        lang,
        "week_summary",
        start_date=week_start.date(),
        end_date=(week_end - timedelta(days=1)).date(),
        session_count=summary["session_count"],
        exercise_count=summary["exercise_count"],
        total_volume=summary["total_volume"],
        warmup_count=summary["warmup_count"],
        warmup_minutes_total=summary["warmup_minutes_total"],
        warmup_distance_total=summary["warmup_distance_total"],
        group_lines=render_group_volume_lines(summary["group_volumes"]),
    )
    await update.effective_message.reply_text(text)


async def pr_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_registered(update, context)
    if user_id is None:
        return
    lang = await ensure_language_selected(update, context, user_id)
    if not lang:
        return

    db = get_db(context)
    records = db.get_personal_records(user_id)
    if not records:
        await update.effective_message.reply_text(tr(lang, "no_prs"))
        return

    lines = [tr(lang, "pr_header")]
    for r in records:
        lines.append(tr(lang, "pr_line", name=r["name"], weight=float(r["max_weight"])))
    await update.effective_message.reply_text("\n".join(lines))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error while processing update", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=tr("en", "error_text"),
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
    for _, chat_id, lang in db.list_users_with_language():
        await set_chat_commands_for_language(application.bot, chat_id, lang)

    logger.info("Startup complete. Scheduled reminders for %d users.", len(users))


def print_deployment_instructions() -> None:
    print("\n=== GymBot Deployment Instructions ===")
    print("1) Install dependencies:")
    print("   pip install -r requirements.txt")
    print("2) Set environment variables:")
    print("   TELEGRAM_BOT_TOKEN=your_bot_token")
    print("   GYMBOT_DB_PATH=./gymbot.db")
    print("   GYMBOT_EXERCISE_DIR=./Exercise")
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
    exercise_catalog = load_exercise_catalog(EXERCISE_ASSETS_DIR)

    app = Application.builder().token(token).post_init(on_startup).build()
    app.bot_data["db"] = db
    app.bot_data["exercise_catalog"] = exercise_catalog
    app.bot_data["has_bodyweight_pdf"] = BODYWEIGHT_EXERCISE_PDF.exists()

    loaded_count = sum(len(options) for options in exercise_catalog.values())
    if loaded_count > 0:
        logger.info("Loaded %d exercise options from %s", loaded_count, EXERCISE_ASSETS_DIR)
    else:
        logger.warning("No exercise assets found in %s; using fallback defaults.", EXERCISE_ASSETS_DIR)
    if app.bot_data["has_bodyweight_pdf"]:
        logger.info("Bodyweight exercise PDF detected: %s", BODYWEIGHT_EXERCISE_PDF)

    workout_conv = ConversationHandler(
        entry_points=[CommandHandler(["workout", "latihan", "tren"], workout_cmd)],
        states={
            SELECT_MUSCLE: [
                CallbackQueryHandler(
                    select_muscle_cb,
                    pattern=r"^(group:.+|end_workout)$",
                )
            ],
            BODYWEIGHT_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bodyweight_input_msg),
                CallbackQueryHandler(finish_workout, pattern=r"^finish_session$"),
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
                    pattern=r"^(sets:[1-6]|back_exercise|finish_session)$",
                ),
            ],
            EX_REPS: [
                CallbackQueryHandler(
                    reps_choice_cb,
                    pattern=r"^(rep:(?:[1-9]|1[0-9]|20)|back_exercise|finish_session)$",
                ),
            ],
            EX_WEIGHT: [
                CallbackQueryHandler(
                    weight_choice_cb,
                    pattern=r"^(wadj:[+-]?(?:\d+(?:\.\d+)?)|wconfirm|wcopy|wbody|back_exercise|finish_session)$",
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

    app.add_handler(CallbackQueryHandler(language_select_cb, pattern=r"^lang:(en|id|ru)$"))
    app.add_handler(CommandHandler(["start", "mulai"], start_cmd))
    app.add_handler(CommandHandler(["help", "bantuan", "pomosh"], help_cmd))
    app.add_handler(CommandHandler(["last", "terakhir", "poslednie"], last_cmd))
    app.add_handler(CommandHandler(["history", "riwayat", "istoriya"], history_cmd))
    app.add_handler(CommandHandler(["today", "hariini", "segodnya"], today_cmd))
    app.add_handler(CommandHandler(["thisweek", "mingguini", "nedelya"], thisweek_cmd))
    app.add_handler(CommandHandler(["pr", "rekor", "rekord"], pr_cmd))
    app.add_handler(CommandHandler(["cancel", "batal", "otmena"], cancel_cmd))
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
