from contextlib import closing
from datetime import datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

legacy_main = import_module("main")
GymDB = legacy_main.GymDB


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _today_start_utc() -> datetime:
    now = _utc_now()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


class WorkoutRepository:
    def __init__(self, db_path: str) -> None:
        self.db = GymDB(db_path)
        self.db.init_schema()

    def summary_today(self, user_id: int) -> Dict[str, Any]:
        start = _today_start_utc()
        end = start + timedelta(days=1)
        return self.db.get_summary(user_id, start, end)

    def summary_week(self, user_id: int) -> Dict[str, Any]:
        now = _utc_now()
        week_start = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        week_end = week_start + timedelta(days=7)
        return self.db.get_summary(user_id, week_start, week_end)

    def summary_month(self, user_id: int, year: int, month: int) -> Dict[str, Any]:
        month_start = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
        return self.db.get_summary(user_id, month_start, month_end)

    def summary_period(
        self, user_id: int, start_date: datetime, end_date: datetime
    ) -> Dict[str, Any]:
        start = start_date.astimezone(timezone.utc)
        # include end date day by making it exclusive next-day midnight
        end = (end_date.astimezone(timezone.utc) + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return self.db.get_summary(user_id, start, end)

    def user_profile(self, user_id: int) -> Optional[Dict[str, Any]]:
        with closing(self.db.connect()) as conn:
            row = conn.execute(
                """
                SELECT user_id, username, first_name, language, registered_at, updated_at
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()

        if row is None:
            return None

        return {
            "user_id": int(row["user_id"]),
            "username": str(row["username"] or ""),
            "first_name": str(row["first_name"] or ""),
            "language": str(row["language"] or "en"),
            "registered_at": str(row["registered_at"]),
            "updated_at": str(row["updated_at"]),
        }

    def recent_workouts(self, user_id: int, limit: int = 3) -> List[Dict[str, Any]]:
        if hasattr(self.db, "get_last_completed_workouts"):
            rows = self.db.get_last_completed_workouts(user_id, limit)
            return [
                {
                    "session_id": int(row["id"]),
                    "muscle_group": str(row["muscle_group"]),
                    "ended_at": str(row["ended_at"] or ""),
                    "exercise_count": int(row["exercise_count"] or 0),
                    "total_volume": float(row["total_volume"] or 0.0),
                    "body_weight_kg": float(row["body_weight_kg"]) if row["body_weight_kg"] is not None else None,
                }
                for row in rows
            ]

        with closing(self.db.connect()) as conn:
            rows = conn.execute(
                """
                SELECT id, muscle_group, ended_at, body_weight_kg
                FROM workout_sessions
                WHERE user_id = ? AND status = 'completed'
                ORDER BY ended_at DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()

        return [
            {
                "session_id": int(row["id"]),
                "muscle_group": str(row["muscle_group"]),
                "ended_at": str(row["ended_at"] or ""),
                "exercise_count": 0,
                "total_volume": 0.0,
                "body_weight_kg": float(row["body_weight_kg"]) if row["body_weight_kg"] is not None else None,
            }
            for row in rows
        ]

    def exercise_history(self, user_id: int, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        with closing(self.db.connect()) as conn:
            rows = conn.execute(
                """
                SELECT
                    e.id,
                    e.session_id,
                    e.created_at,
                    e.muscle_group,
                    e.name,
                    e.sets,
                    e.reps,
                    e.reps_sequence,
                    e.weight,
                    e.weight_sequence,
                    e.volume
                FROM exercises e
                WHERE e.user_id = ?
                ORDER BY e.created_at DESC, e.id DESC
                LIMIT ? OFFSET ?
                """,
                (user_id, limit, offset),
            ).fetchall()

        return [
            {
                "exercise_id": int(row["id"]),
                "session_id": int(row["session_id"]),
                "created_at": str(row["created_at"]),
                "muscle_group": str(row["muscle_group"]),
                "name": str(row["name"]),
                "sets": int(row["sets"]),
                "reps": int(row["reps"]),
                "reps_sequence": str(row["reps_sequence"] or ""),
                "weight": float(row["weight"]),
                "weight_sequence": str(row["weight_sequence"] or ""),
                "volume": float(row["volume"]),
            }
            for row in rows
        ]
