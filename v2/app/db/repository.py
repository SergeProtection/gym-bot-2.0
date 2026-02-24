from datetime import datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path
import sys
from typing import Any, Dict


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
