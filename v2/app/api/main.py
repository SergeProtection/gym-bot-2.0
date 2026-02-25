from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Query

from app.core.config import get_settings
from app.core.logging import setup_logging
from app.db.repository import WorkoutRepository


setup_logging()
settings = get_settings()
repo = WorkoutRepository(settings.gymbot_db_path)

app = FastAPI(
    title="GymBot API",
    version="1.0.0",
    description="API for GymBot summaries and health checks.",
)


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "service": "gymbot-api",
        "status": "ok",
        "message": "Use /health or /docs",
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "service": "gymbot-api"}


@app.get("/users/{user_id}")
def get_user_profile(user_id: int) -> Dict[str, Any]:
    profile = repo.user_profile(user_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="User not found.")
    return profile


@app.get("/users/{user_id}/workouts/recent")
def get_recent_workouts(
    user_id: int,
    limit: int = Query(default=3, ge=1, le=20),
) -> Dict[str, Any]:
    return {
        "user_id": user_id,
        "items": repo.recent_workouts(user_id, limit=limit),
    }


@app.get("/users/{user_id}/exercises")
def get_exercise_history(
    user_id: int,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    return {
        "user_id": user_id,
        "limit": limit,
        "offset": offset,
        "items": repo.exercise_history(user_id, limit=limit, offset=offset),
    }


@app.get("/summary/today/{user_id}")
def summary_today(user_id: int) -> Dict[str, Any]:
    return repo.summary_today(user_id)


@app.get("/summary/week/{user_id}")
def summary_week(user_id: int) -> Dict[str, Any]:
    return repo.summary_week(user_id)


@app.get("/summary/month/{user_id}")
def summary_month(
    user_id: int,
    month: str = Query(
        default="",
        description="Month in YYYY-MM format. Empty means current UTC month.",
    ),
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    year = now.year
    month_num = now.month

    if month:
        try:
            parsed = datetime.strptime(month, "%Y-%m")
            year = parsed.year
            month_num = parsed.month
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid month format. Use YYYY-MM.") from exc

    return repo.summary_month(user_id, year, month_num)


@app.get("/summary/period/{user_id}")
def summary_period(
    user_id: int,
    start: str = Query(description="Start date in YYYY-MM-DD"),
    end: str = Query(description="End date in YYYY-MM-DD"),
) -> Dict[str, Any]:
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.") from exc

    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="End date must be on/after start date.")

    return repo.summary_period(user_id, start_dt, end_dt)
