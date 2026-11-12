"""History, period summaries, personal records and chart series."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from gymbot.db.models import Exercise, ExerciseKind, Workout, WorkoutSet, WorkoutStatus
from gymbot.services.workouts import WorkoutSummary, set_volume, summarize
from gymbot.utils import epley_1rm, local_range, local_today, to_local


@dataclass
class GroupTotals:
    sets: int = 0
    volume_kg: float = 0.0


@dataclass
class PeriodSummary:
    first: date
    last: date
    workouts: int = 0
    active_days: int = 0
    sets: int = 0
    volume_kg: float = 0.0
    cardio_km: float = 0.0
    cardio_s: int = 0
    hold_s: int = 0
    training_s: float = 0.0
    prs: int = 0
    groups: dict[str, GroupTotals] = field(default_factory=dict)

    @property
    def groups_sorted(self) -> list[tuple[str, GroupTotals]]:
        return sorted(self.groups.items(), key=lambda kv: (-kv[1].sets, kv[0]))


def _completed(user_id: int):
    return (Workout.user_id == user_id, Workout.status == WorkoutStatus.COMPLETED.value)


async def completed_between(session: AsyncSession, user_id: int, start: datetime, end: datetime) -> list[Workout]:
    rows = await session.scalars(
        select(Workout)
        .where(*_completed(user_id), Workout.started_at >= start, Workout.started_at < end)
        .order_by(Workout.started_at)
        .options(selectinload(Workout.sets))
    )
    return list(rows.all())


async def period_summary(session: AsyncSession, user_id: int, tz_name: str, first: date, last: date) -> PeriodSummary:
    start, end = local_range(first, last, tz_name)
    workouts = await completed_between(session, user_id, start, end)
    out = PeriodSummary(first=first, last=last, workouts=len(workouts))
    days: set[date] = set()
    for w in workouts:
        days.add(to_local(w.started_at, tz_name).date())
        out.training_s += summarize(w).duration_s
        for s in w.sets:
            kind = s.exercise.kind
            out.sets += 1
            out.prs += int(s.is_pr)
            if kind == ExerciseKind.CARDIO.value:
                out.cardio_km += float(s.distance_km or 0)
                out.cardio_s += int(s.duration_s or 0)
            elif kind == ExerciseKind.TIME.value:
                out.hold_s += int(s.duration_s or 0)
            g = out.groups.setdefault(s.exercise.muscle_group, GroupTotals())
            g.sets += 1
            g.volume_kg += set_volume(s)
            out.volume_kg += set_volume(s)
    out.active_days = len(days)
    return out


def week_bounds(today: date) -> tuple[date, date]:
    monday = today - timedelta(days=today.weekday())
    return monday, monday + timedelta(days=6)


def month_bounds(year: int, month: int) -> tuple[date, date]:
    first = date(year, month, 1)
    nxt = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return first, nxt - timedelta(days=1)


async def history(session: AsyncSession, user_id: int, *, limit: int = 10, offset: int = 0) -> list[WorkoutSummary]:
    """Completed workouts, newest first."""
    rows = await session.scalars(
        select(Workout)
        .where(*_completed(user_id))
        .order_by(Workout.started_at.desc())
        .offset(offset)
        .limit(limit)
        .options(selectinload(Workout.sets))
    )
    return [summarize(w) for w in rows.all()]


async def count_completed(session: AsyncSession, user_id: int) -> int:
    return int(await session.scalar(select(func.count(Workout.id)).where(*_completed(user_id))) or 0)


async def last_workout(session: AsyncSession, user_id: int) -> WorkoutSummary | None:
    items = await history(session, user_id, limit=1)
    return items[0] if items else None


@dataclass
class ExerciseRecord:
    exercise: Exercise
    sessions: int
    best_weight: float | None = None
    best_weight_reps: int | None = None
    best_e1rm: float | None = None
    best_reps: int | None = None  # bodyweight sets
    best_hold_s: int | None = None
    best_distance_km: float | None = None
    best_pace_s: float | None = None  # seconds per km, runs of 1 km+
    last_at: datetime | None = None


async def personal_records(session: AsyncSession, user_id: int, exercise_id: int | None = None) -> list[ExerciseRecord]:
    """Best results per exercise, most recently trained first."""
    stmt = select(WorkoutSet).join(Workout).where(*_completed(user_id))
    if exercise_id is not None:
        stmt = stmt.where(WorkoutSet.exercise_id == exercise_id)
    sets = (await session.scalars(stmt.order_by(WorkoutSet.created_at))).all()
    records: dict[int, ExerciseRecord] = {}
    workouts_seen: dict[int, set[int]] = {}
    for s in sets:
        rec = records.get(s.exercise_id)
        if rec is None:
            rec = records[s.exercise_id] = ExerciseRecord(exercise=s.exercise, sessions=0)
        workouts_seen.setdefault(s.exercise_id, set()).add(s.workout_id)
        rec.last_at = s.created_at
        kind = s.exercise.kind
        if kind == ExerciseKind.STRENGTH.value:
            if s.weight_kg:
                if rec.best_weight is None or s.weight_kg > rec.best_weight or (
                    s.weight_kg == rec.best_weight and (s.reps or 0) > (rec.best_weight_reps or 0)
                ):
                    rec.best_weight, rec.best_weight_reps = s.weight_kg, s.reps
                e1rm = epley_1rm(s.weight_kg, s.reps)
                if rec.best_e1rm is None or e1rm > rec.best_e1rm:
                    rec.best_e1rm = e1rm
            elif s.reps and (rec.best_reps is None or s.reps > rec.best_reps):
                rec.best_reps = s.reps
        elif kind == ExerciseKind.TIME.value:
            if s.duration_s and (rec.best_hold_s is None or s.duration_s > rec.best_hold_s):
                rec.best_hold_s = s.duration_s
        elif kind == ExerciseKind.CARDIO.value:
            if s.distance_km and (rec.best_distance_km is None or s.distance_km > rec.best_distance_km):
                rec.best_distance_km = s.distance_km
            if s.distance_km and s.distance_km >= 1 and s.duration_s:
                pace = s.duration_s / s.distance_km
                if rec.best_pace_s is None or pace < rec.best_pace_s:
                    rec.best_pace_s = pace
    for ex_id, rec in records.items():
        rec.sessions = len(workouts_seen[ex_id])
    return sorted(records.values(), key=lambda r: r.last_at, reverse=True)


@dataclass
class ProgressPoint:
    day: date
    value: float
    extra: float | None = None


@dataclass
class ExerciseProgress:
    exercise: Exercise
    metric: str  # what `value` means; see METRIC_LABELS
    points: list[ProgressPoint]


METRIC_LABELS = {
    "e1rm": "Estimated 1RM (kg)",
    "reps": "Best set (reps)",
    "hold": "Longest hold (s)",
    "distance": "Distance (km)",
}


async def exercise_progress(session: AsyncSession, user_id: int, exercise: Exercise, tz_name: str) -> ExerciseProgress:
    """One point per training day.

    strength: best estimated 1RM that day (extra = volume); bodyweight-only: best reps
    time: longest hold; cardio: total distance (extra = best pace s/km)
    """
    sets = (
        await session.scalars(
            select(WorkoutSet)
            .join(Workout)
            .where(*_completed(user_id), WorkoutSet.exercise_id == exercise.id)
            .order_by(WorkoutSet.created_at)
        )
    ).all()
    by_day: dict[date, list[WorkoutSet]] = {}
    for s in sets:
        by_day.setdefault(to_local(s.created_at, tz_name).date(), []).append(s)

    points: list[ProgressPoint] = []
    if exercise.kind == ExerciseKind.CARDIO.value:
        metric = "distance"
        for day, day_sets in by_day.items():
            paces = [s.duration_s / s.distance_km for s in day_sets if s.distance_km and s.duration_s]
            points.append(ProgressPoint(day, sum(float(s.distance_km or 0) for s in day_sets), min(paces) if paces else None))
    elif exercise.kind == ExerciseKind.TIME.value:
        metric = "hold"
        for day, day_sets in by_day.items():
            points.append(ProgressPoint(day, float(max(s.duration_s or 0 for s in day_sets))))
    elif any(s.weight_kg for s in sets):
        metric = "e1rm"
        for day, day_sets in by_day.items():
            best = max(epley_1rm(s.weight_kg, s.reps) for s in day_sets)
            if best > 0:
                points.append(ProgressPoint(day, round(best, 1), sum(set_volume(s) for s in day_sets)))
    else:
        metric = "reps"
        for day, day_sets in by_day.items():
            points.append(ProgressPoint(day, float(max(s.reps or 0 for s in day_sets))))
    return ExerciseProgress(exercise=exercise, metric=metric, points=points)


@dataclass
class WeekPoint:
    week_start: date
    workouts: int
    sets: int
    volume_kg: float


async def weekly_series(session: AsyncSession, user_id: int, tz_name: str, weeks: int = 12) -> list[WeekPoint]:
    """Totals per Monday-based week for the last `weeks` weeks, oldest first (zeros included)."""
    this_monday, _ = week_bounds(local_today(tz_name))
    first_monday = this_monday - timedelta(weeks=weeks - 1)
    start, end = local_range(first_monday, this_monday + timedelta(days=6), tz_name)
    series = {first_monday + timedelta(weeks=i): WeekPoint(first_monday + timedelta(weeks=i), 0, 0, 0.0) for i in range(weeks)}
    for w in await completed_between(session, user_id, start, end):
        monday, _ = week_bounds(to_local(w.started_at, tz_name).date())
        point = series.get(monday)
        if point is None:
            continue
        point.workouts += 1
        point.sets += len(w.sets)
        point.volume_kg += sum(set_volume(s) for s in w.sets)
    return list(series.values())


async def week_streak(session: AsyncSession, user_id: int, tz_name: str) -> int:
    """Consecutive weeks (ending this week, or last week if none yet this week) with a workout."""
    starts = (
        await session.scalars(select(Workout.started_at).where(*_completed(user_id)).order_by(Workout.started_at.desc()))
    ).all()
    mondays = {week_bounds(to_local(dt, tz_name).date())[0] for dt in starts}
    if not mondays:
        return 0
    cursor, _ = week_bounds(local_today(tz_name))
    if cursor not in mondays:
        cursor -= timedelta(weeks=1)
    streak = 0
    while cursor in mondays:
        streak += 1
        cursor -= timedelta(weeks=1)
    return streak


async def trained_on(session: AsyncSession, user_id: int, tz_name: str, day: date) -> bool:
    """True if the user has any workout (active or completed) that started on this local day."""
    start, end = local_range(day, day, tz_name)
    found = await session.scalar(
        select(Workout.id)
        .where(
            Workout.user_id == user_id,
            Workout.status != WorkoutStatus.CANCELLED.value,
            Workout.started_at >= start,
            Workout.started_at < end,
        )
        .limit(1)
    )
    return found is not None
