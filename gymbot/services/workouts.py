"""Workout lifecycle: start, log sets (with PR detection), finish."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from gymbot.db.models import Exercise, ExerciseKind, Plan, Workout, WorkoutSet, WorkoutStatus, utcnow
from gymbot.utils import as_utc, epley_1rm, fmt_clock, fmt_num, fmt_pace, fmt_weight


@dataclass
class ExerciseBlock:
    exercise: Exercise
    sets: list[WorkoutSet] = field(default_factory=list)


@dataclass
class WorkoutSummary:
    workout: Workout
    duration_s: float
    blocks: list[ExerciseBlock]
    set_count: int
    volume_kg: float
    cardio_km: float
    pr_count: int

    @property
    def groups(self) -> list[str]:
        counts: dict[str, int] = {}
        for b in self.blocks:
            counts[b.exercise.muscle_group] = counts.get(b.exercise.muscle_group, 0) + len(b.sets)
        return [g for g, _ in sorted(counts.items(), key=lambda kv: -kv[1])]


def set_volume(s: WorkoutSet) -> float:
    return float(s.weight_kg or 0) * int(s.reps or 0)


def describe_set(s: WorkoutSet, kind: str) -> str:
    if kind == ExerciseKind.TIME.value:
        text = fmt_clock(s.duration_s)
        if s.weight_kg:
            text += f" @ {fmt_weight(s.weight_kg)}"
    elif kind == ExerciseKind.CARDIO.value:
        text = f"{fmt_num(s.distance_km)} km in {fmt_clock(s.duration_s)} ({fmt_pace(s.duration_s, s.distance_km)})"
    else:
        text = f"{fmt_weight(s.weight_kg)} × {s.reps}"
    return text + (" 🏆" if s.is_pr else "")


async def get_active(session: AsyncSession, user_id: int) -> Workout | None:
    return await session.scalar(
        select(Workout)
        .where(Workout.user_id == user_id, Workout.status == WorkoutStatus.ACTIVE.value)
        .order_by(Workout.started_at.desc())
        .options(selectinload(Workout.sets), selectinload(Workout.plan).selectinload(Plan.items))
        .limit(1)
    )


async def start(session: AsyncSession, user_id: int, plan_id: int | None = None) -> tuple[Workout, bool]:
    """Start a workout. Returns (workout, created); an existing active workout is reused."""
    active = await get_active(session, user_id)
    if active is not None:
        return active, False
    workout = Workout(user_id=user_id, plan_id=plan_id, status=WorkoutStatus.ACTIVE.value, started_at=utcnow())
    session.add(workout)
    await session.flush()
    return await get_active(session, user_id), True


async def load(session: AsyncSession, user_id: int, workout_id: int) -> Workout | None:
    return await session.scalar(
        select(Workout)
        .where(Workout.id == workout_id, Workout.user_id == user_id)
        .options(selectinload(Workout.sets), selectinload(Workout.plan))
    )


async def _previous_bests(session: AsyncSession, user_id: int, exercise_id: int, weight: float | None, reps: int | None):
    est = case((WorkoutSet.reps == 1, WorkoutSet.weight_kg), else_=WorkoutSet.weight_kg * (1 + WorkoutSet.reps / 30.0))
    base = select(
        func.count(WorkoutSet.id),
        func.max(WorkoutSet.weight_kg),
        func.max(case((WorkoutSet.weight_kg > 0, est), else_=None)),
        func.max(case(((WorkoutSet.weight_kg.is_(None)) | (WorkoutSet.weight_kg == 0), WorkoutSet.reps), else_=None)),
        func.max(case((WorkoutSet.weight_kg == weight, WorkoutSet.reps), else_=None)) if weight else func.max(None),
        func.max(WorkoutSet.duration_s),
        func.max(WorkoutSet.distance_km),
        func.min(case((WorkoutSet.distance_km >= 1, WorkoutSet.duration_s / WorkoutSet.distance_km), else_=None)),
    ).where(WorkoutSet.user_id == user_id, WorkoutSet.exercise_id == exercise_id)
    row = (await session.execute(base)).one()
    keys = ["count", "weight", "e1rm", "bw_reps", "reps_at_weight", "duration", "distance", "pace"]
    return dict(zip(keys, row))


def _pr_messages(kind: str, prev: dict, *, reps, weight, duration, distance) -> list[str]:
    if not prev["count"]:
        return []  # first time doing this exercise: nothing to beat yet
    msgs: list[str] = []
    if kind == ExerciseKind.STRENGTH.value:
        if weight and weight > 0:
            if prev["weight"] is not None and weight > prev["weight"]:
                msgs.append(f"Heaviest weight: {fmt_weight(weight)} (was {fmt_weight(prev['weight'])})")
            else:
                e1rm = epley_1rm(weight, reps)
                if prev["e1rm"] is not None and e1rm > prev["e1rm"] + 1e-9:
                    msgs.append(f"Best estimated 1RM: {fmt_num(e1rm, 1)} kg (was {fmt_num(prev['e1rm'], 1)} kg)")
                elif prev["reps_at_weight"] is not None and reps > prev["reps_at_weight"]:
                    msgs.append(f"Most reps at {fmt_weight(weight)}: {reps} (was {prev['reps_at_weight']})")
        elif prev["bw_reps"] is not None and reps and reps > prev["bw_reps"]:
            msgs.append(f"Most reps: {reps} (was {prev['bw_reps']})")
    elif kind == ExerciseKind.TIME.value:
        if prev["duration"] is not None and duration and duration > prev["duration"]:
            msgs.append(f"Longest hold: {fmt_clock(duration)} (was {fmt_clock(prev['duration'])})")
    elif kind == ExerciseKind.CARDIO.value:
        if prev["distance"] is not None and distance and distance > prev["distance"]:
            msgs.append(f"Longest distance: {fmt_num(distance)} km (was {fmt_num(prev['distance'])} km)")
        if distance and distance >= 1 and duration and prev["pace"] is not None:
            pace = duration / distance
            if pace < prev["pace"]:
                msgs.append(f"Fastest pace: {fmt_clock(int(pace))} /km (was {fmt_clock(int(prev['pace']))} /km)")
    return msgs


async def log_set(
    session: AsyncSession,
    workout: Workout,
    exercise: Exercise,
    *,
    reps: int | None = None,
    weight: float | None = None,
    duration: int | None = None,
    distance: float | None = None,
) -> tuple[WorkoutSet, list[str]]:
    """Record one set. Returns the set and a list of PR messages (empty if none)."""
    if exercise.kind == ExerciseKind.STRENGTH.value:
        if not reps or reps <= 0:
            raise ValueError("reps must be positive")
        weight = max(0.0, float(weight or 0))
        duration = distance = None
    elif exercise.kind == ExerciseKind.TIME.value:
        if not duration or duration <= 0:
            raise ValueError("duration must be positive")
        reps, distance = None, None
        weight = float(weight) if weight else None
    else:
        if not duration or duration <= 0 or not distance or distance <= 0:
            raise ValueError("distance and duration must be positive")
        reps, weight = None, None

    prev = await _previous_bests(session, workout.user_id, exercise.id, weight, reps)
    prs = _pr_messages(exercise.kind, prev, reps=reps, weight=weight, duration=duration, distance=distance)
    s = WorkoutSet(
        workout_id=workout.id,
        exercise_id=exercise.id,
        user_id=workout.user_id,
        reps=reps,
        weight_kg=weight,
        duration_s=duration,
        distance_km=distance,
        is_pr=bool(prs),
        created_at=utcnow(),
    )
    session.add(s)
    await session.flush()
    await session.refresh(workout, ["sets"])
    return s, prs


async def undo_last_set(session: AsyncSession, workout: Workout, exercise_id: int | None = None) -> WorkoutSet | None:
    stmt = select(WorkoutSet).where(WorkoutSet.workout_id == workout.id)
    if exercise_id is not None:
        stmt = stmt.where(WorkoutSet.exercise_id == exercise_id)
    last = await session.scalar(stmt.order_by(WorkoutSet.id.desc()).limit(1))
    if last is not None:
        await session.delete(last)
        await session.flush()
        await session.refresh(workout, ["sets"])
    return last


def sets_for(workout: Workout, exercise_id: int) -> list[WorkoutSet]:
    return [s for s in workout.sets if s.exercise_id == exercise_id]


async def last_time_sets(session: AsyncSession, user_id: int, exercise_id: int, exclude_workout_id: int | None) -> list[WorkoutSet]:
    """Sets of this exercise from the most recent *other* workout that included it."""
    stmt = select(WorkoutSet.workout_id).where(WorkoutSet.user_id == user_id, WorkoutSet.exercise_id == exercise_id)
    if exclude_workout_id is not None:
        stmt = stmt.where(WorkoutSet.workout_id != exclude_workout_id)
    last_wid = await session.scalar(stmt.order_by(WorkoutSet.created_at.desc()).limit(1))
    if last_wid is None:
        return []
    rows = await session.scalars(
        select(WorkoutSet)
        .where(WorkoutSet.workout_id == last_wid, WorkoutSet.exercise_id == exercise_id)
        .order_by(WorkoutSet.id)
    )
    return list(rows.all())


def summarize(workout: Workout) -> WorkoutSummary:
    blocks: dict[int, ExerciseBlock] = {}
    for s in workout.sets:
        blocks.setdefault(s.exercise_id, ExerciseBlock(s.exercise)).sets.append(s)
    end = as_utc(workout.ended_at or utcnow())
    start = as_utc(workout.started_at)
    return WorkoutSummary(
        workout=workout,
        duration_s=max(0.0, (end - start).total_seconds()),
        blocks=list(blocks.values()),
        set_count=len(workout.sets),
        volume_kg=sum(set_volume(s) for s in workout.sets),
        cardio_km=sum(float(s.distance_km or 0) for s in workout.sets),
        pr_count=sum(1 for s in workout.sets if s.is_pr),
    )


async def finish(session: AsyncSession, workout: Workout) -> WorkoutSummary | None:
    """Complete the workout. An empty workout is deleted and None is returned."""
    await session.refresh(workout, ["sets"])
    if not workout.sets:
        await session.delete(workout)
        await session.flush()
        return None
    workout.status = WorkoutStatus.COMPLETED.value
    workout.ended_at = max(utcnow(), as_utc(workout.sets[-1].created_at))
    await session.flush()
    return summarize(workout)


async def cancel(session: AsyncSession, workout: Workout) -> None:
    await session.delete(workout)
    await session.flush()


async def delete_workout(session: AsyncSession, user_id: int, workout_id: int) -> bool:
    workout = await load(session, user_id, workout_id)
    if workout is None:
        return False
    await session.delete(workout)
    await session.flush()
    return True
