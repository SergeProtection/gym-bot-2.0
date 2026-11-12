"""Plain-dict JSON shapes returned by the API."""

from __future__ import annotations

from urllib.parse import quote

from gymbot.db.models import Exercise, Plan, PlanItem, Reminder, User, Workout, WorkoutSet
from gymbot.services import reminders as reminder_svc
from gymbot.services.plans import plan_progress
from gymbot.services.stats import ExerciseProgress, ExerciseRecord, PeriodSummary, WeekPoint
from gymbot.services.workouts import WorkoutSummary, describe_set
from gymbot.utils import as_utc, tz_label


def iso(dt) -> str | None:
    return as_utc(dt).isoformat() if dt else None


def media_url(rel: str | None) -> str | None:
    return f"/media/{quote(rel)}" if rel else None


def exercise(ex: Exercise) -> dict:
    return {
        "id": ex.id,
        "name": ex.name,
        "group": ex.muscle_group,
        "kind": ex.kind,
        "image": media_url(ex.image),
        "custom": ex.owner_id is not None,
    }


def workout_set(s: WorkoutSet) -> dict:
    return {
        "id": s.id,
        "exercise_id": s.exercise_id,
        "reps": s.reps,
        "weight_kg": s.weight_kg,
        "duration_s": s.duration_s,
        "distance_km": s.distance_km,
        "is_pr": s.is_pr,
        "created_at": iso(s.created_at),
        "text": describe_set(s, s.exercise.kind),
    }


def summary(sm: WorkoutSummary) -> dict:
    w = sm.workout
    return {
        "id": w.id,
        "status": w.status,
        "plan_id": w.plan_id,
        "started_at": iso(w.started_at),
        "ended_at": iso(w.ended_at),
        "duration_s": round(sm.duration_s),
        "set_count": sm.set_count,
        "volume_kg": round(sm.volume_kg, 1),
        "cardio_km": round(sm.cardio_km, 2),
        "pr_count": sm.pr_count,
        "groups": sm.groups,
        "exercises": [{"exercise": exercise(b.exercise), "sets": [workout_set(s) for s in b.sets]} for b in sm.blocks],
    }


def active_workout(sm: WorkoutSummary) -> dict:
    data = summary(sm)
    w: Workout = sm.workout
    if w.plan is not None:
        data["plan"] = {
            "id": w.plan.id,
            "name": w.plan.name,
            "items": [
                {**plan_item(item), "done": done} for item, done in plan_progress(w)
            ],
        }
    return data


def plan_item(item: PlanItem) -> dict:
    return {
        "id": item.id,
        "position": item.position,
        "target_sets": item.target_sets,
        "target_reps": item.target_reps,
        "exercise": exercise(item.exercise),
    }


def plan(p: Plan) -> dict:
    return {"id": p.id, "name": p.name, "items": [plan_item(i) for i in p.items]}


def reminder(r: Reminder) -> dict:
    return {
        "id": r.id,
        "weekdays": reminder_svc.days_of(r),
        "at_time": r.at_time,
        "enabled": r.enabled,
        "label": reminder_svc.describe(r),
    }


def user(u: User) -> dict:
    return {
        "id": u.id,
        "first_name": u.first_name,
        "username": u.username,
        "timezone": u.timezone,
        "timezone_label": tz_label(u.timezone),
        "rest_seconds": u.rest_seconds,
    }


def period(p: PeriodSummary) -> dict:
    return {
        "first": p.first.isoformat(),
        "last": p.last.isoformat(),
        "workouts": p.workouts,
        "active_days": p.active_days,
        "sets": p.sets,
        "volume_kg": round(p.volume_kg, 1),
        "cardio_km": round(p.cardio_km, 2),
        "cardio_s": p.cardio_s,
        "hold_s": p.hold_s,
        "training_s": round(p.training_s),
        "prs": p.prs,
        "groups": [{"group": g, "sets": t.sets, "volume_kg": round(t.volume_kg, 1)} for g, t in p.groups_sorted],
    }


def record(r: ExerciseRecord) -> dict:
    return {
        "exercise": exercise(r.exercise),
        "sessions": r.sessions,
        "best_weight_kg": r.best_weight,
        "best_weight_reps": r.best_weight_reps,
        "best_e1rm_kg": round(r.best_e1rm, 1) if r.best_e1rm else None,
        "best_reps": r.best_reps,
        "best_hold_s": r.best_hold_s,
        "best_distance_km": r.best_distance_km,
        "best_pace_s": round(r.best_pace_s) if r.best_pace_s else None,
        "last_at": iso(r.last_at),
    }


def progress(p: ExerciseProgress, labels: dict[str, str]) -> dict:
    return {
        "exercise": exercise(p.exercise),
        "metric": p.metric,
        "label": labels[p.metric],
        "points": [{"day": pt.day.isoformat(), "value": pt.value, "extra": pt.extra} for pt in p.points],
    }


def week(w: WeekPoint) -> dict:
    return {"week_start": w.week_start.isoformat(), "workouts": w.workouts, "sets": w.sets, "volume_kg": round(w.volume_kg, 1)}
