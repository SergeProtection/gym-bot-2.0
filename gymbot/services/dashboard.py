"""Home screen data: today, personal stats and what to train next."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from gymbot.db.models import BodyWeight, Plan, User
from gymbot.services import plans as plan_svc
from gymbot.services import stats, users
from gymbot.services.workouts import WorkoutSummary
from gymbot.utils import local_range, local_today, to_local

# Training split, in order (the same rotation the original bot used).
ROTATION = ["Chest", "Back", "Shoulders", "Legs"]


@dataclass
class Dashboard:
    today: date
    week_days: list[bool]  # Mon..Sun: trained that day this week
    week: stats.PeriodSummary
    streak_weeks: int
    total_workouts: int
    last: WorkoutSummary | None
    body_weight: BodyWeight | None
    body_weight_change: float | None
    next_group: str
    next_plan: Plan | None
    plans: list[Plan] = field(default_factory=list)


def plan_main_group(plan: Plan) -> str | None:
    counts = Counter(item.exercise.muscle_group for item in plan.items)
    return counts.most_common(1)[0][0] if counts else None


def next_in_rotation(last_groups: list[str]) -> str:
    """The group after the last workout's main rotation group; Chest if there's no history."""
    for group in last_groups:  # most-trained first
        if group in ROTATION:
            return ROTATION[(ROTATION.index(group) + 1) % len(ROTATION)]
    return ROTATION[0]


def pick_plan(plans: list[Plan], group: str) -> Plan | None:
    """A plan for that group: one whose name mentions it, else one whose exercises are mostly that group."""
    usable = [p for p in plans if p.items]
    for p in usable:
        if group.lower() in p.name.lower():
            return p
    for p in usable:
        if plan_main_group(p) == group:
            return p
    return None


async def build(session: AsyncSession, user: User) -> Dashboard:
    tz = user.timezone
    today = local_today(tz)
    monday, sunday = stats.week_bounds(today)
    week = await stats.period_summary(session, user.id, tz, monday, sunday)

    start, end = local_range(monday, sunday, tz)
    trained = {to_local(w.started_at, tz).date() for w in await stats.completed_between(session, user.id, start, end)}
    week_days = [monday + timedelta(days=i) in trained for i in range(7)]

    last = await stats.last_workout(session, user.id)
    weights = await users.body_weights(session, user.id, limit=2)
    latest = weights[-1] if weights else None
    change = round(weights[-1].weight_kg - weights[-2].weight_kg, 2) if len(weights) == 2 else None

    all_plans = await plan_svc.list_plans(session, user.id)
    group = next_in_rotation(last.groups if last else [])
    return Dashboard(
        today=today,
        week_days=week_days,
        week=week,
        streak_weeks=await stats.week_streak(session, user.id, tz),
        total_workouts=await stats.count_completed(session, user.id),
        last=last,
        body_weight=latest,
        body_weight_change=change,
        next_group=group,
        next_plan=pick_plan(all_plans, group),
        plans=all_plans,
    )
