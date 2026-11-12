"""Ready-made workout days users can add to their plans with one tap."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from gymbot.db.models import Exercise, Plan, PlanItem
from gymbot.services import plans as plan_svc

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TemplateItem:
    exercise: str  # built-in catalog name
    sets: int
    reps: int | None  # None for timed exercises


@dataclass(frozen=True)
class Template:
    key: str
    name: str  # contains the muscle group, so the home screen's Next up can match it
    description: str
    items: tuple[TemplateItem, ...]


def _t(key: str, name: str, description: str, *items: tuple[str, int, int | None]) -> Template:
    return Template(key, name, description, tuple(TemplateItem(*i) for i in items))


TEMPLATES: tuple[Template, ...] = (
    _t("chest", "Chest day", "Chest with some triceps",
       ("Barbell Bench Press", 4, 8), ("Incline Dumbbell Bench Press", 3, 10), ("Chest Press Machine", 3, 10),
       ("Cable Crossover", 3, 12), ("Dumbbell Fly", 3, 12), ("Triceps Pressdown", 3, 12)),
    _t("back", "Back day", "Back with some biceps",
       ("Barbell Deadlift", 3, 5), ("Wide Grip Pulldown", 4, 10), ("Barbell Row", 3, 8),
       ("Seated Cable Row", 3, 10), ("Straight Arm Lat Pulldown", 3, 12), ("Barbell Curl", 3, 10)),
    _t("shoulders", "Shoulders day", "Shoulders, traps and core",
       ("Dumbbell Shoulder Press", 4, 8), ("Dumbbell Lateral Raise", 4, 12), ("Dumbbell Front Raise", 3, 12),
       ("Bent Over Lateral Raise", 3, 12), ("Barbell Shrug", 3, 12), ("Plank", 3, None)),
    _t("legs", "Legs day", "Quads, hamstrings, glutes and calves",
       ("Squat", 4, 8), ("Leg Press", 3, 10), ("Barbell Romanian Deadlift", 3, 10),
       ("Leg Extension", 3, 12), ("Lying Leg Curl", 3, 12), ("Standing Calf Raise", 4, 15)),
    _t("arms", "Arms day", "Biceps and triceps",
       ("Barbell Curl", 3, 10), ("Hammer Curl", 3, 12), ("Incline Dumbbell Curl", 3, 12),
       ("Cable Rope Pushdown", 3, 12), ("Lying Triceps Extension", 3, 10), ("Bench Dips", 3, 12)),
    _t("full", "Full body", "One session for everything — good for 2–3 days a week",
       ("Squat", 3, 8), ("Dumbbell Bench Press", 3, 10), ("Seated Cable Row", 3, 10),
       ("Dumbbell Shoulder Press", 3, 10), ("Lying Leg Curl", 3, 12), ("Plank", 3, None)),
)


def by_key(key: str) -> Template | None:
    return next((t for t in TEMPLATES if t.key == key), None)


async def _builtin_by_name(session: AsyncSession) -> dict[str, Exercise]:
    rows = await session.scalars(select(Exercise).where(Exercise.owner_id.is_(None)))
    return {ex.name.lower(): ex for ex in rows.all()}


async def existing_names(session: AsyncSession, user_id: int) -> set[str]:
    rows = await session.scalars(select(func.lower(Plan.name)).where(Plan.user_id == user_id))
    return set(rows.all())


async def add_templates(session: AsyncSession, user_id: int, keys: list[str]) -> list[Plan]:
    """Create the chosen templates as plans. Ones the user already has (same name) are skipped."""
    catalog = await _builtin_by_name(session)
    have = await existing_names(session, user_id)
    created: list[Plan] = []
    for key in dict.fromkeys(keys):  # de-duplicate, keep order
        tpl = by_key(key)
        if tpl is None or tpl.name.lower() in have:
            continue
        plan = await plan_svc.create_plan(session, user_id, tpl.name)
        position = 0
        for item in tpl.items:
            ex = catalog.get(item.exercise.lower())
            if ex is None:
                log.warning("Template %s: exercise %r is not in the catalog", tpl.key, item.exercise)
                continue
            session.add(PlanItem(plan_id=plan.id, exercise_id=ex.id, position=position, target_sets=item.sets, target_reps=item.reps))
            position += 1
        await session.flush()
        await session.refresh(plan, ["items"])
        have.add(tpl.name.lower())
        created.append(plan)
    return created
