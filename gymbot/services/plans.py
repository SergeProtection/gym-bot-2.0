"""Workout plans (templates): an ordered list of exercises with target sets/reps."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from gymbot.db.models import Exercise, Plan, PlanItem, Workout
from gymbot.services.workouts import sets_for

MAX_PLANS = 30
MAX_ITEMS = 30


class PlanError(ValueError):
    pass


def _clean_name(name: str) -> str:
    name = " ".join(name.split())[:80]
    if not name:
        raise PlanError("Plan name cannot be empty")
    return name


async def list_plans(session: AsyncSession, user_id: int) -> list[Plan]:
    rows = await session.scalars(
        select(Plan).where(Plan.user_id == user_id).order_by(Plan.name).options(selectinload(Plan.items))
    )
    return list(rows.all())


async def get_plan(session: AsyncSession, user_id: int, plan_id: int) -> Plan | None:
    return await session.scalar(
        select(Plan).where(Plan.id == plan_id, Plan.user_id == user_id).options(selectinload(Plan.items))
    )


async def create_plan(session: AsyncSession, user_id: int, name: str) -> Plan:
    count = await session.scalar(select(func.count(Plan.id)).where(Plan.user_id == user_id))
    if (count or 0) >= MAX_PLANS:
        raise PlanError(f"You can have at most {MAX_PLANS} plans")
    plan = Plan(user_id=user_id, name=_clean_name(name))
    session.add(plan)
    await session.flush()
    return await get_plan(session, user_id, plan.id)


async def rename_plan(session: AsyncSession, plan: Plan, name: str) -> Plan:
    plan.name = _clean_name(name)
    await session.flush()
    return plan


async def delete_plan(session: AsyncSession, user_id: int, plan_id: int) -> bool:
    plan = await get_plan(session, user_id, plan_id)
    if plan is None:
        return False
    await session.delete(plan)
    await session.flush()
    return True


async def _reload(session: AsyncSession, plan: Plan) -> Plan:
    await session.flush()
    await session.refresh(plan, ["items"])
    return plan


async def add_item(
    session: AsyncSession, plan: Plan, exercise: Exercise, *, target_sets: int = 3, target_reps: int | None = None
) -> Plan:
    if len(plan.items) >= MAX_ITEMS:
        raise PlanError(f"A plan can have at most {MAX_ITEMS} exercises")
    if exercise.owner_id is not None and exercise.owner_id != plan.user_id:
        raise PlanError("Unknown exercise")
    position = max((i.position for i in plan.items), default=-1) + 1
    session.add(
        PlanItem(
            plan_id=plan.id,
            exercise_id=exercise.id,
            position=position,
            target_sets=max(1, min(20, int(target_sets))),
            target_reps=max(1, min(1000, int(target_reps))) if target_reps else None,
        )
    )
    return await _reload(session, plan)


def _item(plan: Plan, item_id: int) -> PlanItem:
    for item in plan.items:
        if item.id == item_id:
            return item
    raise PlanError("Unknown plan item")


async def update_item(
    session: AsyncSession, plan: Plan, item_id: int, *, target_sets: int | None = None, target_reps: int | None = None
) -> Plan:
    item = _item(plan, item_id)
    if target_sets is not None:
        item.target_sets = max(1, min(20, int(target_sets)))
    if target_reps is not None:
        item.target_reps = max(1, min(1000, int(target_reps))) if target_reps > 0 else None
    return await _reload(session, plan)


async def remove_item(session: AsyncSession, plan: Plan, item_id: int) -> Plan:
    await session.delete(_item(plan, item_id))
    await _reload(session, plan)
    for pos, item in enumerate(plan.items):
        item.position = pos
    return await _reload(session, plan)


async def move_item(session: AsyncSession, plan: Plan, item_id: int, delta: int) -> Plan:
    items = list(plan.items)
    item = _item(plan, item_id)
    old = items.index(item)
    new = max(0, min(len(items) - 1, old + delta))
    items.insert(new, items.pop(old))
    for pos, it in enumerate(items):
        it.position = pos
    return await _reload(session, plan)


async def plan_from_workout(session: AsyncSession, user_id: int, workout: Workout, name: str) -> Plan:
    """Save a workout's exercises (in order) as a new plan, using its set counts and top reps."""
    plan = await create_plan(session, user_id, name)
    seen: list[int] = []
    for s in workout.sets:
        if s.exercise_id not in seen:
            seen.append(s.exercise_id)
    for ex_id in seen[:MAX_ITEMS]:
        ex_sets = sets_for(workout, ex_id)
        reps = max((s.reps or 0 for s in ex_sets), default=0) or None
        session.add(
            PlanItem(plan_id=plan.id, exercise_id=ex_id, position=len(plan.items), target_sets=len(ex_sets), target_reps=reps)
        )
        await _reload(session, plan)
    return plan


def plan_progress(workout: Workout) -> list[tuple[PlanItem, int]]:
    """For a workout started from a plan: (item, sets done) in plan order."""
    if workout.plan is None:
        return []
    return [(item, len(sets_for(workout, item.exercise_id))) for item in workout.plan.items]


def next_plan_item(workout: Workout) -> PlanItem | None:
    """First plan exercise that still has sets left to do."""
    for item, done in plan_progress(workout):
        if done < item.target_sets:
            return item
    return None
