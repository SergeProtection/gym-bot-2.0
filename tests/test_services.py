from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from gymbot.db import Exercise, Workout
from gymbot.services import plans, reminders, stats, workouts
from gymbot.utils import local_today


async def _ex(s, name):
    return await s.scalar(select(Exercise).where(Exercise.name == name))


async def _any(s, kind):
    return await s.scalar(select(Exercise).where(Exercise.kind == kind).limit(1))


async def test_catalog_has_groups(db):
    async with db.session() as s:
        from gymbot.services import catalog

        groups = await catalog.list_groups(s, 1)
        assert "Chest" in groups and "Cardio" in groups
        assert await _any(s, "time") is not None


async def test_workout_pr_and_summary(db):
    async with db.session() as s:
        bench = await _any(s, "strength")
        w, created = await workouts.start(s, 1)
        assert created
        _, prs = await workouts.log_set(s, w, bench, weight=60, reps=8)
        assert prs == []  # first time: no PR
        summary = await workouts.finish(s, w)
        assert summary.set_count == 1 and summary.volume_kg == 480

        w2, _ = await workouts.start(s, 1)
        _, prs = await workouts.log_set(s, w2, bench, weight=65, reps=5)
        assert any("Heaviest" in p for p in prs)
        _, prs = await workouts.log_set(s, w2, bench, weight=65, reps=4)
        assert prs == []
        await workouts.finish(s, w2)

        recs = await stats.personal_records(s, 1)
        assert recs[0].best_weight == 65 and recs[0].sessions == 2

        tz = "Europe/Berlin"
        today = local_today(tz)
        first, last = stats.week_bounds(today)
        summ = await stats.period_summary(s, 1, tz, first, last)
        assert summ.workouts == 2 and summ.sets == 3 and summ.prs == 1

        prog = await stats.exercise_progress(s, 1, bench, tz)
        assert prog.metric == "e1rm" and len(prog.points) == 1
        weeks = await stats.weekly_series(s, 1, tz, weeks=4)
        assert len(weeks) == 4 and weeks[-1].workouts == 2
        assert await stats.week_streak(s, 1, tz) == 1
        assert len(await stats.history(s, 1)) == 2


async def test_empty_workout_is_discarded(db):
    async with db.session() as s:
        w, _ = await workouts.start(s, 1)
        assert await workouts.finish(s, w) is None
        assert await s.scalar(select(Workout.id)) is None


async def test_plans(db):
    async with db.session() as s:
        a = await _any(s, "strength")
        b = await _any(s, "time")
        plan = await plans.create_plan(s, 1, "  Push   day ")
        assert plan.name == "Push day"
        await plans.add_item(s, plan, a, target_sets=2, target_reps=10)
        await plans.add_item(s, plan, b, target_sets=1)
        await plans.move_item(s, plan, plan.items[1].id, -1)
        assert [i.exercise_id for i in plan.items] == [b.id, a.id]

        w, _ = await workouts.start(s, 1, plan_id=plan.id)
        w = await workouts.get_active(s, 1)
        assert plans.next_plan_item(w).exercise_id == b.id
        await workouts.log_set(s, w, b, duration=60)
        assert plans.next_plan_item(w).exercise_id == a.id
        await workouts.log_set(s, w, a, weight=20, reps=10)
        await workouts.log_set(s, w, a, weight=20, reps=12)
        assert plans.next_plan_item(w) is None

        copy = await plans.plan_from_workout(s, 1, w, "Copy")
        assert [(i.exercise_id, i.target_sets, i.target_reps) for i in copy.items] == [(b.id, 1, None), (a.id, 2, 12)]

        await plans.remove_item(s, plan, plan.items[0].id)
        assert [i.position for i in plan.items] == [0]
        assert await plans.delete_plan(s, 1, plan.id)


def test_parse_weekdays():
    assert reminders.parse_weekdays("daily") == list(range(7))
    assert reminders.parse_weekdays("mon wed, fri") == [0, 2, 4]
    assert reminders.parse_weekdays("fri-mon") == [0, 4, 5, 6]
    assert reminders.parse_weekdays("someday") is None
    assert reminders.describe_days([0, 1, 2, 3, 4]) == "weekdays"


async def test_reminders_fire_once(db):
    async with db.session() as s:
        from gymbot.services import users

        user = await users.get_user(s, 1)
        await users.set_timezone(s, user, "Etc/GMT-3")  # UTC+3
        # 2026-11-11 is a Wednesday
        r = await reminders.create_reminder(s, 1, [2], "18:00")
        before = datetime(2026, 11, 11, 14, 59, tzinfo=timezone.utc)  # 17:59 local
        at = datetime(2026, 11, 11, 15, 5, tzinfo=timezone.utc)  # 18:05 local
        late = at + timedelta(hours=2)
        assert await reminders.collect_due(s, before) == []
        due = await reminders.collect_due(s, at)
        assert len(due) == 1 and due[0].reminder.id == r.id and not due[0].already_trained
        assert await reminders.collect_due(s, at + timedelta(minutes=1)) == []
        assert await reminders.collect_due(s, late) == []
        thursday = at + timedelta(days=1)
        assert await reminders.collect_due(s, thursday) == []
