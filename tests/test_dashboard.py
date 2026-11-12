import json
import time

from sqlalchemy import select
from starlette.testclient import TestClient

from gymbot.api.app import create_app
from gymbot.api.auth import sign_init_data
from gymbot.config import Settings
from gymbot.db import Exercise
from gymbot.services import dashboard, plans, users, workouts

TOKEN = "123:TEST"


def test_rotation():
    assert dashboard.next_in_rotation([]) == "Chest"
    assert dashboard.next_in_rotation(["Chest", "Triceps"]) == "Back"
    assert dashboard.next_in_rotation(["Biceps", "Legs"]) == "Chest"  # wraps around; non-rotation groups skipped
    assert dashboard.next_in_rotation(["Abs"]) == "Chest"


async def test_dashboard_suggests_matching_plan(db):
    async with db.session() as s:
        chest = await s.scalar(select(Exercise).where(Exercise.muscle_group == "Chest").limit(1))
        back = await s.scalar(select(Exercise).where(Exercise.muscle_group == "Back").limit(1))
        pull = await plans.create_plan(s, 1, "Pull")
        await plans.add_item(s, pull, back)

        d = await dashboard.build(s, await users.get_user(s, 1))
        assert d.next_group == "Chest" and d.next_plan is None and d.last is None

        w, _ = await workouts.start(s, 1)
        await workouts.log_set(s, w, chest, weight=60, reps=8)
        await workouts.finish(s, w)
        await users.log_body_weight(s, 1, 80)
        await users.log_body_weight(s, 1, 79.4)

        d = await dashboard.build(s, await users.get_user(s, 1))
        assert d.next_group == "Back"
        assert d.next_plan is not None and d.next_plan.name == "Pull"  # matched by its exercises' group
        assert d.week.workouts == 1 and sum(d.week_days) == 1
        assert d.body_weight.weight_kg == 79.4 and d.body_weight_change == -0.6


def test_dashboard_endpoint(db):
    data = sign_init_data({"auth_date": str(int(time.time())), "user": json.dumps({"id": 1})}, TOKEN)
    with TestClient(create_app(Settings(telegram_bot_token=TOKEN), db)) as c:
        r = c.get("/api/dashboard", headers={"Authorization": f"tma {data}"})
        assert r.status_code == 200
        body = r.json()
        assert body["next"]["group"] == "Chest"
        assert body["next"]["image"] == "/media/Chest%20Exercise/Chest.png"
        assert body["next"]["rotation"] == ["Chest", "Back", "Shoulders", "Legs"]
        assert len(body["week_days"]) == 7 and body["body_weight"] is None
