import json
import time

import pytest
from sqlalchemy import select
from starlette.testclient import TestClient

from gymbot.api.app import create_app
from gymbot.api.auth import sign_init_data
from gymbot.config import Settings
from gymbot.db import Plan
from gymbot.services import catalog, dashboard, templates

TOKEN = "123:TEST"


def headers(user_id=1):
    data = sign_init_data({"auth_date": str(int(time.time())), "user": json.dumps({"id": user_id})}, TOKEN)
    return {"Authorization": f"tma {data}"}


async def test_every_template_exercise_exists(db):
    async with db.session() as s:
        names = {ex.name for ex in await catalog.list_exercises(s, 1)}
    missing = [(t.key, i.exercise) for t in templates.TEMPLATES for i in t.items if i.exercise not in names]
    assert missing == []


async def test_templates_match_rotation_and_skip_duplicates(db):
    async with db.session() as s:
        created = await templates.add_templates(s, 1, ["chest", "back", "chest", "nope"])
        assert [p.name for p in created] == ["Chest day", "Back day"]
        assert [i.target_sets for i in created[0].items] == [4, 3, 3, 3, 3, 3]
        again = await templates.add_templates(s, 1, ["chest", "legs"])
        assert [p.name for p in again] == ["Legs day"]
        plans = (await s.scalars(select(Plan).where(Plan.user_id == 1))).all()
        assert dashboard.pick_plan(list(plans), "Back").name == "Back day"


def test_first_record_and_record_line(client_factory):
    c, h = client_factory()
    ex = c.get("/api/exercises", headers=h, params={"group": "Shoulders"}).json()["exercises"][0]
    c.post("/api/workout/start", headers=h, json={})
    first = c.post("/api/workout/sets", headers=h, json={"exercise_id": ex["id"], "weight_kg": 10, "reps": 12}).json()
    assert first["prs"][0].startswith("First record: 10 kg × 12") and not first["set"]["is_pr"]
    assert c.get(f"/api/exercises/{ex['id']}/last", headers=h).json()["record"] is None  # nothing completed yet
    c.post("/api/workout/finish", headers=h)
    rec = c.get(f"/api/exercises/{ex['id']}/last", headers=h).json()["record"]
    assert rec["best_weight_kg"] == 10 and rec["best_weight_reps"] == 12


def test_wipe_requires_phrase_and_only_touches_own_data(client_factory, db):
    c, h = client_factory()
    other = headers(2)
    for who in (h, other):
        c.post("/api/plans/templates", headers=who, json={"keys": ["legs"]})
        ex = c.get("/api/exercises", headers=who, params={"group": "Chest"}).json()["exercises"][0]
        c.post("/api/workout/start", headers=who, json={})
        c.post("/api/workout/sets", headers=who, json={"exercise_id": ex["id"], "weight_kg": 50, "reps": 5})
        c.post("/api/workout/finish", headers=who)
        c.post("/api/bodyweight", headers=who, json={"weight_kg": 80})
        c.post("/api/reminders", headers=who, json={"weekdays": "daily", "at_time": "18:00"})
        c.post("/api/exercises", headers=who, json={"name": "My Move", "group": "Chest"})
    c.patch("/api/me", headers=h, json={"rest_seconds": 120})

    assert c.request("DELETE", "/api/me/data", headers=h, json={"confirm": "delete"}).status_code == 422
    r = c.request("DELETE", "/api/me/data", headers=h, json={"confirm": "  Delete Data "})
    assert r.status_code == 200
    assert r.json()["deleted"] == {"sets": 1, "workouts": 1, "plans": 1, "reminders": 1, "body_weights": 1, "custom_exercises": 1}

    assert c.get("/api/history", headers=h).json()["total"] == 0
    assert c.get("/api/plans", headers=h).json()["plans"] == []
    assert c.get("/api/stats/records", headers=h).json()["records"] == []
    assert c.get("/api/me", headers=h).json()["user"]["rest_seconds"] == 120  # settings kept
    # user 2 untouched
    assert c.get("/api/history", headers=other).json()["total"] == 1
    assert len(c.get("/api/plans", headers=other).json()["plans"]) == 1
    assert len(c.get("/api/reminders", headers=other).json()["reminders"]) == 1


@pytest.fixture
def client_factory(db):
    clients = []

    def make():
        c = TestClient(create_app(Settings(telegram_bot_token=TOKEN), db))
        c.__enter__()
        clients.append(c)
        return c, headers(1)

    yield make
    for c in clients:
        c.__exit__(None, None, None)
