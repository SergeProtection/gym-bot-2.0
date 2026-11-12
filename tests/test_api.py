import json
import time

import pytest
from starlette.testclient import TestClient

from gymbot.api.app import create_app
from gymbot.api.auth import AuthError, sign_init_data, validate_init_data
from gymbot.config import Settings

TOKEN = "123:TEST"


def auth(user_id=1):
    data = sign_init_data({"auth_date": str(int(time.time())), "user": json.dumps({"id": user_id, "first_name": "T"})}, TOKEN)
    return {"Authorization": f"tma {data}"}


@pytest.fixture
def client(db):
    with TestClient(create_app(Settings(telegram_bot_token=TOKEN), db)) as c:
        yield c


def test_init_data_validation():
    data = sign_init_data({"auth_date": "1000", "user": json.dumps({"id": 5})}, TOKEN)
    assert validate_init_data(data, TOKEN, now=1000).id == 5
    with pytest.raises(AuthError):
        validate_init_data(data, "other:token", now=1000)
    with pytest.raises(AuthError):
        validate_init_data(data, TOKEN, now=1000 + 2 * 86400)
    with pytest.raises(AuthError):
        validate_init_data(data.replace("5", "6"), TOKEN, now=1000)


def test_requires_auth(client):
    assert client.get("/api/me").status_code == 401
    assert client.get("/health").json() == {"ok": True}


def test_full_flow(client):
    h = auth()
    me = client.get("/api/me", headers=h).json()
    assert me["active_workout"] is None and me["user"]["rest_seconds"] == 90
    assert client.patch("/api/me", headers=h, json={"timezone": "+3", "rest_seconds": 120}).json()["timezone_label"] == "UTC+3"

    exs = client.get("/api/exercises", headers=h, params={"group": "Chest"}).json()["exercises"]
    assert exs and exs[0]["image"].startswith("/media/")
    ex = exs[0]
    assert client.get(ex["image"]).status_code == 200

    plan = client.post("/api/plans", headers=h, json={"name": "Push"}).json()
    plan = client.post(f"/api/plans/{plan['id']}/items", headers=h, json={"exercise_id": ex["id"], "target_sets": 2}).json()
    assert plan["items"][0]["target_sets"] == 2

    w = client.post("/api/workout/start", headers=h, json={"plan_id": plan["id"]})
    assert w.status_code == 201 and w.json()["workout"]["plan"]["items"][0]["done"] == 0
    r = client.post("/api/workout/sets", headers=h, json={"exercise_id": ex["id"], "weight_kg": 50, "reps": 10})
    assert r.status_code == 201 and r.json()["rest_seconds"] == 120
    assert r.json()["workout"]["plan"]["items"][0]["done"] == 1
    bad = client.post("/api/workout/sets", headers=h, json={"exercise_id": ex["id"], "weight_kg": 50})
    assert bad.status_code == 422
    assert client.post("/api/workout/sets", headers=h, json={"exercise_id": 999999, "reps": 1}).status_code == 404
    done = client.post("/api/workout/finish", headers=h).json()["summary"]
    assert done["set_count"] == 1 and done["volume_kg"] == 500

    assert client.get("/api/stats/summary", headers=h, params={"period": "week"}).json()["workouts"] == 1
    assert client.get("/api/stats/summary", headers=h, params={"period": "custom", "from": "2026-01-01", "to": "2026-12-31"}).status_code == 200
    assert client.get("/api/stats/summary", headers=h, params={"period": "year"}).status_code == 422
    assert len(client.get("/api/stats/weekly", headers=h, params={"weeks": 8}).json()["weeks"]) == 8
    assert client.get("/api/stats/records", headers=h).json()["records"][0]["best_weight_kg"] == 50
    assert client.get(f"/api/exercises/{ex['id']}/progress", headers=h).json()["metric"] == "e1rm"
    hist = client.get("/api/history", headers=h).json()
    assert hist["total"] == 1
    copy = client.post(f"/api/workouts/{hist['workouts'][0]['id']}/plan", headers=h, json={"name": "Copy"})
    assert copy.status_code == 201

    rem = client.post("/api/reminders", headers=h, json={"weekdays": "mon-fri", "at_time": "7:30"}).json()
    assert rem["at_time"] == "07:30" and rem["label"] == "weekdays at 07:30"
    assert client.patch(f"/api/reminders/{rem['id']}", headers=h, json={"enabled": False}).json()["enabled"] is False

    # another user can't touch this user's data
    other = auth(2)
    assert client.get(f"/api/plans/{plan['id']}", headers=other).status_code == 404
    assert client.delete(f"/api/reminders/{rem['id']}", headers=other).status_code == 404
    assert client.delete(f"/api/workouts/{hist['workouts'][0]['id']}", headers=other).status_code == 404
