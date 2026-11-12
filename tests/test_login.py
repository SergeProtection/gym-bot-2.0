from datetime import timedelta

from sqlalchemy import select
from starlette.testclient import TestClient

from gymbot.api.app import create_app
from gymbot.config import Settings
from gymbot.db import LoginCode
from gymbot.services import auth, users

TOKEN = "123:TEST"


async def _code(db, user_id=1):
    async with db.session() as s:
        return await auth.create_login_code(s, user_id)


def test_session_tokens():
    secret = auth.session_secret(TOKEN)

    class U:
        id, session_version = 7, 3

    token = auth.issue_session(U, secret, now=1000)
    assert auth.read_session(token, secret, now=1001) == (7, 3)
    assert auth.read_session(token, auth.session_secret("other:token"), now=1001) is None
    assert auth.read_session(token, secret, now=1000 + auth.SESSION_TTL_S + 1) is None
    assert auth.read_session(token[:-2] + "xx", secret, now=1001) is None
    assert auth.read_session("garbage", secret) is None


async def test_code_is_single_use_and_replaced(db):
    first = await _code(db)
    second = await _code(db)  # issuing a new code cancels the old one
    async with db.session() as s:
        assert await auth.redeem_login_code(s, first) is None
        user = await auth.redeem_login_code(s, auth.format_code(second).lower())  # dash + lowercase accepted
        assert user.id == 1
        assert await auth.redeem_login_code(s, second) is None


async def test_expired_code(db):
    code = await _code(db)
    async with db.session() as s:
        row = await s.scalar(select(LoginCode))
        row.expires_at = row.expires_at - timedelta(minutes=11)
    async with db.session() as s:
        assert await auth.redeem_login_code(s, code) is None


async def test_standalone_flow(db):
    app = create_app(Settings(telegram_bot_token=TOKEN), db)
    app.state.bot_username = "TestGymBot"
    with TestClient(app) as c:
        assert c.get("/api/public/config").json()["bot_url"] == "https://t.me/TestGymBot?start=login"
        assert c.post("/api/auth/redeem", json={"code": "AAAAAA"}).status_code == 400

        code = await _code(db)
        r = c.post("/api/auth/redeem", json={"code": code})
        assert r.status_code == 200 and r.json()["user"]["id"] == 1
        h = {"Authorization": f"Bearer {r.json()['token']}"}
        assert c.get("/api/me", headers=h).json()["user"]["id"] == 1
        assert c.post("/api/auth/redeem", json={"code": code}).status_code == 400  # already used

        assert c.post("/api/auth/signout-all", headers=h).json() == {"ok": True}
        assert c.get("/api/me", headers=h).status_code == 401  # old session revoked

        # a session for a user that doesn't exist is rejected, not auto-created
        class Ghost:
            id, session_version = 999, 0

        ghost = auth.issue_session(Ghost, app.state.session_secret)
        assert c.get("/api/me", headers={"Authorization": f"Bearer {ghost}"}).status_code == 401
    async with db.session() as s:
        assert await users.get_user(s, 999) is None


async def test_wrong_codes_are_rate_limited(db):
    app = create_app(Settings(telegram_bot_token=TOKEN), db)
    app.state.login_limiter = auth.FailureLimiter(max_failures=3)
    with TestClient(app) as c:
        for _ in range(3):
            assert c.post("/api/auth/redeem", json={"code": "ZZZZZZ"}).status_code == 400
        code = await _code(db)
        assert c.post("/api/auth/redeem", json={"code": code}).status_code == 429
