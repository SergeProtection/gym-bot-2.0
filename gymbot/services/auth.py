"""Standalone-app sign-in: one-time login codes from the bot, then signed session tokens."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from gymbot.db.models import LoginCode, User, utcnow
from gymbot.utils import as_utc

CODE_TTL = timedelta(minutes=10)
SESSION_TTL_S = 180 * 24 * 3600
# No 0/O, 1/I/L: easy to read off one screen and type on another.
CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
CODE_LEN = 6


def session_secret(bot_token: str, explicit: str = "") -> bytes:
    """Explicit SESSION_SECRET if set, else derived from the bot token (rotating the token signs everyone out)."""
    if explicit:
        return explicit.encode()
    return hmac.new(b"gymbot-session", (bot_token or "dev-only").encode(), hashlib.sha256).digest()


def normalize_code(raw: str) -> str:
    return "".join(ch for ch in raw.upper() if ch.isalnum())


def _hash(code: str) -> str:
    return hashlib.sha256(normalize_code(code).encode()).hexdigest()


def format_code(code: str) -> str:
    return f"{code[:3]}-{code[3:]}"


async def create_login_code(session: AsyncSession, user_id: int) -> str:
    """New one-time code for this user; any earlier unused codes stop working."""
    now = utcnow()
    await session.execute(
        update(LoginCode).where(LoginCode.user_id == user_id, LoginCode.used_at.is_(None)).values(used_at=now)
    )
    code = "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_LEN))
    session.add(LoginCode(user_id=user_id, code_hash=_hash(code), expires_at=now + CODE_TTL, created_at=now))
    await session.flush()
    return code


async def redeem_login_code(session: AsyncSession, raw: str) -> User | None:
    """Consume a code. Returns the user, or None if it's wrong, used or expired."""
    code = normalize_code(raw)
    if len(code) != CODE_LEN:
        return None
    row = await session.scalar(select(LoginCode).where(LoginCode.code_hash == _hash(code)))
    now = utcnow()
    if row is None or row.used_at is not None or as_utc(row.expires_at) < now:
        return None
    row.used_at = now
    await session.flush()
    return await session.get(User, row.user_id)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def issue_session(user: User, secret: bytes, *, now: float | None = None) -> str:
    issued = int(now or time.time())
    payload = _b64(json.dumps({"uid": user.id, "v": user.session_version, "iat": issued, "exp": issued + SESSION_TTL_S}, separators=(",", ":")).encode())
    sig = _b64(hmac.new(secret, payload.encode(), hashlib.sha256).digest())
    return f"{payload}.{sig}"


def read_session(token: str, secret: bytes, *, now: float | None = None) -> tuple[int, int] | None:
    """(user_id, session_version) for a valid, unexpired token; None otherwise."""
    try:
        payload, sig = token.split(".", 1)
        expected = _b64(hmac.new(secret, payload.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(expected, sig):
            return None
        data = json.loads(_unb64(payload))
        if int(data["exp"]) < (now or time.time()):
            return None
        return int(data["uid"]), int(data["v"])
    except (ValueError, KeyError, TypeError):
        return None


class FailureLimiter:
    """Caps failed code attempts across the whole service, so codes can't be brute-forced."""

    def __init__(self, max_failures: int = 50, window_s: int = 600) -> None:
        self.max_failures = max_failures
        self.window_s = window_s
        self._failures: list[float] = []

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_s
        self._failures = [t for t in self._failures if t > cutoff]

    def blocked(self, now: float | None = None) -> bool:
        self._prune(now or time.time())
        return len(self._failures) >= self.max_failures

    def record_failure(self, now: float | None = None) -> None:
        self._failures.append(now or time.time())
