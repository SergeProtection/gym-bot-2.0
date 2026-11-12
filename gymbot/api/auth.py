"""Telegram Mini App authentication (initData signature check)."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from urllib.parse import parse_qsl

MAX_AGE_S = 24 * 3600


class AuthError(Exception):
    pass


@dataclass(frozen=True)
class TelegramUser:
    id: int
    first_name: str | None = None
    username: str | None = None


def validate_init_data(init_data: str, bot_token: str, *, max_age: int = MAX_AGE_S, now: float | None = None) -> TelegramUser:
    """Verify Telegram.WebApp.initData as described in the Telegram Mini Apps docs."""
    if not init_data or not bot_token:
        raise AuthError("missing init data")
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=True))
    except ValueError as exc:
        raise AuthError("malformed init data") from exc
    received = pairs.pop("hash", "")
    check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    expected = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, received):
        raise AuthError("bad signature")
    try:
        auth_date = int(pairs.get("auth_date", "0"))
    except ValueError as exc:
        raise AuthError("bad auth_date") from exc
    if max_age and (now or time.time()) - auth_date > max_age:
        raise AuthError("init data expired")
    try:
        user = json.loads(pairs["user"])
        return TelegramUser(id=int(user["id"]), first_name=user.get("first_name"), username=user.get("username"))
    except (KeyError, ValueError, TypeError) as exc:
        raise AuthError("no user in init data") from exc


def sign_init_data(fields: dict[str, str], bot_token: str) -> str:
    """Build a signed initData string (used by tests and local tooling)."""
    from urllib.parse import urlencode

    check_string = "\n".join(f"{k}={v}" for k, v in sorted(fields.items()))
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    digest = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
    return urlencode({**fields, "hash": digest})
