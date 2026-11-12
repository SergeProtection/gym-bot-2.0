"""Starlette JSON API for the Telegram Mini App, plus static files."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any, Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from gymbot import __version__
from gymbot.api import serializers as ser
from gymbot.api.auth import AuthError, validate_init_data
from gymbot.config import PROJECT_ROOT, Settings
from gymbot.db import Database, ExerciseKind, User
from gymbot.services import auth as auth_svc
from gymbot.services import catalog, plans, reminders, stats, users, workouts
from gymbot.services import dashboard as dashboard_svc
from gymbot.services import templates
from gymbot.services.plans import PlanError
from gymbot.services.reminders import ReminderError
from gymbot.utils import local_today, parse_date_token, parse_hhmm, parse_timezone

log = logging.getLogger(__name__)

WEBAPP_DIR = PROJECT_ROOT / "gymbot" / "webapp"
MAX_BODY = 64 * 1024


class ApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


# ---------------------------------------------------------------- input helpers


async def body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if len(raw) > MAX_BODY:
        raise ApiError(413, "Request body too large")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise ApiError(400, "Body must be JSON") from exc
    if not isinstance(data, dict):
        raise ApiError(400, "Body must be a JSON object")
    return data


def num(data: dict, key: str, *, kind=float, required: bool = False, lo: float | None = None, hi: float | None = None):
    value = data.get(key)
    if value is None or value == "":
        if required:
            raise ApiError(422, f"'{key}' is required")
        return None
    if isinstance(value, bool):
        raise ApiError(422, f"'{key}' must be a number")
    try:
        out = kind(value)
    except (TypeError, ValueError) as exc:
        raise ApiError(422, f"'{key}' must be a number") from exc
    if kind is int and isinstance(value, float) and not value.is_integer():
        raise ApiError(422, f"'{key}' must be a whole number")
    if (lo is not None and out < lo) or (hi is not None and out > hi):
        raise ApiError(422, f"'{key}' must be between {lo} and {hi}")
    return out


def text(data: dict, key: str, *, required: bool = True, max_len: int = 120) -> str | None:
    value = data.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        if required:
            raise ApiError(422, f"'{key}' is required")
        return None
    if not isinstance(value, str):
        raise ApiError(422, f"'{key}' must be a string")
    return value.strip()[:max_len]


def path_int(request: Request, key: str) -> int:
    try:
        return int(request.path_params[key])
    except (KeyError, ValueError) as exc:
        raise ApiError(404, "Not found") from exc


def query_int(request: Request, key: str, default: int, lo: int, hi: int) -> int:
    raw = request.query_params.get(key)
    if raw is None:
        return default
    try:
        return max(lo, min(hi, int(raw)))
    except ValueError as exc:
        raise ApiError(422, f"'{key}' must be a whole number") from exc


# ---------------------------------------------------------------- auth / handler wrapper

Handler = Callable[[Request, AsyncSession, User], Awaitable[Any]]


@dataclass(frozen=True)
class Identity:
    user_id: int
    first_name: str | None = None
    username: str | None = None
    session_version: int | None = None  # set for standalone-app sessions


def _authenticate(request: Request) -> Identity:
    settings: Settings = request.app.state.settings
    header = request.headers.get("authorization", "")
    scheme, _, credentials = header.partition(" ")
    if scheme.lower() == "tma":
        try:
            tg = validate_init_data(credentials.strip(), settings.telegram_bot_token)
        except AuthError as exc:
            raise ApiError(401, f"Unauthorized: {exc}") from exc
        return Identity(tg.id, tg.first_name, tg.username)
    if scheme.lower() == "bearer":
        found = auth_svc.read_session(credentials.strip(), request.app.state.session_secret)
        if found is None:
            raise ApiError(401, "Unauthorized: your sign-in expired, send /login to the bot")
        return Identity(found[0], session_version=found[1])
    if settings.dev_user_id:
        return Identity(settings.dev_user_id, "Dev")
    raise ApiError(401, "Unauthorized: open this page from the Telegram bot")


def api(handler: Handler) -> Callable[[Request], Awaitable[Response]]:
    async def endpoint(request: Request) -> Response:
        ident = _authenticate(request)
        db: Database = request.app.state.db
        settings: Settings = request.app.state.settings
        async with db.session() as session:
            if ident.session_version is None:
                user = await users.upsert_user(
                    session,
                    ident.user_id,
                    first_name=ident.first_name,
                    username=ident.username,
                    default_rest=settings.default_rest_seconds,
                )
            else:
                user = await users.get_user(session, ident.user_id)
                if user is None or user.session_version != ident.session_version:
                    raise ApiError(401, "Unauthorized: you were signed out, send /login to the bot")
            result = await handler(request, session, user)
        if isinstance(result, Response):
            return result
        return JSONResponse(result)

    endpoint.__name__ = handler.__name__
    return endpoint


# ---------------------------------------------------------------- standalone-app sign-in


async def public_config(request: Request) -> Response:
    username = request.app.state.bot_username
    return JSONResponse({"bot_username": username, "bot_url": f"https://t.me/{username}?start=login" if username else None})


async def redeem_code(request: Request) -> Response:
    limiter: auth_svc.FailureLimiter = request.app.state.login_limiter
    if limiter.blocked():
        raise ApiError(429, "Too many wrong codes. Wait a few minutes and try again.")
    code = text(await body(request), "code", max_len=20)
    db: Database = request.app.state.db
    async with db.session() as session:
        user = await auth_svc.redeem_login_code(session, code)
        if user is None:
            limiter.record_failure()
            raise ApiError(400, "That code is wrong or expired. Send /login to the bot for a new one.")
        token = auth_svc.issue_session(user, request.app.state.session_secret)
        data = {"token": token, "user": ser.user(user)}
    return JSONResponse(data)


async def sign_out_everywhere(request: Request, s: AsyncSession, user: User):
    user.session_version += 1
    await s.flush()
    return {"ok": True}


# ---------------------------------------------------------------- profile


async def me(request: Request, s: AsyncSession, user: User):
    active = await workouts.get_active(s, user.id)
    return {
        "user": ser.user(user),
        "streak_weeks": await stats.week_streak(s, user.id, user.timezone),
        "total_workouts": await stats.count_completed(s, user.id),
        "active_workout": ser.active_workout(workouts.summarize(active)) if active else None,
    }


async def dashboard(request: Request, s: AsyncSession, user: User):
    d = await dashboard_svc.build(s, user)
    covers = catalog.group_covers(str(request.app.state.settings.exercise_dir))
    bw = d.body_weight
    return {
        "user": ser.user(user),
        "today": d.today.isoformat(),
        "week_days": d.week_days,
        "week": ser.period(d.week),
        "streak_weeks": d.streak_weeks,
        "total_workouts": d.total_workouts,
        "last_workout": ser.summary(d.last) if d.last else None,
        "body_weight": {"weight_kg": bw.weight_kg, "measured_at": ser.iso(bw.measured_at), "change_kg": d.body_weight_change} if bw else None,
        "next": {
            "group": d.next_group,
            "image": ser.media_url(covers.get(d.next_group)),
            "plan": ser.plan(d.next_plan) if d.next_plan else None,
            "rotation": dashboard_svc.ROTATION,
        },
        "plans": [ser.plan(p) for p in d.plans if p.items],
    }


async def update_me(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    if "timezone" in data:
        tz = parse_timezone(text(data, "timezone", max_len=64))
        if tz is None:
            raise ApiError(422, "Unknown timezone (try Europe/Berlin or +3)")
        await users.set_timezone(s, user, tz)
    if "rest_seconds" in data:
        await users.set_rest(s, user, num(data, "rest_seconds", kind=int, required=True, lo=1, hi=3600))
    return ser.user(user)


# ---------------------------------------------------------------- exercises


async def groups(request: Request, s: AsyncSession, user: User):
    return {"groups": await catalog.list_groups(s, user.id)}


async def exercises(request: Request, s: AsyncSession, user: User):
    group = request.query_params.get("group") or None
    return {"exercises": [ser.exercise(e) for e in await catalog.list_exercises(s, user.id, group)]}


async def create_exercise(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    kind = data.get("kind", ExerciseKind.STRENGTH.value)
    if kind not in {k.value for k in ExerciseKind}:
        raise ApiError(422, "'kind' must be strength, time or cardio")
    ex = await catalog.add_custom_exercise(s, user.id, text(data, "name"), text(data, "group", max_len=40), kind)
    return JSONResponse(ser.exercise(ex), status_code=201)


async def _exercise_or_404(s: AsyncSession, user: User, exercise_id: int | None):
    ex = await catalog.get_exercise(s, user.id, exercise_id) if exercise_id else None
    if ex is None:
        raise ApiError(404, "Exercise not found")
    return ex


async def exercise_last(request: Request, s: AsyncSession, user: User):
    ex = await _exercise_or_404(s, user, path_int(request, "exercise_id"))
    active = await workouts.get_active(s, user.id)
    last = await workouts.last_time_sets(s, user.id, ex.id, active.id if active else None)
    records = await stats.personal_records(s, user.id, ex.id)
    return {
        "exercise": ser.exercise(ex),
        "last_time": [ser.workout_set(x) for x in last],
        "record": ser.record(records[0]) if records else None,  # best before today's workout
    }


# ---------------------------------------------------------------- active workout


async def _active_or_404(s: AsyncSession, user: User):
    w = await workouts.get_active(s, user.id)
    if w is None:
        raise ApiError(404, "No active workout")
    return w


def _active_json(w) -> dict:
    return ser.active_workout(workouts.summarize(w))


async def get_active(request: Request, s: AsyncSession, user: User):
    w = await workouts.get_active(s, user.id)
    return {"workout": _active_json(w) if w else None}


async def start_workout(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    plan_id = num(data, "plan_id", kind=int)
    if plan_id is not None and await plans.get_plan(s, user.id, plan_id) is None:
        raise ApiError(404, "Plan not found")
    w, created = await workouts.start(s, user.id, plan_id)
    return JSONResponse({"workout": _active_json(w), "created": created}, status_code=201 if created else 200)


async def add_set(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    w = await _active_or_404(s, user)
    ex = await _exercise_or_404(s, user, num(data, "exercise_id", kind=int, required=True))
    try:
        new_set, prs = await workouts.log_set(
            s,
            w,
            ex,
            reps=num(data, "reps", kind=int, lo=1, hi=1000),
            weight=num(data, "weight_kg", lo=0, hi=1000),
            duration=num(data, "duration_s", kind=int, lo=1, hi=24 * 3600),
            distance=num(data, "distance_km", lo=0.01, hi=1000),
        )
    except ValueError as exc:
        raise ApiError(422, str(exc)) from exc
    return JSONResponse(
        {"set": ser.workout_set(new_set), "prs": prs, "workout": _active_json(w), "rest_seconds": user.rest_seconds},
        status_code=201,
    )


async def undo_set(request: Request, s: AsyncSession, user: User):
    w = await _active_or_404(s, user)
    removed = await workouts.undo_last_set(s, w)
    if removed is None:
        raise ApiError(404, "No sets to undo")
    return {"removed_id": removed.id, "workout": _active_json(w)}


async def finish_workout(request: Request, s: AsyncSession, user: User):
    w = await _active_or_404(s, user)
    sm = await workouts.finish(s, w)
    return {"summary": ser.summary(sm) if sm else None}


async def cancel_workout(request: Request, s: AsyncSession, user: User):
    await workouts.cancel(s, await _active_or_404(s, user))
    return {"ok": True}


# ---------------------------------------------------------------- history & stats


async def history(request: Request, s: AsyncSession, user: User):
    limit = query_int(request, "limit", 10, 1, 50)
    offset = query_int(request, "offset", 0, 0, 100_000)
    items = await stats.history(s, user.id, limit=limit, offset=offset)
    return {"workouts": [ser.summary(x) for x in items], "total": await stats.count_completed(s, user.id)}


async def delete_workout(request: Request, s: AsyncSession, user: User):
    if not await workouts.delete_workout(s, user.id, path_int(request, "workout_id")):
        raise ApiError(404, "Workout not found")
    return {"ok": True}


async def save_workout_as_plan(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    w = await workouts.load(s, user.id, path_int(request, "workout_id"))
    if w is None or not w.sets:
        raise ApiError(404, "Workout not found")
    try:
        p = await plans.plan_from_workout(s, user.id, w, text(data, "name", max_len=80))
    except PlanError as exc:
        raise ApiError(422, str(exc)) from exc
    return JSONResponse(ser.plan(p), status_code=201)


def _period_bounds(request: Request, tz: str) -> tuple[date, date]:
    q = request.query_params
    today = local_today(tz)
    period = q.get("period", "week")
    if period == "today":
        return today, today
    if period == "week":
        return stats.week_bounds(today)
    if period == "month":
        return stats.month_bounds(today.year, today.month)
    if period == "custom":
        first = parse_date_token(q.get("from", ""), today.year)
        last = parse_date_token(q.get("to", ""), today.year)
        if not first or not last:
            raise ApiError(422, "'from' and 'to' must be dates like 2026-10-01")
        if first > last:
            first, last = last, first
        if (last - first).days > 3660:
            raise ApiError(422, "Period too long")
        return first, last
    raise ApiError(422, "'period' must be today, week, month or custom")


async def summary(request: Request, s: AsyncSession, user: User):
    first, last = _period_bounds(request, user.timezone)
    return ser.period(await stats.period_summary(s, user.id, user.timezone, first, last))


async def weekly(request: Request, s: AsyncSession, user: User):
    weeks = query_int(request, "weeks", 12, 1, 104)
    return {"weeks": [ser.week(w) for w in await stats.weekly_series(s, user.id, user.timezone, weeks)]}


async def records(request: Request, s: AsyncSession, user: User):
    return {"records": [ser.record(r) for r in await stats.personal_records(s, user.id)]}


async def exercise_progress(request: Request, s: AsyncSession, user: User):
    ex = await _exercise_or_404(s, user, path_int(request, "exercise_id"))
    prog = await stats.exercise_progress(s, user.id, ex, user.timezone)
    return ser.progress(prog, stats.METRIC_LABELS)


async def body_weight(request: Request, s: AsyncSession, user: User):
    rows = await users.body_weights(s, user.id, limit=query_int(request, "limit", 90, 1, 1000))
    return {"entries": [{"id": r.id, "weight_kg": r.weight_kg, "measured_at": ser.iso(r.measured_at)} for r in rows]}


async def add_body_weight(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    entry = await users.log_body_weight(s, user.id, num(data, "weight_kg", required=True, lo=20, hi=400))
    return JSONResponse({"id": entry.id, "weight_kg": entry.weight_kg, "measured_at": ser.iso(entry.measured_at)}, status_code=201)


# ---------------------------------------------------------------- plans


async def _plan_or_404(request: Request, s: AsyncSession, user: User):
    p = await plans.get_plan(s, user.id, path_int(request, "plan_id"))
    if p is None:
        raise ApiError(404, "Plan not found")
    return p


async def list_plans(request: Request, s: AsyncSession, user: User):
    return {"plans": [ser.plan(p) for p in await plans.list_plans(s, user.id)]}


async def list_templates(request: Request, s: AsyncSession, user: User):
    have = await templates.existing_names(s, user.id)
    return {
        "templates": [
            {
                "key": t.key,
                "name": t.name,
                "description": t.description,
                "exercises": [i.exercise for i in t.items],
                "added": t.name.lower() in have,
            }
            for t in templates.TEMPLATES
        ]
    }


async def add_templates(request: Request, s: AsyncSession, user: User):
    keys = (await body(request)).get("keys")
    if not isinstance(keys, list) or not keys or not all(isinstance(k, str) for k in keys):
        raise ApiError(422, "'keys' must be a list of template keys")
    try:
        created = await templates.add_templates(s, user.id, keys)
    except PlanError as exc:
        raise ApiError(422, str(exc)) from exc
    return JSONResponse({"created": [ser.plan(p) for p in created]}, status_code=201)


async def delete_my_data(request: Request, s: AsyncSession, user: User):
    confirm = str((await body(request)).get("confirm", "")).strip().lower()
    if confirm != users.DELETE_CONFIRMATION:
        raise ApiError(422, f"Type '{users.DELETE_CONFIRMATION}' to confirm")
    counts = await users.delete_all_data(s, user.id)
    log.info("User %s deleted all their data: %s", user.id, counts)
    return {"deleted": counts}


async def create_plan(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    try:
        p = await plans.create_plan(s, user.id, text(data, "name", max_len=80))
    except PlanError as exc:
        raise ApiError(422, str(exc)) from exc
    return JSONResponse(ser.plan(p), status_code=201)


async def get_plan(request: Request, s: AsyncSession, user: User):
    return ser.plan(await _plan_or_404(request, s, user))


async def rename_plan(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    p = await _plan_or_404(request, s, user)
    try:
        await plans.rename_plan(s, p, text(data, "name", max_len=80))
    except PlanError as exc:
        raise ApiError(422, str(exc)) from exc
    return ser.plan(p)


async def delete_plan(request: Request, s: AsyncSession, user: User):
    if not await plans.delete_plan(s, user.id, path_int(request, "plan_id")):
        raise ApiError(404, "Plan not found")
    return {"ok": True}


async def add_plan_item(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    p = await _plan_or_404(request, s, user)
    ex = await _exercise_or_404(s, user, num(data, "exercise_id", kind=int, required=True))
    try:
        await plans.add_item(
            s,
            p,
            ex,
            target_sets=num(data, "target_sets", kind=int, lo=1, hi=20) or 3,
            target_reps=num(data, "target_reps", kind=int, lo=1, hi=1000),
        )
    except PlanError as exc:
        raise ApiError(422, str(exc)) from exc
    return JSONResponse(ser.plan(p), status_code=201)


async def update_plan_item(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    p = await _plan_or_404(request, s, user)
    item_id = path_int(request, "item_id")
    try:
        if "move" in data:
            await plans.move_item(s, p, item_id, num(data, "move", kind=int, required=True, lo=-50, hi=50))
        await plans.update_item(
            s,
            p,
            item_id,
            target_sets=num(data, "target_sets", kind=int, lo=1, hi=20),
            # 0 clears the rep target
            target_reps=num(data, "target_reps", kind=int, lo=0, hi=1000),
        )
    except PlanError as exc:
        raise ApiError(404, str(exc)) from exc
    return ser.plan(p)


async def delete_plan_item(request: Request, s: AsyncSession, user: User):
    p = await _plan_or_404(request, s, user)
    try:
        await plans.remove_item(s, p, path_int(request, "item_id"))
    except PlanError as exc:
        raise ApiError(404, str(exc)) from exc
    return ser.plan(p)


# ---------------------------------------------------------------- reminders


def _weekdays(data: dict) -> list[int]:
    value = data.get("weekdays")
    if isinstance(value, str):
        days = reminders.parse_weekdays(value)
    elif isinstance(value, list) and all(isinstance(d, int) and not isinstance(d, bool) and 0 <= d <= 6 for d in value):
        days = sorted(set(value))
    else:
        days = None
    if not days:
        raise ApiError(422, "'weekdays' must be a list of 0-6 (Mon=0) or text like 'mon wed fri'")
    return days


async def list_reminders(request: Request, s: AsyncSession, user: User):
    return {"reminders": [ser.reminder(r) for r in await reminders.list_reminders(s, user.id)], "timezone": user.timezone}


async def create_reminder(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    try:
        r = await reminders.create_reminder(s, user.id, _weekdays(data), text(data, "at_time", max_len=5))
    except ReminderError as exc:
        raise ApiError(422, str(exc)) from exc
    return JSONResponse(ser.reminder(r), status_code=201)


async def update_reminder(request: Request, s: AsyncSession, user: User):
    data = await body(request)
    r = await reminders.get_reminder(s, user.id, path_int(request, "reminder_id"))
    if r is None:
        raise ApiError(404, "Reminder not found")
    if "enabled" in data:
        if not isinstance(data["enabled"], bool):
            raise ApiError(422, "'enabled' must be true or false")
        await reminders.set_enabled(s, r, data["enabled"])
    if "weekdays" in data:
        r.weekdays = ",".join(map(str, _weekdays(data)))
    if "at_time" in data:
        hhmm = parse_hhmm(text(data, "at_time", max_len=5))
        if hhmm is None:
            raise ApiError(422, "Time must look like 18:30")
        r.at_time = hhmm
    if "weekdays" in data or "at_time" in data:
        r.last_sent_on = None
    await s.flush()
    return ser.reminder(r)


async def delete_reminder(request: Request, s: AsyncSession, user: User):
    if not await reminders.delete_reminder(s, user.id, path_int(request, "reminder_id")):
        raise ApiError(404, "Reminder not found")
    return {"ok": True}


# ---------------------------------------------------------------- app


async def root(request: Request) -> Response:
    return JSONResponse({"name": "GymBot", "version": __version__, "app": "/app/", "health": "/health"})


async def health(request: Request) -> Response:
    return JSONResponse({"ok": True})


async def _api_error(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, ApiError)
    return JSONResponse({"error": exc.message}, status_code=exc.status)


async def _http_error(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, HTTPException)
    return JSONResponse({"error": exc.detail}, status_code=exc.status_code)


async def _server_error(request: Request, exc: Exception) -> Response:
    log.exception("Unhandled API error", exc_info=exc)
    return JSONResponse({"error": "Internal server error"}, status_code=500)


class NoCacheStatic(StaticFiles):
    """Mini App assets must not be cached by Telegram's webview between deploys."""

    async def get_response(self, path: str, scope) -> Response:
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-cache"
        return response


def routes(settings: Settings) -> list:
    r = [
        Route("/", root),
        Route("/health", health),
        Route("/app", lambda request: RedirectResponse("/app/")),
        Route("/api/public/config", public_config, methods=["GET"]),
        Route("/api/auth/redeem", redeem_code, methods=["POST"]),
        Route("/api/auth/signout-all", api(sign_out_everywhere), methods=["POST"]),
        Route("/api/me", api(me), methods=["GET"]),
        Route("/api/dashboard", api(dashboard), methods=["GET"]),
        Route("/api/me", api(update_me), methods=["PATCH"]),
        Route("/api/groups", api(groups), methods=["GET"]),
        Route("/api/exercises", api(exercises), methods=["GET"]),
        Route("/api/exercises", api(create_exercise), methods=["POST"]),
        Route("/api/exercises/{exercise_id}/last", api(exercise_last), methods=["GET"]),
        Route("/api/exercises/{exercise_id}/progress", api(exercise_progress), methods=["GET"]),
        Route("/api/workout", api(get_active), methods=["GET"]),
        Route("/api/workout/start", api(start_workout), methods=["POST"]),
        Route("/api/workout/sets", api(add_set), methods=["POST"]),
        Route("/api/workout/sets/last", api(undo_set), methods=["DELETE"]),
        Route("/api/workout/finish", api(finish_workout), methods=["POST"]),
        Route("/api/workout/cancel", api(cancel_workout), methods=["POST"]),
        Route("/api/history", api(history), methods=["GET"]),
        Route("/api/workouts/{workout_id}", api(delete_workout), methods=["DELETE"]),
        Route("/api/workouts/{workout_id}/plan", api(save_workout_as_plan), methods=["POST"]),
        Route("/api/stats/summary", api(summary), methods=["GET"]),
        Route("/api/stats/weekly", api(weekly), methods=["GET"]),
        Route("/api/stats/records", api(records), methods=["GET"]),
        Route("/api/bodyweight", api(body_weight), methods=["GET"]),
        Route("/api/bodyweight", api(add_body_weight), methods=["POST"]),
        Route("/api/plans/templates", api(list_templates), methods=["GET"]),
        Route("/api/plans/templates", api(add_templates), methods=["POST"]),
        Route("/api/me/data", api(delete_my_data), methods=["DELETE"]),
        Route("/api/plans", api(list_plans), methods=["GET"]),
        Route("/api/plans", api(create_plan), methods=["POST"]),
        Route("/api/plans/{plan_id}", api(get_plan), methods=["GET"]),
        Route("/api/plans/{plan_id}", api(rename_plan), methods=["PATCH"]),
        Route("/api/plans/{plan_id}", api(delete_plan), methods=["DELETE"]),
        Route("/api/plans/{plan_id}/items", api(add_plan_item), methods=["POST"]),
        Route("/api/plans/{plan_id}/items/{item_id}", api(update_plan_item), methods=["PATCH"]),
        Route("/api/plans/{plan_id}/items/{item_id}", api(delete_plan_item), methods=["DELETE"]),
        Route("/api/reminders", api(list_reminders), methods=["GET"]),
        Route("/api/reminders", api(create_reminder), methods=["POST"]),
        Route("/api/reminders/{reminder_id}", api(update_reminder), methods=["PATCH"]),
        Route("/api/reminders/{reminder_id}", api(delete_reminder), methods=["DELETE"]),
        Mount("/app", app=NoCacheStatic(directory=WEBAPP_DIR, html=True, check_dir=False), name="app"),
    ]
    if settings.exercise_dir.is_dir():
        r.append(Mount("/media", app=StaticFiles(directory=settings.exercise_dir), name="media"))
    return r


def create_app(settings: Settings, db: Database, lifespan=None) -> Starlette:
    @asynccontextmanager
    async def _lifespan(app: Starlette):
        if lifespan is None:
            yield
        else:
            async with lifespan(app):
                yield

    app = Starlette(
        routes=routes(settings),
        exception_handlers={ApiError: _api_error, HTTPException: _http_error, Exception: _server_error},
        lifespan=_lifespan,
    )
    app.state.settings = settings
    app.state.db = db
    app.state.session_secret = auth_svc.session_secret(settings.telegram_bot_token, settings.session_secret)
    app.state.login_limiter = auth_svc.FailureLimiter()
    app.state.bot_username = None  # filled in once the bot has started
    return app
