"""Training reminders: weekdays + local time, sent at most once per local day."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from gymbot.db.models import Reminder, User
from gymbot.services.stats import trained_on
from gymbot.utils import get_zone, parse_hhmm

MAX_REMINDERS = 10
# How long after the scheduled time a missed reminder (e.g. after a restart) is still sent.
GRACE = timedelta(minutes=30)

DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_DAY_KEYS = {
    "mo": 0, "mon": 0, "monday": 0,
    "tu": 1, "tue": 1, "tues": 1, "tuesday": 1,
    "we": 2, "wed": 2, "wednesday": 2,
    "th": 3, "thu": 3, "thur": 3, "thurs": 3, "thursday": 3,
    "fr": 4, "fri": 4, "friday": 4,
    "sa": 5, "sat": 5, "saturday": 5,
    "su": 6, "sun": 6, "sunday": 6,
}


class ReminderError(ValueError):
    pass


def parse_weekdays(text: str) -> list[int] | None:
    """'daily' / 'every day', 'weekdays', 'weekend', 'mon wed fri', 'mon-fri', 'tue,thu'."""
    t = text.strip().lower()
    if t in {"daily", "every day", "everyday", "all"}:
        return list(range(7))
    if t in {"weekdays", "workdays"}:
        return list(range(5))
    if t in {"weekend", "weekends"}:
        return [5, 6]
    days: set[int] = set()
    for token in re.split(r"[\s,;]+", t):
        if not token:
            continue
        if "-" in token:
            a, _, b = token.partition("-")
            if a not in _DAY_KEYS or b not in _DAY_KEYS:
                return None
            i, j = _DAY_KEYS[a], _DAY_KEYS[b]
            span = range(i, j + 1) if i <= j else [*range(i, 7), *range(0, j + 1)]
            days.update(span)
        elif token in _DAY_KEYS:
            days.add(_DAY_KEYS[token])
        else:
            return None
    return sorted(days) or None


def days_of(reminder: Reminder) -> list[int]:
    return [int(d) for d in reminder.weekdays.split(",") if d.strip().isdigit()]


def describe_days(days: list[int]) -> str:
    days = sorted(set(days))
    if days == list(range(7)):
        return "every day"
    if days == list(range(5)):
        return "weekdays"
    if days == [5, 6]:
        return "weekends"
    return ", ".join(DAY_NAMES[d] for d in days)


def describe(reminder: Reminder) -> str:
    return f"{describe_days(days_of(reminder))} at {reminder.at_time}"


async def list_reminders(session: AsyncSession, user_id: int) -> list[Reminder]:
    rows = await session.scalars(select(Reminder).where(Reminder.user_id == user_id).order_by(Reminder.at_time))
    return list(rows.all())


async def get_reminder(session: AsyncSession, user_id: int, reminder_id: int) -> Reminder | None:
    r = await session.get(Reminder, reminder_id)
    return r if r is not None and r.user_id == user_id else None


async def create_reminder(session: AsyncSession, user_id: int, days: list[int], at_time: str) -> Reminder:
    hhmm = parse_hhmm(at_time)
    if hhmm is None:
        raise ReminderError("Time must look like 18:30")
    days = sorted({d for d in days if 0 <= d <= 6})
    if not days:
        raise ReminderError("Pick at least one day")
    count = await session.scalar(select(func.count(Reminder.id)).where(Reminder.user_id == user_id))
    if (count or 0) >= MAX_REMINDERS:
        raise ReminderError(f"You can have at most {MAX_REMINDERS} reminders")
    r = Reminder(user_id=user_id, weekdays=",".join(map(str, days)), at_time=hhmm, enabled=True)
    session.add(r)
    await session.flush()
    return r


async def set_enabled(session: AsyncSession, reminder: Reminder, enabled: bool) -> Reminder:
    reminder.enabled = enabled
    await session.flush()
    return reminder


async def delete_reminder(session: AsyncSession, user_id: int, reminder_id: int) -> bool:
    r = await get_reminder(session, user_id, reminder_id)
    if r is None:
        return False
    await session.delete(r)
    await session.flush()
    return True


def scheduled_today(reminder: Reminder, local_now: datetime) -> datetime | None:
    """The reminder's local fire time for today, or None if today isn't one of its days."""
    if local_now.weekday() not in days_of(reminder):
        return None
    h, m = (int(p) for p in reminder.at_time.split(":"))
    return local_now.replace(hour=h, minute=m, second=0, microsecond=0)


def is_due(reminder: Reminder, local_now: datetime) -> bool:
    if not reminder.enabled or reminder.last_sent_on == local_now.date():
        return False
    at = scheduled_today(reminder, local_now)
    return at is not None and at <= local_now < at + GRACE


@dataclass
class DueReminder:
    reminder: Reminder
    user: User
    already_trained: bool


async def collect_due(session: AsyncSession, now_utc: datetime) -> list[DueReminder]:
    """Find reminders due now and mark them sent for today.

    Callers should skip sending when `already_trained` is True; it is still marked so it
    doesn't fire later the same day.
    """
    rows = await session.execute(select(Reminder, User).join(User, User.id == Reminder.user_id).where(Reminder.enabled.is_(True)))
    due: list[DueReminder] = []
    for reminder, user in rows.all():
        local_now = now_utc.astimezone(get_zone(user.timezone))
        if not is_due(reminder, local_now):
            continue
        reminder.last_sent_on = local_now.date()
        trained = await trained_on(session, user.id, user.timezone, local_now.date())
        due.append(DueReminder(reminder, user, trained))
    await session.flush()
    return due
