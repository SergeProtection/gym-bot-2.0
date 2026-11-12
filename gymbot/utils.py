"""Parsing and formatting helpers shared by the bot and the API."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_NUM = r"(\d+(?:[.,]\d+)?)"


def _num(value: str) -> float:
    return float(value.replace(",", "."))


def fmt_num(value: float | None, digits: int = 2) -> str:
    """80.0 -> '80', 82.5 -> '82.5', 1.234 -> '1.23'."""
    if value is None:
        return "-"
    text = f"{value:.{digits}f}".rstrip("0").rstrip(".")
    return text or "0"


def fmt_weight(kg: float | None) -> str:
    if not kg:
        return "BW"
    return f"{fmt_num(kg)} kg"


def fmt_clock(seconds: int | None) -> str:
    """90 -> '1:30', 3725 -> '1:02:05'."""
    if seconds is None:
        return "-"
    seconds = int(round(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def fmt_span(seconds: float) -> str:
    """Human workout length: '47 min', '1 h 05 min'."""
    minutes = int(round(seconds / 60))
    if minutes < 60:
        return f"{minutes} min"
    return f"{minutes // 60} h {minutes % 60:02d} min"


def fmt_pace(duration_s: int | None, distance_km: float | None) -> str:
    if not duration_s or not distance_km:
        return "-"
    return f"{fmt_clock(int(duration_s / distance_km))} /km"


def epley_1rm(weight: float | None, reps: int | None) -> float:
    """Estimated one-rep max. Zero for bodyweight or empty sets."""
    if not weight or not reps:
        return 0.0
    if reps == 1:
        return float(weight)
    return float(weight) * (1 + reps / 30.0)


def parse_duration(text: str) -> int | None:
    """'45' / '45s' -> 45, '1:30' -> 90, '2m' -> 120, '1m30s' -> 90, '1:02:03' -> 3723."""
    t = text.strip().lower().replace(" ", "")
    if not t:
        return None
    if re.fullmatch(r"\d+(:\d{1,2}){1,2}", t):
        parts = [int(p) for p in t.split(":")]
        if any(p >= 60 for p in parts[1:]):
            return None
        total = 0
        for p in parts:
            total = total * 60 + p
        return total or None
    m = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)(?:m|min))?(?:(\d+)(?:s|sec)?)?", t)
    if not m or not any(m.groups()):
        return None
    h, mi, s = (int(g) if g else 0 for g in m.groups())
    total = h * 3600 + mi * 60 + s
    return total or None


def parse_strength(text: str) -> tuple[float | None, int] | None:
    """Parse a strength set.

    '80x8', '80 x 8', '80*8', '82,5 8' -> (weight, reps)
    'x12', 'bw 12', 'bwx12'           -> (0.0, 12)   bodyweight
    '12'                              -> (None, 12)  keep current weight
    """
    t = text.strip().lower().replace("kg", "")
    m = re.fullmatch(rf"{_NUM}\s*(?:x|\*|×|\s)\s*(\d+)", t)
    if m:
        weight, reps = _num(m.group(1)), int(m.group(2))
        return (weight, reps) if reps > 0 and weight <= 1000 else None
    m = re.fullmatch(r"(?:bw)?\s*(?:x|\*|×)?\s*(\d+)", t)
    if m and (t.startswith(("x", "*", "×", "bw"))):
        reps = int(m.group(1))
        return (0.0, reps) if reps > 0 else None
    if re.fullmatch(r"\d+", t):
        reps = int(t)
        return (None, reps) if 0 < reps <= 1000 else None
    return None


def parse_cardio(text: str) -> tuple[float, int] | None:
    """'5.2 28:30', '5,2km 28:30', '5 km 30m' -> (distance_km, duration_s)."""
    t = text.strip().lower()
    m = re.fullmatch(rf"{_NUM}\s*(?:km)?\s+(.+)", t)
    if not m:
        return None
    distance = _num(m.group(1))
    duration = parse_duration(m.group(2))
    if duration is None or distance <= 0 or distance > 1000:
        return None
    return distance, duration


def parse_hhmm(text: str) -> str | None:
    m = re.fullmatch(r"\s*(\d{1,2})[:.](\d{2})\s*", text)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if h > 23 or mi > 59:
        return None
    return f"{h:02d}:{mi:02d}"


def parse_timezone(text: str) -> str | None:
    """Accept IANA names ('Europe/Berlin') or UTC offsets ('+3', 'UTC-5', '+5:30')."""
    t = text.strip()
    if not t:
        return None
    m = re.fullmatch(r"(?i)(?:utc|gmt)?\s*([+-])(\d{1,2})(?::?(\d{2}))?", t)
    if m:
        sign, hours, minutes = m.group(1), int(m.group(2)), int(m.group(3) or 0)
        if minutes == 0 and hours <= 14:
            # Etc/GMT zones use inverted signs: UTC+3 == Etc/GMT-3.
            return "UTC" if hours == 0 else f"Etc/GMT{'-' if sign == '+' else '+'}{hours}"
        return None
    if t.upper() in {"UTC", "GMT"}:
        return "UTC"
    try:
        ZoneInfo(t)
    except (ZoneInfoNotFoundError, ValueError):
        return None
    return t


def tz_label(name: str) -> str:
    """'Etc/GMT-3' -> 'UTC+3'."""
    m = re.fullmatch(r"Etc/GMT([+-])(\d+)", name)
    if m:
        return f"UTC{'-' if m.group(1) == '+' else '+'}{m.group(2)}"
    return name


def get_zone(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(name or "UTC")
    except (ZoneInfoNotFoundError, ValueError):
        return ZoneInfo("UTC")


def local_day_bounds(day: date, tz_name: str) -> tuple[datetime, datetime]:
    """[start, end) of a local calendar day, returned in UTC."""
    tz = get_zone(tz_name)
    start = datetime.combine(day, time.min, tzinfo=tz)
    end = start + timedelta(days=1)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def local_range(first: date, last: date, tz_name: str) -> tuple[datetime, datetime]:
    """UTC [start, end) covering local days first..last inclusive."""
    start, _ = local_day_bounds(first, tz_name)
    _, end = local_day_bounds(last, tz_name)
    return start, end


def local_today(tz_name: str) -> date:
    return datetime.now(get_zone(tz_name)).date()


def as_utc(dt: datetime) -> datetime:
    """SQLite hands back naive datetimes; everything we store is UTC."""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def to_local(dt: datetime, tz_name: str) -> datetime:
    return as_utc(dt).astimezone(get_zone(tz_name))


def parse_date_token(token: str, default_year: int) -> date | None:
    """'2026-10-01', '01.10.2026', '01.10', '1/10' (day first)."""
    token = token.strip()
    try:
        if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", token):
            y, m, d = (int(p) for p in token.split("-"))
            return date(y, m, d)
        m = re.fullmatch(r"(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?", token)
        if m:
            d, mo = int(m.group(1)), int(m.group(2))
            y = int(m.group(3)) if m.group(3) else default_year
            if y < 100:
                y += 2000
            return date(y, mo, d)
    except ValueError:
        return None
    return None


def parse_period(text: str, today: date) -> tuple[date, date] | None:
    """'01.10-15.10', '2026-10-01 2026-10-15', '01.10.2026 - 15.10.2026'."""
    parts = re.findall(r"\d{4}-\d{1,2}-\d{1,2}|\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?", text)
    if len(parts) != 2:
        return None
    first = parse_date_token(parts[0], today.year)
    last = parse_date_token(parts[1], today.year)
    if not first or not last:
        return None
    if first > last:
        first, last = last, first
    return first, last
