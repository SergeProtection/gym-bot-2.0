"""Message text for the Telegram bot (HTML parse mode)."""

from __future__ import annotations

from html import escape

from gymbot.db.models import Exercise, ExerciseKind, WorkoutSet
from gymbot.services.stats import ExerciseRecord, PeriodSummary
from gymbot.services.workouts import WorkoutSummary, describe_set
from gymbot.utils import fmt_clock, fmt_num, fmt_span, fmt_weight, to_local, tz_label

HELP = """<b>GymBot</b> — log workouts, track PRs and progress.

<b>Training</b>
/workout — start or continue a workout
/plans — start from a plan, or manage plans
/finish — finish the current workout
/cancel — discard the current workout

<b>Logging a set</b> (after picking an exercise, just type it)
• weights: <code>80x8</code>, <code>82.5 6</code>, bodyweight <code>x12</code>, same weight <code>10</code>
• holds (plank): <code>1:30</code>, <code>45s</code>, <code>2m</code>
• cardio: <code>5.2 28:30</code> (km, time)

<b>History &amp; stats</b>
/last — your last workout
/history — recent workouts
/today, /week, /month — summaries
/period 01.10-15.10 — any date range
/pr — personal records
/weight 82.5 — log body weight

<b>Settings</b>
/rest 90 — rest timer in seconds
/timezone Europe/Berlin (or +3)
/remind — training reminders
/app — open the Mini App (charts, plans, everything)"""


def input_hint(ex: Exercise) -> str:
    if ex.kind == ExerciseKind.TIME.value:
        return "Type the hold time, e.g. <code>1:30</code> or <code>45s</code>."
    if ex.kind == ExerciseKind.CARDIO.value:
        return "Type distance and time, e.g. <code>5.2 28:30</code>."
    return "Type weight × reps, e.g. <code>80x8</code> (bodyweight: <code>x12</code>)."


def exercise_card(ex: Exercise, done: list[WorkoutSet], last_time: list[WorkoutSet], target: str | None = None) -> str:
    lines = [f"<b>{escape(ex.name)}</b> · {escape(ex.muscle_group)}"]
    if target:
        lines.append(f"🎯 {target}")
    if last_time:
        lines.append("Last time: " + ", ".join(describe_set(s, ex.kind) for s in last_time))
    if done:
        lines.append("Today: " + ", ".join(describe_set(s, ex.kind) for s in done))
    lines.append("")
    lines.append(input_hint(ex))
    return "\n".join(lines)


def set_logged(ex: Exercise, s: WorkoutSet, number: int, prs: list[str], rest_s: int) -> str:
    lines = [f"✅ Set {number}: <b>{describe_set(s, ex.kind)}</b> — {escape(ex.name)}"]
    for p in prs:
        lines.append(f"🏆 <b>New PR!</b> {escape(p)}")
    lines.append(f"⏱ Rest {fmt_clock(rest_s)} — I'll ping you. Type the next set any time.")
    return "\n".join(lines)


def workout_summary(sm: WorkoutSummary, tz: str, title: str = "Workout complete 💪") -> str:
    w = sm.workout
    start = to_local(w.started_at, tz)
    lines = [f"<b>{title}</b>", f"{start:%a %d %b %Y, %H:%M} · {fmt_span(sm.duration_s)}"]
    if sm.groups:
        lines.append(" · ".join(escape(g) for g in sm.groups))
    lines.append("")
    for block in sm.blocks:
        sets = ", ".join(describe_set(s, block.exercise.kind) for s in block.sets)
        lines.append(f"<b>{escape(block.exercise.name)}</b>: {sets}")
    lines.append("")
    stats = [f"{sm.set_count} sets"]
    if sm.volume_kg:
        stats.append(f"{fmt_num(sm.volume_kg, 0)} kg volume")
    if sm.cardio_km:
        stats.append(f"{fmt_num(sm.cardio_km)} km")
    if sm.pr_count:
        stats.append(f"{sm.pr_count} PR{'s' if sm.pr_count > 1 else ''} 🏆")
    lines.append(" · ".join(stats))
    return "\n".join(lines)


def history_line(sm: WorkoutSummary, tz: str) -> str:
    start = to_local(sm.workout.started_at, tz)
    groups = ", ".join(sm.groups[:3]) or "—"
    extra = f" · {sm.pr_count}🏆" if sm.pr_count else ""
    return f"<b>{start:%d %b}</b> {escape(groups)} — {sm.set_count} sets, {fmt_span(sm.duration_s)}{extra}"


def period(p: PeriodSummary, title: str) -> str:
    span = p.first.strftime("%d %b") if p.first == p.last else f"{p.first:%d %b} – {p.last:%d %b %Y}"
    lines = [f"<b>{title}</b> · {span}"]
    if not p.workouts:
        lines.append("No workouts in this period.")
        return "\n".join(lines)
    lines += [
        f"🏋️ {p.workouts} workout{'s' if p.workouts != 1 else ''} on {p.active_days} day{'s' if p.active_days != 1 else ''}"
        f" · {fmt_span(p.training_s)}",
        f"🔢 {p.sets} sets · {fmt_num(p.volume_kg, 0)} kg volume",
    ]
    if p.cardio_km:
        lines.append(f"🏃 {fmt_num(p.cardio_km)} km in {fmt_clock(p.cardio_s)}")
    if p.hold_s:
        lines.append(f"🧘 {fmt_clock(p.hold_s)} of holds")
    if p.prs:
        lines.append(f"🏆 {p.prs} PR{'s' if p.prs != 1 else ''}")
    lines.append("")
    for group, totals in p.groups_sorted:
        vol = f", {fmt_num(totals.volume_kg, 0)} kg" if totals.volume_kg else ""
        lines.append(f"• {escape(group)}: {totals.sets} sets{vol}")
    return "\n".join(lines)


def record_line(r: ExerciseRecord) -> str:
    kind = r.exercise.kind
    if kind == ExerciseKind.CARDIO.value:
        parts = []
        if r.best_distance_km:
            parts.append(f"{fmt_num(r.best_distance_km)} km")
        if r.best_pace_s:
            parts.append(f"{fmt_clock(int(r.best_pace_s))} /km")
        best = " · ".join(parts)
    elif kind == ExerciseKind.TIME.value:
        best = fmt_clock(r.best_hold_s)
    elif r.best_weight:
        best = f"{fmt_weight(r.best_weight)} × {r.best_weight_reps} (e1RM {fmt_num(r.best_e1rm, 1)} kg)"
    else:
        best = f"{r.best_reps} reps"
    return f"<b>{escape(r.exercise.name)}</b>: {best}"


def settings_text(tz: str, rest_s: int) -> str:
    return f"Timezone: <b>{escape(tz_label(tz))}</b>\nRest timer: <b>{fmt_clock(rest_s)}</b>"
