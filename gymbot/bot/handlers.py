"""Telegram bot: commands, inline-keyboard workout flow, rest timer, reminders."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from html import escape

from sqlalchemy.ext.asyncio import AsyncSession
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    Update,
    WebAppInfo,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from gymbot.bot import texts
from gymbot.config import Settings
from gymbot.db import Database, Exercise, ExerciseKind, User
from gymbot.services import catalog, plans, reminders, stats, users, workouts
from gymbot.services.plans import PlanError
from gymbot.services.reminders import ReminderError
from gymbot.utils import (
    fmt_clock,
    local_today,
    parse_cardio,
    parse_duration,
    parse_hhmm,
    parse_period,
    parse_strength,
    parse_timezone,
    tz_label,
)

log = logging.getLogger(__name__)

PAGE = 8  # exercises per page in the picker
HTML = ParseMode.HTML

COMMANDS = [
    ("workout", "Start or continue a workout"),
    ("plans", "Workout plans"),
    ("finish", "Finish the workout"),
    ("last", "Last workout"),
    ("history", "Recent workouts"),
    ("week", "This week's summary"),
    ("month", "This month's summary"),
    ("pr", "Personal records"),
    ("remind", "Training reminders"),
    ("app", "Open the Mini App"),
    ("help", "How to use the bot"),
]


# ---------------------------------------------------------------- plumbing


def _db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.application.bot_data["db"]


def _settings(context: ContextTypes.DEFAULT_TYPE) -> Settings:
    return context.application.bot_data["settings"]


@asynccontextmanager
async def user_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """DB session plus the (upserted) current user."""
    tg = update.effective_user
    async with _db(context).session() as session:
        user = await users.upsert_user(
            session,
            tg.id,
            chat_id=update.effective_chat.id if update.effective_chat else None,
            username=tg.username,
            first_name=tg.first_name,
            default_rest=_settings(context).default_rest_seconds,
        )
        yield session, user


async def reply(update: Update, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
    await update.effective_message.reply_text(text, parse_mode=HTML, reply_markup=markup, disable_web_page_preview=True)


async def edit_or_reply(update: Update, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
    q = update.callback_query
    if q is not None and q.message is not None and q.message.text is not None:
        try:
            await q.edit_message_text(text, parse_mode=HTML, reply_markup=markup, disable_web_page_preview=True)
            return
        except BadRequest as exc:
            if "not modified" in str(exc).lower():
                return
    await reply(update, text, markup)


def kb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=data) for label, data in row] for row in rows])


def app_button(settings: Settings, label: str = "📱 Open GymBot app", path: str = "") -> InlineKeyboardButton | None:
    url = settings.miniapp_url
    return InlineKeyboardButton(label, web_app=WebAppInfo(url + path)) if url else None


def chunk(items: list, n: int) -> list[list]:
    return [items[i : i + n] for i in range(0, len(items), n)]


# ---------------------------------------------------------------- keyboards


def workout_menu(active_has_plan_next: tuple[int, str] | None = None) -> InlineKeyboardMarkup:
    rows = []
    if active_has_plan_next:
        ex_id, name = active_has_plan_next
        rows.append([(f"▶️ Next: {name[:40]}", f"e:{ex_id}")])
    rows += [
        [("➕ Pick exercise", "groups")],
        [("✅ Finish", "w:finish"), ("🗑 Cancel", "w:cancel")],
    ]
    return kb(rows)


def after_set_menu(ex_id: int, next_item: tuple[int, str] | None) -> InlineKeyboardMarkup:
    rows = []
    if next_item and next_item[0] != ex_id:
        rows.append([(f"▶️ Next: {next_item[1][:40]}", f"e:{next_item[0]}")])
    rows += [
        [("↩️ Undo", "s:undo"), ("🔁 Other exercise", "groups")],
        [("✅ Finish workout", "w:finish")],
    ]
    return kb(rows)


async def groups_markup(session: AsyncSession, user: User) -> InlineKeyboardMarkup:
    groups = await catalog.list_groups(session, user.id)
    rows = [[(g, f"g:{g}:0") for g in row] for row in chunk(groups, 3)]
    rows.append([("✅ Finish", "w:finish"), ("⬅️ Back", "w:menu")])
    return kb(rows)


async def exercises_markup(session: AsyncSession, user: User, group: str, page: int) -> InlineKeyboardMarkup:
    items = await catalog.list_exercises(session, user.id, group)
    pages = max(1, (len(items) + PAGE - 1) // PAGE)
    page = max(0, min(page, pages - 1))
    rows = [[(ex.name[:60], f"e:{ex.id}")] for ex in items[page * PAGE : (page + 1) * PAGE]]
    nav = []
    if page > 0:
        nav.append(("◀️", f"g:{group}:{page - 1}"))
    if pages > 1:
        nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"g:{group}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([("⬅️ Groups", "groups")])
    return kb(rows)


def _next_item(workout) -> tuple[int, str] | None:
    item = plans.next_plan_item(workout)
    return (item.exercise_id, item.exercise.name) if item else None


# ---------------------------------------------------------------- basic commands


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with user_session(update, context) as (s, user):
        active = await workouts.get_active(s, user.id)
    name = escape(update.effective_user.first_name or "there")
    text = (
        f"Hey {name}! 👋 I'm your gym log.\n\n"
        "Tap <b>Start workout</b>, pick an exercise and type your sets like <code>80x8</code>. "
        "I track PRs, rest time and your progress.\n\n"
        f"Your timezone is <b>{escape(tz_label(user.timezone))}</b> — change it with /timezone."
    )
    rows = [[InlineKeyboardButton("▶️ Continue workout" if active else "🏋️ Start workout", callback_data="w:start")],
            [InlineKeyboardButton("📋 Plans", callback_data="plans"), InlineKeyboardButton("❓ Help", callback_data="help")]]
    btn = app_button(_settings(context))
    if btn:
        rows.append([btn])
    await reply(update, text, InlineKeyboardMarkup(rows))


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await reply(update, texts.HELP)


async def app_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    btn = app_button(_settings(context))
    if btn is None:
        await reply(update, "The Mini App isn't configured yet (WEBAPP_URL must be an https address).")
        return
    await reply(update, "Charts, plans, history and settings:", InlineKeyboardMarkup([[btn]]))


# ---------------------------------------------------------------- workout flow


async def show_workout(update: Update, context: ContextTypes.DEFAULT_TYPE, s: AsyncSession, user: User, workout, created: bool) -> None:
    sm = workouts.summarize(workout)
    if created:
        head = "🏋️ <b>Workout started</b>"
        if workout.plan:
            head += f" — {escape(workout.plan.name)}"
    else:
        head = f"🏋️ <b>Workout in progress</b> · {sm.set_count} sets so far"
    lines = [head]
    if workout.plan:
        for item, done in plans.plan_progress(workout):
            mark = "✅" if done >= item.target_sets else "▫️"
            reps = f" × {item.target_reps}" if item.target_reps else ""
            lines.append(f"{mark} {escape(item.exercise.name)} — {done}/{item.target_sets}{reps}")
    elif sm.blocks:
        for b in sm.blocks:
            lines.append(f"• {escape(b.exercise.name)}: {len(b.sets)} sets")
    lines.append("\nPick an exercise to log sets.")
    await edit_or_reply(update, "\n".join(lines), workout_menu(_next_item(workout)))


async def workout_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, plan_id: int | None = None) -> None:
    async with user_session(update, context) as (s, user):
        workout, created = await workouts.start(s, user.id, plan_id)
        if created:
            context.user_data.pop("exercise_id", None)
        await show_workout(update, context, s, user, workout, created)


async def open_exercise(update: Update, context: ContextTypes.DEFAULT_TYPE, exercise_id: int) -> None:
    async with user_session(update, context) as (s, user):
        ex = await catalog.get_exercise(s, user.id, exercise_id)
        if ex is None:
            await edit_or_reply(update, "That exercise no longer exists.")
            return
        workout, _ = await workouts.start(s, user.id)
        done = workouts.sets_for(workout, ex.id)
        last = await workouts.last_time_sets(s, user.id, ex.id, workout.id)
        target = None
        if workout.plan:
            for item in workout.plan.items:
                if item.exercise_id == ex.id:
                    target = f"{item.target_sets} sets" + (f" × {item.target_reps}" if item.target_reps else "")
                    break
        card = texts.exercise_card(ex, done, last, target)
    context.user_data["exercise_id"] = ex.id
    context.user_data.pop("awaiting", None)
    markup = kb([[("🔁 Other exercise", "groups"), ("✅ Finish", "w:finish")]])
    image = ex.image and (_settings(context).exercise_dir / ex.image)
    if image and image.is_file():
        cache: dict = context.application.bot_data.setdefault("photo_ids", {})
        try:
            msg = await update.effective_message.reply_photo(
                cache.get(ex.image) or image, caption=card, parse_mode=HTML, reply_markup=markup
            )
            if msg.photo:
                cache[ex.image] = msg.photo[-1].file_id
            return
        except TelegramError as exc:
            log.warning("Could not send exercise image %s: %s", ex.image, exc)
    await reply(update, card, markup)


def parse_set_text(ex: Exercise, raw: str) -> dict | None:
    if ex.kind == ExerciseKind.TIME.value:
        d = parse_duration(raw)
        return {"duration": d} if d else None
    if ex.kind == ExerciseKind.CARDIO.value:
        parsed = parse_cardio(raw)
        return {"distance": parsed[0], "duration": parsed[1]} if parsed else None
    parsed = parse_strength(raw)
    return {"weight": parsed[0], "reps": parsed[1]} if parsed else None


async def log_set_text(update: Update, context: ContextTypes.DEFAULT_TYPE, raw: str) -> None:
    exercise_id = context.user_data.get("exercise_id")
    async with user_session(update, context) as (s, user):
        workout = await workouts.get_active(s, user.id)
        if workout is None:
            await reply(update, "No workout running.", kb([[("🏋️ Start workout", "w:start")]]))
            return
        ex = await catalog.get_exercise(s, user.id, exercise_id) if exercise_id else None
        if ex is None:
            await reply(update, "Pick an exercise first:", await groups_markup(s, user))
            return
        values = parse_set_text(ex, raw)
        if values is None:
            await reply(update, f"I didn't get that. {texts.input_hint(ex)}")
            return
        if ex.kind == ExerciseKind.STRENGTH.value and values["weight"] is None:
            # Bare reps: reuse the weight from the previous set of this exercise.
            prev = workouts.sets_for(workout, ex.id) or await workouts.last_time_sets(s, user.id, ex.id, workout.id)
            values["weight"] = prev[-1].weight_kg if prev else 0.0
        try:
            new_set, prs = await workouts.log_set(s, workout, ex, **values)
        except ValueError:
            await reply(update, f"That doesn't look right. {texts.input_hint(ex)}")
            return
        number = len(workouts.sets_for(workout, ex.id))
        rest = user.rest_seconds
        text = texts.set_logged(ex, new_set, number, prs, rest)
        markup = after_set_menu(ex.id, _next_item(workout))
    await reply(update, text, markup)
    schedule_rest(context, user.id, update.effective_chat.id, rest)


def schedule_rest(context: ContextTypes.DEFAULT_TYPE, user_id: int, chat_id: int, seconds: int) -> None:
    jq = context.job_queue
    if jq is None:
        return
    name = f"rest:{user_id}"
    for job in jq.get_jobs_by_name(name):
        job.schedule_removal()
    jq.run_once(rest_done, seconds, chat_id=chat_id, user_id=user_id, name=name, data=seconds)


async def rest_done(context: ContextTypes.DEFAULT_TYPE) -> None:
    job = context.job
    try:
        await context.bot.send_message(job.chat_id, f"⏰ Rest over ({fmt_clock(job.data)}) — next set!")
    except (Forbidden, BadRequest) as exc:
        log.info("Rest notification failed for %s: %s", job.user_id, exc)


def cancel_rest(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> None:
    if context.job_queue:
        for job in context.job_queue.get_jobs_by_name(f"rest:{user_id}"):
            job.schedule_removal()


async def finish_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with user_session(update, context) as (s, user):
        workout = await workouts.get_active(s, user.id)
        if workout is None:
            await edit_or_reply(update, "No workout running.", kb([[("🏋️ Start workout", "w:start")]]))
            return
        sm = await workouts.finish(s, workout)
        cancel_rest(context, user.id)
        context.user_data.pop("exercise_id", None)
        if sm is None:
            await edit_or_reply(update, "Workout discarded — no sets were logged.")
            return
        text = texts.workout_summary(sm, user.timezone)
        streak = await stats.week_streak(s, user.id, user.timezone)
        if streak > 1:
            text += f"\n🔥 {streak}-week streak"
        markup = kb([[("💾 Save as plan", f"w:saveplan:{sm.workout.id}")], [("📊 This week", "stats:week")]])
    await edit_or_reply(update, text, markup)


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.pop("awaiting", None):
        await reply(update, "OK, cancelled.")
        return
    async with user_session(update, context) as (s, user):
        workout = await workouts.get_active(s, user.id)
        if workout is None:
            await edit_or_reply(update, "Nothing to cancel.")
            return
        n = len(workout.sets)
    if n == 0:
        await confirm_cancel(update, context)
        return
    await edit_or_reply(
        update,
        f"Discard this workout and its {n} set{'s' if n != 1 else ''}? This can't be undone.",
        kb([[("🗑 Yes, discard", "w:cancel!"), ("⬅️ Keep it", "w:menu")]]),
    )


async def confirm_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with user_session(update, context) as (s, user):
        workout = await workouts.get_active(s, user.id)
        if workout is not None:
            await workouts.cancel(s, workout)
        cancel_rest(context, user.id)
    context.user_data.pop("exercise_id", None)
    await edit_or_reply(update, "Workout discarded.", kb([[("🏋️ Start a new one", "w:start")]]))


async def undo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with user_session(update, context) as (s, user):
        workout = await workouts.get_active(s, user.id)
        removed = await workouts.undo_last_set(s, workout) if workout else None
        if removed is None:
            await reply(update, "Nothing to undo.")
            return
        text = f"↩️ Removed {workouts.describe_set(removed, removed.exercise.kind)} — {escape(removed.exercise.name)}"
        context.user_data["exercise_id"] = removed.exercise_id
    cancel_rest(context, update.effective_user.id)
    await edit_or_reply(update, text, kb([[("🔁 Other exercise", "groups"), ("✅ Finish", "w:finish")]]))


# ---------------------------------------------------------------- plans


async def plans_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with user_session(update, context) as (s, user):
        items = await plans.list_plans(s, user.id)
    if not items:
        text = (
            "You have no plans yet.\n\nFinish a workout and tap <b>Save as plan</b>, "
            "or build one in the Mini App."
        )
    else:
        text = "<b>Your plans</b> — tap one to start it:\n\n" + "\n".join(
            f"• <b>{escape(p.name)}</b>: " + (", ".join(escape(i.exercise.name) for i in p.items) or "empty")
            for p in items
        )
    rows = [[InlineKeyboardButton(f"▶️ {p.name[:50]}", callback_data=f"w:plan:{p.id}")] for p in items if p.items]
    btn = app_button(_settings(context), "✏️ Edit plans in the app", "#plans")
    if btn:
        rows.append([btn])
    await edit_or_reply(update, text, InlineKeyboardMarkup(rows) if rows else None)


async def ask_plan_name(update: Update, context: ContextTypes.DEFAULT_TYPE, workout_id: int) -> None:
    context.user_data["awaiting"] = f"plan_name:{workout_id}"
    await reply(update, "Send a name for the new plan (e.g. <i>Push day</i>), or /cancel.")


async def save_plan_named(update: Update, context: ContextTypes.DEFAULT_TYPE, workout_id: int, name: str) -> None:
    async with user_session(update, context) as (s, user):
        workout = await workouts.load(s, user.id, workout_id)
        if workout is None or not workout.sets:
            await reply(update, "That workout is gone.")
            return
        try:
            plan = await plans.plan_from_workout(s, user.id, workout, name)
        except PlanError as exc:
            await reply(update, escape(str(exc)))
            return
        text = f"💾 Saved plan <b>{escape(plan.name)}</b> with {len(plan.items)} exercises."
        markup = kb([[("▶️ Start it now", f"w:plan:{plan.id}")]])
    await reply(update, text, markup)


# ---------------------------------------------------------------- history & stats


async def last_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with user_session(update, context) as (s, user):
        sm = await stats.last_workout(s, user.id)
        text = texts.workout_summary(sm, user.timezone, "Last workout") if sm else "No workouts yet — /workout to start."
    await reply(update, text)


async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, offset: int = 0) -> None:
    async with user_session(update, context) as (s, user):
        items = await stats.history(s, user.id, limit=10, offset=offset)
        total = await stats.count_completed(s, user.id)
        if not items:
            await edit_or_reply(update, "No workouts yet — /workout to start.")
            return
        lines = [f"<b>History</b> ({offset + 1}–{offset + len(items)} of {total})", ""]
        lines += [texts.history_line(sm, user.timezone) for sm in items]
    nav = []
    if offset > 0:
        nav.append(("◀️ Newer", f"h:{max(0, offset - 10)}"))
    if offset + len(items) < total:
        nav.append(("Older ▶️", f"h:{offset + 10}"))
    await edit_or_reply(update, "\n".join(lines), kb([nav]) if nav else None)


async def _period_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, which: str) -> None:
    async with user_session(update, context) as (s, user):
        today = local_today(user.timezone)
        if which == "today":
            first, last, title = today, today, "Today"
        elif which == "week":
            first, last = stats.week_bounds(today)
            title = "This week"
        else:
            first, last = stats.month_bounds(today.year, today.month)
            title = "This month"
        p = await stats.period_summary(s, user.id, user.timezone, first, last)
    await reply(update, texts.period(p, title))


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _period_reply(update, context, "today")


async def week_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _period_reply(update, context, "week")


async def month_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _period_reply(update, context, "month")


async def period_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    raw = " ".join(context.args or [])
    async with user_session(update, context) as (s, user):
        parsed = parse_period(raw, local_today(user.timezone))
        if parsed is None:
            await reply(update, "Usage: <code>/period 01.10-15.10</code> or <code>/period 2026-10-01 2026-10-15</code>")
            return
        p = await stats.period_summary(s, user.id, user.timezone, *parsed)
    await reply(update, texts.period(p, "Period"))


async def pr_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with user_session(update, context) as (s, user):
        records = await stats.personal_records(s, user.id)
    if not records:
        await reply(update, "No records yet — log some sets first.")
        return
    lines = ["<b>Personal records</b>", ""] + [texts.record_line(r) for r in records[:40]]
    btn = app_button(_settings(context), "📈 Progress charts", "#progress")
    await reply(update, "\n".join(lines), InlineKeyboardMarkup([[btn]]) if btn else None)


async def weight_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    raw = (context.args or [""])[0].replace(",", ".")
    try:
        kg = float(raw)
    except ValueError:
        kg = 0
    if not 20 <= kg <= 400:
        await reply(update, "Usage: <code>/weight 82.5</code>")
        return
    async with user_session(update, context) as (s, user):
        await users.log_body_weight(s, user.id, kg)
        history = await users.body_weights(s, user.id, limit=2)
    text = f"⚖️ Logged {kg:g} kg."
    if len(history) == 2:
        diff = history[-1].weight_kg - history[0].weight_kg
        text += f" ({diff:+.1f} kg since last time)"
    await reply(update, text)


# ---------------------------------------------------------------- settings & reminders


async def rest_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    raw = " ".join(context.args or [])
    seconds = parse_duration(raw) if raw else None
    async with user_session(update, context) as (s, user):
        if seconds is None:
            await reply(update, f"Rest timer is <b>{fmt_clock(user.rest_seconds)}</b>. Change it: <code>/rest 90</code> or <code>/rest 2m</code>")
            return
        await users.set_rest(s, user, seconds)
        await reply(update, f"⏱ Rest timer set to <b>{fmt_clock(user.rest_seconds)}</b>.")


async def timezone_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    raw = " ".join(context.args or [])
    async with user_session(update, context) as (s, user):
        if not raw:
            await reply(update, texts.settings_text(user.timezone, user.rest_seconds) + "\n\nChange it: <code>/timezone Europe/Berlin</code> or <code>/timezone +3</code>")
            return
        tz = parse_timezone(raw)
        if tz is None:
            await reply(update, "Unknown timezone. Try <code>Europe/Berlin</code>, <code>Asia/Makassar</code> or <code>+8</code>.")
            return
        await users.set_timezone(s, user, tz)
        await reply(update, f"🌍 Timezone set to <b>{escape(tz_label(tz))}</b>.")


REMIND_USAGE = (
    "Add one: <code>/remind mon wed fri 18:00</code>\n"
    "Days can be <code>daily</code>, <code>weekdays</code>, <code>weekend</code>, <code>mon-fri</code>."
)


async def remind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    async with user_session(update, context) as (s, user):
        if args:
            at = parse_hhmm(args[-1])
            days = reminders.parse_weekdays(" ".join(args[:-1]) or "daily")
            if at is None or days is None:
                await reply(update, "I couldn't read that.\n" + REMIND_USAGE)
                return
            try:
                await reminders.create_reminder(s, user.id, days, at)
            except ReminderError as exc:
                await reply(update, escape(str(exc)))
                return
        items = await reminders.list_reminders(s, user.id)
        tz = tz_label(user.timezone)
    if not items:
        await edit_or_reply(update, f"No reminders yet (times are in {escape(tz)}).\n\n{REMIND_USAGE}")
        return
    lines = [f"<b>Reminders</b> (times in {escape(tz)})", ""]
    rows = []
    for r in items:
        state = "🔔" if r.enabled else "🔕"
        lines.append(f"{state} {reminders.describe(r)}")
        rows.append([(f"{'Pause' if r.enabled else 'Resume'} {r.at_time}", f"r:t:{r.id}"), ("🗑", f"r:d:{r.id}")])
    lines += ["", REMIND_USAGE]
    await edit_or_reply(update, "\n".join(lines), kb(rows))


async def reminder_action(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str, reminder_id: int) -> None:
    async with user_session(update, context) as (s, user):
        if action == "d":
            await reminders.delete_reminder(s, user.id, reminder_id)
        else:
            r = await reminders.get_reminder(s, user.id, reminder_id)
            if r is not None:
                await reminders.set_enabled(s, r, not r.enabled)
    context.args = []
    await remind_cmd(update, context)


async def reminder_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Runs every minute: send due reminders."""
    db: Database = context.application.bot_data["db"]
    async with db.session() as s:
        due = await reminders.collect_due(s, datetime.now(timezone.utc))
    for item in due:
        if item.already_trained:
            continue
        try:
            await context.bot.send_message(
                item.user.chat_id,
                "💪 Time to train! Your workout is waiting.",
                reply_markup=kb([[("🏋️ Start workout", "w:start"), ("📋 Plans", "plans")]]),
            )
        except (Forbidden, BadRequest) as exc:
            log.info("Reminder to %s failed: %s", item.user.id, exc)
        except TelegramError:
            log.exception("Reminder to %s failed", item.user.id)


# ---------------------------------------------------------------- routing


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    raw = (update.effective_message.text or "").strip()
    awaiting = context.user_data.pop("awaiting", None)
    if awaiting and awaiting.startswith("plan_name:"):
        await save_plan_named(update, context, int(awaiting.split(":", 1)[1]), raw)
        return
    await log_set_text(update, context, raw)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    data = q.data or ""
    await q.answer()
    try:
        if data == "noop":
            return
        if data == "help":
            await reply(update, texts.HELP)
        elif data == "plans":
            await plans_cmd(update, context)
        elif data in {"w:start", "w:menu"}:
            await workout_cmd(update, context)
        elif data.startswith("w:plan:"):
            await workout_cmd(update, context, plan_id=int(data.split(":")[2]))
        elif data == "w:finish":
            await finish_cmd(update, context)
        elif data == "w:cancel":
            await cancel_cmd(update, context)
        elif data == "w:cancel!":
            await confirm_cancel(update, context)
        elif data.startswith("w:saveplan:"):
            await ask_plan_name(update, context, int(data.split(":")[2]))
        elif data == "groups":
            async with user_session(update, context) as (s, user):
                markup = await groups_markup(s, user)
            await edit_or_reply(update, "Pick a muscle group:", markup)
        elif data.startswith("g:"):
            _, rest = data.split(":", 1)
            group, _, page = rest.rpartition(":")
            async with user_session(update, context) as (s, user):
                markup = await exercises_markup(s, user, group, int(page))
            await edit_or_reply(update, f"<b>{escape(group)}</b> — pick an exercise:", markup)
        elif data.startswith("e:"):
            await open_exercise(update, context, int(data[2:]))
        elif data == "s:undo":
            await undo(update, context)
        elif data.startswith("h:"):
            await history_cmd(update, context, offset=int(data[2:]))
        elif data == "stats:week":
            await week_cmd(update, context)
        elif data.startswith("r:"):
            _, action, rid = data.split(":")
            await reminder_action(update, context, action, int(rid))
        else:
            log.warning("Unknown callback data: %r", data)
    except ValueError:
        log.warning("Malformed callback data: %r", data)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Update failed", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("Something went wrong on my side — please try again.")
        except TelegramError:
            pass


async def post_init(app: Application) -> None:
    await app.bot.set_my_commands([BotCommand(c, d) for c, d in COMMANDS])
    settings: Settings = app.bot_data["settings"]
    if settings.miniapp_url:
        try:
            await app.bot.set_chat_menu_button(menu_button=MenuButtonWebApp("GymBot", WebAppInfo(settings.miniapp_url)))
        except TelegramError as exc:
            log.warning("Could not set the Mini App menu button: %s", exc)
    if app.job_queue:
        app.job_queue.run_repeating(reminder_tick, interval=60, first=5, name="reminders")


def build_application(settings: Settings, db: Database) -> Application:
    app = Application.builder().token(settings.telegram_bot_token).post_init(post_init).build()
    app.bot_data["db"] = db
    app.bot_data["settings"] = settings
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("app", app_cmd))
    app.add_handler(CommandHandler(["workout", "train"], workout_cmd))
    app.add_handler(CommandHandler("finish", finish_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("undo", undo))
    app.add_handler(CommandHandler("plans", plans_cmd))
    app.add_handler(CommandHandler("last", last_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("today", today_cmd))
    app.add_handler(CommandHandler(["week", "thisweek"], week_cmd))
    app.add_handler(CommandHandler("month", month_cmd))
    app.add_handler(CommandHandler("period", period_cmd))
    app.add_handler(CommandHandler("pr", pr_cmd))
    app.add_handler(CommandHandler("weight", weight_cmd))
    app.add_handler(CommandHandler("rest", rest_cmd))
    app.add_handler(CommandHandler("timezone", timezone_cmd))
    app.add_handler(CommandHandler("remind", remind_cmd))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)
    return app
