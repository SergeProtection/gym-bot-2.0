"""Exercise catalog: built-in exercises come from the Exercise/ image folders."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from gymbot.db.models import Exercise, ExerciseKind

log = logging.getLogger(__name__)

CARDIO_GROUP = "Cardio"

# Display order for muscle groups; unknown groups are appended alphabetically.
GROUP_ORDER = ["Chest", "Back", "Shoulders", "Legs", "Biceps", "Triceps", "Abs", "Calves", CARDIO_GROUP]

# Folder names that do not match the display name.
FOLDER_ALIASES = {
    "chest exercise": "Chest",
    "triceps exercise": "Triceps",
    "abdominals": "Abs",
}

# Files in a group folder that are the group cover image, not an exercise.
COVER_STEMS = {"abs", "back", "biceps", "calves", "chest", "legs", "shoulders", "triceps"}

BUILTIN_CARDIO = ["Running", "Treadmill", "Cycling", "Rowing", "Walking", "Swimming"]

IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp"}


@dataclass(frozen=True)
class CatalogEntry:
    name: str
    muscle_group: str
    kind: str
    image: str | None


def group_name_for_folder(folder: str) -> str:
    key = folder.strip().lower()
    if key in FOLDER_ALIASES:
        return FOLDER_ALIASES[key]
    return re.sub(r"\s+exercises?$", "", folder.strip(), flags=re.I).title()


def pretty_name(stem: str) -> str:
    words = re.sub(r"[-_]+", " ", stem).split()
    small = {"a", "the", "with", "to", "of", "on", "and"}
    out = []
    for i, w in enumerate(words):
        if w.isupper() and len(w) > 1:  # keep acronyms like EZ / T
            out.append(w)
        elif i and w.lower() in small:
            out.append(w.lower())
        else:
            out.append(w[:1].upper() + w[1:].lower())
    return " ".join(out)


def kind_for_name(name: str) -> str:
    key = name.lower()
    if "wall sit" in key:
        return ExerciseKind.TIME.value
    if "plank" in key and "get up" not in key and "get-up" not in key:
        return ExerciseKind.TIME.value
    return ExerciseKind.STRENGTH.value


def scan_assets(base_dir: Path) -> tuple[list[CatalogEntry], dict[str, str]]:
    """Return (exercises, {group: cover image}) found under base_dir."""
    entries: list[CatalogEntry] = []
    covers: dict[str, str] = {}
    if not base_dir.is_dir():
        log.warning("Exercise asset folder not found: %s", base_dir)
    else:
        for folder in sorted(p for p in base_dir.iterdir() if p.is_dir()):
            group = group_name_for_folder(folder.name)
            for img in sorted(folder.iterdir()):
                if img.suffix.lower() not in IMAGE_SUFFIXES:
                    continue
                rel = img.relative_to(base_dir).as_posix()
                if img.stem.lower() in COVER_STEMS:
                    covers[group] = rel
                    continue
                name = pretty_name(img.stem)
                entries.append(CatalogEntry(name, group, kind_for_name(name), rel))
    for name in BUILTIN_CARDIO:
        entries.append(CatalogEntry(name, CARDIO_GROUP, ExerciseKind.CARDIO.value, None))
    return entries, covers


async def sync_catalog(session: AsyncSession, base_dir: Path) -> int:
    """Insert new built-in exercises and refresh image paths. Returns number added."""
    entries, _ = scan_assets(base_dir)
    existing = {
        ex.name.lower(): ex
        for ex in (await session.scalars(select(Exercise).where(Exercise.owner_id.is_(None)))).all()
    }
    added = 0
    for entry in entries:
        ex = existing.get(entry.name.lower())
        if ex is None:
            ex = Exercise(name=entry.name, muscle_group=entry.muscle_group, kind=entry.kind, image=entry.image)
            session.add(ex)
            existing[entry.name.lower()] = ex  # guards against duplicate names across folders
            added += 1
        elif entry.image and ex.image != entry.image:
            ex.image = entry.image
    await session.flush()
    if added:
        log.info("Exercise catalog: added %d exercises", added)
    return added


def sort_groups(groups: set[str] | list[str]) -> list[str]:
    known = [g for g in GROUP_ORDER if g in groups]
    rest = sorted(g for g in groups if g not in GROUP_ORDER)
    return known + rest


def _visible_to(user_id: int):
    return or_(Exercise.owner_id.is_(None), Exercise.owner_id == user_id)


async def list_groups(session: AsyncSession, user_id: int) -> list[str]:
    rows = await session.scalars(
        select(Exercise.muscle_group).where(_visible_to(user_id), Exercise.archived.is_(False)).distinct()
    )
    return sort_groups(set(rows.all()))


async def list_exercises(session: AsyncSession, user_id: int, group: str | None = None) -> list[Exercise]:
    stmt = select(Exercise).where(_visible_to(user_id), Exercise.archived.is_(False))
    if group:
        stmt = stmt.where(Exercise.muscle_group == group)
    stmt = stmt.order_by(Exercise.muscle_group, Exercise.name)
    return list((await session.scalars(stmt)).all())


async def get_exercise(session: AsyncSession, user_id: int, exercise_id: int) -> Exercise | None:
    ex = await session.get(Exercise, exercise_id)
    if ex is None or (ex.owner_id is not None and ex.owner_id != user_id):
        return None
    return ex


async def add_custom_exercise(
    session: AsyncSession, user_id: int, name: str, group: str, kind: str = ExerciseKind.STRENGTH.value
) -> Exercise:
    name = " ".join(name.split())[:120]
    existing = await session.scalar(
        select(Exercise).where(_visible_to(user_id), Exercise.name.ilike(name))
    )
    if existing:
        return existing
    ex = Exercise(name=name, muscle_group=group, kind=kind, owner_id=user_id)
    session.add(ex)
    await session.flush()
    return ex
