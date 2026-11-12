from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from gymbot.db.models import BodyWeight, User, utcnow

MIN_REST, MAX_REST = 15, 900


async def upsert_user(
    session: AsyncSession,
    user_id: int,
    *,
    chat_id: int | None = None,
    username: str | None = None,
    first_name: str | None = None,
    default_rest: int = 90,
) -> User:
    user = await session.get(User, user_id)
    if user is None:
        user = User(
            id=user_id,
            chat_id=chat_id or user_id,
            username=username,
            first_name=first_name,
            rest_seconds=default_rest,
            timezone="UTC",
        )
        session.add(user)
        await session.flush()
        return user
    if chat_id:
        user.chat_id = chat_id
    if username is not None:
        user.username = username
    if first_name is not None:
        user.first_name = first_name
    return user


async def get_user(session: AsyncSession, user_id: int) -> User | None:
    return await session.get(User, user_id)


def clamp_rest(seconds: int) -> int:
    return max(MIN_REST, min(MAX_REST, int(seconds)))


async def log_body_weight(session: AsyncSession, user_id: int, weight_kg: float) -> BodyWeight:
    entry = BodyWeight(user_id=user_id, weight_kg=round(float(weight_kg), 2), measured_at=utcnow())
    session.add(entry)
    await session.flush()
    return entry


async def body_weights(session: AsyncSession, user_id: int, limit: int | None = None) -> list[BodyWeight]:
    """Body weight entries, oldest first (latest `limit` if given)."""
    stmt = select(BodyWeight).where(BodyWeight.user_id == user_id).order_by(BodyWeight.measured_at.desc())
    if limit:
        stmt = stmt.limit(limit)
    rows = list((await session.scalars(stmt)).all())
    rows.reverse()
    return rows


async def set_rest(session: AsyncSession, user: User, seconds: int) -> User:
    user.rest_seconds = clamp_rest(seconds)
    await session.flush()
    return user


async def set_timezone(session: AsyncSession, user: User, tz_name: str) -> User:
    user.timezone = tz_name
    await session.flush()
    return user
