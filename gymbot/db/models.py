from __future__ import annotations

import enum
from datetime import date, datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    type_annotation_map = {datetime: DateTime(timezone=True)}


class ExerciseKind(str, enum.Enum):
    STRENGTH = "strength"  # weight (0 = bodyweight) x reps
    TIME = "time"  # hold duration, e.g. plank
    CARDIO = "cardio"  # distance + duration, e.g. running


class WorkoutStatus(str, enum.Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)  # Telegram user id
    chat_id: Mapped[int] = mapped_column(BigInteger)
    username: Mapped[str | None] = mapped_column(String(64))
    first_name: Mapped[str | None] = mapped_column(String(128))
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    rest_seconds: Mapped[int] = mapped_column(Integer, default=90)
    # Bumped to sign out every standalone-app session of this user.
    session_version: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(default=utcnow)


class Exercise(Base):
    __tablename__ = "exercises"
    __table_args__ = (UniqueConstraint("owner_id", "name", name="uq_exercise_owner_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120))
    muscle_group: Mapped[str] = mapped_column(String(40), index=True)
    kind: Mapped[str] = mapped_column(String(16), default=ExerciseKind.STRENGTH.value)
    image: Mapped[str | None] = mapped_column(String(255))  # path relative to the exercise asset dir
    # NULL = built-in catalog exercise; otherwise a user's custom exercise.
    owner_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"))
    archived: Mapped[bool] = mapped_column(Boolean, default=False)


class Workout(Base):
    __tablename__ = "workouts"
    __table_args__ = (Index("ix_workouts_user_started", "user_id", "started_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"))
    plan_id: Mapped[int | None] = mapped_column(ForeignKey("plans.id", ondelete="SET NULL"))
    status: Mapped[str] = mapped_column(String(16), default=WorkoutStatus.ACTIVE.value)
    started_at: Mapped[datetime] = mapped_column(default=utcnow)
    ended_at: Mapped[datetime | None] = mapped_column()
    note: Mapped[str | None] = mapped_column(Text)

    sets: Mapped[list[WorkoutSet]] = relationship(
        back_populates="workout",
        order_by="WorkoutSet.id",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    plan: Mapped[Plan | None] = relationship()


class WorkoutSet(Base):
    __tablename__ = "workout_sets"
    __table_args__ = (Index("ix_sets_user_exercise_time", "user_id", "exercise_id", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    workout_id: Mapped[int] = mapped_column(ForeignKey("workouts.id", ondelete="CASCADE"), index=True)
    exercise_id: Mapped[int] = mapped_column(ForeignKey("exercises.id", ondelete="CASCADE"))
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"))
    reps: Mapped[int | None] = mapped_column(Integer)
    weight_kg: Mapped[float | None] = mapped_column(Float)
    duration_s: Mapped[int | None] = mapped_column(Integer)
    distance_km: Mapped[float | None] = mapped_column(Float)
    is_pr: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    workout: Mapped[Workout] = relationship(back_populates="sets")
    exercise: Mapped[Exercise] = relationship(lazy="joined")


class Plan(Base):
    __tablename__ = "plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(80))
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    items: Mapped[list[PlanItem]] = relationship(
        back_populates="plan",
        order_by="PlanItem.position",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class PlanItem(Base):
    __tablename__ = "plan_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    plan_id: Mapped[int] = mapped_column(ForeignKey("plans.id", ondelete="CASCADE"), index=True)
    exercise_id: Mapped[int] = mapped_column(ForeignKey("exercises.id", ondelete="CASCADE"))
    position: Mapped[int] = mapped_column(Integer, default=0)
    target_sets: Mapped[int] = mapped_column(Integer, default=3)
    target_reps: Mapped[int | None] = mapped_column(Integer)

    plan: Mapped[Plan] = relationship(back_populates="items")
    exercise: Mapped[Exercise] = relationship(lazy="joined")


class Reminder(Base):
    __tablename__ = "reminders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    # Comma-separated weekdays, Monday=0 ... Sunday=6.
    weekdays: Mapped[str] = mapped_column(String(20), default="0,1,2,3,4,5,6")
    at_time: Mapped[str] = mapped_column(String(5), default="18:00")  # HH:MM in the user's timezone
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    # Local date of the last send, so a reminder fires at most once per day.
    last_sent_on: Mapped[date | None] = mapped_column(Date)


class LoginCode(Base):
    """One-time code (sent by the bot) that signs a user in to the standalone app."""

    __tablename__ = "login_codes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    code_hash: Mapped[str] = mapped_column(String(64), unique=True)  # sha256 hex; the code itself is never stored
    expires_at: Mapped[datetime] = mapped_column()
    used_at: Mapped[datetime | None] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(default=utcnow)


class BodyWeight(Base):
    __tablename__ = "body_weights"
    __table_args__ = (Index("ix_bodyweight_user_time", "user_id", "measured_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"))
    weight_kg: Mapped[float] = mapped_column(Float)
    measured_at: Mapped[datetime] = mapped_column(default=utcnow)
