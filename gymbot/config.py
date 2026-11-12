from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def normalize_db_url(value: str) -> str:
    """Railway/Heroku style URLs -> async SQLAlchemy driver."""
    value = value.strip()
    if value.startswith("postgres://"):
        value = "postgresql+asyncpg://" + value[len("postgres://"):]
    elif value.startswith("postgresql://"):
        value = "postgresql+asyncpg://" + value[len("postgresql://"):]
    return value


def _bool(value: str | None, default: bool) -> bool:
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int(value: str | None, default: int | None) -> int | None:
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError as exc:
        raise ValueError(f"Expected an integer, got {value!r}") from exc


def default_database_url() -> str:
    """SQLite fallback when DATABASE_URL is unset; uses a mounted /data volume if there is one."""
    data_dir = Path("/data")
    folder = data_dir if data_dir.is_dir() else PROJECT_ROOT
    return f"sqlite+aiosqlite:///{(folder / 'gymbot.db').as_posix()}"


@dataclass(frozen=True)
class Settings:
    """Runtime configuration, read from environment variables or a .env file."""

    telegram_bot_token: str = ""
    database_url: str = field(default_factory=default_database_url)
    # Public HTTPS base URL of this service (used for the Mini App button).
    # On Railway this falls back to RAILWAY_PUBLIC_DOMAIN automatically.
    webapp_url: str = ""
    railway_public_domain: str = ""
    exercise_dir: Path = PROJECT_ROOT / "Exercise"
    host: str = "0.0.0.0"
    port: int = 8000
    run_bot: bool = True
    run_migrations: bool = True
    default_rest_seconds: int = 90
    log_level: str = "INFO"
    # Local development only: lets the Mini App API accept requests without
    # Telegram initData, acting as this user id. Never set this in production.
    dev_user_id: int | None = None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        e = os.environ if env is None else env
        d = cls()
        return cls(
            telegram_bot_token=e.get("TELEGRAM_BOT_TOKEN", d.telegram_bot_token).strip(),
            database_url=normalize_db_url(e.get("DATABASE_URL") or d.database_url),
            webapp_url=e.get("WEBAPP_URL", d.webapp_url),
            railway_public_domain=e.get("RAILWAY_PUBLIC_DOMAIN", d.railway_public_domain),
            exercise_dir=Path(e["EXERCISE_DIR"]) if e.get("EXERCISE_DIR") else d.exercise_dir,
            host=e.get("HOST", d.host),
            port=_int(e.get("PORT"), d.port),
            run_bot=_bool(e.get("RUN_BOT"), d.run_bot),
            run_migrations=_bool(e.get("RUN_MIGRATIONS"), d.run_migrations),
            default_rest_seconds=_int(e.get("DEFAULT_REST_SECONDS"), d.default_rest_seconds),
            log_level=e.get("LOG_LEVEL", d.log_level).upper(),
            dev_user_id=_int(e.get("DEV_USER_ID"), None),
        )

    @property
    def public_url(self) -> str:
        url = self.webapp_url.strip().rstrip("/")
        if not url and self.railway_public_domain:
            url = f"https://{self.railway_public_domain.strip()}"
        return url

    @property
    def miniapp_url(self) -> str:
        """Telegram only opens Mini Apps over HTTPS."""
        return f"{self.public_url}/app/" if self.public_url.startswith("https://") else ""


@lru_cache
def get_settings() -> Settings:
    load_dotenv(PROJECT_ROOT / ".env")
    return Settings.from_env()
