import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class AppSettings:
    app_mode: str
    host: str
    port: int
    gymbot_db_path: str
    telegram_bot_token: str


def get_settings() -> AppSettings:
    app_mode = os.getenv("APP_MODE", "bot").strip().lower()
    if app_mode not in {"bot", "api"}:
        app_mode = "bot"

    port_raw = os.getenv("PORT", "8080").strip()
    try:
        port = int(port_raw)
    except ValueError:
        port = 8080

    return AppSettings(
        app_mode=app_mode,
        host=os.getenv("APP_HOST", "0.0.0.0").strip(),
        port=port,
        gymbot_db_path=os.getenv("GYMBOT_DB_PATH", "gymbot.db").strip(),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
    )
