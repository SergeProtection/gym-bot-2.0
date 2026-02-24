from app.core.config import get_settings
from app.core.logging import setup_logging


def run_api() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run("app.api.main:app", host=settings.host, port=settings.port, reload=False)


def run_bot() -> None:
    from app.bot.runner import run_bot as start_bot

    start_bot()


def main() -> None:
    setup_logging()
    settings = get_settings()

    if settings.app_mode == "api":
        run_api()
    else:
        run_bot()


if __name__ == "__main__":
    main()
