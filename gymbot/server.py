"""Process entrypoint: migrations, then the API and the bot in one event loop."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from alembic import command
from alembic.config import Config

from gymbot.api.app import create_app
from gymbot.config import PROJECT_ROOT, Settings, get_settings
from gymbot.db import Database
from gymbot.services import catalog

log = logging.getLogger("gymbot")


def run_migrations(database_url: str) -> None:
    cfg = Config(str(PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    cfg.attributes["configure_logging"] = False
    command.upgrade(cfg, "head")


def build(settings: Settings):
    db = Database(settings.database_url)
    bot_app = None
    if settings.run_bot and settings.telegram_bot_token:
        from gymbot.bot.handlers import build_application

        bot_app = build_application(settings, db)
    elif settings.run_bot:
        log.warning("TELEGRAM_BOT_TOKEN is not set: running the API only")

    @asynccontextmanager
    async def lifespan(app):
        async with db.session() as session:
            await catalog.sync_catalog(session, settings.exercise_dir)
        if bot_app is not None:
            await bot_app.initialize()
            if bot_app.post_init:
                await bot_app.post_init(bot_app)
            await bot_app.start()
            await bot_app.updater.start_polling(allowed_updates=["message", "callback_query"])
            app.state.bot_username = bot_app.bot.username
            log.info("Bot polling started (@%s)", bot_app.bot.username)
        if settings.miniapp_url:
            log.info("Mini App: %s", settings.miniapp_url)
        else:
            log.info("Mini App button disabled (set WEBAPP_URL to an https:// address)")
        try:
            yield
        finally:
            if bot_app is not None:
                await bot_app.updater.stop()
                await bot_app.stop()
                await bot_app.shutdown()
            await db.dispose()

    return create_app(settings, db, lifespan=lifespan)


def main() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if settings.run_migrations:
        run_migrations(settings.database_url)
    uvicorn.run(build(settings), host=settings.host, port=settings.port, log_level=settings.log_level.lower(), proxy_headers=True)


if __name__ == "__main__":
    main()
