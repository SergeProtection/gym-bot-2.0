import os
from importlib import import_module

from telegram import Update

from app.core.logging import setup_logging


def run_bot() -> None:
    setup_logging()

    legacy_main = import_module("main")
    app = legacy_main.build_application()
    legacy_main.print_deployment_instructions()

    use_webhook = os.getenv("GYMBOT_USE_WEBHOOK", "false").strip().lower() == "true"
    if use_webhook:
        webhook_url = os.getenv("GYMBOT_WEBHOOK_URL", "").strip()
        if not webhook_url:
            raise RuntimeError("GYMBOT_WEBHOOK_URL is required when GYMBOT_USE_WEBHOOK=true.")

        listen = os.getenv("GYMBOT_WEBHOOK_LISTEN", "0.0.0.0").strip()
        port = int(os.getenv("PORT", "8080"))
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        url_path = os.getenv("GYMBOT_WEBHOOK_PATH", token)

        app.run_webhook(
            listen=listen,
            port=port,
            url_path=url_path,
            webhook_url=f"{webhook_url.rstrip('/')}/{url_path}",
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)
