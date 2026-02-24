# GymBot Deployment

This folder supports two runtime modes through one entrypoint:

- `APP_MODE=bot` -> Telegram bot service
- `APP_MODE=api` -> FastAPI service

Entrypoint: `python v2/app_runner.py`

## 1) Local Run

Install dependencies:

```powershell
pip install -r requirements.txt
```

Set environment variables (copy `v2/.env.example` to `.env` and fill values), then:

```powershell
python v2/app_runner.py
```

## 2) Railway Bot Service

Create a Railway service from this repo and set start command to:

```powershell
python v2/app_runner.py
```

Then set:

- `APP_MODE=bot`
- `TELEGRAM_BOT_TOKEN=<your_bot_token>`
- `GYMBOT_DB_PATH=/data/gymbot.db`
- Optional reminder vars:
  - `GYMBOT_REMINDER_HOUR_UTC`
  - `GYMBOT_REMINDER_MINUTE_UTC`

If using webhook mode:

- `GYMBOT_USE_WEBHOOK=true`
- `GYMBOT_WEBHOOK_URL=https://<your-domain>`

Attach a persistent volume and mount it to `/data` for SQLite persistence.

## 3) Railway API Service

Create another Railway service from the same repo and set start command to:

```powershell
python v2/app_runner.py
```

Then set:

- `APP_MODE=api`
- `GYMBOT_DB_PATH=/data/gymbot.db`

API endpoints:

- `GET /health`
- `GET /summary/today/{user_id}`
- `GET /summary/week/{user_id}`
- `GET /summary/month/{user_id}?month=YYYY-MM`
- `GET /summary/period/{user_id}?start=YYYY-MM-DD&end=YYYY-MM-DD`

## 4) Important Note (SQLite)

If bot and API run as separate Railway services, each service needs access to the same persistent data store.

For true multi-service production, migrate to PostgreSQL next. This modular structure is ready for that migration.
