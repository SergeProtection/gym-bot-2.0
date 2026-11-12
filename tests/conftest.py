import pytest_asyncio

from pathlib import Path
from gymbot.db import Base, Database
from gymbot.services import catalog, users

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest_asyncio.fixture
async def db():
    database = Database("sqlite+aiosqlite:///:memory:")
    async with database.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with database.session() as s:
        await catalog.sync_catalog(s, PROJECT_ROOT / "Exercise")
        await users.upsert_user(s, 1, first_name="Tester")
    yield database
    await database.dispose()
