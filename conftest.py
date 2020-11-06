#!/usr/bin/env python3

import asyncpg
import pytest
from database import create_database, drop_database, database_exists
import settings

settings.DATABASE_URL = 'postgres://localhost/aiven_test'


@pytest.fixture
async def database():
    url = settings.DATABASE_URL
    if await database_exists(url):
        await drop_database(url)
    await create_database(url)
    conn = await asyncpg.connect(dsn=url)
    yield conn
    await conn.close()
