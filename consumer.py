#!/usr/bin/env python3

import json
import logging
import asyncio
import asyncpg

from aiven_kafka import get_kafka_consumer
from database import create_tables, save
import settings


def get_data(value):

    try:
        data = json.loads(value.decode('utf-8'))
    except ValueError as e:
        logging.exception(e)

    if 'url' in data:
        return data

async def main():

    consumer = await get_kafka_consumer()

    conn = await asyncpg.connect(dsn=settings.DATABASE_URL)
    await create_tables(conn)

    async for msg in consumer:
        data = get_data(msg.value)
        asyncio.create_task(save(conn, data))

    await consumer.stop()
    await conn.close()


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
