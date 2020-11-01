#!/usr/bin/env python3

import json
import logging
import asyncio
import asyncpg
import uvloop

from aiven_kafka import get_kafka_consumer
from database import create_tables, create_ptables, save
import settings


def get_data(value):

    try:
        data = json.loads(value.decode('utf-8'))
    except ValueError as e:
        logging.exception(e)

    if 'url' in data:
        return data


async def create_ptables_worker(conn_pool):
    " periodically trying to create new partition tables if needed "
    while True:
        await asyncio.sleep(86400)
        try:
            async with conn_pool.acquire() as conn:
                await create_ptables(conn)
        except Exception as e:
            logging.exception(e)


async def main():

    consumer = await get_kafka_consumer()

    conn_pool = await asyncpg.create_pool(dsn=settings.DATABASE_URL)
    async with conn_pool.acquire() as conn:
        await create_tables(conn)

    asyncio.create_task(create_ptables_worker(conn_pool))

    async for msg in consumer:
        data = get_data(msg.value)
        asyncio.create_task(save(conn_pool, data))

    await consumer.stop()
    await conn.close()


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    uvloop.install()
    asyncio.run(main())
