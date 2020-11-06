#!/usr/bin/env python3

import os
import json
import time
import logging
import asyncio
import aiojobs
import asyncpg
import uvloop

from utils import forever
from aiven_kafka import get_kafka_consumer
from database import create_tables, create_ptables, save
import settings

logger = logging.getLogger(os.path.basename(__file__))


def get_data(value):

    try:
        data = json.loads(value.decode('utf-8'))
    except ValueError as e:
        logger.exception(e)
        return

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
            logger.exception(e)


async def iter_batches(consumer, timeout, max_size, stop_on_error=False):

    batch = []
    start = time.monotonic()

    while True:

        consumer_timeout = 86400
        if batch:
            consumer_timeout = max(start + timeout - time.monotonic(), 0)

        records = await consumer.getmany(timeout_ms=consumer_timeout * 1000)
        for _, messages in records.items():
            for message in messages:
                data = get_data(message.value)
                if data:
                    batch.append(data)
                    if len(batch) == max_size:
                        yield batch
                        batch = []
                        start = time.monotonic()
                elif stop_on_error:
                    if len(batch) > 0:
                        yield batch
                    await asyncio.sleep(1)
                    return

        if len(batch) > 0 and time.monotonic() - start > timeout:
            yield batch
            batch = []
            start = time.monotonic()


async def consumer(stop_on_error=False):

    kafka_consumer = await get_kafka_consumer()
    conn_pool = await asyncpg.create_pool(dsn=settings.DATABASE_URL)

    async with kafka_consumer, conn_pool:
        async with conn_pool.acquire() as conn:
            await create_tables(conn)

        scheduler = await aiojobs.create_scheduler()
        try:
            await scheduler.spawn(create_ptables_worker(conn_pool))

            async for batch in iter_batches(kafka_consumer, timeout=5, max_size=20, stop_on_error=stop_on_error):
                await scheduler.spawn(save(conn_pool, batch))
        finally:
            await scheduler.close()


async def main():
    await forever(consumer)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    uvloop.install()
    asyncio.run(main())
