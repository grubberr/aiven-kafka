#!/usr/bin/env python3

import time
import json
import logging
import asyncio
import aiohttp
import uvloop

from url_parser import url_parser
from scheduler import Scheduler
from aiven_kafka import get_kafka_producer
import settings


async def fetch(session, url):

    start = time.monotonic()
    async with session.get(url, allow_redirects=False) as response:
        text = await response.text()
        end = time.monotonic()
        return (response.status, text, end - start)


async def checker(kafka_producer, session, url, url_dict):

    result = {
        'url': url
    }

    try:
        response_status, response_text, response_time = await fetch(session, url)
    except Exception as e:
        result['error'] = str(e)
    else:
        result['status_code'] = response_status
        result['response_time'] = response_time
        if 'regex' in url_dict:
            m = url_dict['regex'].search(response_text)
            if m:
                result['text'] = m.group()

    message = json.dumps(result)
    await kafka_producer.send_and_wait(settings.KAFKA_TOPIC, message.encode('utf-8'))


async def main():

    kafka_producer = await get_kafka_producer()

    scheduler = Scheduler()
    for url_item in url_parser('urls.yaml'):
        await scheduler.add(**url_item)

    async with aiohttp.ClientSession() as session:
        async for url, url_dict in scheduler.gen():
            asyncio.create_task(checker(kafka_producer, session, url, url_dict))


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    uvloop.install()
    asyncio.run(main())
