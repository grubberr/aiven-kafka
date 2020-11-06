#!/usr/bin/env python3

import os
import time
import random
import logging
import asyncio
from collections import defaultdict

logger = logging.getLogger(os.path.basename(__file__))


class Scheduler:

    DEFAULT_DELAY = 60

    def __init__(self):
        self.urls = defaultdict(dict)
        self.queue = asyncio.PriorityQueue()

    def _get_delay(self, delay):
        if not delay:
            if delay == 0:
                logger.warning(
                    'delay cannot be 0, setting delay = %d',
                    self.DEFAULT_DELAY)
            return self.DEFAULT_DELAY
        return delay

    async def add(self, url, delay=None, **kwargs):
        " add new url to scheduler "
        delay = self._get_delay(delay)
        if url in self.urls:
            self.urls[url]['delay'] = delay
            self.urls[url].update(kwargs)
            logger.info("URL '%s' updated, delay = %d", url, delay)
        else:
            self.urls[url]['delay'] = delay
            self.urls[url].update(kwargs)
            priority = time.monotonic() + random.random() * delay
            entry = [priority, url]
            await self.queue.put(entry)

    async def remove(self, url):
        " remove url from scheduler "
        raise NotImplementedError

    async def gen(self):
        " iterate over all urls "
        while True:

            entry = await self.queue.get()
            [priority, url] = entry

            current_time = time.monotonic()
            if priority >= current_time:
                await asyncio.sleep(priority - current_time)

            yield url, self.urls[url]

            priority = time.monotonic() + self.urls[url]['delay']
            entry = [priority, url]
            await self.queue.put(entry)
