#!/usr/bin/env python3

import time
import asyncio
import random

import pytest
from scheduler import Scheduler


class MockTime:
    def __init__(self, start):
        self.time = start

    def monotonic(self):
        return self.time

    async def sleep(self, n):
        self.time += n
        return None


@pytest.mark.asyncio
async def test_scheduler(monkeypatch):
    scheduler = Scheduler()
    mock_time = MockTime(start=1)

    with monkeypatch.context() as m:
        m.setattr(time, 'monotonic', mock_time.monotonic)
        m.setattr(random, 'random', lambda :1)
        m.setattr(asyncio, 'sleep', mock_time.sleep)

        await scheduler.add('http:/test1.com', delay=6)
        await scheduler.add('http:/test2.com', delay=10)

        G = scheduler.gen()
        url, _ = await G.__anext__()
        assert url == 'http:/test1.com'
        url, _ = await G.__anext__()
        assert url == 'http:/test2.com'
        url, _ = await G.__anext__()
        assert url == 'http:/test1.com'
        url, _ = await G.__anext__()
        assert url == 'http:/test1.com'
        url, _ = await G.__anext__()
        assert url == 'http:/test2.com'
