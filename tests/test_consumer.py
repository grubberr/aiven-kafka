#!/usr/bin/env python3

import json
from types import SimpleNamespace
import pytest
import consumer
import settings


class FakeKafkaConsumer:

    def __init__(self, topic):
        self.topic = topic
        self.batches = []

    def add(self, batch):
        if not isinstance(batch, list):
            batch = [batch]
        self.batches.append(batch)

    async def getmany(self, *args, **kwargs):
        batch = self.batches.pop(0) if self.batches else []
        batch = [SimpleNamespace(value=json.dumps(i).encode('utf-8')) for i in batch]
        return {self.topic: batch}

    async def __aenter__(self):
        pass
    async def __aexit__(self, *args):
        pass


@pytest.mark.asyncio
async def test_consumer(monkeypatch, database):

    kafka_consumer = FakeKafkaConsumer(settings.KAFKA_TOPIC)
    kafka_consumer.add([
        dict(url='http://google.com', status_code=200, response_time=0.2),
        dict(url='http://twitter.com', status_code=301, response_time=0.3, text='some text'),
        dict(url='http://microsoft.com', error='connection error'),
        {}
    ])

    async def get_kafka_consumer():
        return kafka_consumer
    monkeypatch.setattr(consumer, 'get_kafka_consumer', get_kafka_consumer)

    await consumer.consumer(stop_on_error=True)

    res = await database.fetch('SELECT * FROM checks')

    assert res[0]['url'] == 'http://google.com'
    assert res[0]['error'] is None
    assert res[0]['response_time'] == 0.2
    assert res[0]['status_code'] == 200
    assert res[0]['text'] is None

    assert res[1]['url'] == 'http://twitter.com'
    assert res[1]['error'] is None
    assert res[1]['response_time'] == 0.3
    assert res[1]['status_code'] == 301
    assert res[1]['text'] == 'some text'

    assert res[2]['url'] == 'http://microsoft.com'
    assert res[2]['error'] == 'connection error'
    assert res[2]['response_time'] is None
    assert res[2]['status_code'] is None
    assert res[2]['text'] is None
