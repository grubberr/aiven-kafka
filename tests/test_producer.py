#!/usr/bin/env python3

import re
import json
from contextlib import asynccontextmanager
import unittest.mock
import pytest
from producer import checker


class FakeResponse:
    def __init__(self, status, text):
        self.status = status
        self._text = text

    def __call__(self):
        return self

    async def text(self):
        return self._text


class FakeSession:
    def __init__(self, *, response=None, exception=None):
        self.response = response
        self.exception = exception

    @asynccontextmanager
    async def get(self, *args, **kwargs):
        try:
            if self.exception:
                raise self.exception
            yield self.response
        finally:
            pass


@pytest.mark.asyncio
async def test_producer():
    mock_producer = unittest.mock.AsyncMock()

    session = FakeSession(exception=Exception('Cannot connect to host'))
    await checker(mock_producer, session, 'http://microsoft.com', {})

    assert mock_producer.send_and_wait.call_args_list[0][0][0] == 'topic1'
    message = mock_producer.send_and_wait.call_args_list[0][0][1]
    d = json.loads(message.decode('utf-8'))
    assert d.pop('url') == 'http://microsoft.com'
    assert d.pop('error') == 'Cannot connect to host'
    assert len(d) == 0

    session = FakeSession(response=FakeResponse(status=200, text="I'm feeling lucky"))

    await checker(
        mock_producer,
        session,
        'http://google.com',
        {'regex': re.compile(r'feeling\s+lucky')})

    assert mock_producer.send_and_wait.call_args_list[1][0][0] == 'topic1'
    message = mock_producer.send_and_wait.call_args_list[1][0][1]
    d = json.loads(message.decode('utf-8'))
    assert d.pop('url') == 'http://google.com'
    assert 'error' not in d
    assert d.pop('status_code') == 200
    assert d.pop('response_time')
    assert d.pop('text') == 'feeling lucky'
    assert len(d) == 0
