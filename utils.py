#!/usr/bin/env python3

import os
import logging
import datetime
from calendar import timegm

logger = logging.getLogger(os.path.basename(__file__))


def get_next_months(N):
    " get next N+1 months starting from current month in GMT "
    months = []

    this_month = datetime.datetime.utcnow().replace(
        microsecond=0, second=0, minute=0, hour=0, day=1)

    t = list(this_month.timetuple())
    months.append(datetime.date.fromtimestamp(timegm(tuple(t))))

    for _ in range(N):

        if 1 <= t[1] <= 11:
            t[1] += 1
        else:
            t[1] = 1
            t[0] += 1

        months.append(datetime.date.fromtimestamp(timegm(tuple(t))))

    return months


def get_month_pairs(N):
    month_pairs = []
    months = get_next_months(N)
    for start, end in zip(months[:-1], months[1:]):
        month_pairs.append((start, end))

    return month_pairs


async def forever(f):
    while True:
        try:
            await f()
        except Exception as e:
            logger.exception(e)
