#!/usr/bin/env python3

import asyncio
import asyncpg

from utils import get_month_pairs
import settings

N = 5


async def get_tables(conn):
    query = """
        SELECT tablename
          FROM pg_catalog.pg_tables
         WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"""
    result = await conn.fetch(query)
    return [r['tablename'] for r in result]

async def create_table(conn):
    with open('schema.sql') as fp:
        schema = fp.read()
        return await conn.execute(schema)

def get_ptable_name(date):
    return settings.DATABASE_TABLE + '_' + date.strftime('y%Ym%m')

async def create_ptable(conn, start, end):
    sql_template = "CREATE TABLE {ptable} PARTITION OF {table} FOR VALUES FROM ('{start}') TO ('{end}')"
    sql = sql_template.format(
        ptable=get_ptable_name(start),
        table=settings.DATABASE_TABLE,
        start=start.strftime('%Y-%m-%d'),
        end=end.strftime('%Y-%m-%d'))
    await conn.execute(sql)

async def create_ptables(conn, tables=None):
    for start, end in get_month_pairs(N):
        ptable_name = get_ptable_name(start)
        if ptable_name not in tables:
            await create_ptable(conn, start, end)

async def create_tables(conn):

    tables = await get_tables(conn)
    if settings.DATABASE_TABLE not in tables:
        await create_table(conn)
    await create_ptables(conn, tables)

async def save(conn, data):
    sql_template = "INSERT INTO {table} (url, error, status_code, response_time, text) VALUES($1, $2, $3, $4, $5)"
    sql = sql_template.format(table=settings.DATABASE_TABLE)
    await conn.execute(sql,
        data['url'],
        data.get('error'),
        data.get('status_code'),
        data.get('response_time'),
        data.get('text'))
