#!/usr/bin/env python3

from urllib.parse import urlsplit
import asyncpg
from utils import get_month_pairs
import settings


TABLE_SCHEMA = """
CREATE TABLE {} (
    url character varying(1024) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    error character varying(1024),
    status_code integer,
    response_time double precision,
    text character varying(1024)
) PARTITION BY RANGE (created_at);
""".format(settings.DATABASE_TABLE)


async def get_tables(conn):
    query = """
        SELECT tablename
          FROM pg_catalog.pg_tables
         WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"""
    result = await conn.fetch(query)
    return [r['tablename'] for r in result]

async def create_table(conn):
    return await conn.execute(TABLE_SCHEMA)

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
    if tables is None:
        tables = await get_tables(conn)
    for start, end in get_month_pairs(settings.WARM_UP_PARTITIONS):
        ptable_name = get_ptable_name(start)
        if ptable_name not in tables:
            await create_ptable(conn, start, end)

async def create_tables(conn):

    tables = await get_tables(conn)
    if settings.DATABASE_TABLE not in tables:
        await create_table(conn)
    await create_ptables(conn, tables)

async def save(conn_pool, batch):
    sql_template = "INSERT INTO {table} (url, error, status_code, response_time, text) VALUES($1, $2, $3, $4, $5)"
    sql = sql_template.format(table=settings.DATABASE_TABLE)
    async with conn_pool.acquire() as conn:
        params = [(
            data['url'],
            data.get('error'),
            data.get('status_code'),
            data.get('response_time'),
            data.get('text')) for data in batch]
        await conn.executemany(sql, params)

async def drop_database(url):
    res = urlsplit(url)
    database = res.path.lstrip('/')
    res = res._replace(path='/' + 'postgres')
    url = res.geturl()
    conn = await asyncpg.connect(dsn=url)
    await conn.execute('DROP DATABASE "{}"'.format(database))

async def create_database(url):
    res = urlsplit(url)
    database = res.path.lstrip('/')
    res = res._replace(path='/' + 'postgres')
    url = res.geturl()
    conn = await asyncpg.connect(dsn=url)
    await conn.execute('CREATE DATABASE "{}"'.format(database))

async def database_exists(url):
    res = urlsplit(url)
    database = res.path.lstrip('/')
    res = res._replace(path='/' + 'postgres')
    url = res.geturl()
    conn = await asyncpg.connect(dsn=url)
    row = await conn.fetchrow('SELECT 1 FROM pg_database WHERE datname = $1', database)
    return bool(row)
