#!/usr/bin/env python3

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context
import settings

async def get_kafka_producer():

    context = create_ssl_context(
        cafile='./ca.pem',
        certfile='./service.cert',
        keyfile='./service.key'
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SSL",
        ssl_context=context)

    await producer.start()
    return producer


async def get_kafka_consumer():

    context = create_ssl_context(
        cafile='./ca.pem',
        certfile='./service.cert',
        keyfile='./service.key'
    )

    consumer = AIOKafkaConsumer(
        'topic1',
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SSL",
        client_id="demo-client-1",
        group_id="demo-group",
        ssl_context=context)

    await consumer.start()
    return consumer
