# -*- coding: utf-8 -*-
import argparse
import functools
import timeit
import random
import uuid


def pika_publish(host, port, body, count):
    import pika

    qname = str(uuid.uuid4())
    param = pika.ConnectionParameters(host=host, port=port)
    conn = pika.BlockingConnection(param)
    channel = conn.channel()
    channel.queue_declare(queue=qname, auto_delete=True, exclusive=True)
    for _ in range(1, count):
        channel.basic_publish(
            exchange="", routing_key=qname, body=body, mandatory=False
        )
    channel.close()
    conn.close()


def aio_pika_publish(host, port, body, count):
    import asyncio
    import aio_pika
    import aio_pika.abc

    qname = str(uuid.uuid4())

    async def do_publish(loop, body, count):
        connection: aio_pika.RobustConnection = await aio_pika.connect_robust(
            host=host, port=port, loop=loop
        )
        channel: aio_pika.abc.AbstractChannel = await connection.channel(publisher_confirms=False)
        q = await channel.declare_queue(name=qname, auto_delete=True, exclusive=True)
        aio_msg = aio_pika.Message(body=body)
        for _ in range(1, count):
            await channel.default_exchange.publish(
                aio_msg, routing_key=q.name, mandatory=False
            )
        await channel.close()
        await connection.close()

    loop = asyncio.new_event_loop()
    loop.run_until_complete(do_publish(loop, body, count))
    loop.close()


def aiorabbit_publish(host, port, body, count):
    import asyncio
    import aiorabbit

    qname = str(uuid.uuid4())
    uri = "amqp://guest:guest@{0}:{1}/%2f".format(host, port)

    async def do_publish(body, count):
        async with aiorabbit.connect(url=uri) as client:
            # await client.confirm_select()
            await client.queue_declare(queue=qname, auto_delete=True, exclusive=True)
            for _ in range(1, count):
                await client.publish(
                    exchange="", routing_key=qname, message_body=body, mandatory=False
                )

    loop = asyncio.new_event_loop()
    loop.run_until_complete(do_publish(body, count))
    loop.close()


def kombu_publish(host, port, body, count):
    # https://github.com/celery/kombu/blob/main/examples/complete_send.py
    from kombu import Connection, Exchange, Producer, Queue

    qname = str(uuid.uuid4())
    uri = "amqp://guest:guest@{0}:{1}/%2f".format(host, port)

    #: By default messages sent to exchanges are persistent (delivery_mode=2),
    #: and queues and exchanges are durable.
    exchange = Exchange("", type="direct")
    _ = Queue(qname, exchange, routing_key=qname, auto_delete=True, exclusive=True)

    with Connection(uri) as connection:
        producer = Producer(connection, exchange=exchange, routing_key=qname)
        for _ in range(1, count):
            producer.publish(
                body,
                content_type="application/octet-stream",
                content_encoding="binary",
                delivery_mode=1,
                mandatory=False,
            )


parser = argparse.ArgumentParser(add_help=False)
parser.add_argument(
    "-h",
    "--host",
    dest="host",
    default="shostakovich",
    help="RabbitMQ host name",
)
parser.add_argument(
    "-p",
    "--port",
    dest="port",
    default=5672,
    type=int,
    help="RabbitMQ port",
)
parser.add_argument(
    "-C",
    "--pmessages",
    dest="msgcount",
    default=100000,
    type=int,
    help="producer message count",
)
parser.add_argument(
    "-s",
    "--size",
    dest="msgsize",
    default=1024,
    type=int,
    help="message size in bytes",
)
args = parser.parse_args()

body = random.randbytes(args.msgsize)

pika_func = functools.partial(pika_publish, args.host, args.port, body, args.msgcount)
t = timeit.timeit(pika_func, number=1)
print("pika: publishing {0} messages took {1} seconds".format(args.msgcount, t))

aio_pika_func = functools.partial(
    aio_pika_publish, args.host, args.port, body, args.msgcount
)
t = timeit.timeit(aio_pika_func, number=1)
print("aio-pika: publishing {0} messages took {1} seconds".format(args.msgcount, t))

aiorabbit_func = functools.partial(
    aiorabbit_publish, args.host, args.port, body, args.msgcount
)
t = timeit.timeit(aiorabbit_func, number=1)
print("aiorabbit: publishing {0} messages took {1} seconds".format(args.msgcount, t))

kombu_func = functools.partial(kombu_publish, args.host, args.port, body, args.msgcount)
t = timeit.timeit(kombu_func, number=1)
print("kombu: publishing {0} messages took {1} seconds".format(args.msgcount, t))
