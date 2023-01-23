# -*- coding: utf-8 -*-
import argparse

# import logging
import time
import random
import uuid

# LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
# LOGGER = logging.getLogger(__name__)
# logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


def pika_setup(host, port):
    import pika

    param = pika.ConnectionParameters(host=host, port=port)
    conn = pika.BlockingConnection(param)
    channel = conn.channel()
    qname = str(uuid.uuid4())
    channel.queue_declare(queue=qname, auto_delete=True, exclusive=True)
    return (conn, channel, qname)


def pika_publish(args, body, count):
    (conn, channel, qname) = args
    for _ in range(1, count):
        channel.basic_publish(exchange="", routing_key=qname, body=body)
    channel.close()
    conn.close()


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

pika_data = pika_setup(args.host, args.port)
print("start: %s" % (time.ctime(time.time())))
pika_publish(pika_data, body, args.msgcount)
print("end: %s" % (time.ctime(time.time())))
