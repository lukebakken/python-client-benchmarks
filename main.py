import optparse
import time
import random
import uuid


def pika_setup():
    import pika

    conn = pika.BlockingConnection(pika.ConnectionParameters())
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


parser = optparse.OptionParser()
parser.add_option(
    "-C",
    "--pmessages",
    dest="msgcount",
    default=10000,
    type="int",
    help="producer message count",
)
parser.add_option(
    "-s",
    "--size",
    dest="msgsize",
    default=1024,
    type="int",
    help="message size in bytes",
)
(options, args) = parser.parse_args()

body = random.randbytes(options.msgsize)

args = pika_setup()
print("start: %s" % (time.ctime(time.time())))
pika_publish(args, body, options.msgcount)
print("end: %s" % (time.ctime(time.time())))
