from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from kafka import KafkaConsumer
import json


def main():
    topic = "kuhnm_test"
    server = ["asap3-events-01", "asap3-events-02"]

    consumer = KafkaConsumer(topic, bootstrap_servers=server)

    # while True:
    #    msg = next(consumer)
    #    print(msg)

    for message in consumer:
        # message value is raw byte string -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
        #                                      message.offset, message.key,
        #                                      message.value))
        msg = json.loads(message.value.decode('utf-8'))
        print(msg['path'])


if __name__ == "__main__":
    main()
