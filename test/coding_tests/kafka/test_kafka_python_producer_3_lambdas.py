from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import json
from kafka import KafkaProducer
import threading
import time

topic = "kuhnm_test2"
server = ["asap3-events-01", "asap3-events-02"]

class LambdaSimulator(threading.Thread):

    def __init__(self, server, topic, detid):
        super().__init__()

        self.detid = detid

        self.producer = KafkaProducer(
            bootstrap_servers=server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def run(self):

        filename = "{}_{:03}.h5"

        message = {
            "finishTime": 1556031799.7914205,
            "inotifyTime": 1556031799.791173,
            "md5sum": "",
            "operation":"copy",
            "path": "/my_dir/current/raw/my_subdir/",
            "retries":1,
            "size":43008,
            "source":"./current/raw/my_subdir/"
        }

        for i in range(1):
            message["path"] += filename.format(self.detid, i)
            message["source"] += filename.format(self.detid, i)
            future = self.producer.send(topic, message)
            future.get(timeout=60)

if __name__ == "__main__":
    n_det = 3

    lambdas = []

    for i in range(n_det):
        lambdas.append(LambdaSimulator(server, topic, "DET{}".format(i)))

    for l in lambdas:
        l.start()

    for l in lambdas:
        l.join()
#    while True:
#        time.sleep(1)
