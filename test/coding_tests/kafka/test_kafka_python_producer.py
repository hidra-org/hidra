from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from kafka import KafkaProducer
import json

topic = "kuhnm_test"
server = ["asap3-events-01", "asap3-events-02"]

producer = KafkaProducer(
    bootstrap_servers=server,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

message = {
    "finishTime": 1556031799.7914205,
    "inotifyTime": 1556031799.791173,
    "md5sum": "",
    "operation":"copy",
    "path": "/my_dir/my_subdir/file.test",
    "retries":1,
    "size":43008,
    "source":"./my_subdir/file.test"
}

for _ in range(1):
    future = producer.send(topic, message)
    result = future.get(timeout=60)
