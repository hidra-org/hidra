from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import time
import zmq

BASE_DIR = "/opt/hidra"

endpoint = "tcp://131.169.185.121:50100"

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect(endpoint)
print("Socket connecting to {}".format(endpoint))

metadata = {
    "source_path": os.path.join(BASE_DIR, "data", "source"),
    "relative_path": os.sep + "local",
    "filename": "100.cbf",
    "filesize": 12345,
    "file_mod_time": time.time(),
    "file_create_time": time.time(),
    "chunksize": 10485760,
    "chunk_number": 0,
}

socket.send_multipart([json.dumps(metadata).encode("utf-8"), b"incoming_data"])

socket.close()
context.destroy()
