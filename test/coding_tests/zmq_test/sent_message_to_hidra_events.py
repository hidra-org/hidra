import zmq
import json
import os
import time

BASE_PATH = "/opt/hidra"

con_str = "tcp://131.169.185.121:50100"

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect(con_str)
print "Socket connecting to {0}".format(con_str)

metadata = {
    "source_path": os.path.join(BASE_PATH, "data", "source"),
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
