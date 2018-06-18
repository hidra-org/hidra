import zmq
import json
import os
import tempfile

ipc_path = os.path.join(tempfile.gettempdir(), "hidra")
current_pid = 12345

out_con_str = "ipc://{0}/{1}_{2}".format(ipc_path, current_pid, "out")

context = zmq.Context()
socket = context.socket(zmq.PULL)
socket.connect(out_con_str)

message = socket.recv_multipart()
print "Received - {0}".format(message)

socket.close()
context.destroy()
