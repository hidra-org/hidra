from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import socket
import sys
import zmq

port = "50300"
ip=socket.gethostbyname(socket.gethostname())

context = zmq.Context()
print("Connecting to server...")
socket = context.socket(zmq.PULL)
socket.bind("tcp://{0}:{1}".format(ip, port))

print("using zmq version", zmq.__version__)

for request in range (1,10):
    message = socket.recv()
    print("received reply", request, message)
