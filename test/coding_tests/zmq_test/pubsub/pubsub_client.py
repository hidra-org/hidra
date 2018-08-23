import sys
import os
import tempfile
import zmq

port = "5556"
ip="*"

context = zmq.Context()
print "Connecting to server..."
socket = context.socket(zmq.SUB)

ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
endpoint = "ipc://{}:{}".format(ipc_dir, "pubsub")
#endpoint = "tcp://{}:{}".format(ip, port)

socket.bind(endpoint)

socket.setsockopt(zmq.SUBSCRIBE, "10001")
socket.setsockopt(zmq.SUBSCRIBE, "10002")

#  Do 10 requests, waiting each time for a response
for request in range (1,5):
    #  Get the reply.
    topic, messagedata = socket.recv_multipart()
#    message = socket.recv()
#    topic, messagedata = message.split()
    print "received reply ", request, "[", topic, messagedata, "]"
#    message = socket.recv_multipart()
#    print "received reply ", request, "[", message, "]"
