import os
import random
import socket as m_socket
import sys
import time
import tempfile
import zmq

port = "5556"
ip = m_socket.getfqdn(m_socket.gethostname())

context = zmq.Context()
socket = context.socket(zmq.PUB)

ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
endpoint = "ipc://{}:{}".format(ipc_dir, "pubsub")
#endpoint = "tcp://{}:{}".format(ip, port)

socket.connect(endpoint)
time.sleep(0.1)

while True:
    topic = random.randrange(9999,10005)
    messagedata = random.randrange(1,215) - 80
    print "%d %d" % (topic, messagedata)

    message = [str(topic), str(messagedata)]
#    socket.send("%d %d" % (topic, messagedata))
    socket.send_multipart(message)

    print "sleeping..."
    time.sleep (1)
    print "sleeping...done"
