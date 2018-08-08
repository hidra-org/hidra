import zmq
import time
import sys
import random

port = "5556"
#ip = "*"
ip="zitpcx19282.desy.de"

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://" + ip + ":%s" % port)
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
