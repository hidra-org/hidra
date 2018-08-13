import socket as socket_m
import sys
import time
import zmq

port = "5556"
#ip="localhost"
#ip="*"
ip="haso111k.desy.de"
host = socket_m.getfqdn()

context = zmq.Context()
print "Connecting to server..."
socket = context.socket(zmq.PUSH)
socket.connect("tcp://"+ ip + ":" + port)


#  Do 10 requests, waiting each time for a response
for request in range (1,10):
    try:
        message = ["World"]
        print "Send: ", message
        res = socket.send_multipart(message, copy=False, track=True)
        if res.done:
            print "res: done"
        else:
            print "res: waiting"
            res.wait()
            print "res: waiting..."
        print "sleeping..."
        time.sleep (1)
        print "sleeping...done"

    except:
        break
