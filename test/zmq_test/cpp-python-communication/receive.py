import zmq
import sys
import json

port = "6000"
ip="localhost"
#ip="*"
#ip="zitpcx19282.desy.de"

context = zmq.Context()
print "Connecting to server..."
socket = context.socket(zmq.REQ)
socket.connect ("tcp://"+ ip + ":%s" % port)


#  Do 10 requests, waiting each time for a response
for request in range (1,10):
    # Send request
    socket.send("test")
    print "send request"


    #  Get the reply.
    message = socket.recv()
    print "received reply ", request, "[", message, "]"
    print "as json: ", json.loads(message)
