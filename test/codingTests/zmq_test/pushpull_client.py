import zmq
import sys

port = "5556"
#ip="localhost"
ip="*"
#ip="zitpcx19282.desy.de"

context = zmq.Context()
print "Connecting to server..."
socket = context.socket(zmq.PULL)
socket.bind ("tcp://"+ ip + ":%s" % port)


#  Do 10 requests, waiting each time for a response
for request in range (1,10):
    #  Get the reply.
    message = socket.recv_multipart()
    print "received reply ", request, "[", message, "]"
