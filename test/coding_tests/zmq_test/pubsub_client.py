import zmq
import sys

port = "5556"
#ip="localhost"
ip="*"
#ip="zitpcx19282.desy.de"

context = zmq.Context()
print "Connecting to server..."
socket = context.socket(zmq.SUB)
socket.bind ("tcp://"+ ip + ":%s" % port)

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
