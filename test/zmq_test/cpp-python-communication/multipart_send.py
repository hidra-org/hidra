import zmq
import sys
import time

port = "6000"
ip="localhost"
#ip="*"
#ip="zitpcx19282.desy.de"

context = zmq.Context()
print "Connecting to server..."
socket = context.socket(zmq.PUSH)
socket.connect ("tcp://"+ ip + ":%s" % port)


#  Do 10 requests, waiting each time for a response
for nr in range (1,10):
    # Send request
    header = '{ "filePart": %d, "filename": "' + str(nr) + '.cbf" }'
    body = "asdfasdfasdfsdfasd_" + str(nr)
    message = [header, body]
    socket.send_multipart (message)
    print "send multipart", message

time.sleep(5)
socket.close()
context.destroy()
