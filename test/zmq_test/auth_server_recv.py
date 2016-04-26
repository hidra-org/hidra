import zmq
from zmq.auth.thread import ThreadAuthenticator
import time
import sys

port = "5556"
ip = "*"
#ip="zitpcx19282.desy.de"

context = zmq.Context()
socket = context.socket(zmq.PULL)
socket.zap_domain = b'global'
socket.bind("tcp://" + ip + ":%s" % port)

auth = ThreadAuthenticator(context)
#whitelist = "131.169.185.121"
whitelist = "131.169.185.34"
auth.start()
auth.allow(whitelist)


try:
    while True:
        message = socket.recv_multipart()
        print "received reply ", message
except KeyboardInterrupt:
    pass
finally:
    auth.stop()
