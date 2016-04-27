import zmq
from zmq.auth.thread import ThreadAuthenticator
import time
import sys

port = "5556"
ip = "*"
#ip="zitpcx19282.desy.de"

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.zap_domain = b'global'
socket.bind("tcp://" + ip + ":%s" % port)

auth = ThreadAuthenticator(context)
auth.start()

whitelist = ["131.169.185.34", "131.169.185.121"]
for host in whitelist:
    auth.allow(host)


while True:
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
    finally:
        auth.stop()
