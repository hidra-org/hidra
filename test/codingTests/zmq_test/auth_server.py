import zmq
from zmq.auth.thread import ThreadAuthenticator
import time
import sys
import socket

port = "5556"
socket_ip = "*"
#ip="zitpcx19282.desy.de"

context = zmq.Context()
auth = ThreadAuthenticator(context)
auth.start()


#whitelist = ["131.169.185.34", "131.169.185.121"]
whitelist = ["zitpcx22614", "zitpcx19282"]
for host in whitelist:
    hostname, tmp, ip = socket.gethostbyaddr(host)
    auth.allow(ip[0])

zmq_socket = context.socket(zmq.PUSH)
zmq_socket.zap_domain = b'global'
zmq_socket.bind("tcp://" + socket_ip + ":%s" % port)


try:
#    while True:
    for i in range(5):
        message = ["World"]
        print "Send: ", message
        res = zmq_socket.send_multipart(message, copy=False, track=True)
        if res.done:
            print "res: done"
        else:
            print "res: waiting"
            res.wait()
            print "res: waiting..."
        print "sleeping..."
        if i == 1:
            auth.stop()
            zmq_socket.close(0)

            auth.start()
#            ip = socket.gethostbyaddr("zitpcx22614")[2]
#            auth.allow(ip[0])
            # returns (hostname, aliaslist, ipaddrlist)
            ip = socket.gethostbyaddr("zitpcx19282")[2]
            auth.deny(ip[0])
            zmq_socket = context.socket(zmq.PUSH)
            zmq_socket.zap_domain = b'global'
            zmq_socket.bind("tcp://" + socket_ip + ":%s" % port)
        time.sleep (1)
        print "sleeping...done"
        i += 1
finally:
    auth.stop()
