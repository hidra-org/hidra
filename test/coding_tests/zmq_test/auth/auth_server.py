from __future__ import print_function

import zmq
from zmq.auth.thread import ThreadAuthenticator
import time
import socket


def main():
    port = "5556"
    socket_ip = "*"
    # ip = socket.getfqdn()

    context = zmq.Context()
    auth = ThreadAuthenticator(context)
    auth.start()

    whitelist = [socket.getfqdn()]
    for host in whitelist:
        hostname, tmp, ip = socket.gethostbyaddr(host)
        auth.allow(ip[0])

    zmq_socket = context.socket(zmq.PUSH)
    zmq_socket.zap_domain = b'global'
    zmq_socket.bind("tcp://" + socket_ip + ":%s" % port)

    try:
        for i in range(5):
            message = ["World"]
            print("Send: ", message)
            res = zmq_socket.send_multipart(message, copy=False, track=True)
            if res.done:
                print("res: done")
            else:
                print("res: waiting")
                res.wait()
                print("res: waiting...")
            print("sleeping...")
            if i == 1:
                auth.stop()
                zmq_socket.close(0)

                auth.start()
#                ip = socket.gethostbyaddr(socket.getfqdn())[2]
#                auth.allow(ip[0])
                ip = socket.gethostbyaddr(socket.getfqdn())[2]
                auth.deny(ip[0])
                zmq_socket = context.socket(zmq.PUSH)
                zmq_socket.zap_domain = b'global'
                zmq_socket.bind("tcp://" + socket_ip + ":%s" % port)
            time.sleep(1)
            print("sleeping...done")
            i += 1
    finally:
        auth.stop()


if __name__ == "__main__":
    main()
