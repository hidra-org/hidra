from __future__ import print_function

import zmq
from zmq.auth.thread import ThreadAuthenticator
import socket as socket_m


def main():
    localhost = socket_m.getfqdn()

    port = "5556"
    # ip = "*"
    ip = socket_m.gethostbyaddr(localhost)[2][0]

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.zap_domain = b'global'
    socket.bind("tcp://" + ip + ":%s" % port)

    auth = ThreadAuthenticator(context)

    host = localhost
    # host = asap3-p00
    whitelist = socket_m.gethostbyaddr(host)[2][0]
    # whitelist = None
    auth.start()

    if whitelist is None:
        auth.auth = None
    else:
        auth.allow(whitelist)

    try:
        while True:
            message = socket.recv_multipart()
            print("received reply ", message)
    except KeyboardInterrupt:
        pass
    finally:
        auth.stop()


if __name__ == "__main__":
    main()
