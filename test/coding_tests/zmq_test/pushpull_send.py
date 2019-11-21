from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import zmq
import time
import socket as socket_m


def main():
    port = "50300"
    ip = socket_m.getfqdn()

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://{0}:{1}".format(ip, port))

    print("using zmq version", zmq.__version__)

    while True:
        message = b"World"
        print("Send: ", message)
        res = socket.send(message, copy=False, track=True)

        if res.done:
            print("res: done")
        else:
            print("res: waiting")
            res.wait()
            print("res: waiting...")
        print("sleeping...")
        time.sleep(1)
        print("sleeping...done")


if __name__ == "__main__":
    main()
