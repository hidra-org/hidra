from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import socket as socket_m
import zmq


def main():
    port = "50300"
    ip = socket_m.gethostbyname(socket_m.gethostname())

    context = zmq.Context()
    print("Connecting to server...")
    socket = context.socket(zmq.PULL)
    socket.bind("tcp://{0}:{1}".format(ip, port))

    print("using zmq version", zmq.__version__)

    for request in range(1, 10):
        message = socket.recv()
        print("received reply", request, message)


if __name__ == "__main__":
    main()
