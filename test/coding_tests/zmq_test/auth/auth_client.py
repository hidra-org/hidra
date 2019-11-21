from __future__ import print_function

import socket as socket_m
import zmq


def main():
    port = "5556"
    # ip = "localhost"
    # ip = "*"
    ip = socket_m.getfqdn()

    context = zmq.Context()
    print("Connecting to server...")
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://" + ip + ":" + port)

    # Do 10 requests, waiting each time for a response
    for request in range(1, 10):
        #  Get the reply.
        message = socket.recv_multipart()
        print("received reply ", request, "[", message, "]")


if __name__ == "__main__":
    main()
