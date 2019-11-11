from __future__ import print_function

import socket as socket_m
import zmq


def main():
    port = "5556"
    # ip = "localhost"
    ip = socket_m.getfqdn()

    context = zmq.Context()
    print("Connecting to server...")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://" + ip + ":%s" % port)

    # Do 10 requests, waiting each time for a response
    for request in range(1, 10):
        print("Sending request ", request, "...")
        socket.send("Hello")
        #  Get the reply.
        message = socket.recv()
        print("Received reply ", request, "[", message, "]")


if __name__ == "__main__":
    main()
