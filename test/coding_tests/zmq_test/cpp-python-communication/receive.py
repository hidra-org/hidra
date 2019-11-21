from __future__ import print_function

import json
# import socket as socket_m
import zmq


def main():
    port = "6000"
    ip = "localhost"
    # ip = "*"
    # ip = socket_m.getfqdn()

    context = zmq.Context()
    print("Connecting to server...")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://" + ip + ":%s" % port)

    # Do 10 requests, waiting each time for a response
    for request in range(1, 10):
        # Send request
        socket.send("test")
        print("send request")

        # Get the reply.
        message = socket.recv()
        print("received reply ", request, "[", message, "]")
        print("as json: ", json.loads(message))


if __name__ == "__main__":
    main()
