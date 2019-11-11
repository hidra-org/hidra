from __future__ import print_function

import time
# import socket as socket_m
import zmq


def main():
    port = "5556"
    ip = "*"
    # ip = socket_m.getfqdn()

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://" + ip + ":%s" % port)

    while True:
        #  Wait for next request from client
        message = socket.recv()
        print("Received request: ", message)
        time.sleep(1)
        socket.send("World from %s" % port)


if __name__ == "__main__":
    main()
