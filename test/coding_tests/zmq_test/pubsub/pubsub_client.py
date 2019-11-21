from __future__ import print_function

import os
import tempfile
import zmq


def main():
    # port = "5556"
    # ip = "*"

    context = zmq.Context()
    print("Connecting to server...")
    socket = context.socket(zmq.SUB)

    ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
    endpoint = "ipc://{}:{}".format(ipc_dir, "pubsub")
    # endpoint = "tcp://{}:{}".format(ip, port)

    socket.bind(endpoint)

    socket.setsockopt(zmq.SUBSCRIBE, "10001")
    socket.setsockopt(zmq.SUBSCRIBE, "10002")

    # Do 10 requests, waiting each time for a response
    for request in range(1, 5):
        # Get the reply.
        topic, message_data = socket.recv_multipart()
        # message = socket.recv()
        # topic, message_data = message.split()
        print("received reply ", request, "[", topic, message_data, "]")
        # message = socket.recv_multipart()
        # print("received reply ", request, "[", message, "]")


if __name__ == "__main__":
    main()
