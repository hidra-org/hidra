from __future__ import print_function
from __future__ import unicode_literals

import json
import socket as socket_m
import zmq


def main():
    localhost = socket_m.getfqdn()
    local_ip = socket_m.gethostbyaddr(localhost)[2][0]

    context = zmq.Context()

    signal_socket = context.socket(zmq.REQ)
    signal_socket.connect("tcp://{}:50000".format(localhost))

    data_socket = context.socket(zmq.PULL)
    data_socket.bind("tcp://{}:50101".format(local_ip))

    request_socket = context.socket(zmq.PUSH)
    request_socket.connect("tcp://{}:50001".format(localhost))

    signal_socket.send_multipart(
        [b"2.4.2",
         b"START_QUERY_NEXT",
         json.dumps([["{}:50101".format(localhost), 1, "cbf"]])])
    print("Signal responded:", signal_socket.recv_multipart())

    try:
        request_socket.send_multipart(
            [b"NEXT",
             "{}:50101".format(localhost).encode("utf-8")])

        multipart_message = data_socket.recv_multipart()
        metadata = json.loads(multipart_message[0])
#        payload = multipart_message[1:]

        print("metadata:", metadata)

    finally:
        signal_socket.send_multipart(
            [b"2.4.2",
             b"STOP_QUERY_NEXT",
             json.dumps([["{}:50101".format(localhost), 1, "cbf"]])])

        signal_socket.close()
        data_socket.close()
        request_socket.close()

        context.destroy()


if __name__ == "__main__":
    main()
