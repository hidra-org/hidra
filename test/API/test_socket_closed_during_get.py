from __future__ import print_function
from __future__ import unicode_literals

import zmq
import json

context = zmq.Context()

signal_socket = context.socket(zmq.REQ)
signal_socket.connect("tcp://zitpcx19282.desy.de:50000")

data_socket = context.socket(zmq.PULL)
data_socket.bind("tcp://131.169.185.121:50101")

request_socket = context.socket(zmq.PUSH)
request_socket.connect("tcp://zitpcx19282.desy.de:50001")

signal_socket.send_multipart(
    [b"2.4.2",
     b"START_QUERY_NEXT",
     json.dumps([["zitpcx19282.desy.de:50101", 1, "cbf"]])])
print("Signal responded: ", signal_socket.recv_multipart())

try:
    request_socket.send_multipart(
        [b"NEXT",
         "zitpcx19282.desy.de:50101".encode("utf-8")])

    multipart_message = data_socket.recv_multipart()
    metadata = json.loads(multipart_message[0])
    payload = multipart_message[1:]

    print("metadata: ", metadata)

finally:
    signal_socket.send_multipart(
        [b"2.4.2",
         b"STOP_QUERY_NEXT",
         json.dumps([["zitpcx19282.desy.de:50101", 1, "cbf"]])])

    signal_socket.close()
    data_socket.close()
    request_socket.close()

    context.destroy()
