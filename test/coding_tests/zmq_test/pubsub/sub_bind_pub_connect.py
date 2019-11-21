"""
source:
https://sebastianwallkoetter.wordpress.com/2017/07/09/pyzmq-bind-connect-vs-pub-sub/
"""

from __future__ import print_function

import os
import socket as m_socket
import tempfile
# import time
import zmq


def get_endpoint(protocol):
    if protocol == "inproc":
        endpoint = "inproc://test"

    elif protocol == "ipc":
        ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
        endpoint = "ipc://{}:{}".format(ipc_dir, "pubsub")

    else:  # tcp
        port = "5556"
        ip = m_socket.gethostbyname(m_socket.gethostname())
        endpoint = "tcp://{}:{}".format(ip, port)

    return endpoint


def main():
    context = zmq.Context()
    # endpoint = get_endpoint("inproc")  # -> works (with poll)
    # endpoint = get_endpoint("ipc")  # -> works (with poll)
    endpoint = get_endpoint("tcp")  # -> works (with poll)

    # subscriber
    sub = context.socket(zmq.SUB)
    sub.bind(endpoint)
    sub.setsockopt(zmq.SUBSCRIBE, "test")
    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    # publisher
    pub = context.socket(zmq.PUB)
    pub.connect(endpoint)

#    time.sleep(0.5)

    # now the messages are delivered -- updating something internally??
    poller.poll(100)

    # send some messages
    pub.send("test 123")
    pub.send("test 345")
    pub.send("foo 23")
    pub.send("test hello")

    # retrieve
    has_next = True
    while has_next:
        has_next = False
        sock = dict(poller.poll(100))
        if sub in sock:
            has_next = True
            msg = sub.recv()
            print(msg)
