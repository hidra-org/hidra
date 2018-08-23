"""
based on:
https://sebastianwallkoetter.wordpress.com/2017/07/09/pyzmq-bind-connect-vs-pub-sub/
"""

import os
import socket as m_socket
import tempfile
import time
import zmq

def get_endpoint(protocol):
    if protocol == "inproc":
        endpoint = "inproc://test"

    elif protocol == "ipc":
        ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
        endpoint = "ipc://{}:{}".format(ipc_dir, "pubsub")

    else:  #tcp
        port = "5556"
        ip = m_socket.gethostbyname(m_socket.gethostname())
        endpoint = "tcp://{}:{}".format(ip, port)

    return endpoint


context = zmq.Context()
endpoint = get_endpoint("inproc")  # -> works
#endpoint = get_endpoint("ipc")  # -> works (with sleep)
#endpoint = get_endpoint("tcp")  # -> works (with sleep)

#publisher
pub = context.socket(zmq.PUB)
pub.bind(endpoint)

#subscriber
sub = context.socket(zmq.SUB)
sub.connect(endpoint)
sub.setsockopt(zmq.SUBSCRIBE, "test")
poller = zmq.Poller()
poller.register(sub, zmq.POLLIN)

# due to slow joiner problem when using ipc and tcp
time.sleep(0.1)

#send some messages
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
        print msg
