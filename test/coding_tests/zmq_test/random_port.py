from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import socket as socket_m
import threading
import time
import zmq

port = None
ip=socket_m.gethostbyname(socket_m.gethostname())

class Puller(threading.Thread):
    def __init__(self):
        global port

        threading.Thread.__init__(self)

        context = zmq.Context()
        print("Connecting to server...")
        self.socket = context.socket(zmq.PULL)
        port = self.socket.bind_to_random_port("tcp://{}".format(ip))

    def run(self):
        for request in range(10):
            message = self.socket.recv()
            print("received reply", request, message)


class Puller2(threading.Thread):
    def __init__(self):
        global port

        threading.Thread.__init__(self)

        context = zmq.Context()
        print("Connecting to server...")
        self.socket = context.socket(zmq.PULL)
        self.socket.bind("tcp://*:*")
        port = self.socket.getsockopt(zmq.LAST_ENDPOINT).split(":")[-1]
        print("port", port)

    def run(self):
        for request in range(10):
            message = self.socket.recv()
            print("received reply", request, message)


class Pusher(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

        context = zmq.Context()
        self.socket = context.socket(zmq.PUSH)
        self.socket.connect("tcp://{}:{}".format(ip, port))

    def run(self):
        for request in range(10):
            message = b"World"
            print("Send: ", message)
            self.socket.send(message)
            time.sleep (0.1)


if __name__ == "__main__":
    print("using zmq version", zmq.__version__)

    pll = Puller2()
    psh = Pusher()

    pll.start()
    psh.start()

    pll.join()
    psh.join()