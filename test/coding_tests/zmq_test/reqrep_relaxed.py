from __future__ import print_function

import socket as socket_m
import time
import zmq

from threading import Thread, Event


def requesting(endpoint, stop_event):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    # socket.REQ_RELAXED = True  # is equal to setsockopt
    # socket.REQ_CORRELATE = True  # is equal to setsockopt
    socket.setsockopt(zmq.REQ_RELAXED, True)
    socket.setsockopt(zmq.REQ_CORRELATE, True)  # is not set automatically!
    socket.SNDTIMEO = 100
    socket.connect(endpoint)

    try:
        socket.send(b"test1")
        print(socket.recv())

        socket.send(b"test2")
        # no recv

        socket.send(b"test3")
        print(socket.recv())
    finally:
        stop_event.set()

    print("requesting finished")

def repling(endpoint, stop_event):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.RCVTIMEO = 1000
    socket.bind(endpoint)

    while not stop_event.is_set():
        try:
            time.sleep(1)
            request = socket.recv()
            print("answer to request:", request)
            socket.send(request)
        except zmq.error.Again:
            pass

    print("repling finished")


def main():
    stop_event = Event()
    endpoint = "tcp://{}:50000".format(socket_m.gethostbyname(socket_m.getfqdn()))

    requester = Thread(target=requesting, args=(endpoint, stop_event))
    requester.start()

    replier = Thread(target=repling, args=(endpoint, stop_event))
    replier.start()

    requester.join()
    replier.join()


if __name__ == "__main__":
    main()

# output without REQ_RELAXED:
# test1
# repling finished
# Exception in thread Thread-1:
# Traceback (most recent call last):
#   File "/usr/lib/python2.7/threading.py", line 801, in __bootstrap_inner
#     self.run()
#   File "/usr/lib/python2.7/threading.py", line 754, in run
#     self.__target(*self.__args, **self.__kwargs)
#   File "reqrep_relaxed.py", line 23, in requesting
#     socket.send(b"test3")
#   File "zmq/backend/cython/socket.pyx", line 636, in zmq.backend.cython.socket.Socket.send (zmq/backend/cython/socket.c:7314)
#   File "zmq/backend/cython/socket.pyx", line 683, in zmq.backend.cython.socket.Socket.send (zmq/backend/cython/socket.c:7057)
#   File "zmq/backend/cython/socket.pyx", line 206, in zmq.backend.cython.socket._send_copy (zmq/backend/cython/socket.c:3041)
#   File "zmq/backend/cython/socket.pyx", line 201, in zmq.backend.cython.socket._send_copy (zmq/backend/cython/socket.c:2929)
#   File "zmq/backend/cython/checkrc.pxd", line 25, in zmq.backend.cython.checkrc._check_rc (zmq/backend/cython/socket.c:8400)
#     raise ZMQError(errno)
# ZMQError: Operation cannot be accomplished in current state


# output with REQ_RELAXED:
# test1
# test3
# requesting finished
# repling finished

