from __future__ import print_function

import zmq
import time

def main():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://localhost:50300")

    print("using zmq version", zmq.__version__)
    print("using libzmq version", zmq.zmq_version())

    message = b"World"
    print("Send: ", message)
    res = socket.send(message, copy=False, track=True)

    if res.done:
        print("res: done")
    else:
        print("res: waiting")
        res.wait()
        print("res: waiting...")

if __name__ == "__main__":
    main()
