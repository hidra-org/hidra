from __future__ import print_function

import os
import random
# import socket as m_socket
import time
import tempfile
import zmq


def main():
    # port = "5556"
    # ip = m_socket.getfqdn(m_socket.gethostname())

    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
    endpoint = "ipc://{}:{}".format(ipc_dir, "pubsub")
    # endpoint = "tcp://{}:{}".format(ip, port)

    socket.connect(endpoint)
    time.sleep(0.1)

    while True:
        topic = random.randrange(9999, 10005)
        message_data = random.randrange(1, 215) - 80
        print("%d %d" % (topic, message_data))

        message = [str(topic), str(message_data)]
    #    socket.send("%d %d" % (topic, message_data))
        socket.send_multipart(message)

        print("sleeping...")
        time.sleep(1)
        print("sleeping...done")


if __name__ == "__main__":
    main()
