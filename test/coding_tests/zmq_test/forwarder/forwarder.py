from __future__ import print_function

import os
import socket as m_socket
import tempfile
import time
import zmq


def get_endpoint(protocol):
    if protocol == "inproc":
        in_endpoint = "inproc://in"
        out_endpoint = "inproc://out"

    elif protocol == "ipc":
        ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
        in_endpoint = "ipc://{}:{}".format(ipc_dir, "in")
        out_endpoint = "ipc://{}:{}".format(ipc_dir, "out")

    else:  # tcp
        ip = m_socket.gethostbyname(m_socket.gethostname())
        in_endpoint = "tcp://{}:{}".format(ip, "50100")
        out_endpoint = "tcp://{}:{}".format(ip, "50101")

    return in_endpoint, out_endpoint


def main():
    context = zmq.Context()
    pub_endpoint, sub_endpoint = get_endpoint("ipc")
    # pub_endpoint, sub_endpoint = get_endpoint("tcp")

    # forwarder
    try:
        device = zmq.devices.ThreadDevice(zmq.FORWARDER,
                                          zmq.SUB,
                                          zmq.PUB)
        device.bind_in(pub_endpoint)
        device.bind_out(sub_endpoint)
        device.setsockopt_in(zmq.SUBSCRIBE, "test")
        device.start()
        print("Start thread device forwarding messages from '{}' to '{}'"
              .format(pub_endpoint, sub_endpoint))
    except Exception:
        print("Failed to start thread device forwarding messages from '{}' to "
              "'{}'".format(pub_endpoint, sub_endpoint))
        raise

    # publisher
    try:
        pub_socket = context.socket(zmq.PUB)
        pub_socket.connect(pub_endpoint)
        print("Start pub_socket (connect): {}".format(pub_endpoint))
    except Exception:
        print("Failed to start pub_socket (connect) : {}".format(pub_endpoint))
        raise

    # subscriber
    try:
        sub_socket = context.socket(zmq.SUB)
        sub_socket.connect(sub_endpoint)
        sub_socket.setsockopt(zmq.SUBSCRIBE, "test")
        print("Start sub_socket (connect): {}".format(sub_endpoint))
    except Exception:
        print("Failed to start sub_socket (connect): {}".format(sub_endpoint))
        raise

    time.sleep(0.1)

    pub_socket.send_multipart(["test", "There?"])
    print("Sent")

    answer = sub_socket.recv_multipart()
    print("Received answer {}".format(answer))

    pub_socket.close()
    sub_socket.close()
    context.destroy()
    device.join(1)


if __name__ == "__main__":
    main()
