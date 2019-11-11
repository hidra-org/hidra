"""
to be used together with forwarder_device.py
"""
from __future__ import print_function

import os
import tempfile
import zmq


def main():
    ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
#    control_pub_endpoint = os.path.join(ipc_dir, "control_sub")
    control_sub_endpoint = os.path.join(ipc_dir, "control_pub")
    check_endpoint = os.path.join(ipc_dir, "check")

#    control_pub_str = "ipc://{}".format(control_pub_endpoint)
    control_sub_str = "ipc://{}".format(control_sub_endpoint)
    check_str = "ipc://{}".format(check_endpoint)

    context = zmq.Context()

    try:
        control_socket = context.socket(zmq.SUB)
        control_socket.connect(control_sub_str)
        control_socket.setsockopt_string(zmq.SUBSCRIBE, u"control")
        print("Start control socket (connect): {}".format(control_sub_str))
    except Exception:
        print("Failed to start control socket (connect): {}"
              .format(control_sub_str))
        raise

    try:
        check_socket = context.socket(zmq.REP)
        check_socket.connect(check_str)
        print("Start check socket (connect): {}".format(check_str))
    except Exception:
        print("Failed to start check socket (connect): {}".format(check_str))
        raise

    request = check_socket.recv()
    print("Received request {}".format(request))
    check_socket.send("Yes")

    msg = control_socket.recv_multipart()
    print("Received control message {}".format(msg))

    control_socket.close()
    check_socket.close()
    context.destroy()


if __name__ == "__main__":
    main()
