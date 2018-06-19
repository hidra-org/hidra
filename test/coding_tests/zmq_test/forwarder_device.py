from __future__ import print_function

import os
import tempfile
import zmq

ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
control_pub_endpoint = os.path.join(ipc_dir, "control_sub")
control_sub_endpoint = os.path.join(ipc_dir, "control_pub")
check_endpoint = os.path.join(ipc_dir, "check")

control_pub_str = "ipc://{}".format(control_pub_endpoint)
control_sub_str = "ipc://{}".format(control_sub_endpoint)
check_str = "ipc://{}".format(check_endpoint)

try:
    device = zmq.devices.ThreadDevice(zmq.FORWARDER,
                                      zmq.SUB,
                                      zmq.PUB)
    device.bind_in(control_pub_str)
    device.bind_out(control_sub_str)
    device.setsockopt_in(zmq.SUBSCRIBE, b"")
    device.start()
    print("Start thead device forwarding messages from '{}' to '{}'"
          .format(control_pub_str, control_sub_str))
except:
    print("Failed to start thead device forwarding messages from '{}' to '{}'"
          .format(control_pub_str, control_sub_str))
    raise

context = zmq.Context()

try:
    control_socket = context.socket(zmq.PUB)
    control_socket.connect(control_pub_str)
    print("Start control socket (connect): {}".format(control_pub_str))
except:
    print("Failed to start control socket (connect) : {}".format(control_pub_str))
    raise

try:
    check_socket = context.socket(zmq.REQ)
    check_socket.bind(check_str)
    print("Start check socket (bind): {}".format(check_str))
except:
    print("Failed to start check socket (bind): {}".format(check_str))

check_socket.send("There?")
answer = check_socket.recv()
print("Received answer {}".format(answer))

control_socket.send_multipart([b"control", b"EXIT"])

device.join(1)
control_socket.close()
check_socket.close()
context.destroy()
