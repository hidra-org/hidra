from __future__ import print_function

import multiprocessing
import os
import tempfile
import threading
import zmq


class ClientThread(threading.Thread):
    def __init__(self, control_sub_str, check_str):
        threading.Thread.__init__(self)

        self.control_sub_str = control_sub_str
        self.check_str = check_str

        self.context = zmq.Context()

    def run(self):
        try:
            control_socket = self.context.socket(zmq.SUB)
            control_socket.connect(self.control_sub_str)
            control_socket.setsockopt_string(zmq.SUBSCRIBE, u"control")
            print("Start control socket (connect): {}"
                  .format(self.control_sub_str))
        except Exception:
            print("Failed to start control socket (connect): {}"
                  .format(self.control_sub_str))
            raise

        try:
            check_socket = self.context.socket(zmq.REP)
            check_socket.connect(self.check_str)
            print("Start check socket (connect): {}".format(self.check_str))
        except Exception:
            print("Failed to start check socket (connect): {}"
                  .format(self.check_str))
            raise

        request = check_socket.recv()
        print("Received request {}".format(request))
        check_socket.send("Yes")

        msg = control_socket.recv_multipart()
        print("Received control message {}".format(msg))

        control_socket.close()
        check_socket.close()

    def stop(self):
        self.context.term()


class ClientProcess(multiprocessing.Process):
    def __init__(self, control_sub_str, check_str):
        multiprocessing.Process.__init__(self)

        self.control_sub_str = control_sub_str
        self.check_str = check_str

        self.context = zmq.Context()

    def run(self):
        try:
            control_socket = self.context.socket(zmq.SUB)
            control_socket.connect(self.control_sub_str)
            control_socket.setsockopt_string(zmq.SUBSCRIBE, u"control")
            print("Start control socket (connect): {}"
                  .format(self.control_sub_str))
        except Exception:
            print("Failed to start control socket (connect): {}"
                  .format(self.control_sub_str))
            raise

        try:
            check_socket = self.context.socket(zmq.REP)
            check_socket.connect(self.check_str)
            print("Start check socket (connect): {}".format(self.check_str))
        except Exception:
            print("Failed to start check socket (connect): {}"
                  .format(self.check_str))
            raise

        request = check_socket.recv()
        print("Received request {}".format(request))
        check_socket.send("Yes")

        msg = control_socket.recv_multipart()
        print("Received control message {}".format(msg))

        control_socket.close()
        check_socket.close()

    def stop(self):
        self.context.destroy()


def main():
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
        print("Start thread device forwarding messages from '{}' to '{}'"
              .format(control_pub_str, control_sub_str))
    except Exception:
        print("Failed to start thread device forwarding messages from '{}' to "
              "'{}'".format(control_pub_str, control_sub_str))
        raise

    context = zmq.Context()

    try:
        control_socket = context.socket(zmq.PUB)
        control_socket.connect(control_pub_str)
        print("Start control socket (connect): {}".format(control_pub_str))
    except Exception:
        print("Failed to start control socket (connect) : {}"
              .format(control_pub_str))
        raise

    try:
        check_socket = context.socket(zmq.REQ)
        check_socket.bind(check_str)
        print("Start check socket (bind): {}".format(check_str))
    except Exception:
        print("Failed to start check socket (bind): {}".format(check_str))
        raise

    client = ClientThread(control_sub_str, check_str)
#    client = ClientProcess(control_sub_str, check_str)
    client.start()

    check_socket.send("There?")
    answer = check_socket.recv()
    print("Received answer {}".format(answer))

    control_socket.send_multipart([b"control", b"EXIT"])

    device.join(1)

    client.stop()
    client.join()

    control_socket.close()
    check_socket.close()
    context.destroy()


if __name__ == "__main__":
    main()

    print("\n -------- run 2 --------\n")

    main()
