from __future__ import print_function

import zmq
import os
import tempfile


def main():
    ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
    current_pid = 12345

    out_endpoint = "ipc://{}/{}_{}".format(ipc_dir, current_pid, "out")

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect(out_endpoint)

    message = socket.recv_multipart()
    print("Received - {}".format(message))

    socket.close()
    context.destroy()


if __name__ == "__main__":
    main()
