from __future__ import print_function

import json
import os
import tempfile
import zmq

try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except NameError:
    CURRENT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
print(BASE_DIR)


def main():
    target_file = os.path.join(BASE_DIR, "data", "target", "local", "test.cbf")
    connection_str = "ipc://{}".format(
        os.path.join(tempfile.gettempdir(), "hidra", "file_sending_test")
    )
#    connection_str = "tcp://0.0.0.0:55555"

    # Set up ZMQ
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind(connection_str)
    print("Socket started (bind) for", connection_str)

    # Open file
    target_fp = open(target_file, "wb")
    print("Opened file:", target_file)

    while True:
        # Get file content
        print("Receiving")
        [metadata, data] = socket.recv_multipart()
        print("Received file:", metadata)

        # decode metadata
        metadata = json.loads(metadata)

        # write file
        target_fp.write(data)
        print("Write file content")

        if len(data) < metadata["chunkSize"]:
            print("break")
            break

    # Close file
    target_fp.close()
    print("Closed file:", target_file)

    # Clean up ZMQ
    socket.close(0)
    context.destroy(0)


if __name__ == '__main__':
    main()

