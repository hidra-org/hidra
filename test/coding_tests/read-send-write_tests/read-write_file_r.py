from __future__ import print_function

import os
import json
import tempfile
import zmq

try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except NameError:
    CURRENT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
print(BASE_DIR)


def main():

    source_file = os.path.join(BASE_DIR, "test", "test_files", "test_file.cbf")
    chunksize = 10485760  # 1024*1024*10 = 10MB
    filepart = 0
    connection_str = "ipc://{}".format(
        os.path.join(tempfile.gettempdir(), "hidra", "file_sending_test")
    )

    # Set up ZMQ
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(connection_str)
    print("Socket started (connect) for", connection_str)

    # Set up metadata
    metadata = {
            "filename": "test.cbf",
            "filePart": 0,
            "chunkSize": chunksize
            }

    # Open file
    source_fp = open(source_file, "rb")
    print("Opened file:", source_file)

    while True:
        # Read file content
        content = source_fp.read(chunksize)
        print("Read file content")

        if not content:
            print("break")
            break

        # Send content over ZMQ
        metadata["filePart"] = filepart

        payload = [json.dumps(metadata), content]

        tracker = socket.send_multipart(payload, copy=False, track=True)
        if not tracker.done:
            tracker.wait(2)
        if not tracker.done:
            print("Failed to send ALIVE_TEST")
        else:
            print("Sending multipart message...success")

        filepart += 1

    # Close file
    source_fp.close()
    print("Closed file:", source_file)

    # Clean up ZMQ
    socket.close(0)
    context.destroy(0)


if __name__ == '__main__':
    main()

