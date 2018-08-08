import os
import json
import zmq
import tempfile

try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except:
    CURRENT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
print BASE_DIR


if __name__ == '__main__':

    targetFile = os.path.join(BASE_DIR, "data", "target", "local", "test.cbf")
    connectionStr = "ipc://{}".format(os.path.join(tempfile.gettempdir(), "hidra", "file_sending_test")
#    connectionStr = "tcp://0.0.0.0:55555"

    # Set up ZMQ
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind(connectionStr)
    print "Socket started (bind) for {}".format(connectionStr)

    # Open file
    target_fp = open(targetFile, "wb")
    print "Opened file:", targetFile

    while True:
        # Get file content
        print "Receiving"
        [metadata, data] = socket.recv_multipart()
        print "Received file:", metadata

        # decode metadata
        metadata = json.loads(metadata)

        # write file
        target_fp.write(data)
        print "Write file content"

        if len(data) < metadata["chunkSize"]:
            print "break"
            break

    # Close file
    target_fp.close()
    print "Closed file:", targetFile

    # Clean up ZMQ
    socket.close(0)
    context.destroy(0)
