import os
import json
import zmq

try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
print BASE_PATH


if __name__ == '__main__':

    targetFile = os.path.join(BASE_PATH, "data", "target", "local", "test.cbf")
    connectionStr = "tcp://0.0.0.0:55555"
#    connectionStr = "ipc:///tmp/HiDRA/file_sending_test"

    # Set up ZMQ
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind(connectionStr)
    print "Socket started (bind) for {c}".format(c=connectionStr)

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
