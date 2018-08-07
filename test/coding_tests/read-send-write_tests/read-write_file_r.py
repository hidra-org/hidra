import os
import zmq
import json
import tempfile

try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
print BASE_PATH


if __name__ == '__main__':

    sourceFile = os.path.join(BASE_PATH, "test", "test_files", "test_file.cbf")
    chunkSize = 10485760 # 1024*1024*10 = 10MB
    filepart = 0
    connectionStr = "ipc://{}".format(os.path.join(tempfile.gettempdir(), "hidra", "file_sending_test")


    # Set up ZMQ
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(connectionStr)
    print "Socket started (connect) for {}".format(connectionStr)

    # Set up metadata
    metadata = {
            "filename" : "test.cbf",
            "filePart" : 0,
            "chunkSize": chunkSize
            }

    # Open file
    source_fp = open(sourceFile, "rb")
    print "Opened file:", sourceFile

    while True:
        # Read file content
        content = source_fp.read(chunkSize)
        print "Read file content"

        if not content:
            print "break"
            break

        # Send content over ZMQ
        metadata["filePart"] = filepart

        payload = []
        payload.append(json.dumps(metadata))
        payload.append(content)

        tracker = socket.send_multipart(payload, copy=False, track=True)
        if not tracker.done:
            tracker.wait(2)
        if not tracker.done:
            print "Failed to send ALIVE_TEST"
        else:
            print "Sending multipart message...success"

        filepart += 1


    # Close file
    source_fp.close()
    print "Closed file:", sourceFile

    # Clean up ZMQ
    socket.close(0)
    context.destroy(0)
