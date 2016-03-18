import os
import sys
import time
import traceback


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH
del BASE_PATH

from dataTransferAPI import dataTransfer


dataPort   = "50100"

print
print "==== TEST: Query for the newest filename ===="
print

query = dataTransfer("stream")

query.start(dataPort)

while True:
    try:
        [metadata, data] = query.get()
    except KeyboardInterrupt:
        break
    except Exception as e:
        print "Getting data failed."
        print "Error was: " + str(e)
        break

    print
    print "metadata of file",  metadata["filename"]
    print "data", str(data)[:10]
    print

query.stop()

print
print "==== TEST END: Query for the newest filename ===="
print


