import os
import sys
import time
import traceback


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH

from dataTransferAPI import dataTransfer

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

#enable logging
logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
logfile     = os.path.join(logfilePath, "testAPI.log")
helpers.initLogging(logfile, True, "DEBUG")

del BASE_PATH

dataPort   = "50100"

print
print "==== TEST: Query for the newest filename ===="
print

query = dataTransfer("stream", useLog = True)

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

#    try:
#        query.store("/space/projects/live-viewer/data/target/testStore", result)
#    except Exception as e:
#        print "Storing data failed."
#        print "Error was:", e
#        break

query.stop()

print
print "==== TEST END: Query for the newest filename ===="
print


