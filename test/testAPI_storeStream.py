import os
import sys
import time


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH

from dataTransferAPI import dataTransfer


if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helperScript

#enable logging
logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
logfileFullPath = os.path.join(logfilePath, "testAPI.log")
helperScript.initLogging(logfileFullPath, True, "DEBUG")

del BASE_PATH


signalIp   = "zitpcx19282.desy.de"
#signalIp   = "zitpcx22614.desy.de"
dataPort   = "50100"

print
print "==== TEST: Stream all files ===="
print


query = dataTransfer( signalIp, dataPort, useLog = True )

query.start("stream")


while True:
    try:
        result = query.get()
    except Exception as e:
        print "Getting data failed."
        print "Error was: " + str(e)
        break

    try:
        query.store("/space/projects/live-viewer/data/target/testStore", result)
    except Exception as e:
        print e
        break


query.stop()

print
print "==== TEST END: Stream for all files ===="
print



