import os
import sys
import time


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

import helperScript

#enable logging
logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
logfileFullPath = os.path.join(logfilePath, "testAPI2.log")
helperScript.initLogging(logfileFullPath, True, "DEBUG")

del BASE_PATH


signalHost   = "zitpcx19282.desy.de"
#isignalHost   = "zitpcx22614.desy.de"
#dataPort   = ["50205", "50206"]

print
print "==== TEST: Query for the newest filename ===="
print

query = dataTransfer("queryNext", signalHost, useLog = True)

query.start(50206)

while True:
    try:
        [metadata, data] = query.get()
    except:
        break

    print
    print "metadata"
    print metadata
    print "data", str(data)[:10]
    print
    time.sleep(0.1)

query.stop()

print
print "==== TEST END: Query for the newest filename ===="
print


