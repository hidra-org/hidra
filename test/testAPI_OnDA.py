import os
import sys
import time


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) )

API_PATH    = BASE_PATH + os.sep + "APIs"
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH

#if not SHARED_PATH in sys.path:
#    sys.path.append ( SHARED_PATH )
#del SHARED_PATH

from dataTransferAPI import dataTransfer
#import helperScript


#enable logging
#logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
#logfileFullPath = os.path.join(logfilePath, "testAPI.log")
#helperScript.initLogging(logfileFullPath, True, "DEBUG")


signalIp   = "zitpcx19282.desy.de"
#signalIp   = "zitpcx22614.desy.de"
dataPort   = "50200"

print
print "==== TEST: OnDA ===="
print

query = dataTransfer( signalIp, dataPort )

query.start("OnDA")

while True:
    try:
        [metadata, data] = query.getData()
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
print "==== TEST END: OnDA ===="
print


