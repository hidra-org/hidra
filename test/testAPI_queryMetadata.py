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

from dataTransferAPI import dataTransferQuery
#import helperScript


#enable logging
#logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
#logfileFullPath = os.path.join(logfilePath, "testAPI.log")
#helperScript.initLogging(logfileFullPath, True, "DEBUG")


signalIp   = "zitpcx19282.desy.de"
signalPort = "50021"
dataPort   = "50022"

print
print "==== TEST: Query for the newest filename ===="
print

query = dataTransferQuery( signalPort, signalIp, dataPort )

query.initConnection("queryMetadata")

for i in range(5):
    message = query.getData()
    print message
    time.sleep(0.5)

query.stop()

print
print "==== TEST END: Query for the newest filename ===="
print


