import os
import sys
import time


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = os.path.join(BASE_PATH, "src", "APIs")
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

print BASE_PATH

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

try:
    # search in global python modules first
    from hidra import dataTransfer
except:
    # then search in local modules
    if not API_PATH in sys.path:
        sys.path.append ( API_PATH )
    del API_PATH

    from hidra import dataTransfer


#enable logging
logfilePath = os.path.join(BASE_PATH, "logs")
logfile     = os.path.join(logfilePath, "testAPI.log")
helpers.initLogging(logfile, True, "DEBUG")

del BASE_PATH


signalHost = "zitpcx19282.desy.de"
#signalHost = "zitpcx22614.desy.de"
targets = ["zitpcx19282.desy.de", "50100", 0]

print
print "==== TEST: Stream all files and store them ===="
print


query = dataTransfer("stream", signalHost, useLog = True)

query.initiate(targets)

query.start()


while True:
    try:
        result = query.get()
    except KeyboardInterrupt:
        break
    except Exception as e:
        print "Getting data failed."
        print "Error was: " + str(e)
        break

    try:
        query.store("/opt/hidra/data/target/testStore", result)
    except Exception as e:
        print "Storing data failed."
        print "Error was:", e
        break


query.stop()

print
print "==== TEST END: Stream all files and store them ===="
print



