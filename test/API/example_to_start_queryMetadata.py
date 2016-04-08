import os
import sys
import time


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH
del BASE_PATH

from dataTransferAPI import dataTransfer


signalHost   = "zitpcx19282.desy.de"
#isignalHost   = "zitpcx22614.desy.de"
targets = ["zitpcx19282.desy.de", "50101", 0]
basePath = "/space/projects/zeromq-data-transfer/data/target"

print
print "==== TEST: Query for the newest filename ===="
print

query = dataTransfer("queryMetadata", signalHost)

query.initiate(targets)

query.start()

while True:
    try:
        [metadata, data] = query.get()
    except:
        break

    print
    print query.generateTargetFilepath(basePath, metadata)
    print
#    time.sleep(0.1)

query.stop()

print
print "==== TEST END: Query for the newest filename ===="
print


