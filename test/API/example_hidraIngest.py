import os
import sys
import time
import zmq
import logging
import threading

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = os.path.join(BASE_PATH, "src", "APIs")
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

try:
    # search in global python modules first
    from hidra.ingest import dataIngest
except:
    # then search in local modules
    if not API_PATH in sys.path:
        sys.path.append ( API_PATH )
    del API_PATH

    from hidra.ingest import dataIngest


#enable logging
logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
logfile     = os.path.join(logfilePath, "test_dataIngest.log")
helpers.initLogging(logfile, True, "DEBUG")


print
print "==== TEST: dataIngest ===="
print

sourceFile = BASE_PATH + os.sep + "test_file.cbf"
chunksize = 524288

context    = zmq.Context()

obj = dataIngest(useLog = True, context = context)

obj.createFile("test/1.h5")

#for i in range(5):
#    try:
#        data = "asdfasdasdfasd"
#        obj.write(data)
#        print "write"

#    except:
#        logging.error("break", exc_info=True)
#        break

# Open file
source_fp = open(sourceFile, "rb")
print "Opened file:", sourceFile

while True:
    try:
        # Read file content
        content = source_fp.read(chunksize)
        logging.debug("Read file content")

        if not content:
            logging.debug("break")
            break

        obj.write(content)
        logging.debug("write")

    except:
        logging.error("break", exc_info=True)
        break

# Close file
source_fp.close()
logging.debug("Closed file: {f}".format(f=sourceFile))


try:
    obj.closeFile()
except:
    logging.error("Failed to close file", exc_info=True)

logging.info("Stopping")

obj.stop()

print
print "==== TEST END: dataIngest ===="
print



