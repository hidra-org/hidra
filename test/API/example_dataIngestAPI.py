import os
import sys
import time
import zmq
import logging
import threading
import cPickle

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH

from dataIngestAPI import dataIngest

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

#enable logging
logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
logfile     = os.path.join(logfilePath, "testDataIngestAPI.log")
helpers.initLogging(logfile, True, "DEBUG")


print
print "==== TEST: data ingest ===="
print

sourceFile = BASE_PATH + os.sep + "test_file.cbf"
chunksize = 524288

context    = zmq.Context()

obj = dataIngest(useLog = True, context = context)

obj.createFile("1.h5")

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
print "==== TEST END: data Ingest ===="
print



