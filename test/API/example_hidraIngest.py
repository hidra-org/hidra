from __future__ import print_function
# from __future__ import unicode_literals

import os
import zmq
import logging

from __init__ import BASE_DIR
import utils

from hidra import Ingest


# enable logging
logfile_path = os.path.join(BASE_DIR, "logs")
logfile = os.path.join(logfile_path, "test_ingest.log")
utils.init_logging(logfile, True, "DEBUG")

print("\n==== TEST: Ingest ====\n")

source_file = os.path.join(BASE_DIR, "test_file.cbf")
chunksize = 524288

context = zmq.Context()

obj = Ingest(use_log=True, context=context)

obj.create_file(os.path.join("test", "1.h5"))

# for i in range(5):
#    try:
#        data = "asdfasdasdfasd"
#        obj.write(data)
#        print "write"

#    except:
#        logging.error("break", exc_info=True)
#        break

# Open file
source_fp = open(source_file, "rb")
print("Opened file:", source_file)

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
logging.debug("Closed file: {0}".format(source_file))


try:
    obj.close_file()
except:
    logging.error("Failed to close file", exc_info=True)

logging.info("Stopping")

obj.stop()

print("\n==== TEST END: Ingest ====\n")
