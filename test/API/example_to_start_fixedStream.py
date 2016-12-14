from __future__ import print_function
from __future__ import unicode_literals

import os

from __init__ import BASE_PATH
import helpers

from hidra import Transfer


# enable logging
logfile_path = os.path.join(BASE_PATH, "logs")
logfile = os.path.join(logfile_path, "test_fixedStream.log")
helpers.init_logging(logfile, True, "DEBUG")

data_port = "50100"

print ("\n==== TEST: Fixed stream ====\n")

query = Transfer("STREAM", use_log=True)

query.start(data_port)

while True:
    try:
        [metadata, data] = query.get()
    except KeyboardInterrupt:
        break
    except Exception as e:
        print ("Getting data failed.")
        print ("Error was: {0}".format(e))
        break

    print
    print ("metadata of file", metadata["filename"])
    print ("data", str(data)[:10])
    print

query.stop()

print ("\n==== TEST END: Fixed Stream ====\n")
