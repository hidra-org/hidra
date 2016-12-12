from __future__ import print_function
from __future__ import unicode_literals

import os

from __init__ import BASE_PATH
import helpers

from hidra import Transfer


# enable logging
logfile_path = os.path.join(BASE_PATH, "logs")
logfile = os.path.join(logfile_path, "testAPI.log")
helpers.init_logging(logfile, True, "DEBUG")

signal_host = "zitpcx19282.desy.de"
#signal_host = "zitpcx22614.desy.de"
targets = ["zitpcx19282.desy.de", "50100", 0]

print ("\n==== TEST: Stream all files and store them ====\n")

query = Transfer("stream", signal_host, use_log=True)

query.initiate(targets)

query.start()

while True:
    try:
        result = query.get()
    except KeyboardInterrupt:
        break
    except Exception as e:
        print ("Getting data failed.")
        print ("Error was:", e)
        break

    try:
        query.store("/opt/hidra/data/target/testStore", result)
    except Exception as e:
        print ("Storing data failed.")
        print ("Error was:", e)
        break

query.stop()

print ("\n==== TEST END: Stream all files and store them ====\n")
