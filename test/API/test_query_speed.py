from __future__ import print_function
from __future__ import unicode_literals

import time
import os
import sys

BASE_PATH = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__))))
API_PATH = os.path.join(BASE_PATH, "src", "APIs")
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)

try:
    # search in global python modules first
    from hidra import Transfer  # noqa F401
except:
    # then search in local modules
    if API_PATH not in sys.path:
        sys.path.append(API_PATH)

    from hidra import Transfer


signal_host = "asap3-p00.desy.de"
# signal_host = "zitpcx19282.desy.de"
# signal_host = "zitpcx22614w.desy.de"
targets = ["zitpcx19282.desy.de", "50101", 0]
# targets = ["zitpcx22614w.desy.de", "50101", 0]

print ("\n==== TEST: Query for the newest filename ====\n")

number_of_files = 0

query = Transfer("QUERY_NEXT", signal_host)

query.initiate(targets)

query.start()

try:
    while True:
        try:
            [metadata, data] = query.get(2000)
        except:
            break

        if metadata and data:
            number_of_files += 1

except:
    query.stop()

print ("\n==== TEST END: Query for the newest filename ====")
print ("number of received files:", number_of_files)
