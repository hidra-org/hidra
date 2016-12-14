from __future__ import print_function
from __future__ import unicode_literals

import os
# import time

from __init__ import BASE_PATH
from hidra import Transfer


signal_host = "zitpcx19282.desy.de"
# signal_host = "zitpcx22614w.desy.de"
targets = ["zitpcx19282.desy.de", "50101", 0]
# targets = ["zitpcx22614w.desy.de", "50101", 0]
base_target_path = os.path.join(BASE_PATH, "data", "target")

print ("\n==== TEST: Query for the newest filename ====\n")

query = Transfer("QUERY_METADATA", signal_host)

query.initiate(targets)

query.start()

while True:
    try:
        [metadata, data] = query.get()
    except:
        break

    print
    print (query.generate_target_filepath(base_target_path, metadata))
    print
#    time.sleep(0.1)

query.stop()

print ("\n==== TEST END: Query for the newest filename ====\n")
