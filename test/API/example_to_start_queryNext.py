from __future__ import print_function
from __future__ import unicode_literals

# import time
import __init__
from hidra import Transfer


signal_host = "asap3-p00.desy.de"
# signal_host = "zitpcx19282.desy.de"
# signal_host = "zitpcx22614w.desy.de"
targets = [["zitpcx19282.desy.de", "50101", 0]]
# targets = [["zitpcx22614w.desy.de", "50101", 0]]

print ("\n==== TEST: Query for the newest filename ====\n")

query = Transfer("QUERY_NEXT", signal_host)

query.initiate(targets)

query.start()

while True:
    try:
        [metadata, data] = query.get(2000)
    except:
        break

    print
    if metadata and data:
        print ("metadata", metadata["filename"])
        print ("data", str(data)[:10])
    else:
        print ("metadata", metadata)
        print ("data", data)
    print
#    time.sleep(0.1)

query.stop()

print ("\n==== TEST END: Query for the newest filename ====\n")
