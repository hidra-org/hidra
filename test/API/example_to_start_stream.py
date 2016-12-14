from __future__ import print_function
from __future__ import unicode_literals

# import time
from hidra import Transfer


signal_host = "zitpcx19282.desy.de"
# signal_host = "zitpcx22614.desy.de"
targets = ["zitpcx19282.desy.de", "50101", 0]

print ("\n==== TEST: Stream all files ====\n")

query = Transfer("STREAM", signal_host)

query.initiate(targets)

query.start()

while True:
    try:
        [metadata, data] = query.get()
    except:
        break

    print
    print ("metadata", metadata["filename"])
#    print ("data", str(data)[:10])
    print

query.stop()

print ("\n==== TEST END: Stream all files ====\n")
