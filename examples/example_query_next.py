from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket

import __init__  # noqa E401
from hidra import Transfer

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is runnning",
                        default=socket.gethostname())
    parser.add_argument("--target_host",
                        type=str,
                        help="Host where the data should be send to",
                        default=socket.gethostname())

    arguments = parser.parse_args()

    targets = [[arguments.target_host, "50101", 1]]

    print ("\n==== TEST: Query for the newest filename ====\n")

    query = Transfer("QUERY_NEXT", arguments.signal_host)

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

    query.stop()

    print ("\n==== TEST END: Query for the newest filename ====\n")
