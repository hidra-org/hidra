from __future__ import print_function
from __future__ import unicode_literals

import argparse
import hashlib
import socket
import sys

import __init__  # noqa E401
from hidra import Transfer

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is runnning",
                        default=socket.getfqdn())
    parser.add_argument("--target_host",
                        type=str,
                        help="Host where the data should be send to",
                        default=socket.getfqdn())

    arguments = parser.parse_args()

#    targets = [[arguments.target_host, "50101", 1]]
    targets = [[arguments.target_host, "50101", 1, ".*(tif|cbf)$"]]
#    targets = [[arguments.target_host, "50101", 1, [".tif", ".cbf"]]]

    print("\n==== TEST: Query for the newest filename ====\n")

    query = Transfer("QUERY_NEXT", arguments.signal_host)

    query.initiate(targets)

    query.start()

    md5sum = False
    timeout = None
    #timeout = 2000  # in ms
    #while True:
    for i in range(2):
        try:
            [metadata, data] = query.get(timeout)
        except:
            print(sys.exc_info())
            break

        print
        if metadata and data:
            print("metadata", metadata["filename"])
            print("data", str(data)[:10])

            # generate md5sum
            if md5sum:
                m = hashlib.md5()
                m.update(data)
                print("md5sum", m.hexdigest())

        else:
            print("metadata", metadata)
            print("data", data)
        print

    query.stop()

    print("\n==== TEST END: Query for the newest filename ====\n")
