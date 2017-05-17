from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket

import __init__  # noqa F401
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

#    targets = [arguments.target_host, "50101", 0]
    targets = [[arguments.target_host, "50101", 0, ".*(tif|cbf)$"]]
#    targets = [[arguments.target_host, "50101", 0, [".tif", ".cbf"]]]

    print("\n==== TEST: Stream all files ====\n")

    query = Transfer("STREAM", arguments.signal_host)

    query.initiate(targets)

    query.start()

    while True:
        try:
            [metadata, data] = query.get()
        except:
            break

        print
        print("metadata", metadata["filename"])
    #    print ("data", str(data)[:10])
        print

    query.stop()

    print("\n==== TEST END: Stream all files ====\n")
