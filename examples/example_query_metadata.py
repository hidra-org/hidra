from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket
import os

from __init__ import BASE_PATH
from hidra import Transfer, generate_filepath

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

    targets = [[arguments.target_host, "50101", 0]]
    base_target_path = os.path.join(BASE_PATH, "data", "target")

    print ("\n==== TEST: Query for the newest filename ====\n")

    query = Transfer("QUERY_METADATA", arguments.signal_host)

    query.initiate(targets)

    query.start()

    while True:
        try:
            [metadata, data] = query.get()
        except:
            break

        print
        print (generate_filepath(base_target_path, metadata))
        print

    query.stop()

    print ("\n==== TEST END: Query for the newest filename ====\n")
