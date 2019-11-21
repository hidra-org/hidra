from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket

from hidra import Transfer


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is running",
                        default=socket.getfqdn())
    parser.add_argument("--target_host",
                        type=str,
                        help="Host where the data should be send to",
                        default=socket.getfqdn())
    parser.add_argument("--procname",
                        type=str,
                        help="Name with which the service should be running",
                        default="example_onda")

    arguments = parser.parse_args()

    transfer_type = "QUERY_NEXT"

    number_of_worker = 3
    workers = []

    targets = []

    # Create <number_of_worker> workers to receive and process data
    for n in range(number_of_worker):
        p = str(50100 + n)
        targets.append([arguments.target_host, p, 1, [".cbf"]])

    # register these workers on the sending side
    # this is done from the master to enforce that the data received from the
    # workers is disjoint
    query = Transfer(transfer_type, arguments.signal_host, use_log=False)
    query.initiate(targets)

    query.stop = lambda: None

    try:
        while True:
            pass
    except Exception:
        pass
