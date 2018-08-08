from __future__ import print_function
# from __future__ import unicode_literals

import os
import argparse
import socket

from __init__ import BASE_DIR
import utils

from hidra import Transfer


# enable logging
logfile_path = os.path.join(BASE_DIR, "logs")
logfile = os.path.join(logfile_path, "example_force_stop.log")
utils.init_logging(logfile, True, "DEBUG")

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

#    targets = [[arguments.target_host, "50101", 1, ".*(tif|cbf)$"]]
    targets = [[arguments.target_host, "50100", 1, [".cbf"]],
               [arguments.target_host, "50101", 1, [".cbf"]],
               [arguments.target_host, "50102", 1, [".cbf"]]]

    transfer_type = "QUERY_NEXT"
#    transfer_type = "STREAM"
#    transfer_type = "STREAM_METADATA"
#    transfer_type = "QUERY_NEXT_METADATA"

    query = Transfer(transfer_type, arguments.signal_host, use_log=True)
    query.force_stop(targets)
