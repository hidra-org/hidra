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
logfile = os.path.join(logfile_path, "example_get_remote_version.log")
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

    transfer_type = "QUERY_NEXT"

    query = Transfer(transfer_type, arguments.signal_host, use_log=None)
    version = query.get_remote_version()

    print("Remote version: {0}".format(version))
