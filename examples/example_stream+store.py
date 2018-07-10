from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket
import os

from __init__ import BASE_PATH
import utils

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

    # enable logging
    logfile_path = os.path.join(BASE_PATH, "logs")
    logfile = os.path.join(logfile_path, "testAPI.log")
    utils.init_logging(logfile, True, "DEBUG")

    targets = [arguments.target_host, "50100", 0]

    print("\n==== TEST: Stream all files and store them ====\n")

    query = Transfer("STREAM", arguments.signal_host, use_log=True)

    query.initiate(targets)

    query.start()

    target_dir = os.path.join(BASE_PATH, "data", "zmq_target")
    target_file = os.path.join(target_dir, "test_store")
    try:
        query.store(target_file)
    except Exception as e:
        print("Storing data failed.")
        print("Error was:", e)

    query.stop()

    print("\n==== TEST END: Stream all files and store them ====\n")
