from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket
import os

from __init__ import BASE_PATH
import helpers

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

    # enable logging
    logfile_path = os.path.join(BASE_PATH, "logs")
    logfile = os.path.join(logfile_path, "testAPI.log")
    helpers.init_logging(logfile, True, "DEBUG")

    targets = [arguments.target_host, "50100", 0]

    print("\n==== TEST: Stream all files and store them ====\n")

    query = Transfer("STREAM", arguments.signal_host, use_log=True)

    query.initiate(targets)

    query.start()

    while True:
        try:
            result = query.get()
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("Getting data failed.")
            print("Error was:", e)
            break

        try:
            query.store("/opt/hidra/data/target/testStore", result)
        except Exception as e:
            print("Storing data failed.")
            print("Error was:", e)
            break

    query.stop()

    print("\n==== TEST END: Stream all files and store them ====\n")
