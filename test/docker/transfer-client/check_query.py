from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import hashlib
import logging
import os
import socket
import sys

from hidra import Transfer


def get_filename(metadata):
    return os.path.join(metadata["relative_path"], metadata["filename"])


def main():
    """Requests data from hidra on a query basis.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is running",
                        default=socket.getfqdn())
    parser.add_argument("--target_host",
                        type=str,
                        help="Host where the data should be send to",
                        default=socket.getfqdn())
    parser.add_argument("--detector_id",
                        type=str,
                        help="Used for connection with control server",
                        default=None)
    parser.add_argument("--query_type",
                        type=str,
                        help="Query type like QUERY_NEXT",
                        default=True)
    parser.add_argument("--max_queries",
                        type=int,
                        help="Maximum number of queries",
                        default=None)
    parser.add_argument("--timeout",
                        type=int,
                        help="Timeout for get call",
                        default=2000)

    args = parser.parse_args()

    targets = [[args.target_host, "50101", 1, ".*(tif|cbf)$"]]

    timeout = args.timeout

    query = Transfer(
        args.query_type,
        signal_host=args.signal_host,
        detector_id=args.detector_id,
        use_log="DEBUG")  # should use the logging module

    query.initiate(targets)

    try:
        query.start()
    except Exception:
        query.stop()
        return

    print("Begin query...", file=sys.stderr)
    count = 0
    while args.max_queries is None or count < args.max_queries:
        count += 1
        try:
            [metadata, data] = query.get(timeout)
        except Exception:
            print(sys.exc_info(), file=sys.stderr)
            break

        if metadata and data != "null":
            md5sum = hashlib.md5()
            md5sum.update(data)
            print("{}: {}".format(get_filename(metadata), md5sum.hexdigest()))
        elif metadata:
            print("{}: null".format(get_filename(metadata)))
            break
        else:
            break

    query.stop()


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()
