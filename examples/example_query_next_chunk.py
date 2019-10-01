# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""
This module implements an example for the QUERY_NEXT mode where the chunks are
received separately.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket
import hashlib

import _environment  # noqa F401 # pylint: disable=unused-import
from hidra import Transfer


def main():
    """Connects to hidra and requests data chunks separately.
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

    arguments = parser.parse_args()

#    targets = [[arguments.target_host, "50101", 1]]
    targets = [[arguments.target_host, "50101", 1, ".*(tif|cbf)$"]]
#    targets = [[arguments.target_host, "50101", 1, [".tif", ".cbf"]]]

    print("\n==== TEST: Query for the newest filename ====\n")

    query = Transfer("QUERY_NEXT", arguments.signal_host)

    query.initiate(targets)

    query.start()

    timeout = None
#    timeout = 2000  # in ms
    while True:
        try:
            [metadata, data] = query.get_chunk(timeout)
        except Exception:
            break

        print()
        if metadata and data:
            print("metadata", metadata["filename"], metadata["chunk_number"])
            print("data", str(data)[:10])

            # generate md5sum
            md5sum = hashlib.md5()
            md5sum.update(data)
            print("md5sum", md5sum.hexdigest())

            if query.check_file_closed(metadata, data):
                print("File is closed, stopping loop")
                break

        else:
            print("metadata", metadata)
            print("data", data)
            break
        print()

    query.stop()

    print("\n==== TEST END: Query for the newest filename ====\n")


if __name__ == "__main__":
    main()
