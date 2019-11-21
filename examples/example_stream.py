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
This module implements an example for the STREAM mode.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket

from hidra import Transfer


def main():
    """Connect to hidra and get streamed data.
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
        except Exception:
            break

        print()
        print("metadata", metadata["filename"])
        print("data", len(data))
        print()

    query.stop()

    print("\n==== TEST END: Stream all files ====\n")


if __name__ == "__main__":
    main()
