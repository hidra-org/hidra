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
This module implements an example for the QUERY-NEXT mode in.
If there are multiple hidra instances on one signal_host, the port to
communicate to is not fixed and thus has to be acquired over a control
server instance first (enabled with the get_endpoint option).
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket
import sys

import _environment  # noqa F401 # pylint: disable=unused-import
from hidra import Transfer


def get_arguments():
    """ Parse the command line arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is runnning",
                        default=socket.getfqdn())
    parser.add_argument("--target_host",
                        type=str,
                        help="Host where the data should be send to",
                        default=socket.getfqdn())
    parser.add_argument("--debug",
                        help="Host where the data should be send to",
                        action="store_true")

    return parser.parse_args()


def main():
    """Requests data from hidra on a query basis.
    """
    arguments = get_arguments()

    if arguments.debug:
        use_log = "debug"
        print("Using debug mode")
    else:
        use_log = False

    targets = [[arguments.target_host, "50101", 1, ".*(tif|cbf)$"]]
    detector_id = socket.getfqdn() # the hidra instance to connect to

    print("\n==== TEST: Query for the newest filename ====\n")

    query = Transfer("QUERY_NEXT",
                     arguments.signal_host,
                     use_log=use_log,
                     detector_id=detector_id)

    query.initiate(targets)

    try:
        query.start()
    except Exception:
        query.stop()
        return

    timeout = None
#    timeout = 2000  # in ms

    while True:
        try:
            [metadata, data] = query.get(timeout)
        except Exception:
            print(sys.exc_info())
            break

        print()
        if metadata and data:
            print("metadata", metadata["filename"])
            print("data", str(data)[:10])
        else:
            print("metadata", metadata)
            print("data", data)
        print()

    query.stop()

    print("\n==== TEST END: Query for the newest filename ====\n")


if __name__ == "__main__":
    main()
