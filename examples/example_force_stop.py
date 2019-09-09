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
This module implements an example for stopping an open connection when the
associated program shut down without de-registering.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import argparse
import socket

from _environment import BASE_DIR
from hidra import Transfer
import hidra.utils as utils


def main():
    """De-registers a connection from a broken down program.
    """

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


if __name__ == "__main__":
    main()
