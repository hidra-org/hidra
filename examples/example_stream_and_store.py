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
This module implements an example for the STREAM mode together with storing
the data.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import os
import socket

from _environment import BASE_DIR
from hidra import Transfer
import hidra.utils as utils


def main():
    """Connects to hidra and stores the streamed data to disk.
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

    arguments = parser.parse_args()

    # enable logging
    logfile_path = os.path.join(BASE_DIR, "logs")
    logfile = os.path.join(logfile_path, "testAPI.log")
    utils.init_logging(logfile, True, "DEBUG")

    targets = [arguments.target_host, "50100", 0]

    print("\n==== TEST: Stream all files and store them ====\n")

    query = Transfer("STREAM", arguments.signal_host, use_log=True)

    query.initiate(targets)

    query.start()

    target_dir = os.path.join(BASE_DIR, "data", "zmq_target")
    target_file = os.path.join(target_dir, "test_store")
    try:
        query.store(target_file)
    except Exception as excp:
        print("Storing data failed.")
        print("Error was:", excp)

    query.stop()

    print("\n==== TEST END: Stream all files and store them ====\n")


if __name__ == "__main__":
    main()
