# Copyright (C) 2019  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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
This module implements an example how to connect to the statserver and query
the current config.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import json
import zmq

import hidra.utils as utils


def get_arguments():
    """Get the command line arguments.

    Returns;
        Command line arguments in a namespace object
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--pid",
                        type=int,
                        help="PID as which the hidra datamanager is running",
                        required=True)

    return parser.parse_args()


def main():
    """Connect to the StatServer.
    """

    args = get_arguments()

    hidra_pid = args.pid

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("ipc:///tmp/hidra/{}_stats_exposing".format(hidra_pid))

    key = json.dumps("config").encode()
    socket.send(key)

    answer = socket.recv()
    answer = json.loads(answer.decode())
    print("answer=", answer)

    endpt = utils.Endpoints(*answer["network"]["endpoints"])
    print("com_con=%s", endpt.com_con)


if __name__ == "__main__":
    main()
