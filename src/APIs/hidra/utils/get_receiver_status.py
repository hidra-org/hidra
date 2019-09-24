#!/usr/bin/env python

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
This is a helper script to get the status of the receiver.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import os
import sys
import traceback

# to make windows/suse10 freeze work (cx_Freeze 5.x)
try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except NameError:
    CURRENT_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))

if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from _environment import BASE_DIR  # noqa E402
from hidra.control import ReceiverControl
from hidra import CommunicationFailed
import hidra.utils as utils

# colors to print on screen
CEND = '\033[0m'
CBLACK = '\33[30m'
CRED = '\33[31m'
CGREEN = '\33[32m'
CYELLOW = '\33[33m'
CBLUE = '\33[34m'
CVIOLET = '\33[35m'
CBEIGE = '\33[36m'
CWHITE = '\33[37m'


def get_arguments():
    """Parsing the command line arguments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--host",
                        type=str,
                        help="Host where HiDRA is running")
    parser.add_argument("--config_file",
                        type=str,
                        help="Location of the configuration file to extract "
                             "the host from")

    return parser


def main():
    """Connect to the receiver and show status.
    """

    parser = get_arguments()
    args = parser.parse_args()

    host = args.host

    default_config_file = "/opt/hidra/conf/datamanager.yaml"
    config_file = args.config_file or default_config_file

    if host is not None and config_file is not None:
        parser.error("Either use --host or --config_file but not both.")

    if host is None:
        params = utils.load_config(config_file)
        # in case the config file was in the old config format
        # (for backwards compatibility to 4.0.x)
        config = utils.map_conf_format(params, "sender")

        data_stream_targets = params["datafetcher"]["data_stream_targets"]
        hosts = [target[0] for target in data_stream_targets]

        # TODO make generic
        host = hosts[0]

    control = ReceiverControl(host)

    print("Checking for service hidra receiver on", host, ": ", end="")
    try:
        status = control.get_status()
        if status == ["OK"]:
            print(CGREEN + "running." + CEND)
        else:
            print(CYELLOW +
                  "running but in error state:"
                  + CEND)
            print(status)

    except CommunicationFailed as excp:
        print(CRED + "not reachable." + CEND)
        print(traceback.format_exception_only(type(excp), excp)[0], end="")

    except Exception:
        print(CRED +
              "not reachable. Unknown Error.\n"
              + CEND)
        print(traceback.format_exc())

        sys.exit(1)


if __name__ == "__main__":
    main()
