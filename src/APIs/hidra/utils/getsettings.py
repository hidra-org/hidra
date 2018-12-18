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
This is a helper script to get the settings hidra was started with when using
the control-client.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse

#import __init__  # noqa F401  # pylint: disable=unused-import
import hidra.utils as utils


def get_arguments():
    """Parsing the command line arguments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",
                        type=str,
                        default="/opt/hidra/conf/datamanager.conf",
                        help="Location of the configuration file")

    return parser.parse_args()


def main():
    """Parses the settings from the configure file and displays them.
    """

    args = get_arguments()

    config_file = args.config_file
    params = utils.parse_parameters(utils.load_config(config_file))["asection"]

    print("Configured settings:")
    print("Monitored direcory:            {}".format(params["monitored_dir"]))
    print("Watched subdirectories are:    {}".format(params["fix_subdirs"]))

    msg = "Data is written to:            {}"
    if params["store_data"]:
        print(msg.format(params["local_target"]))
    else:
        print(msg.format("Data is not stored locally"))

    msg = "Data is sent to:               {}"
    if params["use_data_stream"]:
        print(msg.format(params["data_stream_targets"]))
    else:
        print(msg.format("Data is not sent as priority stream anywhere"))

    print("Remove data from the detector: {}".format(params["remove_data"]))
    print("Whitelist:                     {}".format(params["whitelist"]))


if __name__ == "__main__":
    main()
