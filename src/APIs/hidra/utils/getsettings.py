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

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import os
import sys

# to make windows freeze work (cx_Freeze 5.x)
try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except NameError:
    CURRENT_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))

if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from _environment import BASE_DIR  # noqa E402  # pylint: disable=wrong-import-position
import hidra.utils as utils  # noqa E402  # pylint: disable=wrong-import-position


def get_arguments():
    """Parsing the command line arguments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",
                        type=str,
                        default="/opt/hidra/conf/datamanager.yaml",
                        help="Location of the configuration file")

    return parser.parse_args()


def main():
    """Parses the settings from the configure file and displays them.
    """

    args = get_arguments()

    config_file = args.config_file
    params = utils.load_config(config_file)
    # in case the config file was in the old config format
    # (for backwards compatibility to 4.0.x)
    config = utils.map_conf_format(params, "sender")

    config_ed = config["eventdetector"][config["eventdetector"]["type"]]

    print("Configured settings:")
    print("Monitored directory: {}".format(config_ed["monitored_dir"]))
    print("Watched subdirectories are: {}".format(config_ed["fix_subdirs"]))

    msg = "Data is written to: {}"
    if config["datafetcher"]["store_data"]:
        print(msg.format(config["datafetcher"]["local_target"]))
    else:
        print(msg.format("Data is not stored locally"))

    msg = "Data is sent to: {}"
    if config["datafetcher"]["use_data_stream"]:
        print(msg.format(config["datafetcher"]["data_stream_targets"]))
    else:
        print(msg.format("Data is not sent as priority stream anywhere"))

    print("Remove data from the detector: {}"
          .format(config["datafetcher"]["remove_data"]))
    print("Whitelist: {}".format(config["general"]["whitelist"]))


if __name__ == "__main__":
    main()
