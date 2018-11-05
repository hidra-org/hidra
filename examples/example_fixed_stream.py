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
This module implements an example for the STREAM to a fixed target mode.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os

from __init__ import BASE_DIR
import utils

from hidra import Transfer


def main():
    """
    The fixes target is configured on the sender side. Start up receiving side.
    """

    # enable logging
    logfile_path = os.path.join(BASE_DIR, "logs")
    logfile = os.path.join(logfile_path, "test_fixedStream.log")
    utils.init_logging(logfile, True, "DEBUG")

    data_port = "50100"

    print("\n==== TEST: Fixed stream ====\n")

    query = Transfer("STREAM", use_log=True)

    query.start(data_port)

    while True:
        try:
            [metadata, data] = query.get()
        except KeyboardInterrupt:
            break
        except Exception as excp:
            print("Getting data failed.")
            print("Error was: {0}".format(excp))
            break

        print()
        try:
            print("metadata of file", metadata["filename"])
            print("data", str(data)[:10])
        except TypeError:
            print("metadata", metadata)
        print()

    query.stop()

    print("\n==== TEST END: Fixed Stream ====\n")


if __name__ == "__main__":
    main()
