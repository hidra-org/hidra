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
This module implements the data dispatcher.
"""

# pylint: disable=redefined-variable-type

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from multiprocessing import freeze_support, Queue
import os
import time

import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def main():
    # see https://docs.python.org/2/library/multiprocessing.html#windows
    freeze_support()

    config = {
        "log_file": os.path.join(BASE_DIR, "logs", "test_logging.log"),
        "log_size": 10485760,
        "verbose": True,
        "onscreen": "debug"
    }

    # Get queue
    log_queue = Queue(-1)

    handler = utils.get_log_handlers(
        config["log_file"],
        config["log_size"],
        config["verbose"],
        config["onscreen"]
    )

    # Start queue listener using the stream handler above.
    log_queue_listener = utils.CustomQueueListener(
        log_queue, *handler
    )

    log_queue_listener.start()

    # Create log and set handler to queue handle
    log = utils.get_logger("TestLogging", log_queue)

    log.debug("START")

    while True:
        log.debug("run")
        time.sleep(1)


if __name__ == '__main__':
    main()
