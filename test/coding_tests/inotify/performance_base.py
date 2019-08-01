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

"""This module contains common part used for all inotify tests.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import multiprocessing
import os
import threading

from external_trigger import create_test_files


class PerformanceBase(object):
    """The base class setting up the environment for the tests.
    """

    def __init__(self, watch_dir, n_files):
        self.create_job = None
        self.timeout = 2

        self.watch_dir = watch_dir
        self.n_files = n_files

        try:
            os.mkdir(self.watch_dir)
        except OSError:
            pass

    def create_job_thread(self):
        """Initiliaze create_job as a thread.
        """

        print("create thread")
        self.create_job = threading.Thread(
            target=create_test_files,
            args=(self.watch_dir, self.n_files)
        )

    def create_job_process(self):
        """Initiliaze create_job as a process.
        """

        print("create process")
        self.create_job = multiprocessing.Process(
            target=create_test_files,
            args=(self.watch_dir, self.n_files)
        )

    def run(self):
        """Get the events.
        """
        pass

    def stop(self):
        """Clean up.
        """
        pass

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


def do_tests(CreateAndGet):
    """Runs performance tests.

    Args:
        CreateAndGet: A class inherited from PerformanceBase to perform tests.
    """

    watch_dir = "/tmp/watch_tree"
    n_files = 1000000

    obj = CreateAndGet(watch_dir, n_files)
    obj.create_job_process()
    obj.run()

    obj = CreateAndGet(watch_dir, n_files)
    obj.create_job_thread()
    obj.run()

    obj = CreateAndGet(watch_dir, n_files)
    print("Please start external trigger, now")
    obj.run()
