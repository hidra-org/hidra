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

"""Perform tests on the inotify library.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import time

import inotify.adapters

from .performance_base import PerformanceBase, do_tests


class CreateAndGet(PerformanceBase):
    """Create and get events with the inotify library.
    """

    def __init__(self, watch_dir, n_files):
        super().__init__(watch_dir, n_files)

        self.inotify = inotify.adapters.InotifyTree(watch_dir)

    def run(self):
        """Run the event detection.
        """

        if self.create_job is not None:
            self.create_job.start()

        n_events = 0
        t_start = 0
        timeout = 2
        while True:
            for event in self.inotify.event_gen(yield_nones=False,
                                                timeout_s=timeout):
                (_, type_names, _, _) = event

                if not t_start:
                    t_start = time.time()

                if "IN_OPEN" in type_names:
                    n_events += 1

                    if n_events == self.n_files:
                        break
            if t_start:
                break

        t_needed = time.time() - t_start
        print("n_events {} in {} s, ({} Hz)"
              .format(n_events, t_needed, n_events / t_needed))

        if self.create_job is not None:
            self.create_job.join()


if __name__ == '__main__':
    do_tests(CreateAndGet)
