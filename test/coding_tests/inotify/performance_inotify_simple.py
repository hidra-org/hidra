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

"""Perform tests on the inotfiy_simple library.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import time

from inotify_simple import INotify, flags

from performance_base import PerformanceBase, do_tests


class CreateAndGet(PerformanceBase):
    """Create and get events with the inotify_simple library.
    """

    def __init__(self, watch_dir, n_files):
        super().__init__(watch_dir, n_files)

        self.wd_to_path = {}

        self.inotify = INotify()
        watch_flags = flags.CREATE | flags.OPEN | flags.MODIFY

        wd = self.inotify.add_watch(self.watch_dir, watch_flags)
        self.wd_to_path[wd] = self.watch_dir

    def run(self):
        """Run the event detection.
        """

        if self.create_job is not None:
            self.create_job.start()

        timeout = self.timeout * 1000  # is in ms
        n_events = 0
        t_start = 0
        run_loop = True
        while run_loop:
            events = self.inotify.read(timeout=timeout)

            if t_start and not events:
                run_loop = False

            for event in events:
                if not t_start:
                    t_start = time.time()

                if event.wd < 0:
                    continue

                event_type = flags.from_mask(event.mask)

                if flags.OPEN in event_type:
                    n_events += 1

        t_needed = time.time() - t_start
        print("n_events {} in {} s, ({} Hz)"
              .format(n_events, t_needed, n_events / t_needed))

        if self.create_job is not None:
            self.create_job.join()


if __name__ == '__main__':
    do_tests(CreateAndGet)
