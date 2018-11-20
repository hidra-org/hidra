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
Simulates file creation by touch one file over and over again to trigger
inotify events.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import time


def create_test_files(watch_dir, n_files):
    """Simulate file creation by continuously touching one file.

    Args:
        watch_dir: The directory to create file in.
        n_files: The number of files to create.
    """

    t_start = time.time()
    for _ in range(n_files):
        with open(os.path.join(watch_dir, "test_file"), "w"):
            pass

    t_needed = time.time() - t_start
    print("created {} in {} s, ({} Hz)".format(n_files, t_needed, n_files / t_needed))


def _main():
    watch_dir = "/tmp/watch_tree"
    n_files = 1000000

    create_test_files(watch_dir, n_files)

if __name__ == '__main__':
    _main()
