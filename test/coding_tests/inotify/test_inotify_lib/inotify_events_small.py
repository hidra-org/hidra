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
This module implements an event detector based on the inotify library usable
for systems running inotify.
"""

# pylint: disable=global-statement

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import collections
import copy
import logging
import os
import re
import sys
import threading
import time

import inotify.adapters
from six import iteritems

CURRENT_DIR = os.path.realpath(__file__)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))))

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetector(object):

    def __init__(self, watch_dir):

        self.watch_dir = watch_dir

        self.inotify = inotify.adapters.InotifyTree(self.watch_dir)

    def get_new_event(self):
        return next(self.inotify.event_gen(yield_nones=False))


def call_class():
    watch_dir = os.path.join(BASE_DIR, "data", "source")

    eventdetector = EventDetector(watch_dir)

    with open(os.path.join(watch_dir, "test_file"), "w"):
        pass

    test_dir = os.path.join(watch_dir, "test_dir")
    try:
        os.mkdir(test_dir)
    except OSError:
        pass

    with open(os.path.join(test_dir, "test_file2"), "w"):
        pass

    while True:
        (_, type_names, path, filename) = eventdetector.get_new_event()

        print("PATH=[{}] FILENAME=[{}] EVENT_TYPES={}"
              .format(path, filename, type_names))


if __name__ == "__main__":
    call_class()
