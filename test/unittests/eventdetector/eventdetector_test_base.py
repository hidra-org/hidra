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

"""Providing a base for the event detector test classes.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import zmq

try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib

from test_base import TestBase, create_dir  # noqa F401  # pylint: disable=unused-import

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetectorTestBase(TestBase):
    """The Base class from which all event detectors should inherit from.
    """
    def setUp(self):
        super().setUp()

        self.ed_base_config = {
            "config": {
                "general": {
                    "config_file": pathlib.Path("testconfig.yaml")
                }
            },
            "context": zmq.Context(),
            "log_queue": self.log_queue,
            "check_dep": True
        }
