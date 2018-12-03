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

"""Testing the inotifyx_events event detector.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import copy
import os
import re

import mock

from inotify_events import EventDetector
from .eventdetector_test_base import EventDetectorTestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

# pylint: disable=protected-access


class TestEventDetector(EventDetectorTestBase):
    """Specification of tests to be performed for the loaded EventDetecor.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestEventDetector, self).setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        self.event_detector_config = {
            "monitored_dir": os.path.join(self.base_dir, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {
                "IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
                "IN_MOVED_TO": [".log"]
            },
            # "event_timeout": 0.1,
            "history_size": 0,
            "use_cleanup": False,
            "time_till_closed": 5,
            "action_time": 120
        }

        self.start = 100
        self.stop = 110

        self.source_file = os.path.join(self.base_dir,
                                        "test",
                                        "test_files",
                                        "test_1024B.file")

        self.target_base_path = os.path.join(self.base_dir, "data", "source")
        self.target_relative_path = os.path.join("local", "raw")

        self.target_file_base = os.path.join(self.target_base_path,
                                             self.target_relative_path)
        # TODO why is this needed?
        self.target_file_base += os.sep

        self.eventdetector = None

    def _start_eventdetector(self):
        """Sets up the event detector.
        """

        self.eventdetector = EventDetector(self.event_detector_config,
                                           self.log_queue)

    def test_setup(self):
        """Simulate incoming data and check if received events are correct.
        """

        # --------------------------------------------------------------------
        # regex + no cleanup
        # --------------------------------------------------------------------

        monitored_events = {
            "IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
            "IN_MOVED_TO": [".log"]
        }

        config = copy.deepcopy(self.event_detector_config)
        config["monitored_events"] = monitored_events

        with mock.patch("inotify_events.EventDetector._setup"):
            with mock.patch("inotify_events.EventDetector.check_config"):
                with mock.patch("utils.get_logger"):
                    self.eventdetector = EventDetector(config, self.log_queue)

        self.eventdetector._setup()

        expected_result1 = {
            "IN_CLOSE_WRITE": re.compile(".*(.tif|.cbf|.file)$"),
            "IN_MOVED_TO": re.compile(".*(.log)$")
        }
        expected_result2 = re.compile("(.*(.log)$|.*(.tif|.cbf|.file)$)$")

        self.assertDictEqual(self.eventdetector.mon_regex_per_event,
                             expected_result1)
        self.assertEqual(self.eventdetector.mon_regex, expected_result2)

        self.assertIsNone(self.eventdetector.cleanup_thread)

        # --------------------------------------------------------------------
        # use cleanup
        # --------------------------------------------------------------------

        config = copy.deepcopy(self.event_detector_config)
        config["use_cleanup"] = True

        with mock.patch("inotify_events.EventDetector._setup"):
            with mock.patch("inotify_events.EventDetector.check_config"):
                with mock.patch("utils.get_logger"):
                    self.eventdetector = EventDetector(config, self.log_queue)

        with mock.patch("inotify_events.CleanUp"):
            self.eventdetector._setup()

            self.assertIsInstance(self.eventdetector.cleanup_thread,
                                  mock.MagicMock)

    def tearDown(self):
        super(TestEventDetector, self).tearDown()
