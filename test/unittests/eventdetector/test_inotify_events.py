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

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import copy
import logging
import os
import re
from shutil import copyfile
import time

try:
    import unittest.mock as mock
except ImportError:
    # for python2
    import mock

from inotify_events import EventDetector
from .eventdetector_test_base import EventDetectorTestBase, create_dir

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

# pylint: disable=protected-access


class TestEventDetector(EventDetectorTestBase):
    """Specification of tests to be performed for the loaded EventDetecor.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super().setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        self.module_name = "inotify_events"
        config_module = {
            "monitored_dir": os.path.join(self.base_dir, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {
                "IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
                "IN_MOVED_TO": [".log"]
            },
            "event_timeout": 0.5,
            "history_size": 0,
            "use_cleanup": False,
            "time_till_closed": 5,
            "action_time": 120
        }

        self.ed_base_config["config"]["eventdetector"] = {
            "type": self.module_name,
            self.module_name: config_module
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

        self.eventdetector = EventDetector(self.ed_base_config)

    def test_setup(self):
        """Simulate setup of event detector.
        """

        # --------------------------------------------------------------------
        # regex + no cleanup
        # --------------------------------------------------------------------

        config_module = (
            self.ed_base_config["config"]["eventdetector"][self.module_name]
        )

        orig_value = config_module["monitored_events"]
        config_module["monitored_events"] = {
            "IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
            "IN_MOVED_TO": [".log"]
        }

        with mock.patch("inotify_events.EventDetector._setup"):
            with mock.patch("inotify_events.EventDetector.check_config"):
                with mock.patch("hidra.utils.get_logger"):
                    self.eventdetector = EventDetector(self.ed_base_config)

        self.eventdetector._setup()

        expected_result1 = {
            "IN_CLOSE_WRITE": re.compile(".*(.tif|.cbf|.file)$"),
            "IN_MOVED_TO": re.compile(".*(.log)$")
        }
        expected_result2 = re.compile("(.*(.log)$|.*(.tif|.cbf|.file)$)$")
        expected_result2b = re.compile("(.*(.tif|.cbf|.file)$|.*(.log)$)$")

        self.assertDictEqual(self.eventdetector.mon_regex_per_event,
                             expected_result1)
        try:
            self.assertEqual(self.eventdetector.mon_regex, expected_result2)
        except AssertionError:
            # depending on the order the dictionary is evaluted the regex
            # also changes
            self.assertEqual(self.eventdetector.mon_regex, expected_result2b)

        self.assertIsNone(self.eventdetector.cleanup_thread)

        # revert changes
        config_module["monitored_events"] = orig_value

        # --------------------------------------------------------------------
        # use cleanup
        # --------------------------------------------------------------------

        config_module = (
            self.ed_base_config["config"]["eventdetector"][self.module_name]
        )

        orig_value = config_module["use_cleanup"]
        config_module["use_cleanup"] = True

        with mock.patch("inotify_events.EventDetector._setup"):
            with mock.patch("inotify_events.EventDetector.check_config"):
                with mock.patch("hidra.utils.get_logger"):
                    self.eventdetector = EventDetector(self.ed_base_config)

        with mock.patch("inotify_events.CleanUp"):
            self.eventdetector._setup()

            self.assertIsInstance(self.eventdetector.cleanup_thread,
                                  mock.MagicMock)

        # revert changes
        config_module["use_cleanup"] = orig_value

    def test_module_functionality(self):
        """Tests the module without simulating anything.
        """

        create_dir(self.target_file_base)
        self._start_eventdetector()

        for i in range(self.start, self.stop):

            filename = "{}.cbf".format(i)
            target_file = "{}{}".format(self.target_file_base, filename)
            self.log.debug("copy %s", target_file)
            copyfile(self.source_file, target_file)
            time.sleep(0.1)

            event_list = self.eventdetector.get_new_event()
            expected_result_dict = {
                u'filename': filename,
                u'source_path': self.target_base_path,
                u'relative_path': self.target_relative_path
            }

            try:
                self.assertEqual(len(event_list), 1)
                self.assertDictEqual(event_list[0],
                                     expected_result_dict)
            except AssertionError:
                self.log.debug("event_list %s", event_list)
                raise

    def manual_testing_test_run(self):
        """Starting an event detector for manual testing.
        """
        self._start_eventdetector()

        for _ in range(self.start, self.stop):
            event_list = self.eventdetector.get_new_event()
            print("event_list", event_list)

    def tearDown(self):
        if self.eventdetector is not None:
            self.eventdetector.stop()
            self.eventdetector = None

        # clean up the created files
        for number in range(self.start, self.stop):
            try:
                target_file = "{}{}.cbf".format(self.target_file_base, number)
                os.remove(target_file)
                logging.debug("remove %s", target_file)
            except OSError:
                pass

        super().tearDown()
