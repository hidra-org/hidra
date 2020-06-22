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

"""Testing the watchdog_events event detector.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import os
import time
import unittest
from shutil import copyfile

from test_base import create_dir
from eventdetectors.watchdog_events import EventDetector
from .eventdetector_test_base import EventDetectorTestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestEventDetector(EventDetectorTestBase):
    """Specification of tests to be performed for the loaded EventDetector.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super().setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        self.module_name = "watchdog_events"
        self.config_module = {
            # TODO normpath to make insensitive to "/" at the end
            "monitored_dir": os.path.join(self.base_dir, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {
                "IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
                "IN_MOVED_TO": [".log"]
            },
            "history_size": 0,
            "use_cleanup": False,
            "time_till_closed": 0.5,  # in s
            "action_time": 1  # in s
        }

        self.ed_base_config["config"]["eventdetector"] = {
            "type": self.module_name,
            self.module_name: self.config_module
        }

        self.start = 100
        self.stop = 105

        self.source_file = os.path.join(self.base_dir,
                                        "test",
                                        "test_files",
                                        "test_1024B.file")

        self.target_base_path = os.path.join(self.base_dir, "data", "source")
        self.target_relative_path = os.path.join("local", "raw")

        self.target_file_base = os.path.join(self.target_base_path,
                                             self.target_relative_path)

        self.eventdetector = None

        self.time_all_events_detected = (
            self.config_module["action_time"]
            + self.config_module["time_till_closed"]
            + 0.01  # reaction time
        )

    def _start_eventdetector(self):
        """Sets up the event detector.
        """
        self.eventdetector = EventDetector(self.ed_base_config)

    def test_single_file(self):
        """Simulate single file creation.

        Simulate incoming files, one created after the other, and check if
        received events are correct.
        """

        create_dir(self.target_file_base)
        self._start_eventdetector()

        for i in range(self.start, self.stop):

            # generate an event
            filename = "{}.cbf".format(i)
            target_file = os.path.join(self.target_file_base,
                                       "{}".format(filename))
            self.log.debug("copy %s", target_file)
            copyfile(self.source_file, target_file)
            time.sleep(self.time_all_events_detected)

            # get all detected events
            event_list = self.eventdetector.get_new_event()
            expected_result_dict = {
                u'filename': filename,
                u'source_path': self.target_base_path,
                u'relative_path': self.target_relative_path
            }

            # check if the generated event was the only detected one
            try:
                self.assertEqual(len(event_list), 1)
                self.assertDictEqual(event_list[0], expected_result_dict)
            except AssertionError:
                self.log.debug("event_list %s", event_list)
                raise

    def test_multiple_files(self):
        """Simulate multiple file creation.

        Simulate incoming files, all created at once, and check if received
        events are correct.
        """

        create_dir(self.target_file_base)
        self._start_eventdetector()

        expected_result = []
        # generate multiple events
        for i in range(self.start, self.stop):

            filename = "{}.cbf".format(i)
            target_file = os.path.join(self.target_file_base,
                                       "{}".format(filename))
            self.log.debug("copy %s", target_file)
            copyfile(self.source_file, target_file)

            expected_result_dict = {
                u'filename': filename,
                u'source_path': self.target_base_path,
                u'relative_path': self.target_relative_path
            }
            expected_result.append(expected_result_dict)

        time.sleep(self.config_module["action_time"]
                   + self.config_module["time_till_closed"])

        # get all detected events
        event_list = self.eventdetector.get_new_event()

        # check that the generated events (and only these) were detected
        try:
            self.assertEqual(len(event_list), self.stop - self.start)
            for res_dict in expected_result:
                self.assertIn(res_dict, event_list)
        except AssertionError:
            # self.log.debug("event_list %s", event_list)
            raise

    # this should not be executed automatically only if needed for debugging
    @unittest.skip("Only needed for debugging")
    def test_memory_usage(self):
        """Testing the memory usage of the event detector.

        This should not be tested automatically but only if really needed.
        """

        import resource
        import gc
        # don't care about stuff that would be garbage collected properly
        gc.collect()
#        from guppy import hpy

#        self._init_logging(loglevel="info")
        create_dir(self.target_file_base)
        self._start_eventdetector()

        self.start = 100
        self.stop = 30000
        steps = 10

        memory_usage_old = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.log.debug("Memory usage at start: %s (kb)", memory_usage_old)

#        hp = hpy()
#        hp.setrelheap()

        step_loop = (self.stop - self.start) / steps
        self.log.debug("Used steps: %s", steps)

        for step in range(steps):
            start = int(self.start + step * step_loop)
            stop = int(start + step_loop)
#            print ("start=", start, "stop=", stop)
            for i in range(start, stop):

                target_file = os.path.join(self.target_file_base,
                                           "{}.cbf".format(i))
                copyfile(self.source_file, target_file)

                if i % 100 == 0:
                    self.log.debug("copy index %s", i)
                    self.eventdetector.get_new_event()

#                time.sleep(0.5)

            memory_usage_new = (
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            self.log.debug("Memory usage in iteration %s: %$ (kb)",
                           step, memory_usage_new)
            if memory_usage_new > memory_usage_old:
                memory_usage_old = memory_usage_new
#                print(hp.heap())

    def tearDown(self):
        if self.eventdetector is not None:
            self.eventdetector.stop()
            self.eventdetector = None

        # clean up the created files
        for i in range(self.start, self.stop):
            try:
                target_file = os.path.join(self.target_file_base,
                                           "{}.cbf".format(i))
                os.remove(target_file)
                self.log.debug("remove %s", target_file)
            except OSError:
                pass

        super().tearDown()
