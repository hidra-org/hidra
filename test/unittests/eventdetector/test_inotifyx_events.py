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

import unittest
import os
import time
import logging
from shutil import copyfile

from inotifyx_events import EventDetector
from .eventdetector_test_base import EventDetectorTestBase, create_dir

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

        self.module_name = "inotifyx_events"

        self.config_module = {
            "monitored_dir": os.path.join(self.base_dir, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {
                "IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
                "IN_MOVED_TO": [".log"]
            },
            "event_timeout": 0.1,
            "history_size": 0,
            "use_cleanup": False,
            "time_till_closed": 5,
            "action_time": 120
        }

        self.ed_base_config["config"]["eventdetector"] = {
            "type": self.module_name,
            self.module_name: self.config_module
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

    def test_eventdetector(self):
        """Simulate incoming data and check if received events are correct.
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

#        self.config_module["use_cleanup"] = True
#        self.config_module["monitored_events"] = {
#            "Some_supid_event": [".tif", ".cbf", ".file"]
#        }
#        self.config_module["time_till_closed"] = 0.2
#        self.config_module["action_time"] = 0.5

        self.start = 100
        self.stop = 30000
        steps = 10

        memory_usage_old = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.log.info("Memory usage at start: %s (kb)", memory_usage_old)

#        hp = hpy()
#        hp.setrelheap()

        step_loop = (self.stop - self.start) / steps
        self.log.info("Used steps: %s", steps)

        try:
            for step in range(steps):
                start = int(self.start + step * step_loop)
                stop = start + step_loop
#                self.log.debug("start=%s, stop=%s", start, stop)
                for i in range(start, stop):

                    target_file = "{}{}.cbf".format(self.target_file_base, i)
                    copyfile(self.source_file, target_file)
                    time.sleep(0.1)

                    if i % 100 == 0:
                        self.log.info("copy index %s", i)
                        event_list = self.eventdetector.get_new_event()

#                    time.sleep(0.5)

                memory_usage_new = (
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                self.log.info("Memory usage in iteration %s: %s (kb)",
                              step, memory_usage_new)
                if memory_usage_new > memory_usage_old:
                    memory_usage_old = memory_usage_new
#                    self.log.debug(hp.heap())

        except KeyboardInterrupt:
            pass
        finally:
            if self.config_module["use_cleanup"]:
                time.sleep(4)
                event_list = self.eventdetector.get_new_event()
                self.log.debug("len of event_list=%s", len(event_list))

                memory_usage_new = (
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                self.log.info("Memory usage: %s (kb)", memory_usage_new)
                time.sleep(1)

                event_list = self.eventdetector.get_new_event()
                self.log.debug("len of event_list=%s", len(event_list))

                memory_usage_new = (
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                self.log.info("Memory usage: %s (kb)", memory_usage_new)

                event_list = self.eventdetector.get_new_event()
                self.log.info("len of event_list=%s", len(event_list))

            memory_usage_new = (
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            self.log.info("Memory usage before stop: %s (kb)",
                          memory_usage_new)
            time.sleep(5)

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
