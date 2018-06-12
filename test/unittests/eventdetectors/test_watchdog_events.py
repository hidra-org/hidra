"""Testing the watchdog_events event detector.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import unittest
import os
import time
from shutil import copyfile

from .__init__ import BASE_DIR
from .test_eventdetector_base import TestEventDetectorBase, create_dir
from watchdog_events import EventDetector

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestEventDetector(TestEventDetectorBase):
    """Specification of tests to be performed for the loaded EventDetecor.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestEventDetector, self).setUp()

        self.config = {
            # TODO normpath to make insensitive to "/" at the end
            "monitored_dir": os.path.join(BASE_DIR, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {
                "IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
                "IN_MOVED_TO": [".log"]
            },
            # "event_timeout": 0.1,
            "history_size": 0,
            "use_cleanup": False,
            "time_till_closed": 1,  # in s
            "action_time": 2  # in s
        }

        self.start = 100
        self.stop = 105

        self.source_file = os.path.join(BASE_DIR, "test_1024B.file")

        self.target_base_path = os.path.join(BASE_DIR, "data", "source")
        self.target_relative_path = os.path.join("local", "raw")

        self.target_file_base = os.path.join(self.target_base_path,
                                             self.target_relative_path)
        # TODO why is this needed?
        self.target_file_base += os.sep

        self.eventdetector = None

        self.time_all_events_detected = (self.config["action_time"]
                                         + self.config["time_till_closed"])

    def _start_eventdetector(self):
        """Sets up the event detector.
        """
        self.eventdetector = EventDetector(self.config, self.log_queue)

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
            target_file = "{}{}".format(self.target_file_base, filename)
            self.log.debug("copy {}".format(target_file))
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
                self.log.debug("event_list", event_list)
                raise

    def test_multiple_files(self):
        """Simulate multiple file creation.

        Simulate incoming files, all created at onces, and check if received
        events are correct.
        """

        create_dir(self.target_file_base)
        self._start_eventdetector()

        expected_result = []
        # generate multiple events
        for i in range(self.start, self.stop):

            filename = "{}.cbf".format(i)
            target_file = "{}{}".format(self.target_file_base, filename)
            self.log.debug("copy {}".format(target_file))
            copyfile(self.source_file, target_file)

            expected_result_dict = {
                u'filename': filename,
                u'source_path': self.target_base_path,
                u'relative_path': self.target_relative_path
            }
            expected_result.append(expected_result_dict)

        time.sleep(self.config["action_time"]
                   + self.config["time_till_closed"])

        # get all detected events
        event_list = self.eventdetector.get_new_event()

        # check that the generated events (and only these) were detected
        try:
            self.assertEqual(len(event_list), self.stop - self.start)
            for res_dict in expected_result:
                self.assertIn(res_dict, event_list)
        except AssertionError:
            # self.log.debug("event_list", event_list)
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
        self.log.debug("Memory usage at start: {} (kb)"
                       .format(memory_usage_old))

#        hp = hpy()
#        hp.setrelheap()

        step_loop = (self.stop - self.start) / steps
        self.log.debug("Used steps:", steps)

        for step in range(steps):
            start = int(self.start + step * step_loop)
            stop = int(start + step_loop)
#            print ("start=", start, "stop=", stop)
            for i in range(start, stop):

                target_file = "{}{}.cbf".format(self.target_file_base, i)
                copyfile(self.source_file, target_file)

                if i % 100 == 0:
                    self.log.debug("copy index {}".format(i))
                    self.eventdetector.get_new_event()

#                time.sleep(0.5)

            memory_usage_new = (
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            self.log.debug("Memory usage in iteration {}: {} (kb)"
                           .format(step, memory_usage_new))
            if memory_usage_new > memory_usage_old:
                memory_usage_old = memory_usage_new
#                print(hp.heap())

    def tearDown(self):
        # to give the eventdetector time to get all events
        # this prevents the other tests to be affected by previour events
        time.sleep(self.time_all_events_detected)
        if self.eventdetector is not None:
            self.eventdetector.stop()
            self.eventdetector = None

        # clean up the created files
        for number in range(self.start, self.stop):
            try:
                target_file = "{}{}.cbf".format(self.target_file_base, number)
                os.remove(target_file)
                self.log.debug("remove {}".format(target_file))
            except OSError:
                pass

        super(TestEventDetector, self).tearDown()
