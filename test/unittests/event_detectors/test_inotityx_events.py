import unittest
import os
import time
import logging
from shutil import copyfile

from __init__ import BASE_DIR
from test_eventdetector_base import TestEventDetectorBase
from inotifyx_events import EventDetector


class TestInotifyxEvents(TestEventDetectorBase):

    def setUp(self):
        super(TestInotifyxEvents, self).setUp()

        # methods inherited from parent class
        # explicit definition here for better readability
        self._init_logging = super(TestInotifyxEvents, self)._init_logging
        self._create_dir = super(TestInotifyxEvents, self)._create_dir

        self.config = {
            "monitored_dir": os.path.join(BASE_DIR, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {"IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
                                 "IN_MOVED_TO": [".log"]},
            # "event_timeout": 0.1,
            "history_size": 0,
            "use_cleanup": False,
            "time_till_closed": 5,
            "action_time": 120
        }

        self.start = 100
        self.stop = 110

        self.source_file = os.path.join(BASE_DIR, "test_1024B.file")

        self.target_base_path = os.path.join(BASE_DIR, "data", "source")
        self.target_relative_path = os.path.join("local", "raw")

        self.target_file_base = os.path.join(self.target_base_path,
                                             self.target_relative_path)
        # TODO why is this needed?
        self.target_file_base += os.sep

    def _start_eventdetector(self):
        self.eventdetector = EventDetector(self.config, self.log_queue)

    def test_eventdetector(self):
        self._init_logging()
        self._create_dir(self.target_file_base)
        self._start_eventdetector()

        for i in range(self.start, self.stop):

            filename = "{}.cbf".format(i)
            target_file = "{}{}".format(self.target_file_base, filename)
            print("copy {}".format(target_file))
            copyfile(self.source_file, target_file)
            time.sleep(0.1)

            event_list = self.eventdetector.get_new_event()
            expected_result_dict = {
                u'filename': filename,
                u'source_path': self.target_base_path,
                u'relative_path': self.target_relative_path
            }

            self.assertEqual(len(event_list), 1)
            self.assertDictEqual(event_list[0],
                                 expected_result_dict)

    def tearDown(self):
        self.eventdetector.stop()

        # clean up the created files
        for number in range(self.start, self.stop):
            try:
                target_file = "{}{}.cbf".format(self.target_file_base, number)
                print("remove {}".format(target_file))
                os.remove(target_file)
            except OSError:
                pass

        super(TestInotifyxEvents, self).tearDown()


if __name__ == '__main__':
    unittest.main()
