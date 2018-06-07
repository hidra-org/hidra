import unittest
import os
import time
import logging
from shutil import copyfile
from multiprocessing import Queue
from logutils.queue import QueueHandler

from __init__ import BASE_DIR
from watchdog_events import EventDetector
import utils


class TestWatchdogEvents(unittest.TestCase):

    def setUp(self):
        self.config = {
            # TODO normpath to make insensitive to "/" at the end
            "monitored_dir": os.path.join(BASE_DIR, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {"IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
                                 "IN_MOVED_TO": [".log"]},
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

        self.log_queue = False
        self.listener = None

    # not all tests want to use these thus it is separated from setUp
    def _init_logging(self, loglevel="debug"):
        # Create log and set handler
#        root = logging.getLogger()
#        root.setLevel(loglevel)

        loglevel = loglevel.lower()

        handler = utils.get_stream_log_handler(loglevel=loglevel)

        # Start queue listener using the stream handler above
        self.log_queue = Queue(-1)
        self.listener = utils.CustomQueueListener(self.log_queue, handler)
        self.listener.start()

        # Create log and set handler to queue handle
        root = logging.getLogger()
        qhandler = QueueHandler(self.log_queue)
        root.addHandler(qhandler)

    def _create_target_dir(self):
        if not os.path.isdir(self.target_file_base):
            os.mkdir(self.target_file_base)

    def _start_eventdetector(self):
        self.eventdetector = EventDetector(self.config, self.log_queue)
    # --------------------

    def test_single_file(self):
        self._init_logging()
        self._create_target_dir()
        self._start_eventdetector()

        for i in range(self.start, self.stop):

            # generate an event
            filename = "{}.cbf".format(i)
            target_file = "{}{}".format(self.target_file_base, filename)
            print("copy {}".format(target_file))
            copyfile(self.source_file, target_file)
            time.sleep(self.config["action_time"] + self.config["time_till_closed"])

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
                print("event_list", event_list)
                raise

    def test_multiple_files(self):
        self._init_logging()
        self._create_target_dir()
        self._start_eventdetector()

        expected_result = []
        # generate multiple events
        for i in range(self.start, self.stop):

            filename = "{}.cbf".format(i)
            target_file = "{}{}".format(self.target_file_base, filename)
            print("copy {}".format(target_file))
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
        self.assertEqual(len(event_list), self.stop - self.start)
        for res_dict in expected_result:
            self.assertIn(res_dict, event_list)

    # this should not be executed automatically only if needed for debugging
    @unittest.skip("Only needed for debugging")
    def test_memory_usage(self):
        import resource
        import gc
        # don't care about stuff that would be garbage collected properly
        gc.collect()
#        from guppy import hpy

        self._init_logging(loglevel="info")
        self._create_target_dir()
        self._start_eventdetector()

        self.start = 100
        self.stop = 30000
        steps = 10

        memory_usage_old = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print("Memory usage at start: {} (kb)".format(memory_usage_old))

#        hp = hpy()
#        hp.setrelheap()

        step_loop = (sef.stop - self.start) / steps
        print("Used steps:", steps)

        for s in range(steps):
            start = int(self.start + s * step_loop)
            stop = int(start + step_loop)
#            print ("start=", start, "stop=", stop)
            for i in range(start, stop):

                target_file = "{}{}.cbf".format(target_file_base, i)
                if not determine_mem_usage:
                    logging.debug("copy to {}".format(target_file))
                copyfile(source_file, target_file)

                if i % 100 == 0 or not determine_mem_usage:
                    event_list = eventdetector.get_new_event()
                    if event_list and not determine_mem_usage:
                        print("event_list:", event_list)

#                time.sleep(0.5)

                memory_usage_new = (
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                print("Memory usage in iteration {}: {} (kb)"
                     .format(s, memory_usage_new))
                if memory_usage_new > memory_usage_old:
                    memory_usage_old = memory_usage_new
#                    print(hp.heap())

    def tearDown(self):
        time.sleep(2)
        self.eventdetector.stop()

        # clean up the created files
        for number in range(self.start, self.stop):
            try:
                target_file = "{}{}.cbf".format(self.target_file_base, number)
                print("remove {}".format(target_file))
                os.remove(target_file)
            except OSError:
                pass

        if self.listener is not None:
            self.log_queue.put_nowait(None)
            self.listener.stop()


if __name__ == '__main__':
    unittest.main()
