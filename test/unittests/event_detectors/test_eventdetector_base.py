"""Providing a base for the event detector test classes.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import logging
import os
import unittest
from multiprocessing import Queue
from logutils.queue import QueueHandler

import utils


def create_dir(directory):
    """Creates the directory if it does not exist.

    Args:
        directory: The absolute path of the directory to be created.
    """

    if not os.path.isdir(directory):
        os.mkdir(directory)


class TestEventDetectorBase(unittest.TestCase):
    """The Base class from which all event detectors should inherit from.
    """

    def setUp(self):
        self.config = {}
        self.log_queue = False
        self.listener = None
        self.log = None

    def _init_logging(self, loglevel="debug"):
        """Initialize log listener and log queue.

        Args:
            loglevel: The log level with of StreamHandler to be started.
        """

        loglevel = loglevel.lower()

        # Create handler
        handler = utils.get_stream_log_handler(loglevel=loglevel)

        # Start queue listener using the stream handler above
        self.log_queue = Queue(-1)
        self.listener = utils.CustomQueueListener(self.log_queue, handler)
        self.listener.start()

        # Create log and set handler to queue handle
        root = logging.getLogger()
        qhandler = QueueHandler(self.log_queue)
        root.addHandler(qhandler)

        self.log = utils.get_logger("test_eventdetector", self.log_queue)



    def tearDown(self):
        if self.listener is not None:
            self.log_queue.put_nowait(None)
            self.listener.stop()
            self.listener = None
