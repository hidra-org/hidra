"""Providing a base for all test classes.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import logging
import os
import unittest
import socket
import tempfile
import zmq
from collections import namedtuple
from multiprocessing import Queue
from logutils.queue import QueueHandler

import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

#LOGLEVEL = "error"
LOGLEVEL = "debug"


def create_dir(directory, chmod=None, log=logging):
    """Creates the directory if it does not exist.

    Args:
        directory: The absolute path of the directory to be created.
        chmod (optional): Mode bits to change the permissions of the directory
                          to.
    """

    if not os.path.isdir(directory):
        os.mkdir(directory)
        log.info("Creating directory: {}".format(directory))

    if chmod is not None:
        # the permission have to changed explicitly because
        # on some platform they are ignored when called within mkdir
        os.chmod(directory, 0o777)


class TestBase(unittest.TestCase):
    """The Base class from which all data fetchers should inherit from.
    """

    def setUp(self):
        global LOGLEVEL

        self.config = {}
        self.log_queue = False
        self.listener = None
        self.log = None

        self._init_logging(loglevel=LOGLEVEL)

#        main_pid = os.getpid()
#        self.log.debug("{} pid {}".format(self.__class__.__name__, main_pid))

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

        self.log = utils.get_logger("test_datafetcher", self.log_queue)

    def tearDown(self):
        if self.listener is not None:
            self.log_queue.put_nowait(None)
            self.listener.stop()
            self.listener = None
