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
from test_base import TestBase, create_dir

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetectorTestBase(TestBase):
    """The Base class from which all event detectors should inherit from.
    """

    def setUp(self):
        super(EventDetectorTestBase, self).setUp()

    def tearDown(self):
        super(EventDetectorTestBase, self).tearDown()
