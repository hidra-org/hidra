"""Providing a base for the event detector test classes.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

from test_base import TestBase, create_dir  # noqa F401  # pylint: disable=unused-import

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetectorTestBase(TestBase):
    """The Base class from which all event detectors should inherit from.
    """

    def setUp(self):
        super(EventDetectorTestBase, self).setUp()

    def tearDown(self):
        super(EventDetectorTestBase, self).tearDown()
