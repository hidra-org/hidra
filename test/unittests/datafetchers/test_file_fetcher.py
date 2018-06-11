"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import tempfile
import zmq

from .__init__ import BASE_DIR
from .test_datafetcher_base import TestDataFetcherBase, create_dir
from file_fetcher import DataFetcher


class TestDataFetcher(TestDataFetcherBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataFetcher, self).setUp()

        # methods inherited from parent class
        # explicit definition here for better readability
        self._init_logging = super(TestDataFetcher, self)._init_logging

        self._init_logging()

    def test_datafetcher(self):
        pass

    def tearDown(self):
        super(TestDataFetcher, self).tearDown()
