"""Providing a base for the data fetchers test classes.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import threading
import zmq

from test_base import TestBase, create_dir

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcherTestBase(TestBase):
    """The Base class from which all data fetchers should inherit from.
    """

    def setUp(self):
        super(DataFetcherTestBase, self).setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self.context = zmq.Context()
        self.lock = threading.Lock()

    def tearDown(self):
        super(DataFetcherTestBase, self).tearDown()
        self.context.destroy(0)
