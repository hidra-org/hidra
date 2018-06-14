"""Providing a base for the data fetchers test classes.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import logging
import os
import socket
import tempfile
import zmq
from collections import namedtuple

import utils
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

        self.context = zmq.Context.instance()

    def _set_up_socket(self, port):
        """Create pull socket and connect to port.

        Args:
            port: Port to connect to.
        """

        sckt = self.context.socket(zmq.PULL)
        con_str = "tcp://{}:{}".format(self.ext_ip, port)
        sckt.bind(con_str)
        self.log.info("Start receiving socket (bind): {}".format(con_str))

        return sckt

    def tearDown(self):
        super(DataFetcherTestBase, self).tearDown()
        self.context.destroy(0)
