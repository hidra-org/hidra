# Copyright (C) 2019  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""Testing the sync_lambda_fetcher data fetcher.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import json
import numpy as np
import os
import zmq

try:
    import unittest.mock as mock
except ImportError:
    # for python2
    import mock

import experimental.sync_lambda_fetcher as fetcher
from .datafetcher_test_base import DataFetcherTestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super().setUp()

        endpoint = ("ipc://{ipc_dir}/{pid}_internal_com"
                    .format(ipc_dir="/tmp/hidra", pid=1234))

        # Set up config
        self.module_name = "sync_lambda_fetcher"
        self.module_config = {
            "context": self.context,
            "internal_com_endpoint": endpoint
        }
        self.df_base_config["config"] = {
            "network": {
                "ipc_dir": self.config["ipc_dir"],
                "main_pid": self.config["main_pid"],
                "ext_ip": self.ext_ip,
                "con_ip": self.con_ip,
                "endpoints": self.config["endpoints"]
            },
            "datafetcher": {
                "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
                "store_data": False,
                "remove_data": False,
                "local_target": None,
                "type": self.module_name,
                self.module_name: self.module_config
            }
        }

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

        self.datafetcher = None
        self.receiving_sockets = None
        self.data_fw_socket = None

    def test_datafetcher(self):
        """Simulate file fetching without taking care of confirmation signals.
        """

        self.datafetcher = fetcher.DataFetcher(self.df_base_config)

        self.internal_com_socket = self.start_socket(
            name="internal_com_socket",
            sock_type=zmq.PUSH,
            sock_con="connect",
            endpoint=self.module_config["internal_com_endpoint"]
        )

        # Set up receiver simulator
        receiving_port = 50100
        receiving_socket = self.set_up_recv_socket(receiving_port)

        result_array = [np.array([1,2,3])]

        metadata = {
            "source_path": "",
            "relative_path": "",
            "filename": "0",
            "additional_info": [
                {
                    "dtype": str(i.dtype),
                    "shape": i.shape
                }
                for i in result_array
            ]
        }

        targets = [
            ["{}:{}".format(self.con_ip, receiving_port), 1, "data"],
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: %s",
                       open_connections)

        self.datafetcher.get_metadata(targets, metadata)

        self.datafetcher.socket = mock.MagicMock()
        self.datafetcher.socket.recv.return_value = result_array
        self.datafetcher.send_data(targets, metadata, open_connections)

        self.datafetcher.finish(targets, metadata, open_connections)

        self.log.debug("open_connections after function call: %s",
                       open_connections)
        try:
            recv_message = receiving_socket.recv_multipart()
            recv_message = json.loads(recv_message[0].decode("utf-8"))
            self.log.info("received: %s", recv_message)
        except KeyboardInterrupt:
            pass

    def tearDown(self):
        self.stop_socket(name="data_fw_socket")

        if self.receiving_sockets is not None:
            for i, sckt in enumerate(self.receiving_sockets):
                self.stop_socket(name="receiving_socket{}".format(i),
                                 socket=sckt)
            self.receiving_sockets = None

        if self.datafetcher is not None:
            self.log.debug("Stopping datafetcher")
            self.datafetcher.stop()
            self.datafetcher = None

        super().tearDown()
