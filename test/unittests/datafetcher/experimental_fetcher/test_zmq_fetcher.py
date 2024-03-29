# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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

"""Testing the zmq_fetcher data fetcher.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import json
import os
import zmq

from zmq_fetcher import (DataFetcher,
                         get_ipc_addresses,
                         get_tcp_addresses,
                         get_endpoints)
from ..datafetcher_test_base import DataFetcherTestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super().setUp()

        # Set up config
        self.module_name = "zmq_fetcher"
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
                "use_cleaner": False,
                "type": self.module_name,
                self.module_name: {
                    "context": self.context,
                }
            }
        }

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

        self.receiving_ports = ["6005", "6006"]

        self.datafetcher = None
        self.receiving_sockets = None
        self.data_fw_socket = None

    def test_no_confirmation(self):
        """Simulate file fetching without taking care of confirmation signals.
        """

        self.datafetcher = DataFetcher(self.df_base_config)
        config = self.df_base_config["config"]

        ipc_addresses = get_ipc_addresses(config=config)
        tcp_addresses = get_tcp_addresses(config=config)
        endpoints = get_endpoints(ipc_addresses=ipc_addresses,
                                  tcp_addresses=tcp_addresses)

        # Set up receiver simulator
        self.receiving_sockets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        self.data_fw_socket = self.start_socket(
            name="data_fw_socket",
            sock_type=zmq.PUSH,
            sock_con="connect",
            endpoint=endpoints.datafetch_con
        )

        # Test data fetcher
        prework_source_file = os.path.join(self.base_dir,
                                           "test",
                                           "test_files",
                                           "test_file.cbf")

        # read file to send it in data pipe
        with open(prework_source_file, "rb") as file_descriptor:
            file_content = file_descriptor.read()
            self.log.debug("File read")

        self.data_fw_socket.send(file_content)
        self.log.debug("File send")

        metadata = {
            "source_path": os.path.join(self.base_dir, "data", "source"),
            "relative_path": os.sep + "local" + os.sep + "raw",
            "filename": "100.cbf"
        }

        targets = [
            ["{}:{}".format(self.con_ip, self.receiving_ports[0]), 1, "data"],
            ["{}:{}".format(self.con_ip, self.receiving_ports[1]), 0, "data"]
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: %s",
                       open_connections)

        self.datafetcher.get_metadata(targets, metadata)

        self.datafetcher.send_data(targets, metadata, open_connections)

        self.datafetcher.finish(targets, metadata, open_connections)

        self.log.debug("open_connections after function call: %s",
                       open_connections)

        try:
            for sckt in self.receiving_sockets:
                recv_message = sckt.recv_multipart()
                recv_message = json.loads(recv_message[0].decode("utf-8"))
                self.log.info("received: %s", recv_message)
        except KeyboardInterrupt:
            pass

    def test_with_confirmation(self):
        """Simulate file fetching while taking care of confirmation signals.
        """
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
