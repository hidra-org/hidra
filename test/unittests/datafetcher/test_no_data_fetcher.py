# Copyright (C)  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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

"""Testing the file_fetcher data fetcher.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
import os

from no_data_fetcher import DataFetcher
from .datafetcher_test_base import DataFetcherTestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    def setUp(self):
        super(TestDataFetcher, self).setUp()

        # Set up config
        self.module_name = "no_data_fetcher"
        self.datafetcher_config = {
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
                self.module_name: {
                }
            }
        }

        self.receiving_ports = [50200]
        self.receiving_sockets = []

        self.datafetcher = None

    def test_general(self):
        """Simulate file fetching without taking care of confirmation signals.
        """

        self.datafetcher = DataFetcher(config=self.datafetcher_config,
                                       log_queue=self.log_queue,
                                       fetcher_id=0,
                                       context=self.context,
                                       lock=self.lock)

        # Set up receiver simulator
        targets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))
            targets.append(
                ["{}:{}".format(self.con_ip, self.receiving_ports[0]),
                 1,
                 "metadata"]
            )

        metadata = {
            'source_path': '/my_dir',
            'relative_path': 'my_subdir',
            'filename': '{}_000.h5'
        }

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

    def tearDown(self):
        if self.receiving_sockets is not None:
            for i, sckt in enumerate(self.receiving_sockets):
                self.stop_socket(name="receiving_socket{}".format(i),
                                 socket=sckt)
            self.receiving_sockets = None

        if self.datafetcher is not None:
            self.log.debug("Stopping datafetcher")
            self.datafetcher.stop()
            self.datafetcher = None

        super(TestDataFetcher, self).tearDown()
