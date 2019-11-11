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

"""Testing the file_fetcher data fetcher.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import json
import os

from datafetcher_template import DataFetcher
from .datafetcher_test_base import DataFetcherTestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super().setUp()

        self.datafetcher = None

        # Set up config
        self.module_name = "datafetcher_template"
        self.df_base_config["config"] = {
            "network": {
                "endpoints": None,
                "main_pid": self.config["main_pid"]
            },
            "datafetcher": {
                "type": self.module_name,
                "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
                "local_target": None,
                "remove_data": False,
                self.module_name: {
                }
            }
        }

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

        self.receiving_ports = ["6005", "6006"]

        self.receiving_sockets = None

    def test_no_confirmation(self):
        """Simulate file fetching without taking care of confirmation signals.
        """

        self.datafetcher = DataFetcher(self.df_base_config)

        # Set up receiver simulator
        self.receiving_sockets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        # Test data fetcher
        metadata = {
            "source_path": os.path.join(self.base_dir, "data", "source"),
            "relative_path": os.sep + "local",
            "filename": "100.cbf"
        }

#        prework_source_file = os.path.join(self.base_dir,
#                                           "test",
#                                           "test_files",
#                                           "test_file.cbf")
#        prework_target_file = os.path.join(metadata["source_path"],
#                                           metadata["relative_path"],
#                                           metadata["filename"])

#        copyfile(prework_source_file, prework_target_file)
#        time.sleep(0.5)

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
        if self.receiving_sockets is not None:
            for i, sckt in enumerate(self.receiving_sockets):
                self.stop_socket(name="receiving_socket{}".format(i),
                                 socket=sckt)
            self.receiving_sockets = None

        super().tearDown()
