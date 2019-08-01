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
import time

try:
    import unittest.mock as mock
except ImportError:
    # for python2
    import mock
import zmq

from hidra_fetcher import DataFetcher
from .datafetcher_test_base import DataFetcherTestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super().setUp()

        # Set up config
        # local_target = None
        local_target = os.path.join(self.base_dir, "data", "zmq_target")
        # local_target = os.path.join(self.base_dir, "data", "target")
        self.module_name = "hidra_fetcher"
        self.datafetcher_config = {
            "network": {
                "ipc_dir": self.config["ipc_dir"],
                "main_pid": self.config["main_pid"],
                "endpoints": self.config["endpoints"],
                "ext_ip": self.ext_ip,
            },
            "datafetcher": {
                "type": self.module_name,
                "store_data": False,
                "remove_data": False,
                "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
                "local_target": local_target,
                self.module_name: {
                    # "fix_subdirs": ["commissioning", "current", "local"],
                    "context": self.context,
                    "status_check_resp_port": "50011",
                    "confirmation_resp_port": "50012",
                }
            }
        }

        self.cleaner_config = {
            "network": {
                "main_pid": self.config["main_pid"]
            }
        }

        self.receiving_ports = ["50102", "50103"]

        self.datafetcher = None
        self.receiving_sockets = None
        self.data_fw_socket = None
        self.data_input = True

    def test_no_confirmation(self):
        """Simulate file fetching without taking care of confirmation signals.
        """
        # mock check_config to be able to enable print_log
        with mock.patch("hidra_fetcher.DataFetcher.check_config"):
            with mock.patch("hidra_fetcher.DataFetcher._setup"):
                self.datafetcher = DataFetcher(config=self.datafetcher_config,
                                               log_queue=self.log_queue,
                                               fetcher_id=0,
                                               context=self.context,
                                               lock=self.lock)

        self.datafetcher.check_config(print_log=True)
        self.datafetcher._setup()

        # Set up receiver simulator
        self.receiving_sockets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        # Set up data forwarding simulator
        fw_endpoint = "ipc://{}/{}_{}".format(self.config["ipc_dir"],
                                              self.config["main_pid"],
                                              "out")

        if self.data_input:
            # create zmq socket to send events
            self.data_fw_socket = self.start_socket(
                name="data_fw_socket",
                sock_type=zmq.PUSH,
                sock_con="bind",
                endpoint=fw_endpoint
            )

        # Test file fetcher
        prework_source_file = os.path.join(self.base_dir,
                                           "test",
                                           "test_files",
                                           "test_file.cbf")

        config_df = self.datafetcher_config["datafetcher"]
        metadata = {
            "source_path": os.path.join(self.base_dir, "data", "source"),
            "relative_path": os.sep + "local",
            "filename": "100.cbf",
            "filesize": os.stat(prework_source_file).st_size,
            "file_mod_time": time.time(),
            "file_create_time": time.time(),
            "chunksize": config_df["chunksize"],
            "chunk_number": 0,
        }

        targets = [
            ["{}:{}".format(self.con_ip, self.receiving_ports[0]), 1, "data"],
            ["{}:{}".format(self.con_ip, self.receiving_ports[1]), 0, "data"]
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: %s",
                       open_connections)

        if self.data_input:

            # simulatate data input sent by an other HiDRA instance
            chunksize = config_df["chunksize"]
            with open(prework_source_file, 'rb') as file_descriptor:
                file_content = file_descriptor.read(chunksize)

            self.data_fw_socket.send_multipart(
                [json.dumps(metadata).encode("utf-8"), file_content]
            )
            self.log.debug("Incoming data sent")

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
        if self.data_input:
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
