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
from multiprocessing import Process
import os
from shutil import copyfile
import time
import zmq

from file_fetcher import DataFetcher, Cleaner
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
        local_target = None
        # local_target = os.path.join(self.base_dir, "data", "target")
        self.module_name = "file_fetcher"
        self.df_base_config["config"] = {
            "network": {
                "main_pid": self.config["main_pid"],
                "endpoints": self.config["endpoints"]
            },
            "datafetcher": {
                "type": self.module_name,
                "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
                "store_data": False,
                "remove_data": False,
                "local_target": local_target,
                self.module_name: {
                    "fix_subdirs": ["commissioning", "current", "local"],
                }
            }
        }

        self.cleaner_config = {
            "network": {
                "main_pid": self.config["main_pid"]
            }
        }

        self.receiving_ports = ["6005", "6006"]

        self.datafetcher = None
        self.receiving_sockets = None
        self.control_pub_socket = None

    def test_no_confirmation(self):
        """Simulate file fetching without taking care of confirmation signals.
        """

        self.datafetcher = DataFetcher(self.df_base_config)

        # Set up receiver simulator
        self.receiving_sockets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        # Test file fetcher
        source_dir = os.path.join(self.base_dir, "data", "source")
        prework_source_file = os.path.join(self.base_dir,
                                           "test",
                                           "test_files",
                                           "test_file.cbf")
        prework_target_file = os.path.join(source_dir, "local", "100.cbf")

        copyfile(prework_source_file, prework_target_file)
        time.sleep(0.5)

        metadata = {
            "source_path": os.path.join(self.base_dir, "data", "source"),
            "relative_path": os.sep + "local",
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

        self.datafetcher = DataFetcher(self.df_base_config)

        self.config["remove_data"] = "with_confirmation"
        endpoints = self.config["endpoints"]

        # Set up cleaner
        kwargs = dict(
            config=self.cleaner_config,
            log_queue=self.log_queue,
            endpoints=endpoints,
            context=self.context
        )
        cleaner_pr = Process(target=Cleaner, kwargs=kwargs)
        cleaner_pr.start()

        # Set up receiver simulator
        self.receiving_sockets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        confirmation_socket = self.start_socket(
            name="confirmation_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            endpoint=endpoints.confirm_bind
        )

        # create control socket
        # control messages are not send over an forwarder, thus the
        # control_sub endpoint is used directly
        self.control_pub_socket = self.start_socket(
            name="control_pub_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            endpoint=endpoints.control_sub_bind
        )

        # Test file fetcher
        source_dir = os.path.join(self.base_dir, "data", "source")
        prework_source_file = os.path.join(self.base_dir,
                                           "test",
                                           "test_files",
                                           "test_file.cbf")
        prework_target_file = os.path.join(source_dir, "local", "100.cbf")

        copyfile(prework_source_file, prework_target_file)
        time.sleep(0.5)

        metadata = {
            "source_path": source_dir,
            "relative_path": os.sep + "local",
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

        # generate file identifier
        if metadata["relative_path"].startswith("/"):
            metadata["relative_path"] = metadata["relative_path"][1:]

        file_id = os.path.join(metadata["relative_path"],
                               metadata["filename"])

        # send file identifier to cleaner
        confirmation_socket.send(file_id.encode("utf-8"))
        self.log.debug("confirmation sent %s", file_id)

        self.log.debug("open_connections after function call: %s",
                       open_connections)

        try:
            for sckt in self.receiving_sockets:
                recv_message = sckt.recv_multipart()
                recv_message = json.loads(recv_message[0].decode("utf-8"))
                self.log.info("received: %s", recv_message)
        except KeyboardInterrupt:
            pass

        self.stop_socket(name="confirmation_socket",
                         socket=confirmation_socket)

    def tearDown(self):
        if self.control_pub_socket is not None:
            self.log.debug("Sending control signal: EXIT")
            self.control_pub_socket.send_multipart([b"control", b"EXIT"])

            # give signal time to arrive
            time.sleep(1)

        if self.receiving_sockets is not None:
            for i, sckt in enumerate(self.receiving_sockets):
                self.stop_socket(name="receiving_socket{}".format(i),
                                 socket=sckt)
            self.receiving_sockets = None

        if self.datafetcher is not None:
            self.log.debug("Stopping datafetcher")
            self.datafetcher.stop()
            self.datafetcher = None

        self.stop_socket(name="control_pub_socket")

        super().tearDown()
