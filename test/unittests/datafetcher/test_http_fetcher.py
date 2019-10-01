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

"""Testing the http_fetcher data fetcher.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import json
import os
import subprocess

from http_fetcher import DataFetcher
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
        self.module_name = "http_fetcher"
        self.df_base_config[config] = {
            "network": {
                "ipc_dir": self.config["ipc_dir"],
                "main_pid": self.config["main_pid"],
                "endpoints": self.config["endpoints"],
            },
            "datafetcher": {
                "store_data": True,
                "remove_data": False,
                "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
                "local_target": os.path.join(self.base_dir, "data", "target"),
                "type": self.module_name,
                self.module_name: {
                    "session": None,
                    "fix_subdirs": ["commissioning", "current", "local"],
                }
            }
        }

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

        self.receiving_ports = ["6005", "6006"]

    def test_no_confirmation(self):
        """Simulate file fetching without taking care of confirmation signals.
        """

        self.datafetcher = DataFetcher(self.df_base_config)

        # Set up receiver simulator
        receiving_socket = []
        for port in self.receiving_ports:
            receiving_socket.append(self.set_up_recv_socket(port))

        # Test data fetcher
        filename = "test01.cbf"
        prework_source_file = os.path.join(self.base_dir,
                                           "test",
                                           "test_files",
                                           "test_file.cbf")

        # read file to send it in data pipe
        self.log.debug("copy file to asap3-mon")
        # os.system('scp "%s" "%s:%s"' % (localfile, remotehost, remotefile) )
        subprocess.call("scp {} root@asap3-mon:/var/www/html/data/{}"
                        .format(prework_source_file, filename), shell=True)

        metadata = {
            "source_path": "http://asap3-mon/data",
            "relative_path": "",
            "filename": filename
        }

        targets = [
            ["{}:{}".format(self.con_ip, self.receiving_ports[0]), 1, "data"],
            ["{}:{}".format(self.con_ip, self.receiving_ports[1]), 0, "data"]
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: %s",
                       open_connections)

        datafetcher.get_metadata(targets, metadata)
        # source_file = "http://131.169.55.170/test_httpget/data/test_file.cbf"

        datafetcher.send_data(targets, metadata, open_connections)

        datafetcher.finish(targets, metadata, open_connections)

        self.log.debug("open_connections after function call: %s",
                       open_connections)

        try:
            for sckt in receiving_socket:
                recv_message = sckt.recv_multipart()
                recv_message = json.loads(recv_message[0].decode("utf-8"))
                self.log.info("received: %s", recv_message)
        except KeyboardInterrupt:
            pass
        finally:

            subprocess.call('ssh root@asap3-mon rm "/var/www/html/data/{}"'
                            .format(filename), shell=True)

            for sckt in receiving_socket:
                sckt.close(0)

            datafetcher.stop()

    def test_with_confirmation(self):
        """Simulate file fetching while taking care of confirmation signals.
        """
        pass
