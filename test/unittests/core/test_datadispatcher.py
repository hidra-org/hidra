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

"""Testing the task provider.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
import os
from multiprocessing import Process, freeze_support
from shutil import copyfile
import time
import zmq

from test_base import TestBase, create_dir
from datadispatcher import DataDispatcher

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataDispatcher(TestBase):
    """Specification of tests to be performed for the DataDispatcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataDispatcher, self).setUp()

        # see https://docs.python.org/2/library/multiprocessing.html#windows
        freeze_support()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip
        # self.base_dir

        self.context = zmq.Context()

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self.local_target = os.path.join(self.base_dir, "data", "target")
        self.chunksize = 10485760  # = 1024*1024*10 = 10 MiB

        self.datadispatcher_config = {
            "datafetcher": {
                "type": "file_fetcher",
                "local_target": self.local_target,
                "store_data": False,
                "remove_data": False,
                "chunksize": self.chunksize,
                "file_fetcher": {
                    "fix_subdirs": ["commissioning", "current", "local"],
                    "store_data": False,
                    "remove_data": False,
                }
            },
            "network": {
                "main_pid": self.config["main_pid"],
                "endpoints": self.config["endpoints"],
            },
            "general": {}
        }

        self.receiving_ports = ["6005", "6006"]

    def test_datadispatcher(self):
        """Simulate incoming data and check if received events are correct.
        """

        source_file = os.path.join(self.base_dir,
                                   "test",
                                   "test_files",
                                   "test_file.cbf")
        target_file = os.path.join(self.base_dir,
                                   "data",
                                   "source",
                                   "local",
                                   "100.cbf")

        copyfile(source_file, target_file)
        time.sleep(0.5)

        fixed_stream_addr = "{}:{}".format(self.con_ip,
                                           self.receiving_ports[1])

        endpoints = self.config["endpoints"]

        router_socket = self.start_socket(
            name="router_socket",
            sock_type=zmq.PUSH,
            sock_con="bind",
            endpoint=endpoints.router_bind
        )

        control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            # it is the sub endpoint because originally this is handled with
            # a zmq thread device
            endpoint=endpoints.control_sub_bind
        )

        kwargs = dict(
            dispatcher_id=1,
            endpoints=endpoints,
            fixed_stream_addr=fixed_stream_addr,
            config=self.datadispatcher_config,
            log_queue=self.log_queue,
        )
        datadispatcher_pr = Process(target=DataDispatcher, kwargs=kwargs)
        datadispatcher_pr.start()

        # Set up receiver simulator
        receiving_sockets = []
        for port in self.receiving_ports:
            receiving_sockets.append(self.set_up_recv_socket(port))

        metadata = {
            "source_path": os.path.join(self.base_dir, "data", "source"),
            "relative_path": "local",
            "filename": "100.cbf"
        }

        recv_ports = self.receiving_ports
        targets = [
            ["{}:{}".format(self.con_ip, recv_ports[0]), 1, "data"],
            ["{}:{}".format(self.con_ip, recv_ports[1]), 1, "data"]
        ]

        message = [json.dumps(metadata).encode("utf-8"),
                   json.dumps(targets).encode("utf-8")]
    #    message = [json.dumps(metadata).encode("utf-8")]

        time.sleep(1)

        router_socket.send_multipart(message)
        self.log.info("send message")

        try:
            for sckt in receiving_sockets:
                recv_message = sckt.recv_multipart()
                recv_message = json.loads(recv_message[0].decode("utf-8"))
                self.log.info("received: %s", recv_message)
        except KeyboardInterrupt:
            pass
        finally:
            self.log.info("send exit signal")
            control_socket.send_multipart([b"control", b"EXIT"])
            datadispatcher_pr.join()

            self.stop_socket(name="router_socket", socket=router_socket)
            self.stop_socket(name="control_socket", socket=router_socket)

            for i, sckt in enumerate(receiving_sockets):
                self.stop_socket(name="receiving_socket{}".format(i),
                                 socket=sckt)

    def tearDown(self):
        self.context.destroy(0)

        super(TestDataDispatcher, self).tearDown()
