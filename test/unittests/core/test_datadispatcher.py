"""Testing the task provider.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import time
import zmq
from multiprocessing import Process, freeze_support
from shutil import copyfile

from .__init__ import BASE_DIR
from test_base import TestBase, create_dir
from datadispatcher import DataDispatcher
# import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataDispatcher(TestBase):
    """Specification of tests to be performed for the TaskProvider.
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

        self.context = zmq.Context()

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self.local_target = os.path.join(BASE_DIR, "data", "target")
        self.chunksize = 10485760  # = 1024*1024*10 = 10 MiB

        self.datadispatcher_config = {
            "data_fetcher_type": "file_fetcher",
            "fix_subdirs": ["commissioning", "current", "local"],
            "store_data": False,
            "remove_data": False,
            "chunksize": self.chunksize,
            "local_target": self.local_target,
            "endpoints": self.config["endpoints"],
            "main_pid": self.config["main_pid"],
        }

        self.receiving_ports = ["6005", "6006"]

    def test_taskprovider(self):
        """Simulate incoming data and check if received events are correct.
        """

        source_file = os.path.join(BASE_DIR,
                                   "test",
                                   "test_files",
                                   "test_file.cbf")
        target_file = os.path.join(BASE_DIR,
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

        kwargs = dict(
            dispatcher_id=1,
            endpoints=endpoints,
            chunksize=self.chunksize,
            fixed_stream_addr=fixed_stream_addr,
            config=self.datadispatcher_config,
            log_queue=self.log_queue,
            local_target=self.local_target,
        )
        datadispatcher_pr = Process(target=DataDispatcher, kwargs=kwargs)
        datadispatcher_pr.start()

        # Set up receiver simulator
        receiving_sockets = []
        for port in self.receiving_ports:
            receiving_sockets.append(self.set_up_recv_socket(port))

        metadata = {
            "source_path": os.path.join(BASE_DIR, "data", "source"),
            "relative_path": "local",
            "filename": "100.cbf"
        }

        recv_ports = self.receiving_ports
        targets = [
            ["{}:{}".format(self.con_ip, recv_ports[0]), [".cbf"], "data"],
            ["{}:{}".format(self.con_ip, recv_ports[1]), [".cbf"], "data"]
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
                self.log.info("received: {}".format(recv_message))
        except KeyboardInterrupt:
            pass
        finally:
            datadispatcher_pr.terminate()

            self.stop_socket(name="router_socket", socket=router_socket)

            for i, sckt in enumerate(receiving_sockets):
                self.stop_socket(name="receiving_socket{}".format(i),
                                 socket=sckt)

    def tearDown(self):
        self.context.destroy(0)

        super(TestDataDispatcher, self).tearDown()
