"""Testing the task provider.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import socket
import tempfile
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

        self.context = zmq.Context.instance()

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
            "cleaner_job_con_str": self.config["con_strs"].cleaner_job_con,
            "main_pid": self.config["main_pid"],
        }

        self.receiving_ports = ["6005", "6006"]

    def test_taskprovider(self):
        """Simulate incoming data and check if received events are correct.
        """

        source_file = os.path.join(BASE_DIR, "test_file.cbf")
        target_file = os.path.join(BASE_DIR,
                                   "data",
                                   "source",
                                   "local",
                                   "100.cbf")

        copyfile(source_file, target_file)
        time.sleep(0.5)

        fixed_stream_con_str = "{}:{}".format(self.con_ip,
                                              self.receiving_ports[1])

        con_strs = self.config["con_strs"]
        kwargs = dict(
            id=1,
            control_con_str=con_strs.control_sub_con,
            router_con_str=con_strs.router_con,
            chunksize=self.chunksize,
            fixed_stream_id=fixed_stream_con_str,
            config=self.datadispatcher_config,
            log_queue=self.log_queue,
            local_target=self.local_target,
            context=self.context
        )
        datadispatcher_pr = Process(target=DataDispatcher, kwargs=kwargs)
        datadispatcher_pr.start()

        router_socket = self.context.socket(zmq.PUSH)
        router_socket.bind(con_strs.router_bind)
        self.log.info("router_socket connected to {}"
                      .format(con_strs.router_bind))

        # Set up receiver simulator
        receiving_sockets = []
        for port in self.receiving_ports:
            receiving_sockets.append(self.set_up_recv_socket(port))

        metadata = {
            "source_path": os.path.join(BASE_DIR, "data", "source"),
            "relative_path": "local",
            "filename": "100.cbf"
        }

        targets = [
            ["{}:{}".format(self.con_ip, self.receiving_ports[0]), [".cbf"], "data"],
            ["{}:{}".format(self.con_ip, self.receiving_ports[1]), [".cbf"], "data"]
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

            router_socket.close(0)
            for sckt in receiving_sockets:
                sckt.close(0)

    def tearDown(self):
        self.context.destroy(0)

        super(TestDataDispatcher, self).tearDown()
