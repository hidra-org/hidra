"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import zmq


from .__init__ import BASE_DIR
from .datafetcher_test_base import DataFetcherTestBase
from zmq_fetcher import (DataFetcher,
                         get_ipc_endpoints,
                         get_tcp_endpoints,
                         get_addrs)

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataFetcher, self).setUp()

        # Set up config
#        local_target = os.path.join(BASE_DIR, "data", "target")

        self.datafetcher_config = {
            "context": self.context,
            "remove_data": False,
            "ipc_dir": self.config["ipc_dir"],
            "main_pid": self.config["main_pid"],
            "ext_ip": self.ext_ip,
            "con_ip": self.con_ip,
            "cleaner_job_con_str": self.config["con_strs"].cleaner_job_con,
            "cleaner_conf_con_str": self.config["con_strs"].confirm_con,
            "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
            "local_target": None
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

        self.datafetcher = DataFetcher(config=self.datafetcher_config,
                                       log_queue=self.log_queue,
                                       fetcher_id=0,
                                       context=self.context)

        ipc_endpoints = get_ipc_endpoints(config=self.datafetcher_config)
        tcp_endpoints = get_tcp_endpoints(config=self.datafetcher_config)
        addrs = get_addrs(ipc_endpoints=ipc_endpoints,
                          tcp_endpoints=tcp_endpoints)

        # Set up receiver simulator
        self.receiving_sockets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        try:
            self.data_fw_socket = self.context.socket(zmq.PUSH)
            self.data_fw_socket.connect(addrs.datafetch_con)
            self.log.info("Start data_fw_socket (connect): '{}'"
                          .format(addrs.datafetch_con))
        except:
            self.log.error("Failed to start data_fw_socket (connect): '{}'"
                           .format(addrs.datafetch_con))
            raise

        # Test data fetcher
        prework_source_file = os.path.join(BASE_DIR, "test_file.cbf")

        # read file to send it in data pipe
        with open(prework_source_file, "rb") as file_descriptor:
            file_content = file_descriptor.read()
            self.log.debug("File read")

        self.data_fw_socket.send(file_content)
        self.log.debug("File send")

        metadata = {
            "source_path": os.path.join(BASE_DIR, "data", "source"),
            "relative_path": os.sep + "local" + os.sep + "raw",
            "filename": "100.cbf"
        }

        targets = [
            ["{}:{}".format(self.con_ip, self.receiving_ports[0]), 1, "data"],
            ["{}:{}".format(self.con_ip, self.receiving_ports[1]), 0, "data"]
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: {}"
                       .format(open_connections))

        self.datafetcher.get_metadata(targets, metadata)

        self.datafetcher.send_data(targets, metadata, open_connections)

        self.datafetcher.finish(targets, metadata, open_connections)

        self.log.debug("open_connections after function call: {}"
                       .format(open_connections))

        try:
            for sckt in self.receiving_sockets:
                recv_message = sckt.recv_multipart()
                recv_message = json.loads(recv_message[0].decode("utf-8"))
                self.log.info("received: {}".format(recv_message))
        except KeyboardInterrupt:
            pass

    def test_with_confirmation(self):
        """Simulate file fetching while taking care of confirmation signals.
        """
        pass

    def tearDown(self):
        if self.data_fw_socket is not None:
            self.log.debug("Closing data_fw_socket")
            self.data_fw_socket.close(0)
            self.data_fw_socket = None

        if self.receiving_sockets is not None:
            self.log.debug("Closing receiving_sockets")
            for sckt in self.receiving_sockets:
                sckt.close(0)
            self.receiving_sockets = None

        if self.datafetcher is not None:
            self.log.debug("Stopping datafetcher")
            self.datafetcher.stop()
            self.datafetcher = None

        super(TestDataFetcher, self).tearDown()
