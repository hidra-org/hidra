"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import mock
import time
import zmq


from .__init__ import BASE_DIR
from .datafetcher_test_base import DataFetcherTestBase
from hidra_fetcher import DataFetcher

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataFetcher, self).setUp()

        # Set up config
        self.data_fetcher_config = {
            "fix_subdirs": ["commissioning", "current", "local"],
            "context": self.context,
            "store_data": False,
            "remove_data": False,
            "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
            # "local_target": None,
            "local_target": os.path.join(BASE_DIR, "data", "zmq_target"),
            # "local_target": os.path.join(BASE_DIR, "data", "target"),
            "ipc_dir": self.config["ipc_dir"],
            "main_pid": self.config["main_pid"],
            "endpoints": self.config["endpoints"],
            "ext_ip": self.ext_ip,
            "status_check_resp_port": "50011",
            "confirmation_resp_port": "50012",
        }

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

        self.receiving_ports = ["6005", "6006"]

        self.datafetcher = None
        self.receiving_sockets = None
        self.data_fw_socket = None
        self.data_input = True

    def test_no_confirmation(self):
        """Simulate file fetching without taking care of confirmation signals.
        """
        # mock check_config to be able to enable print_log
        with mock.patch("hidra_fetcher.DataFetcher.check_config"):
            with mock.patch("hidra_fetcher.DataFetcher.setup"):
                self.datafetcher = DataFetcher(config=self.data_fetcher_config,
                                               log_queue=self.log_queue,
                                               fetcher_id=0,
                                               context=self.context)

        self.datafetcher.check_config(print_log=True)
        self.datafetcher.setup()

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
        prework_source_file = os.path.join(BASE_DIR, "test_file.cbf")

        metadata = {
            "source_path": os.path.join(BASE_DIR, "data", "source"),
            "relative_path": os.sep + "local",
            "filename": "100.cbf",
            "filesize": os.stat(prework_source_file).st_size,
            "file_mod_time": time.time(),
            "file_create_time": time.time(),
            "chunksize": self.data_fetcher_config["chunksize"],
            "chunk_number": 0,
        }

        targets = [
            ["{}:{}".format(self.con_ip, self.receiving_ports[0]), 1, "data"],
            ["{}:{}".format(self.con_ip, self.receiving_ports[1]), 0, "data"]
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: {}"
                       .format(open_connections))

        if self.data_input:

            # simulatate data input sent by an other HiDRA instance
            chunksize = self.data_fetcher_config["chunksize"]
            with open(prework_source_file, 'rb') as file_descriptor:
                file_content = file_descriptor.read(chunksize)

            self.data_fw_socket.send_multipart(
                [json.dumps(metadata), file_content]
            )
            self.log.debug("Incoming data sent")

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

        super(TestDataFetcher, self).tearDown()
