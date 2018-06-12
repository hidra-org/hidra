"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import zmq


from .__init__ import BASE_DIR
from .test_datafetcher_base import TestDataFetcherBase
from zmq_fetcher import DataFetcher

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(TestDataFetcherBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataFetcher, self).setUp()

        # Set up config
#        local_target = os.path.join(BASE_DIR, "data", "target")

        self.data_fetcher_config = {
            "context": self.context,
            "remove_data": False,
            "ipc_path": self.config["ipc_dir"],
            "main_pid": self.config["main_pid"],
            "ext_ip": self.ext_ip,
            "cleaner_job_con_str": self.config["con_strs"].cleaner_job_con,
            "cleaner_conf_con_str": self.config["con_strs"].confirm_con,
            "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
            "local_target": None
        }

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

        self.receiving_ports = ["6005", "6006"]

    def test_no_confirmation(self):
        """Simulate file fetching without taking care of confirmation signals.
        """

        datafetcher = DataFetcher(config=self.data_fetcher_config,
                                  log_queue=self.log_queue,
                                  id=0,
                                  context=self.context)

        # Set up receiver simulator
        receiving_socket = []
        for port in self.receiving_ports:
            receiving_socket.append(self._set_up_socket(port))

        data_fetch_con_str = "ipc://{}/{}".format(self.config["ipc_dir"],
                                                  "dataFetch")

        data_fw_socket = self.context.socket(zmq.PUSH)
        data_fw_socket.connect(data_fetch_con_str)
        self.log.info("Start data_fw_socket (connect): '{}'"
                      .format(data_fetch_con_str))

        # Test data fetcher
        prework_source_file = os.path.join(BASE_DIR, "test_file.cbf")

        # read file to send it in data pipe
        with open(prework_source_file, "rb") as file_descriptor:
            file_content = file_descriptor.read()
            self.log.debug("File read")

        data_fw_socket.send(file_content)
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

        datafetcher.get_metadata(targets, metadata)

        datafetcher.send_data(targets, metadata, open_connections)

        datafetcher.finish(targets, metadata, open_connections)

        self.log.debug("open_connections after function call: {}"
                       .format(open_connections))

        try:
            for sckt in receiving_socket:
                recv_message = sckt.recv_multipart()
                recv_message = json.loads(recv_message[0].decode("utf-8"))
                self.log.info("received: {}".format(recv_message))
        except KeyboardInterrupt:
            pass
        finally:
            data_fw_socket.close(0)

            for sckt in receiving_socket:
                sckt.close(0)

    def test_with_confirmation(self):
        """Simulate file fetching while taking care of confirmation signals.
        """
        pass

    def tearDown(self):
        super(TestDataFetcher, self).tearDown()

        self.context.destroy()
