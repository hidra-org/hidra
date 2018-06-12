"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
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
            "ipc_path": self.config["ipc_dir"],
            "main_pid": self.config["main_pid"],
            "cleaner_job_con_str": self.config["con_strs"].cleaner_job_con,
            "ext_ip": self.ext_ip,
            "status_check_resp_port": "50011",
            "confirmation_resp_port": "50012",
            # config params still to check:
            # "ext_data_port": "50100",
            # "cleaner_job_con_str": None,
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

        # Set up data forwarding simulator
        fw_con_str = "ipc://{}/{}_{}".format(self.config["ipc_dir"],
                                             self.config["main_pid"],
                                             "out")

        data_input = True

        if data_input:
            # create zmq socket to send events
            data_fw_socket = self.context.socket(zmq.PUSH)
            data_fw_socket.bind(fw_con_str)
            self.log.info("Start data_fw_socket (bind): '{}'"
                          .format(fw_con_str))

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

        if data_input:

            # simulatate data input sent by an other HiDRA instance
            chunksize = self.data_fetcher_config["chunksize"]
            with open(prework_source_file, 'rb') as file_descriptor:
                file_content = file_descriptor.read(chunksize)

            data_fw_socket.send_multipart([json.dumps(metadata), file_content])
            self.log.debug("Incoming data sent")

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

            for sckt in receiving_socket:
                sckt.close(0)

            if data_input:
                data_fw_socket.close(0)

    def tearDown(self):
        super(TestDataFetcher, self).tearDown()

        self.context.destroy()
