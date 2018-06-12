"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import tempfile
import socket
import zmq


from .__init__ import BASE_DIR
from .test_datafetcher_base import (
    TestDataFetcherBase,
    create_dir,
    set_con_strs
)
from zmq_fetcher import DataFetcher


class TestDataFetcher(TestDataFetcherBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataFetcher, self).setUp()

        # methods inherited from parent class
        # explicit definition here for better readability
        self._init_logging = super(TestDataFetcher, self)._init_logging

        self._init_logging()

        main_pid = os.getpid()
        self.con_ip = socket.getfqdn()
        self.ext_ip = socket.gethostbyaddr(self.con_ip)[2][0]
        ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")

        create_dir(directory=ipc_dir, chmod=0o777)

        ports = {
            "control": 50005,
            "cleaner": 50051,
            "cleaner_trigger": 50052,
            "confirmation_port": 50053
        }

        self.context = zmq.Context.instance()

        # determine socket connection strings
        con_strs = set_con_strs(ext_ip=self.ext_ip,
                                con_ip=self.con_ip,
                                ipc_dir=ipc_dir,
                                main_pid=main_pid,
                                ports=ports)

        # Set up config
        self.config = {
            "ipc_dir": ipc_dir,
            "main_pid": main_pid,
            "con_strs": con_strs
        }

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

    def _set_up_socket(self, port):
        """Create pull socket and connect to port.

        Args:
            port: Port to connect to.
        """

        sckt = self.context.socket(zmq.PULL)
        connection_str = "tcp://{}:{}".format(self.ext_ip, port)
        sckt.bind(connection_str)
        self.log.info("Start receiving socket (bind): {}"
                      .format(connection_str))

        return sckt

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
        # source_file = "http://131.169.55.170/test_httpget/data/test_file.cbf"

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
