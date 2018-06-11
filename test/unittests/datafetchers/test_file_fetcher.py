"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import tempfile
import time
import socket
import zmq
from shutil import copyfile
from multiprocessing import Process


from .__init__ import BASE_DIR
from .test_datafetcher_base import (
    TestDataFetcherBase,
    create_dir,
    set_con_strs
)
from file_fetcher import DataFetcher, Cleaner


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

        # determine socket connection strings
        con_strs = set_con_strs(ext_ip=self.ext_ip,
                                con_ip=self.con_ip,
                                ipc_dir=ipc_dir,
                                main_pid=main_pid,
                                ports=ports)

        # Set up config
        self.config = {
            "main_pid": main_pid,
            "con_strs": con_strs
        }

        self.data_fetcher_config = {
            "fix_subdirs": ["commissioning", "current", "local"],
            "store_data": False,
            "remove_data": False,
            "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
            "local_target": None,
            # "local_target": os.path.join(BASE_DIR, "data", "target"),
            "main_pid": self.config["main_pid"],
            "cleaner_job_con_str": self.config["con_strs"].cleaner_job_con
        }

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

        self.context = zmq.Context.instance()

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

        # Set up receiver simulator
        receiving_socket = []
        for port in self.receiving_ports:
            receiving_socket.append(self._set_up_socket(port))

        # Test file fetcher
        source_dir = os.path.join(BASE_DIR, "data", "source")
        prework_source_file = os.path.join(BASE_DIR, "test_file.cbf")
        prework_target_file = os.path.join(source_dir, "local", "100.cbf")

        copyfile(prework_source_file, prework_target_file)
        time.sleep(0.5)

        metadata = {
            "source_path": os.path.join(BASE_DIR, "data", "source"),
            "relative_path": os.sep + "local",
            "filename": "100.cbf"
        }

        targets = [
            ["{}:{}".format(self.con_ip, self.receiving_ports[0]), 1, "data"],
            ["{}:{}".format(self.con_ip, self.receiving_ports[1]), 0, "data"]
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: {}"
                       .format(open_connections))

        datafetcher = DataFetcher(config=self.data_fetcher_config,
                                  log_queue=self.log_queue,
                                  id=0,
                                  context=self.context)

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
            time.sleep(0.1)

            for sckt in receiving_socket:
                sckt.close(0)

    def test_with_confirmation(self):
        """Simulate file fetching while taking care of confirmation signals.
        """

        self.config["remove_data"] = "with_confirmation"
        con_strs = self.config["con_strs"]

        # Set up cleaner
        kwargs = dict(
            config=self.cleaner_config,
            log_queue=self.log_queue,
            job_bind_str=con_strs.cleaner_job_con,
            cleaner_trigger_con_str=con_strs.cleaner_trigger_con,
            conf_con_str=con_strs.confirm_con,
            control_con_str=con_strs.control_con,
            context=self.context
        )
        cleaner_pr = Process(target=Cleaner, kwargs=kwargs)
        cleaner_pr.start()

        # Set up receiver simulator
        receiving_socket = []
        for port in self.receiving_ports:
            receiving_socket.append(self._set_up_socket(port))

        confirmation_socket = self.context.socket(zmq.PUSH)
        confirmation_socket.bind(con_strs.confirm_bind)
        self.log.info("Start confirmation_socket (bind): {}"
                      .format(con_strs.confirm_bind))

        # Test file fetcher
        source_dir = os.path.join(BASE_DIR, "data", "source")
        prework_source_file = os.path.join(BASE_DIR, "test_file.cbf")
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

        self.log.debug("open_connections before function call: {}"
                       .format(open_connections))

        datafetcher = DataFetcher(config=self.data_fetcher_config,
                                  log_queue=self.log_queue,
                                  id=0,
                                  context=self.context)

        datafetcher.get_metadata(targets, metadata)

        datafetcher.send_data(targets, metadata, open_connections)

        datafetcher.finish(targets, metadata, open_connections)

        # generate file identifier
        if metadata["relative_path"].startswith("/"):
            metadata["relative_path"] = metadata["relative_path"][1:]

        file_id = os.path.join(metadata["relative_path"],
                               metadata["filename"])

        # send file identifier to cleaner
        confirmation_socket.send(file_id.encode("utf-8"))
        self.log.debug("confirmation sent {}".format(file_id))

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
            time.sleep(0.5)

            for sckt in receiving_socket:
                sckt.close(0)

            cleaner_pr.terminate()

    def tearDown(self):
        super(TestDataFetcher, self).tearDown()

        self.context.destroy()
