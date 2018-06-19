"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import subprocess

from .__init__ import BASE_DIR
from .datafetcher_test_base import DataFetcherTestBase
from file_fetcher import DataFetcher

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
            "session": None,
            "fix_subdirs": ["commissioning", "current", "local"],
            "store_data": True,
            "remove_data": False,
            "ipc_path": self.config["ipc_dir"],
            "main_pid": self.config["main_pid"],
            "cleaner_job_con_str": self.config["con_strs"].cleaner_job_con,
            "cleaner_conf_con_str": self.config["con_strs"].confirm_con,
            "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
            "local_target": os.path.join(BASE_DIR, "data", "target")
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
            receiving_socket.append(self.set_up_recv_socket(port))

        # Test data fetcher
        filename = "test01.cbf"
        prework_source_file = os.path.join(BASE_DIR, "test_file.cbf")

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

            subprocess.call('ssh root@asap3-mon rm "/var/www/html/data/{}"'
                            .format(filename), shell=True)

            for sckt in receiving_socket:
                sckt.close(0)

    def test_with_confirmation(self):
        """Simulate file fetching while taking care of confirmation signals.
        """
        pass

    def tearDown(self):
        super(TestDataFetcher, self).tearDown()
