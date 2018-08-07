"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
# import time
# from shutil import copyfile

from .__init__ import BASE_DIR
from .datafetcher_test_base import DataFetcherTestBase
from datafetcher_template import DataFetcher

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
            "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
            "local_target": None,
            "remove_data": False,
            "endpoints": None,
            "main_pid": self.config["main_pid"]
        }

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

        self.receiving_ports = ["6005", "6006"]

        self.receiving_sockets = None

    def test_no_confirmation(self):
        """Simulate file fetching without taking care of confirmation signals.
        """

        self.datafetcher = DataFetcher(config=self.datafetcher_config,
                                       log_queue=self.log_queue,
                                       fetcher_id=0,
                                       context=self.context)

        # Set up receiver simulator
        self.receiving_sockets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        # Test data fetcher
        metadata = {
            "source_path": os.path.join(BASE_DIR, "data", "source"),
            "relative_path": os.sep + "local",
            "filename": "100.cbf"
        }

#        prework_source_file = os.path.join(BASE_DIR,
#                                           "test",
#                                           "test_files",
#                                           "test_file.cbf")
#        prework_target_file = os.path.join(metadata["source_path"],
#                                           metadata["relative_path"],
#                                           metadata["filename"])

#        copyfile(prework_source_file, prework_target_file)
#        time.sleep(0.5)

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
        if self.receiving_sockets is not None:
            for i, sckt in enumerate(self.receiving_sockets):
                self.stop_socket(name="receiving_socket{}".format(i),
                                 socket=sckt)
            self.receiving_sockets = None

        super(TestDataFetcher, self).tearDown()
