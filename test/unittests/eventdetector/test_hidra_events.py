"""Testing the hidra_events event detector
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import zmq

from hidra_events import (EventDetector,
                          get_ipc_addresses,
                          get_endpoints)
from .eventdetector_test_base import EventDetectorTestBase, create_dir

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestEventDetector(EventDetectorTestBase):
    """Specification of tests to be performed for the loaded EventDetecor.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestEventDetector, self).setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self.context = zmq.Context()

        self.module_name = "hidra_events"
        self.eventdetector_config = {
            "network": {
                "context": self.context,
                "ipc_dir": ipc_dir,
                "main_pid": self.config["main_pid"],
                "ext_ip": self.ext_ip,
                "con_ip": self.con_ip,
            },
            "eventdetector": {
                "event_detector_type": self.module_name,
                self.module_name: {
                    "ext_data_port": "50100"
                }
            }
        }

        self.start = 100
        self.stop = 101

        target_base_path = os.path.join(self.base_dir, "data", "source")
        target_relative_path = os.path.join("local", "raw")
        self.target_path = os.path.join(target_base_path,
                                        target_relative_path)

        self.eventdetector = EventDetector(self.eventdetector_config,
                                           self.log_queue)

        self.ipc_addresses = get_ipc_addresses(
            config=self.eventdetector_config
        )
        self.endpoints = get_endpoints(config=self.eventdetector_config,
                                       ipc_addresses=self.ipc_addresses)

    def test_eventdetector(self):
        """Simulate incoming data and check if received events are correct.
        """

        local_in = True
        local_out = True

        if local_in:
            # create zmq socket to send events
            data_in_socket = self.start_socket(
                name="data_in_socket",
                sock_type=zmq.PUSH,
                sock_con="connect",
                endpoint=self.endpoints.in_con
            )

        if local_out:
            data_out_socket = self.start_socket(
                name="data_out_socket",
                sock_type=zmq.PULL,
                sock_con="connect",
                endpoint=self.endpoints.out_con
            )

        try:
            for i in range(self.start, self.stop):
                self.log.debug("generate event")
                target_file = "{}{}.cbf".format(self.target_path, i)
                message = {
                    "filename": target_file,
                    "filepart": 0,
                    "chunksize": 10
                }

                if local_in:
                    data_in_socket.send_multipart(
                        [json.dumps(message).encode("utf-8"), b"incoming_data"]
                    )

                event_list = self.eventdetector.get_new_event()
                if event_list:
                    self.log.debug("event_list: %s", event_list)

                self.assertIn(message, event_list)
                self.assertEqual(len(event_list), 1)
                self.assertDictEqual(event_list[0], message)

                if local_out:
                    recv_message = data_out_socket.recv_multipart()
                    self.log.debug("Received - %s", recv_message)

        except KeyboardInterrupt:
            pass
        finally:
            if local_in:
                self.stop_socket("data_in_socket", socket=data_in_socket)
            if local_out:
                self.stop_socket("data_out_socket", socket=data_out_socket)

    def tearDown(self):
        self.eventdetector.stop()
        self.context.destroy(0)

        super(TestEventDetector, self).tearDown()
