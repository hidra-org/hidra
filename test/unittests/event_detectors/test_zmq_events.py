from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import logging
import os
import socket
import tempfile
import time
import unittest
import zmq

from .__init__ import BASE_DIR
from .test_eventdetector_base import TestEventDetectorBase
from zmq_events import EventDetector


class TestEventDetector(TestEventDetectorBase):

    def setUp(self):
        super(TestEventDetector, self).setUp()

        # methods inherited from parent class
        # explicit definition here for better readability
        self._init_logging = super(TestEventDetector, self)._init_logging

        self._init_logging()

        main_pid = os.getpid()

        ipc_path = os.path.join(tempfile.gettempdir(), "hidra")
        self._create_dir(ipc_path)

        self.event_det_con_str = "ipc://{}/{}_{}".format(ipc_path, main_pid, "eventDet")
        print("self.event_det_con_str", self.event_det_con_str)

        self.config = {
            "context": None,
            "number_of_streams": 1,
            "ext_ip": "0.0.0.0",
            "event_det_port": 50003,
            "ipc_path": ipc_path,
            "main_pid": main_pid
        }

        self.start = 100
        self.stop = 101

        self.target_base_path = os.path.join(BASE_DIR, "data", "source")
        self.target_relative_path = os.path.join("local", "raw")
        #TODO why?
        self.target_relative_path += os.sep

        self.eventdetector = EventDetector(self.config, self.log_queue)

    def test_eventdetector(self):

        context = zmq.Context.instance()

        # create zmq socket to send events
        event_socket = context.socket(zmq.PUSH)
        event_socket.connect(self.event_det_con_str)
        self.log.info("Start event_socket (connect): '{}'"
                      .format(self.event_det_con_str))

        for i in range(self.start, self.stop):
            try:
                self.log.debug("generate event")
                target_file = "{}{}.cbf".format(self.target_relative_path, i)
                message = {
                    u"filename": target_file,
                    u"filepart": 0,
                    u"chunksize": 10
                }

                event_socket.send_multipart([json.dumps(message).encode("utf-8")])

                event_list = self.eventdetector.get_new_event()
                if event_list:
                    self.log.debug("event_list: {}".format(event_list))

#                self.assertEqual(len(event_list), 1)
#                self.assertDictEqual(event_list[0], expected_result_dict)
                self.assertIn(message, event_list)

                time.sleep(1)
            except KeyboardInterrupt:
                break

        message = [b"CLOSE_FILE", "test_file.cbf".encode("utf8")]
        event_socket.send_multipart(message)

        event_list = self.eventdetector.get_new_event()
        self.log.debug("event_list: {}".format(event_list))

        self.assertIn(message, event_list)

    def tearDown(self):
        self.eventdetector.stop()

        super(TestEventDetector, self).tearDown()


if __name__ == "__main__":
    unittest.main()
