"""Testing the zmq_events event detector.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import tempfile
import time
import zmq

from .__init__ import BASE_DIR
from .eventdetector_test_base import EventDetectorTestBase, create_dir
from zmq_events import EventDetector

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestEventDetector(EventDetectorTestBase):
    """Specification of tests to be performed for the loaded EventDetecor.
    """

    def setUp(self):
        super(TestEventDetector, self).setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self._event_det_con_str = "ipc://{}/{}_{}".format(ipc_dir,
                                                          self.config["main_pid"],
                                                          "eventDet")
        self.log.debug("self.event_det_con_str {}"
                       .format(self._event_det_con_str))

        self.context = zmq.Context.instance()

        self.event_detector_config = {
            "context": self.context,
            "number_of_streams": 1,
            "ext_ip": "0.0.0.0",
            "event_det_port": 50003,
            "ipc_path": ipc_dir,
            "main_pid": self.config["main_pid"]
        }

        self.start = 100
        self.stop = 101

        target_base_path = os.path.join(BASE_DIR, "data", "source")
        target_relative_path = os.path.join("local", "raw")
        self.target_path = os.path.join(target_base_path,
                                        target_relative_path)

        self.eventdetector = EventDetector(self.event_detector_config, self.log_queue)

    def test_eventdetector(self):
        """Simulate incoming data and check if received events are correct.
        """

        # create zmq socket to send events
        event_socket = self.context.socket(zmq.PUSH)
        event_socket.connect(self._event_det_con_str)
        self.log.info("Start event_socket (connect): '{}'"
                      .format(self._event_det_con_str))

        for i in range(self.start, self.stop):
            try:
                self.log.debug("generate event")
                target_file = "{}{}.cbf".format(self.target_path, i)
                message = {
                    u"filename": target_file,
                    u"filepart": 0,
                    u"chunksize": 10
                }

                event_socket.send_multipart(
                    [json.dumps(message).encode("utf-8")]
                )

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
        self.context.destroy(0)

        super(TestEventDetector, self).tearDown()
