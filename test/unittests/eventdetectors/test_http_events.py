"""Testing the http_events event detector
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import socket

from .test_eventdetector_base import TestEventDetectorBase
from http_events import EventDetector


class TestEventDetector(TestEventDetectorBase):
    """Specification of tests to be performed for the loaded EventDetecor.
    """

    def setUp(self):
        super(TestEventDetector, self).setUp()

        # methods inherited from parent class
        # explicit definition here for better readability
        self._init_logging = super(TestEventDetector, self)._init_logging

#        detectorDevice   = "haspp10lab:10000/p10/eigerdectris/lab.01"
#        detectorDevice   = "haspp06:10000/p06/eigerdectris/exp.01"
#        filewriterDevice = "haspp10lab:10000/p10/eigerfilewriter/lab.01"
#        filewriterDevice = "haspp06:10000/p06/eigerfilewriter/exp.01"
#        det_ip          = "192.168.138.52"  # haspp11e1m
        det_ip = "asap3-mon"
        self.config = {
            "det_ip": det_ip,
            "det_api_version": "1.6.0",
            "history_size": 1000,
            "fix_subdirs": ["local"]
        }

        self.target_base_path = "http://{}/data".format(
            socket.gethostbyname(self.config["det_ip"]))

        self._init_logging()
        self.eventdetector = EventDetector(self.config, self.log_queue)

    def test_eventdetector(self):
        """Simulate incoming data and check if received events are correct.
        """

        source_file = u"test_file_local.cbf"
        relative_path = u"local"

        event_list = self.eventdetector.get_new_event()

        expected_result_dict = {
            u"filename": source_file,
            u"source_path": self.target_base_path,
            u"relative_path": relative_path
        }

#        self.assertEqual(len(event_list), 1)
#        self.assertDictEqual(event_list[0], expected_result_dict)
        self.assertIn(expected_result_dict, event_list)

    def tearDown(self):
        self.eventdetector.stop()

        super(TestEventDetector, self).tearDown()
