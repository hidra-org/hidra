import unittest
import socket
import logging

import __init__  # noqa F401
from http_events import EventDetector


class TestHttpEvents(unittest.TestCase):

    def setUp(self):
        # Create log and set handler
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)  # Log level = DEBUG

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
#            "fix_subdirs": None
            "fix_subdirs": ["local"]
        }

        self.target_base_path = 'http://{}/data'.format(
            socket.gethostbyname(self.config["det_ip"]))

        self.eventdetector = EventDetector(self.config, False)

    def test_eventdetector(self):

        source_file = u'test_file_local.cbf'
        relative_path = u'local'

        event_list = self.eventdetector.get_new_event()

        expected_result_dict = {
            u'filename': source_file,
            u'source_path': self.target_base_path,
            u'relative_path': relative_path
        }

#        self.assertEqual(len(event_list), 1)
#        self.assertDictEqual(event_list[0], expected_result_dict)
        self.assertIn(expected_result_dict, event_list)

    def tearDown(self):
        self.eventdetector.stop()


if __name__ == '__main__':
    unittest.main()
