import unittest
import socket
import logging

from __init__ import BASE_PATH  # noqa F401
from http_events import EventDetector


class TestInotifyxEvents(unittest.TestCase):

    def setUp(self):
        # Create log and set handler
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)  # Log level = DEBUG

        self.config = {
            "det_ip": "asap3-mon",
            "det_api_version": "1.5.0",
            "history_size": 1000
        }

        self.source_file = u'test_file.cbf'

        self.target_base_path = 'http://{}/data'.format(
            socket.gethostbyname(self.config["det_ip"]))
        self.target_relative_path = u''

        self.eventdetector = EventDetector(self.config, False)

    def test_eventdetector(self):

        event_list = self.eventdetector.get_new_event()

        expected_result_dict = {
            u'filename': self.source_file,
            u'source_path': self.target_base_path,
            u'relative_path': self.target_relative_path
        }

        self.assertEqual(len(event_list), 1)
        self.assertDictEqual(event_list[0], expected_result_dict)

    def tearDown(self):
        self.eventdetector.stop()


if __name__ == '__main__':
    unittest.main()
