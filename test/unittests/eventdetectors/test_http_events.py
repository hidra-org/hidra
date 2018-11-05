# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""Testing the http_events event detector
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import socket

from http_events import EventDetector
from .eventdetector_test_base import EventDetectorTestBase

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

#        detectorDevice   = "haspp10lab:10000/p10/eigerdectris/lab.01"
#        detectorDevice   = "haspp06:10000/p06/eigerdectris/exp.01"
#        filewriterDevice = "haspp10lab:10000/p10/eigerfilewriter/lab.01"
#        filewriterDevice = "haspp06:10000/p06/eigerfilewriter/exp.01"
#        det_ip          = "192.168.138.52"  # haspp11e1m

        self.event_detector_config = {
            "det_ip": "asap3-mon",
            "det_api_version": "1.6.0",
            "history_size": 1000,
            "fix_subdirs": ["local"],
        }

        self.target_base_path = "http://{}/data".format(
            socket.gethostbyname(self.event_detector_config["det_ip"]))

        self.eventdetector = EventDetector(self.event_detector_config,
                                           self.log_queue)

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
