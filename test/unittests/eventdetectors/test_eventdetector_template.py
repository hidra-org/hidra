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

"""Testing the zmq_events event detector.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from eventdetector_template import EventDetector
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

        self.event_detector_config = {}

        self.start = 100
        self.stop = 101

        self.eventdetector = EventDetector(self.event_detector_config,
                                           self.log_queue)

    def test_eventdetector(self):
        """Simulate incoming data and check if received events are correct.
        """

        for _ in range(self.start, self.stop):
            try:
                event_list = self.eventdetector.get_new_event()
                if event_list:
                    self.log.debug("event_list: %s", event_list)
            except KeyboardInterrupt:
                break

    def tearDown(self):
        self.eventdetector.stop()

        super(TestEventDetector, self).tearDown()
