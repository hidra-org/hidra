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

"""Testing the event detector for synchronizing ewmscp events.
"""

# pylint: disable=missing-docstring

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import time
import sync_ewmscp_events as events
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

        self.module_name = "sync_ewmscp_events"

        self.ed_config = {
            "eventdetector": {
                "type": "sync_ewmscp_events",
                "sync_ewmscp_events": {
                    "buffer_size": 50,
                    "source_path": "/my_dir",
                    "kafka_server": "asap3-events-01",
                    "kafka_topic": "kuhnm_test",
                    "detids": ["DET0", "DET1", "DET2"],
                    "n_detectors": 3
                }
            }
        }

        self.eventdetector = None

    # ------------------------------------------------------------------------
    # Test general
    # ------------------------------------------------------------------------

    def test_general(self):
        # pylint: disable=unused-argument

        self.eventdetector = events.EventDetector(
            self.ed_config,
            self.log_queue
        )

        for i in range(3):
            self.log.debug("run")
            event_list = self.eventdetector.get_new_event()
            self.log.debug("event_list: %s", event_list)

            time.sleep(2)

    def tearDown(self):

        if self.eventdetector is not None:
            self.eventdetector.stop()
            self.eventdetector = None

        super(TestEventDetector, self).tearDown()
