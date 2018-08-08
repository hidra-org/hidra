"""Testing the zmq_events event detector.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

from .eventdetector_test_base import EventDetectorTestBase
from eventdetector_template import EventDetector

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

        for i in range(self.start, self.stop):
            try:
                event_list = self.eventdetector.get_new_event()
                if event_list:
                    self.log.debug("event_list: {}".format(event_list))
            except KeyboardInterrupt:
                break

    def tearDown(self):
        self.eventdetector.stop()

        super(TestEventDetector, self).tearDown()
