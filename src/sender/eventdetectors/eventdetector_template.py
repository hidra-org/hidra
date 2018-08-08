from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

from eventdetectorbase import EventDetectorBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config,
                                   log_queue,
                                   "eventdetector_template")

        self.config = config
        self.log_queue = log_queue

        self.required_params = []

        self.check_config()
        self.setup()

    def setup(self):
        """Sets static configuration parameters.
        """
        pass

    def get_new_event(self):
        """Implmentation of the abstract method get_new_event.
        """

        event_message_list = [{
            "source_path": "/source",
            "relative_path": "relpath/to/dir",
            "filename": "my_file.cbf"
        }]

        self.log.debug("event_message: {}".format(event_message_list))

        return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """
        pass
