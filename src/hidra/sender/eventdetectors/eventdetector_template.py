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

"""
This is a template module for implementing event detectors.

No mandatory configuration needed.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from eventdetectorbase import EventDetectorBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetector(EventDetectorBase):
    """
    Implementation of the event detector.
    """

    def __init__(self, eventdetector_base_config):

        EventDetectorBase.__init__(self, eventdetector_base_config,
                                   name=__name__)

        # base class sets
        #   self.config_all - all configurations
        #   self.config_ed - the config of the event detector
        #   self.config - the module specific config
        #   self.ed_type -  the name of the eventdetector module
        #   self.log_queue
        #   self.log

        self.required_params = []

        # check that the required_params are set inside of module specific
        # config
        self.check_config()
        self._setup()

    def _setup(self):
        """Sets static configuration parameters.
        """
        pass

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """

        event_message_list = [{
            "source_path": "/source",
            "relative_path": "relpath/to/dir",
            "filename": "my_file.cbf"
        }]

        self.log.debug("event_message: %s", event_message_list)

        return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """
        pass
