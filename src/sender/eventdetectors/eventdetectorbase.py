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
This module implements the event detector base class from which all event
detectors inherit from.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys

import abc

#import __init__  # noqa F401 # pylint: disable=unused-import
import hidra.utils as utils
from hidra.utils import WrongConfiguration
from base_class import Base

# source:
# pylint: disable=line-too-long
# http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5  # noqa E501
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC  # pylint: disable=no-member
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetectorBase(Base):
    """
    Implementation of the event detector base class.
    """

    def __init__(self, config, log_queue, logger_name):  # noqa F811
        """Initial setup

        Args:
            config (dict): A dictionary containing the configuration
                           parameters.
            log_queue: The multiprocessing queue which is used for logging.
            logger_name (str): The name to be used for the logger.
        """

        super(EventDetectorBase, self).__init__()

        self.config = config
        self.log_queue = log_queue

        self.log = utils.get_logger(logger_name, log_queue)

        self.required_params = []

    def check_config(self):
        """Check that the configuration containes the nessessary parameters.

        Raises:
            WrongConfiguration: The configuration has missing or
                                wrong parameteres.
        """

        # Check format of config
        check_passed, config_reduced = utils.check_config(self.required_params,
                                                          self.config,
                                                          self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: %s",
                          config_reduced)
        else:
            # self.log.debug("config={}".format(self.config))
            msg = "The configuration has missing or wrong parameteres."
            raise WrongConfiguration(msg)

    @abc.abstractmethod
    def get_new_event(self):
        """Get the events that happened since the last request.

        Returns:
            A list of events. Each event is a dictionary of the from:
            {
                "source_path": ...
                "relative_path": ...
                "filename": ...
            }
        """
        pass

    @abc.abstractmethod
    def stop(self):
        """Stop and clean up.
        """
        pass

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()
