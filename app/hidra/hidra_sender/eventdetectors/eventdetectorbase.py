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

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import os
import sys

import abc

#import __init__  # noqa F401 # pylint: disable=unused-import
import hidra.utils as utils
from hidra_sender.base_class import Base

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

    def __init__(self, eventdetector_base_config, name):
        """Initial setup

        Args:
            eventdetector_base_config: A dictionary containing all needed
                                       parameters encapsulated into a dictionary
                                       to prevent the event detector modules
                                       being affected by adding and removing of
                                       parameters.
                                       eventdetector_base_args should contain the
                                       following keys:
                                           config (dict): A dictionary
                                                          containing the
                                                          configuration
                                                          parameters.
                                           log_queue: The multiprocessing queue
                                                      which is used for logging.
            name (str): The name to be used for the logger.
        """

        super().__init__()

        self.config_all = eventdetector_base_config["config"]
        check_dep = eventdetector_base_config["check_dep"]

        self.log_queue = eventdetector_base_config["log_queue"]
        self.log = utils.get_logger(name, self.log_queue)

        # base_parameters
        self.required_params_base = {"eventdetector": ["type"]}

        self.required_params_dep = {}
        self.config_reduced = {}
        self._base_check(module_class="eventdetector", check_dep=check_dep)

        self.config_ed = self.config_all["eventdetector"]
        self.ed_type = self.config_ed["type"]
        if self.required_params_dep:
            self.config = self.config_ed[self.ed_type]
        else:
            self.config = {}

        self.required_params = []

    def check_config(self):
        """Check that the configuration contains the necessary parameters.

        Raises:
            WrongConfiguration: The configuration has missing or
                                wrong parameters.
        """

        if self.required_params and isinstance(self.required_params, list):
            self.required_params = {
                "eventdetector": {self.ed_type: self.required_params}
            }

        config_reduced = self._check_config_base(
            config=self.config_all,
            required_params=[
                self.required_params_base,
                self.required_params_dep,
                self.required_params
            ],
        )

        self.config_reduced.update(config_reduced)
        super().print_config(self.config_reduced)

    def check_monitored_dir(self):
        """Check that the monitored exists and creates subdirs if needed.
        """

        if "monitored_dir" not in self.config_ed[self.ed_type]:
            return

        # get rid of formatting errors
        self.config["monitored_dir"] = os.path.normpath(
            self.config["monitored_dir"]
        )

        utils.check_existance(self.config["monitored_dir"])
        if ("create_fix_subdirs" in self.config
                and self.config["create_fix_subdirs"]):
            # create the subdirectories which do not exist already
            utils.create_sub_dirs(
                dir_path=self.config["monitored_dir"],
                subdirs=self.config["fix_subdirs"],
                dirs_not_to_create=self.config_ed["dirs_not_to_create"]
            )
        else:
            # the subdirs have to exist because handles can only be added to
            # directories inside a directory in which a handle was already set,
            # e.g. handlers set to current/raw, local:
            # - all subdirs created are detected + handlers are set
            # - new directory on the same as monitored dir
            #   (e.g. current/scratch_bl) cannot be detected
            utils.check_all_sub_dir_exist(self.config["monitored_dir"],
                                          self.config["fix_subdirs"])

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
