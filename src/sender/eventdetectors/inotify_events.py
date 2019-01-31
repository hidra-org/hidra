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
This module implements an event detector based on the inotify library usable
for systems running inotify.
"""

# pylint: disable=global-statement

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import collections
import copy
import os
import re
import threading

import inotify.adapters
from six import iteritems

from eventdetectorbase import EventDetectorBase
from hidra import convert_suffix_list_to_regex
from inotify_utils import get_event_message, CleanUp

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


_file_event_list = []  # pylint: disable=invalid-name


def get_no_events():
    """No events to add.

    Returns:
        An emtpy list
    """

    return []


class EventDetector(EventDetectorBase):
    """
    Implementation of the event detector for inotify based systems using the
    inotifyx library.
    """

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config,
                                   log_queue,
                                   "inotify_events")
        # sets
        #   self.config_all - all configurations
        #   self.config_ed - the config of the event detector
        #   self.config - the module specific config
        #   self.ed_type -  the name of the eventdetector module
        #   self.log_queue
        #   self.log

        self.paths = None
        self.mon_subdirs = None
        self.mon_regex_per_event = None
        self.mon_regex = None
        # TODO decide if this should go into config
#        self.timeout = self.config["event_timeout"]
        self.timeout = 1
        self.history = None
        self.lock = None

        self.cleanup_time = None
        self.action_time = None
        self.cleanup_thread = None

        self._get_remaining_events = None

        self._set_required_params()

        # check that the required_params are set inside of module specific
        # config
        self.check_config()
        self._setup()

    def _set_required_params(self):
        """
        Defines the parameters to be in configuration to run this datafetcher.
        Depending if use_cleanup is configured other parameters are required.
        """

        self.required_params = ["monitored_dir",
                                "fix_subdirs",
                                ["monitored_events", dict],
                                # "event_timeout",
                                "history_size",
                                "use_cleanup"]

        if self.config["use_cleanup"]:
            self.required_params += ["time_till_closed", "action_time"]

    def _setup(self):
        """Initiate class variables and environment.

        Sets static configuration parameters creates ring buffer and starts
        cleanup thread.
        """

        watch_dirs = [
            os.path.normpath(
                os.path.join(self.config["monitored_dir"], directory)
            )
            for directory in self.config["fix_subdirs"]
        ]
        self.inotify = inotify.adapters.InotifyTrees(watch_dirs)
        self.inotify_event_gen = self.inotify.event_gen(yield_nones=False)

        # TODO why is this necessary
        self.paths = [self.config["monitored_dir"]]
        self.mon_subdirs = self.config["fix_subdirs"]

        self.mon_regex_per_event = self.config["monitored_events"]
        self.log.debug("monitored_events=%s", self.config["monitored_events"])

        regexes = []
        for key, value in iteritems(self.config["monitored_events"]):
            self.mon_regex_per_event[key] = (
                convert_suffix_list_to_regex(value,
                                             compile_regex=False,
                                             log=self.log)
            )

            regexes.append(self.mon_regex_per_event[key])

            # cannot be compiled before because regexes needs to be a list
            # of string
            self.mon_regex_per_event[key] = (
                re.compile(self.mon_regex_per_event[key]))

        self.log.debug("regexes=%s", regexes)
        self.mon_regex = convert_suffix_list_to_regex(regexes,
                                                      suffix=False,
                                                      compile_regex=True,
                                                      log=self.log)

        self.history = collections.deque(maxlen=self.config["history_size"])

        self.lock = threading.Lock()

        if self.config["use_cleanup"]:
            self.cleanup_time = self.config["time_till_closed"]
            self.action_time = self.config["action_time"]

            self._get_remaining_events = self._get_events_from_cleanup

            self.cleanup_thread = CleanUp(
                paths=self.paths,
                mon_subdirs=self.mon_subdirs,
                mon_regex=self.mon_regex,
                cleanup_time=self.cleanup_time,
                action_time=self.action_time,
                lock=self.lock,
                log_queue=self.log_queue
            )
            self.cleanup_thread.start()
        else:
            self._get_remaining_events = get_no_events

    def _get_events_from_cleanup(self):
        """Gets the events found by the clean up thread.

        Returns:
            A list of event messages found in cleanup thread.
        """

        # pylint: disable=invalid-name
        global _file_event_list

        event_message_list = []

        with self.lock:
            # get missed files
            event_message_list = copy.deepcopy(_file_event_list)

        _file_event_list = []

#        if event_message_list:
#            self.log.info("Added missed files: {}"
#                          .format(event_message_list))

        return event_message_list

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.

        Returns:
            A list of event messages generated from inotify events.
        """

        remaining_events = self._get_remaining_events()

        # only take the events which are not handles yet
        event_message_list = [
            event for event in remaining_events
            if [os.path.join(event["source_path"], event["relative_path"]),
                event["filename"]] not in self.history
        ]
        self.history += [
            [os.path.join(event["source_path"], event["relative_path"]),
             event["filename"]]
            for event in remaining_events
        ]

        for event in self.inotify_event_gen:
            (_, type_names, path, filename) = event

            current_mon_event = None
            for key, value in iteritems(self.mon_regex_per_event):
                if key in type_names:
                    current_mon_event = key
                    current_mon_regex = value

            # only files of the configured event type are send
            if (current_mon_event and [path, filename] not in self.history):

                # only files matching the regex specified with the current
                # event are monitored
                if current_mon_regex.match(filename) is None:
                    self.log.debug("File ending not in monitored suffixes: "
                                   "%s", filename)
                    self.log.debug("detected events were: %s",
                                   current_mon_event)

                event_message = get_event_message(path, filename, self.paths)
                self.log.debug("event_message %s", event_message)
                event_message_list.append(event_message)

                self.history.append([path, filename])

                return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """

        if self.cleanup_thread is not None:
            self.cleanup_thread.stop()
