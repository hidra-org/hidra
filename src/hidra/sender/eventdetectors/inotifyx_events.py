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
This module implements an event detector based on the inotifyx library usable
for systems running inotify.

Needed configuration in config file:
eventdetector:
    type: inotifyx_events
    inotifyx_events:
        monitored_dir: string
        fix_subdirs: list of string
        create_fix_subdirs: boolean
        monitored_events: dict
        event_timeout: int
        history_size: int
        use_cleanup: boolean
        action_time: float
        time_till_closed: float

Example config:
    inotifyx_events:
        monitored_dir: /ramdisk
        fix_subdirs:
            - "commissioning/raw"
            - "commissioning/scratch_bl"
            - "current/raw"
            - "current/scratch_bl"
            - "local"
        create_fix_subdirs: False
        monitored_events:
            IN_CLOSE_WRITE:
                - ""
        event_timeout: 1
        history_size: 0
        use_cleanup: False
        action_time: 10
        time_till_closed: 2
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import collections
import copy
import os
import re
import threading

import inotifyx
from future.utils import iteritems

try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib

from eventdetectorbase import EventDetectorBase
from hidra import convert_suffix_list_to_regex
from inotify_utils import get_event_message, CleanUp, common_stop

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

_file_event_list = []  # pylint: disable=invalid-name


class EventDetector(EventDetectorBase):
    """
    Implementation of the event detector for inotify based systems using the
    inotifyx library.
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

        self.wd_to_path = {}
        self.file_descriptor = None
        self.paths = None
        self.mon_subdirs = None
        self.mon_regex_per_event = None
        self.mon_regex = None
        self.timeout = None
        self.history = None
        self.lock = None

        self.cleanup_time = None
        self.action_time = None
        self.cleanup_thread = None

        self.get_remaining_events = None

        self._set_required_params()

        # check that the required_params are set inside of module specific
        # config
        self.check_config()
        self.check_monitored_dir()
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

        # to keep backwards compatibility to old config files
        suffix = pathlib.Path(self.config_all["general"]["config_file"]).suffix
        if suffix != ".conf":
            self.required_params.append("event_timeout")

        if self.config["use_cleanup"]:
            self.required_params += ["time_till_closed", "action_time"]

    def _setup(self):
        """Initiate class variables and environment.

        Sets static configuration parameters creates ring buffer and starts
        cleanup thread.
        """

        try:
            self.timeout = self.config["event_timeout"]
        except KeyError:
            # when using old config file type
            self.timeout = 1

        self.file_descriptor = inotifyx.init()

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
                                             log=self.log))

            regexes.append(self.mon_regex_per_event[key])

            # cannot be compiled before because regexes needs to be a list
            # of string
            try:
                self.mon_regex_per_event[key] = (
                    re.compile(self.mon_regex_per_event[key]))
            except Exception:
                self.log.error("Could not compile regex '%s'",
                               self.mon_regex_per_event[key],
                               exc_info=True)
                raise

        self.log.debug("regexes=%s", regexes)
        self.mon_regex = convert_suffix_list_to_regex(regexes,
                                                      suffix=False,
                                                      compile_regex=True,
                                                      log=self.log)

        self.history = collections.deque(maxlen=self.config["history_size"])

        self.lock = threading.Lock()

        self._add_watch()

        if self.config["use_cleanup"]:
            self.cleanup_time = self.config["time_till_closed"]
            self.action_time = self.config["action_time"]

            self.get_remaining_events = self._get_events_from_cleanup

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
            self.get_remaining_events = self._get_no_events

    def _add_watch(self):
        """Add directories to inotify watch.

        Adds all existing directories found inside the source paths to the
        inotify watch.
        """

        for path in self._get_directory_structure():
            try:
                watch_descriptor = inotifyx.add_watch(
                    self.file_descriptor,
                    path
                )
                self.wd_to_path[watch_descriptor] = path
                self.log.debug("Register watch for path: %s", path)
            except Exception:
                self.log.error("Could not register watch for path: %s", path,
                               exc_info=True)

    def _get_directory_structure(self):
        """For all directories configured find all sub-directories contained.

        Returns:
            A list of directories to be monitored.
        """

        # Add the default subdirs
        self.log.debug("paths: %s", self.paths)
        dirs_to_walk = [os.path.normpath(os.path.join(self.paths[0],
                                                      directory))
                        for directory in self.mon_subdirs]
        self.log.debug("dirs_to_walk: %s", dirs_to_walk)
        monitored_dirs = []

        # Walk the tree
        for directory in dirs_to_walk:
            if os.path.isdir(directory):
                monitored_dirs.append(directory)
                for root, _, _ in os.walk(directory):
                    # Add the found dirs to the list for the inotify-watch
                    if root not in monitored_dirs:
                        monitored_dirs.append(root)
                        self.log.info("Add directory to monitor: %s", root)
            else:
                self.log.info("Dir does not exist: %s", directory)

        return monitored_dirs

    def _get_no_events(self):  # pylint: disable=no-self-use
        """No events to add.

        Returns:
            An emtpy list
        """

        return []

    def _get_events_from_cleanup(self):
        """Gets the events found by the clean up thread.

        Returns:
            A list of event messages found in cleanup thread.
        """

        # pylint: disable=invalid-name
        global _file_event_list

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

        remaining_events = self.get_remaining_events()

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

        # event_message_list = self.get_remaining_events()
        event_message = {}

        events = inotifyx.get_events(self.file_descriptor, self.timeout)
        removed_wd = None

        for event in events:

            if not event.name:
                continue

            try:
                path = self.wd_to_path[event.wd]
            except Exception:
                path = removed_wd
            parts = event.get_mask_description()
            parts_array = parts.split("|")

            is_dir = ("IN_ISDIR" in parts_array)
            is_created = ("IN_CREATE" in parts_array)
            is_moved_from = ("IN_MOVED_FROM" in parts_array)
            is_moved_to = ("IN_MOVED_TO" in parts_array)

            current_mon_event = None
            current_mon_regex = None
            for key, value in iteritems(self.mon_regex_per_event):
                if key in parts_array:
                    current_mon_event = key
                    current_mon_regex = value
#                    current_mon_regex = self.mon_regex_per_event[key]

#            if not is_dir:
#                self.log.debug("{} {} {}".format(path, event.name, parts))
#                self.log.debug("current_mon_event: {}"
#                               .format(current_mon_event))
#            self.log.debug(event.name)
#            self.log.debug("is_dir: {}".format(is_dir))
#            self.log.debug("is_created: {}".format(is_created))
#            self.log.debug("is_moved_from: {}".format(is_moved_from))
#            self.log.debug("is_moved_to: {}".format(is_moved_to))

            # if a new directory is created or a directory is renamed inside
            # the monitored one, this one has to be monitored as well
            if is_dir and (is_created or is_moved_to):

                # self.log.debug("is_dir and is_created: {} or is_moved_to: "
                #                "{}".format(is_created, is_moved_to))
                # self.log.debug("{} {} {}".format(path, event.name, parts)
                # self.log.debug(event.name)

                dirname = os.path.join(path, event.name)
                self.log.info("Directory event detected: %s, %s",
                              dirname, parts)
                if dirname in self.paths:
                    self.log.debug("Directory already contained in path list:"
                                   " %s", dirname)
                else:
                    watch_descriptor = inotifyx.add_watch(   # noqa E501
                        self.file_descriptor,
                        dirname
                    )
                    self.wd_to_path[watch_descriptor] = dirname
                    self.log.info("Added new directory to watch: %s", dirname)

                    # because inotify misses subdirectory creations if they
                    # happen to fast, the newly created directory has to be
                    # walked to get catch this misses
                    # http://stackoverflow.com/questions/15806488/
                    #        inotify-missing-events
                    for dirpath, directories, files in os.walk(dirname):
                        # Add the found dirs to the list for the inotify-watch
                        for dname in directories:
                            subdir = os.path.join(dirpath, dname)
                            watch_descriptor = inotifyx.add_watch(
                                self.file_descriptor,
                                subdir
                            )
                            self.wd_to_path[watch_descriptor] = subdir
                            self.log.info("Added new subdirectory to watch: "
                                          "%s", subdir)
                        self.log.debug("files: %s", files)

                        for filename in files:
                            # self.log.debug("filename: {}".format(filename))
                            # pylint: disable=no-member
                            if self.mon_regex.match(filename) is None:
                                self.log.debug("File does not match monitored "
                                               "regex: %s", filename)
                                self.log.debug("detected events were: %s",
                                               parts)
                                continue

                            event_message = get_event_message(dirpath,
                                                              filename,
                                                              self.paths)
                            self.log.debug("event_message: %s", event_message)
                            event_message_list.append(event_message)
#                            self.log.debug("event_message_list: {}"
#                                           .format(event_message_list))
                continue

            # if a directory is renamed the old watch has to be removed
            if is_dir and is_moved_from:

                # self.log.debug("is_dir and is_moved_from")
                # self.log.debug("{} {} {}".format(path, event.name, parts)
                # self.log.debug(event.name)

                dirname = os.path.join(path, event.name)
                found_watch = None
                for watch, watch_path in iteritems(self.wd_to_path):
                    if watch_path == dirname:
                        found_watch = watch
                        break
                inotifyx.rm_watch(self.file_descriptor, found_watch)
                self.log.info("Removed directory from watch: %s", dirname)
                # the IN_MOVE_FROM event always apears before the IN_MOVE_TO
                # (+ additional) events and thus has to be stored till loop
                # is finished
                removed_wd = self.wd_to_path[found_watch]
                # removing the watch out of the dictionary cannot be done
                # inside the loop (would throw error: dictionary changed size
                # during iteration)
                del self.wd_to_path[found_watch]
                continue

            # only files of the configured event type are send
            if (not is_dir and current_mon_event
                    and [path, event.name] not in self.history):

                # only files matching the regex specified with the current
                # event are monitored
                if current_mon_regex.match(event.name) is None:
                    # self.log.debug("File ending not in monitored Suffixes: "
                    #                "%s", event.name)
                    # self.log.debug("detected events were: %s", parts)
                    continue

                event_message = get_event_message(path, event.name, self.paths)
                self.log.debug("event_message %s", event_message)
                event_message_list.append(event_message)

                self.history.append([path, event.name])

        return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """

        if self.cleanup_thread is not None:
            self.cleanup_thread.stop()

        try:
            for watch_descriptor, watch_path in self.wd_to_path.items():
                try:
                    inotifyx.rm_watch(
                        self.file_descriptor,
                        watch_descriptor
                    )
                except Exception:
                    self.log.error("Unable to remove watch: %s for %s",
                                   watch_descriptor, watch_path, exc_info=True)
            self.wd_to_path = {}
        finally:
            if self.file_descriptor is not None:
                try:
                    os.close(self.file_descriptor)
                except OSError:
                    self.log.error("Unable to close file descriptor")

                common_stop(self.config, self.log)
                self.file_descriptor = None
