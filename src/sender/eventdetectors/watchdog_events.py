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
This module implements an event detector based on the the watchdog library.
"""

# pylint: disable=global-statement
# pylint: disable=broad-except
# pylint: disable=global-variable-not-assigned

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import bisect
import copy
import logging
import os
# import pprint
import threading
import time
from multiprocessing.dummy import Pool as ThreadPool

from six import iteritems
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler

from eventdetectorbase import EventDetectorBase
import utils
from hidra import convert_suffix_list_to_regex

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


# Define WindowsError for non Windows systems
# try:
#     WindowsError
# except NameError:
#     WindowsError = None

_event_message_list = []  # pylint: disable=invalid-name
_event_list_to_observe = []  # pylint: disable=invalid-name
_event_list_to_observe_tmp = []  # pylint: disable=invalid-name


# documentation of watchdog: https://pythonhosted.org/watchdog/api.html
class WatchdogEventHandler(RegexMatchingEventHandler):
    """
    Implementation of the watchdog event handle according to the API
    """

    def __init__(self, handler_id, config, log_queue):
        self.handler_id = handler_id

        # Suppress logging messages of watchdog observer
        logging.getLogger("watchdog.observers.inotify_buffer").setLevel(
            logging.WARNING)

        self.log = utils.get_logger(
            "WatchdogEventHandler-{}".format(self.handler_id),
            log_queue
        )
        self.log.debug("init")

        self.paths = [os.path.normpath(config["monitored_dir"])]

        # learn what events to detect
        self.detect_all = False
        self.detect_create = False
        self.detect_modify = False
        self.detect_delete = False
        self.detect_move = False
        self.detect_close = False

        regexes = []
        for event, regex in iteritems(config["monitored_events"]):
            self.log.debug("event: {}, pattern: {}".format(event, regex))
            regex = convert_suffix_list_to_regex(regex,
                                                 compile_regex=True,
                                                 log=self.log)

            regexes.append(regex)

            if "all" in event.lower():
                self.log.info("Activate all event types")
                self.detect_all = regex
            elif "create" in event.lower():
                self.log.info("Activate on create event types")
                self.detect_create = regex
            elif "modify" in event.lower():
                self.log.info("Activate on modify event types")
                self.detect_modify = regex
            elif "delete" in event.lower():
                self.log.info("Activate on delete event types")
                self.detect_delete = regex
            elif "move" in event.lower():
                self.log.info("Activate on move event types")
                self.detect_move = regex
            elif "close" in event.lower():
                self.log.info("Activate on close event types")
                self.detect_close = regex

        WatchdogEventHandler.regexes = regexes

        self.log.debug("init: super")
        super(WatchdogEventHandler, self,).__init__()

        self.log.debug("self.detect_close={}, self.detect_move={}"
                       .format(self.detect_close, self.detect_move))

    def process(self, event):
        """
        Generates an event message according to the schema and add it to the
        list of events.

        Args:
            event: a wachdog event
        """

        self.log.debug("process")

        global _event_message_list   # pylint: disable=invalid-name

        # Directories will be skipped
        if not event.is_directory:

            event_message = get_event_message(event.src_path, self.paths)

            _event_message_list.append(event_message)

    def on_any_event(self, event):
        # pylint: disable=no-member
        if self.detect_all and self.detect_all.match(event.src_path):
            self.log.debug("Any event detected")
            self.process(event)

    def on_created(self, event):
        global _event_list_to_observe   # pylint: disable=invalid-name

        # pylint: disable=no-member
        if self.detect_create and self.detect_create.match(event.src_path):
            # TODO only fire for file-event. skip directory-events.
            self.log.debug("On move event detected")
            self.process(event)

        # pylint: disable=no-member
        if self.detect_close and self.detect_close.match(event.src_path):
            self.log.debug("On close event detected (from create)")
            if not event.is_directory:
                self.log.debug("Append event to _event_list_to_observe: {}"
                               .format(event.src_path))
#                _event_list_to_observe.append(event.src_path)
                bisect.insort_left(_event_list_to_observe, event.src_path)

    def on_modified(self, event):
        global _event_list_to_observe   # pylint: disable=invalid-name

        # pylint: disable=no-member
        if self.detect_modify and self.detect_modify.match(event.src_path):
            self.log.debug("On modify event detected")
            self.process(event)

        # pylint: disable=no-member
        if self.detect_close and self.detect_close.match(event.src_path):
            if (not event.is_directory
                    and event.src_path not in _event_list_to_observe):
                self.log.debug("On close event detected (from modify)")
#                _event_list_to_observe.append(event.src_path)
                bisect.insort_left(_event_list_to_observe, event.src_path)

    def on_deleted(self, event):
        # pylint: disable=no-member
        if self.detect_delete and self.detect_delete.match(event.src_path):
            self.log.debug("On delete event detected")
            self.process(event)

    def on_moved(self, event):
        # pylint: disable=no-member
        if self.detect_move and self.detect_move.match(event.src_path):
            self.log.debug("On move event detected")
            self.process(event)


def get_event_message(filepath, paths):
    """
    Generates an event messages following the overall event detector schema
    e.g. input is:
        filepath = /my_home/source_dir/raw/subdir/test1/my_file.cbf
        paths = [/my_home/source_dir/raw,
                 /my_home/source_dir/scratch_bl]
    will result in
        {
           "source_path" : /my_home/source_dir/raw,
           "relative_path": subdir/test1,
           "filename"   : my_file.cbf
        }


    Args:
        filepath (str): the absolute filename of the file
        paths (list): a list of source paths to break the parent_dir down to

    Returns:
        A dictionary of the form
        {
           "source_path" : ...
           "relative_path": ...
           "filename"   : ...
        }
    """

    (parent_dir, filename) = os.path.split(filepath)
    relative_path = ""
    event_message = {}

    # extract relative pathname and filename for the file.
    while True:
        if parent_dir in paths:
            break
        else:
            (parent_dir, rel_dir) = os.path.split(parent_dir)
            # the os.sep is needed at the beginning because the relative path
            # is built up from the right
            # e.g.
            # self.paths = ["/tmp/test/source"]
            # path = /tmp/test/source/local/testdir
            # first iteration:  parent_dir = /tmp/test/source/local,
            #                   rel_dir = /testdir
            # second iteration: parent_dir = /tmp/test/source,
            #                   rel_dir = /local/testdir
            if relative_path:
                relative_path = os.path.join(rel_dir, relative_path)
                # relative_path = os.sep + rel_dir + relative_path
            else:
                relative_path = rel_dir
    # corresponds to source_path
#    common_prefix = os.path.commonprefix([self.mon_dir,filepath])
    # corresponds to relative_path + filename
#    relative_base_path = os.path.relpath(filepath, common_prefix)
    # corresponds to relative_path
#    (relative_parent, filename_tmp) = os.path.split(relative_base_path)

    # the event for a file /tmp/test/source/local/file1.tif is of the form:
    # {
    #   "source_path" : "/tmp/test/source"
    #   "relative_path": "local"
    #   "filename"   : "file1.tif"
    # }
    event_message = {
        "source_path": os.path.normpath(parent_dir),
        "relative_path": os.path.normpath(relative_path),
        "filename": filename
    }

    return event_message


class CheckModTime(threading.Thread):
    """
    A thread going over the found events on a regular basis and checking them
    for their modification time.
    """

    def __init__(self,
                 number_of_threads,
                 time_till_closed,
                 mon_dir,
                 action_time,
                 lock,
                 log_queue):

        self.log = utils.get_logger("CheckModTime",
                                    log_queue,
                                    log_level="info")

        self.log.debug("init")
        # Make the Pool of workers
        self.pool = ThreadPool(number_of_threads)
        self.mon_dir = mon_dir
        self.time_till_closed = time_till_closed  # s
        self.action_time = action_time
        self.lock = lock
        self.stopper = threading.Event()
        self.pool_running = True

        self.log.debug("threading.Thread init")
        threading.Thread.__init__(self)

    def run(self):
        global _event_list_to_observe   # pylint: disable=invalid-name
        global _event_list_to_observe_tmp   # pylint: disable=invalid-name

        while not self.stopper.is_set():
            try:
                with self.lock:
                    _event_list_to_observe_copy = (
                        copy.deepcopy(_event_list_to_observe))

                # Open the urls in their own threads
#                self.log.debug("List to observe: {}"
#                               .format(_event_list_to_observe))
#                self.log.debug("_event_message_list: {}"
#                               .format(_event_message_list))
                if self.pool_running:
                    self.pool.map(self.check_last_modified,
                                  _event_list_to_observe_copy)
                else:
                    self.log.info("Pool was already closed")
                    break
#                self.log.debug("_event_message_list: {}"
#                               .format(_event_message_list))

#                self.log.debug("List to observe tmp: {}"
#                               .format(_event_list_to_observe_tmp))

                for event in _event_list_to_observe_tmp:
                    try:
                        with self.lock:
                            _event_list_to_observe.remove(event)
                        self.log.debug("Removing event: {}".format(event))
                    except Exception:
                        self.log.error("Removing event failed: {}"
                                       .format(event), exc_info=True)
                        self.log.debug("_event_list_to_observe_tmp={}"
                                       .format(_event_list_to_observe_tmp))
                        self.log.debug("_event_list_to_observe={}"
                                       .format(_event_list_to_observe))
                _event_list_to_observe_tmp = []

#                self.log.debug("List to observe after map-function: {0}"
#                               .format(_event_list_to_observe))
                time.sleep(self.action_time)
            except Exception:
                self.log.error("Stopping loop due to error", exc_info=True)
                break

    def check_last_modified(self, filepath):
        """
        Checks if a files modification time is above the threshold. If so it
        is added to the global event message list.

        Args:
            filepath (str): the filename of the file to check (absolute path).
        """

        global _event_message_list   # pylint: disable=invalid-name
        global _event_list_to_observe_tmp   # pylint: disable=invalid-name

#        thread_name = threading.current_thread().name

        try:
            # check modification time
            time_last_modified = os.stat(filepath).st_mtime
#        except WindowsError:
#            self.log.error("Unable to get modification time for file: {}"
#                           .format(filepath), exc_info=True)
            # remove the file from the observing list
#            with self.lock
#                _event_list_to_observe_tmp.append(filepath)
#            return
        except Exception:
            self.log.error("Unable to get modification time for file: {}"
                           .format(filepath), exc_info=True)
            # remove the file from the observing list
            with self.lock:
                _event_list_to_observe_tmp.append(filepath)
            return

        try:
            # get current time
            time_current = time.time()
        except Exception:
            self.log.error("Unable to get current time for file: {}"
                           .format(filepath), exc_info=True)

        # compare ( >= limit)
        if time_current - time_last_modified >= self.time_till_closed:
            self.log.debug("New closed file detected: {}".format(filepath))

            event_message = get_event_message(filepath, self.mon_dir)
            self.log.debug("event_message: {}".format(event_message))

            # add to result list
#            self.log.debug("check_last_modified-{0} _event_message_list {1}"
#                           .format(thread_name, _event_message_list))
            with self.lock:
                _event_message_list.append(event_message)
                _event_list_to_observe_tmp.append(filepath)
#            self.log.debug("check_last_modified-{0} _event_message_list {1}"
#                           .format(thread_name, _event_message_list))
#            self.log.debug("check_last_modified-{0} "
#                           "_event_list_to_observe_tmp "{1}"
#                           .format(thread_name, _event_list_to_observe_tmp))
        else:
            self.log.debug("File was last modified {} sec ago: {}"
                           .format(time_current - time_last_modified,
                                   filepath))

    def stop(self):
        """ Stopping the loop and closing the pool
        """
        if self.pool_running:
            self.log.info("Stopping CheckModTime")
            self.stopper.set()
            self.pool_running = False

        if self.pool is not None:
            # close the pool and wait for the work to finish
            self.pool.close()
            self.pool.join()
            self.pool = None

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


class EventDetector(EventDetectorBase):
    """
    Implementation of the event detector using the watchdog library.
    """

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config,
                                   log_queue,
                                   "watchdog_events")

        self.config = config
        self.log_queue = log_queue

        self.mon_dir = None
        self.mon_subdirs = None
        self.paths = None
        self.lock = None

        self.observer_threads = None
        self.checking_thread = None

        self.required_params = ["monitored_dir",
                                "fix_subdirs",
                                ["monitored_events", dict],
                                "time_till_closed",
                                "action_time"]
        self.check_config()
        self.setup()

    def setup(self):
        """
        Sets static configuration paramters and starts the observer and
        checking_thread.
        """

        self.mon_dir = os.path.normpath(self.config["monitored_dir"])
        self.mon_subdirs = self.config["fix_subdirs"]

        self.paths = [os.path.normpath(os.path.join(self.mon_dir,
                                                    directory))
                      for directory in self.config["fix_subdirs"]]
        self.log.debug("paths: {}".format(self.paths))

        self.lock = threading.Lock()

        self.observer_threads = []
        for observer_id, path in enumerate(self.paths):
            observer = Observer()
            observer.schedule(
                WatchdogEventHandler(observer_id, self.config, self.log_queue),
                path,
                recursive=True
            )

            self.observer_threads.append(observer)

            observer.start()
            self.log.info("Started observer for directory: {}"
                          .format(path))

        self.checking_thread = CheckModTime(
            number_of_threads=4,
            time_till_closed=self.config["time_till_closed"],
            mon_dir=self.mon_dir,
            action_time=self.config["action_time"],
            lock=self.lock,
            log_queue=self.log_queue
        )
        self.checking_thread.start()

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """

        global _event_message_list   # pylint: disable=invalid-name

        with self.lock:
            _event_message_list_local = copy.deepcopy(_event_message_list)
            # reset global list
            _event_message_list = []

        return _event_message_list_local

    def stop(self):
        """Implementation of the abstract method stop.
        """

        global _event_message_list   # pylint: disable=invalid-name
        global _event_list_to_observe   # pylint: disable=invalid-name
        global _event_list_to_observe_tmp   # pylint: disable=invalid-name

        if self.observer_threads is not None:
            self.log.info("Stopping observer threads")
            for observer in self.observer_threads:
                observer.stop()
                observer.join()

            self.observer_threads = None

        # close the pool and wait for the work to finish
        if self.checking_thread is not None:
            self.log.info("Stopping checking thread")
            self.checking_thread.stop()
            self.checking_thread.join()

            self.checking_thread = None

        # resetting event list
        with self.lock:
            _event_message_list = []
            _event_list_to_observe = []
            _event_list_to_observe_tmp = []

#        pprint.pprint(threading._active)
