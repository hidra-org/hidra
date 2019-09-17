from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

from six import iteritems

import bisect
import copy
import logging
import os
# import pprint
import threading
import time
from multiprocessing.dummy import Pool as ThreadPool
from watchdog.observers import Observer
import watchdog.events
from watchdog.events import RegexMatchingEventHandler

try:
    from pathlib2 import Path
except ImportError:
    # only available for Python3
    from pathlib import Path

from eventdetectorbase import EventDetectorBase
import utils
from hidra import convert_suffix_list_to_regex

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


# Define WindowsError for non Windows systems
# try:
#     WindowsError
# except NameError:
#     WindowsError = None

_potential_close_events = []  # pylint: disable=invalid-name
_events_marked_to_remove = []  # pylint: disable=invalid-name


class EventStore(object):
    def __init__(self):
        self.cond = threading.Condition()
        self.events = []

    def add(self, event):
        with self.cond:
            self.events.append(event)
            # Waking up threads that are waiting for new input
            self.cond.notify()

    def get_all(self, blocking=False, timeout=None):
        """Get and emtpy all stored event.

        Args:
            blocking (oprtional): block until there is at least one event
                                  present.
            timeout (optional): timeout to wait for events

        Returns:
             A list of events.
        """
        with self.cond:
            # If blocking is true, always return at least one event
            while blocking and len(self.events) == 0:
                self.cond.wait(timeout)
            events, self.events = self.events, []

        return events

    def is_empty(self):
        return self.events == []

    def remove_all(self):
        """Remove all events"""
        with self.cond:
            self.events = []


_event_store = EventStore()


# documentation of watchdog: https://pythonhosted.org/watchdog/api.html
class WatchdogEventHandler(RegexMatchingEventHandler):
    def __init__(self, handler_id, config, lock, log_queue):
        self.handler_id = handler_id
        self.lock = lock

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
        self.detect_move_from = False
        self.detect_move_to = False
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
            elif "move_from" in event.lower():
                self.log.info("Activate on move from event types")
                self.detect_move_from = regex
            elif "move_to" in event.lower():
                self.log.info("Activate on move to event types")
                self.detect_move_to = regex
            elif "close" in event.lower():
                self.log.info("Activate on close event types")
                self.detect_close = regex

        WatchdogEventHandler.regexes = regexes

        self.log.debug("init: super")
        super(WatchdogEventHandler, self,).__init__()

    def process(self, event):
        self.log.debug("process")

        global _event_store   # pylint: disable=invalid-name

        # Directories will be skipped
        if not event.is_directory:

            if isinstance(event, watchdog.events.FileMovedEvent):
                event_message = split_file_path(event.dest_path, self.paths)
            else:
                event_message = split_file_path(event.src_path, self.paths)

            _event_store.add(event_message)

    def on_any_event(self, event):
        if self.detect_all and self.detect_all.match(event.src_path):
            self.log.debug("Any event detected")
            self.process(event)

    def on_created(self, event):
        global _potential_close_events   # pylint: disable=invalid-name

        if self.detect_create and self.detect_create.match(event.src_path):
            # TODO only file for file-event. skip directory-events.
            self.log.debug("On move event detected")
            self.process(event)

        if self.detect_close and self.detect_close.match(event.src_path):
            self.log.debug("On close event detected (from create)")
            if (not event.is_directory
                    and event.src_path not in _potential_close_events):
                self.log.debug("Append event to _potential_close_events: %s",
                               event.src_path)
#                _potential_close_events.append(event.src_path)
                bisect.insort_left(_potential_close_events, event.src_path)

    def on_modified(self, event):
        global _potential_close_events   # pylint: disable=invalid-name

        if self.detect_modify and self.detect_modify.match(event.src_path):
            self.log.debug("On modify event detected")
            self.process(event)

        if self.detect_close and self.detect_close.match(event.src_path):
            if (not event.is_directory
                    and event.src_path not in _potential_close_events):
                self.log.debug("On close event detected (from modify)")
#                _potential_close_events.append(event.src_path)
                bisect.insort_left(_potential_close_events, event.src_path)

    def on_deleted(self, event):
        if self.detect_delete and self.detect_delete.match(event.src_path):
            self.log.debug("On delete event detected")
            self.process(event)

    def on_moved(self, event):
        if (self.detect_move_from
                and self.detect_move_from.match(event.src_path)):
            self.log.debug("On move from event detected")
            self.process(event)

        if (self.detect_move_to
                and self.detect_move_to.match(event.dest_path)):
            self.log.debug("On move to event detected")
            self.process(event)


def split_file_path(filepath, paths):

    (parent_dir, filename) = os.path.split(filepath)
    relative_path = ""

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

    # the event for a file /tmp/test/source/local/file1.tif is of the form:
    # {
    #   "source_path" : "/tmp/test/source"
    #   "relative_path": "local"
    #   "filename"   : "file1.tif"
    # }
    event_message = {
        "source_path": Path(os.path.normpath(parent_dir)).as_posix(),
        "relative_path": Path(os.path.normpath(relative_path)).as_posix(),
        "filename": filename
    }

    return event_message


class CheckModTime(threading.Thread):
    def __init__(self,
                 number_of_threads,
                 time_till_closed,
                 mon_dir,
                 action_time,
                 lock,
                 stop_request,
                 log_queue):
        threading.Thread.__init__(self)

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
        self.stopper = stop_request
        self.pool_running = True

    def run(self):
        global _event_store
        global _potential_close_events   # pylint: disable=invalid-name
        global _events_marked_to_remove   # pylint: disable=invalid-name

        self.log.debug("start run")
        while not self.stopper.is_set():
            try:
                with self.lock:
                    tmp_copy_of_events = copy.deepcopy(_potential_close_events)

                if self.pool_running:
                    self.pool.map(self.check_last_modified, tmp_copy_of_events)
                else:
                    self.log.info("Pool was already closed")
                    break

                for event in _events_marked_to_remove:
                    try:
                        with self.lock:
                            _potential_close_events.remove(event)
                        self.log.debug("Removing event: %s", event)
                    except Exception:
                        self.log.error("Removing event failed: %s", event,
                                       exc_info=True)
                        self.log.debug("_events_marked_to_remove=%s",
                                       _events_marked_to_remove)
                        self.log.debug("_potential_close_events=%s",
                                       _potential_close_events)
                _events_marked_to_remove = []

                time.sleep(self.action_time)
            except:
                self.log.error("Stopping loop due to error", exc_info=True)
                break

    def check_last_modified(self, filepath):
        """
        Checks if a files modification time is above the threshold. If so it
        is added to the global event message list.

        Args:
            filepath (str): the filename of the file to check (absolute path).
        """

        global _event_store   # pylint: disable=invalid-name
        global _events_marked_to_remove   # pylint: disable=invalid-name

        try:
            # check modification time
            time_last_modified = os.stat(filepath).st_mtime
#        except WindowsError:
#            self.log.error("Unable to get modification time for file: {}"
#                           .format(filepath), exc_info=True)
            # remove the file from the observing list
#            with self.lock
#                _events_marked_to_remove.append(filepath)
#            return
        except:
            self.log.error("Unable to get modification time for file: {}"
                           .format(filepath), exc_info=True)
            # remove the file from the observing list
            with self.lock:
                _events_marked_to_remove.append(filepath)
            return

        try:
            # get current time
            time_current = time.time()
        except Exception:
            self.log.error("Unable to get current time for file: %s",
                           filepath, exc_info=True)
            return

        # compare ( >= limit)
        if time_current - time_last_modified >= self.time_till_closed:
            self.log.debug("New closed file detected: {}".format(filepath))

            event_message = split_file_path(filepath, self.mon_dir)
            self.log.debug("event_message: {}".format(event_message))

            # add to result list
            _event_store.add(event_message)

            with self.lock:
                _events_marked_to_remove.append(filepath)
        else:
            self.log.debug("File was last modified {} sec ago: {}"
                           .format(time_current - time_last_modified,
                                   filepath))

    def stop(self):
        if self.pool_running:
            self.log.info("Stopping CheckModTime")
            self.stopper.set()
            self.pool_running = False

        if self.pool is not None:
            # close the pool and wait for the work to finish
            self.pool.close()
            self.pool.join()
            self.log.info("Checking pool joined")
            self.pool = None

    def __exit__(self, type, value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


class EventDetector(EventDetectorBase):
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

        #TODO add in watchdog config
        self.timeout = 2

        self.observer_threads = None
        self.checking_thread = None
        self.stop_request = threading.Event()

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
                WatchdogEventHandler(observer_id,
                                     self.config,
                                     self.lock,
                                     self.log_queue),
                path,
                recursive=True
            )

            self.observer_threads.append(observer)

            observer.start()
            self.log.info("Started observer for directory: {}"
                          .format(path))

        self.checking_thread = None
        self.checking_thread = CheckModTime(
            number_of_threads=4,
            time_till_closed=self.config["time_till_closed"],
            mon_dir=self.mon_dir,
            action_time=self.config["action_time"],
            lock=self.lock,
            stop_request=self.stop_request,
            log_queue=self.log_queue
        )
        self.checking_thread.start()

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """
        global _event_store   # pylint: disable=invalid-name

        return _event_store.get_all(blocking=True, timeout=self.timeout)

    def stop(self):
        """Implementation of the abstract method stop.
        """

        global _event_store   # pylint: disable=invalid-name
        global _potential_close_events   # pylint: disable=invalid-name
        global _events_marked_to_remove   # pylint: disable=invalid-name

        self.stop_request.set()

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
            _event_store.remove_all()
            _potential_close_events = []
            _events_marked_to_remove = []

