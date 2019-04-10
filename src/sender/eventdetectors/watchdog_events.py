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

event_message_list = []
event_list_to_observe = []
event_list_to_observe_tmp = []


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

        global event_message_list

        # Directories will be skipped
        if not event.is_directory:

            if isinstance(event, watchdog.events.FileMovedEvent):
                event_message = split_file_path(event.dest_path, self.paths)
            else:
                event_message = split_file_path(event.src_path, self.paths)

            with self.lock:
                event_message_list.append(event_message)

    def on_any_event(self, event):
        if self.detect_all and self.detect_all.match(event.src_path):
            self.log.debug("Any event detected")
            self.process(event)

    def on_created(self, event):
        global event_list_to_observe

        if self.detect_create and self.detect_create.match(event.src_path):
            # TODO only fire for file-event. skip directory-events.
            self.log.debug("On move event detected")
            self.process(event)

        if self.detect_close and self.detect_close.match(event.src_path):
            self.log.debug("On close event detected (from create)")
            if (not event.is_directory
                    and event.src_path not in event_list_to_observe):
                self.log.debug("Append event to event_list_to_observe: {}"
                               .format(event.src_path))
#                event_list_to_observe.append(event.src_path)
                bisect.insort_left(event_list_to_observe, event.src_path)

    def on_modified(self, event):
        global event_list_to_observe

        if self.detect_modify and self.detect_modify.match(event.src_path):
            self.log.debug("On modify event detected")
            self.process(event)

        if self.detect_close and self.detect_close.match(event.src_path):
            if (not event.is_directory
                    and event.src_path not in event_list_to_observe):
                self.log.debug("On close event detected (from modify)")
#                event_list_to_observe.append(event.src_path)
                bisect.insort_left(event_list_to_observe, event.src_path)

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
        global event_list_to_observe
        global event_list_to_observe_tmp

        while not self.stopper.is_set():
            try:
                with self.lock:
                    event_list_to_observe_copy = (
                        copy.deepcopy(event_list_to_observe))

                # Open the urls in their own threads
#                self.log.debug("List to observe: {}"
#                               .format(event_list_to_observe))
#                self.log.debug("event_message_list: {}"
#                               .format(event_message_list))
                if self.pool_running:
                    self.pool.map(self.check_last_modified,
                                  event_list_to_observe_copy)
                else:
                    self.log.info("Pool was already closed")
                    break
#                self.log.debug("event_message_list: {}"
#                               .format(event_message_list))

#                self.log.debug("List to observe tmp: {}"
#                               .format(event_list_to_observe_tmp))

                for event in event_list_to_observe_tmp:
                    try:
                        with self.lock:
                            event_list_to_observe.remove(event)
                        self.log.debug("Removing event: {}".format(event))
                    except:
                        self.log.error("Removing event failed: {}"
                                       .format(event), exc_info=True)
                        self.log.debug("event_list_to_observe_tmp={}"
                                       .format(event_list_to_observe_tmp))
                        self.log.debug("event_list_to_observe={}"
                                       .format(event_list_to_observe))
                event_list_to_observe_tmp = []

#                self.log.debug("List to observe after map-function: {0}"
#                               .format(event_list_to_observe))
                time.sleep(self.action_time)
            except:
                self.log.error("Stopping loop due to error", exc_info=True)
                break

    def check_last_modified(self, filepath):
        global event_message_list
        global event_list_to_observe_tmp

#        thread_name = threading.current_thread().name

        try:
            # check modification time
            time_last_modified = os.stat(filepath).st_mtime
#        except WindowsError:
#            self.log.error("Unable to get modification time for file: {}"
#                           .format(filepath), exc_info=True)
            # remove the file from the observing list
#            with self.lock
#                event_list_to_observe_tmp.append(filepath)
#            return
        except:
            self.log.error("Unable to get modification time for file: {}"
                           .format(filepath), exc_info=True)
            # remove the file from the observing list
            with self.lock:
                event_list_to_observe_tmp.append(filepath)
            return

        try:
            # get current time
            time_current = time.time()
        except:
            self.log.error("Unable to get current time for file: {}"
                           .format(filepath), exc_info=True)

        # compare ( >= limit)
        if time_current - time_last_modified >= self.time_till_closed:
            self.log.debug("New closed file detected: {}".format(filepath))

            event_message = split_file_path(filepath, self.mon_dir)
            self.log.debug("event_message: {}".format(event_message))

            # add to result list
#            self.log.debug("check_last_modified-{0} event_message_list {1}"
#                           .format(thread_name, event_message_list))
            with self.lock:
                event_message_list.append(event_message)
                event_list_to_observe_tmp.append(filepath)
#            self.log.debug("check_last_modified-{0} event_message_list {1}"
#                           .format(thread_name, event_message_list))
#            self.log.debug("check_last_modified-{0} "
#                           "event_list_to_observe_tmp "{1}"
#                           .format(thread_name, event_list_to_observe_tmp))
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
            log_queue=self.log_queue
        )
        self.checking_thread.start()

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """

        global event_message_list

        with self.lock:
            event_message_list_local = copy.deepcopy(event_message_list)
            # reset global list
            event_message_list = []

        return event_message_list_local

    def stop(self):
        """Implementation of the abstract method stop.
        """

        global event_message_list
        global event_list_to_observe
        global event_list_to_observe_tmp

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
            event_message_list = []
            event_list_to_observe = []
            event_list_to_observe_tmp = []

#        pprint.pprint(threading._active)
