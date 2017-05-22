from __future__ import print_function
from __future__ import unicode_literals
from six import iteritems

import os
import logging

import time
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
import copy
from multiprocessing.dummy import Pool as ThreadPool
import threading
import bisect

from eventdetectorbase import EventDetectorBase
import helpers
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
class WatchdogEventHandler (RegexMatchingEventHandler):
    def __init__(self, id, config, log_queue):
        self.id = id

        # Suppress logging messages of watchdog observer
        logging.getLogger("watchdog.observers.inotify_buffer").setLevel(
            logging.WARNING)

        self.log = helpers.get_logger("WatchdogEventHandler-{0}"
                                      .format(self.id), log_queue)
        self.log.debug("init")

        self.paths = [config["monitored_dir"]]

        # learn what events to detect
        self.detect_all = False
        self.detect_create = False
        self.detect_modify = False
        self.detect_delete = False
        self.detect_move = False
        self.detect_close = False

        regexes = []
        for event, regex in iteritems(config["monitored_events"]):
            self.log.debug("event: {0}, pattern: {1}".format(event, regex))
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

        self.log.debug("self.detect_close={0}, self.detect_move={1}"
                       .format(self.detect_close, self.detect_move))

    def process(self, event):
        self.log.debug("process")

        global event_message_list

        # Directories will be skipped
        if not event.is_directory:

            event_message = split_file_path(event.src_path, self.paths)

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
            self.log.debug("event.src_path={0}".format(event.src_path))
            if not event.is_directory:
                self.log.debug("Append event to event_list_to_observe: {0}"
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
        if self.detect_move and self.detect_move.match(event.src_path):
            self.log.debug("On move event detected")
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
        "source_path": os.path.normpath(parent_dir),
        "relative_path": os.path.normpath(relative_path),
        "filename": filename
    }

    return event_message


class CheckModTime (threading.Thread):
    def __init__(self, number_of_threads, time_till_closed, mon_dir,
                 action_time, lock, log_queue):
        self.log = helpers.get_logger("CheckModTime", log_queue)

        self.log.debug("init")
        # Make the Pool of workers
        self.pool = ThreadPool(number_of_threads)
        self.mon_dir = mon_dir
        self.time_till_closed = time_till_closed  # s
        self.action_time = action_time
        self.lock = lock
        self._stop = threading.Event()
        self._pool_running = True

        self.log.debug("threading.Thread init")
        threading.Thread.__init__(self)

    def run(self):
        global event_list_to_observe
        global event_list_to_observe_tmp

        while True:
            try:
                self.lock.acquire()
                event_list_to_observe_copy = (
                    copy.deepcopy(event_list_to_observe))
                self.lock.release()

                # Open the urls in their own threads
#                self.log.debug("List to observe: {0}"
#                               .format(event_list_to_observe))
#                self.log.debug("event_message_list: {0}"
#                               .format(event_message_list))
                if self._pool_running:
                    self.pool.map(self.check_last_modified,
                                  event_list_to_observe_copy)
                else:
                    self.log.info("Pool was already closed")
                    break
#                self.log.debug("event_message_list: {0}"
#                               .format(event_message_list))

#                self.log.debug("List to observe tmp: {0}"
#                               .format(event_list_to_observe_tmp))

                self.lock.acquire()
                for event in event_list_to_observe_tmp:
                    try:
                        event_list_to_observe.remove(event)
                        self.log.debug("Removing event: {0}".format(event))
                    except:
                        self.log.error("Removing event failed: {0}"
                                       .format(event), exc_info=True)
                        self.log.debug("event_list_to_observe_tmp={0}"
                                       .format(event_list_to_observe_tmp))
                        self.log.debug("event_list_to_observe={0}"
                                       .format(event_list_to_observe))
                event_list_to_observe_tmp = []
                self.lock.release()

#                self.log.debug("List to observe after map-function: {0}"
#                               .format(event_list_to_observe))
                time.sleep(self.action_time)
            except:
                self.log.error("Stopping loop due to error", exc_info=True)
                break

    def check_last_modified(self, filepath):
        global event_message_list
        global event_list_to_observe_tmp

        thread_name = threading.current_thread().name

        try:
            # check modification time
            time_last_modified = os.stat(filepath).st_mtime
#        except WindowsError:
#            self.log.error("Unable to get modification time for file: {0}"
#                           .format(filepath), exc_info=True)
            # remove the file from the observing list
#            self.lock.acquire()
#            event_list_to_observe_tmp.append(filepath)
#            self.lock.release()
#            return
        except:
            self.log.error("Unable to get modification time for file: {0}"
                           .format(filepath), exc_info=True)
            # remove the file from the observing list
            self.lock.acquire()
            event_list_to_observe_tmp.append(filepath)
            self.lock.release()
            return

        try:
            # get current time
            time_current = time.time()
        except:
            self.log.error("Unable to get current time for file: {0}"
                           .format(filepath), exc_info=True)

        # compare ( >= limit)
        if time_current - time_last_modified >= self.time_till_closed:
            self.log.debug("New closed file detected: {0}".format(filepath))

            event_message = split_file_path(filepath, self.mon_dir)
            self.log.debug("event_message: {0}".format(event_message))

            # add to result list
            self.lock.acquire()
            self.log.debug("check_last_modified-{0} event_message_list {1}"
                           .format(thread_name, event_message_list))
            event_message_list.append(event_message)
            event_list_to_observe_tmp.append(filepath)
            self.log.debug("check_last_modified-{0} event_message_list {1}"
                           .format(thread_name, event_message_list))
#            self.log.debug("check_last_modified-{0} "
#                           "event_list_to_observe_tmp "{1}"
#                           .format(thread_name, event_list_to_observe_tmp))
            self.lock.release()
        else:
            self.log.debug("File was last modified {d} sec ago: {p}"
                           .format(d=(time_current - time_last_modified),
                                   p=filepath))

    def stop(self):
        # close the pool and wait for the work to finish
        self.pool.close()
        self.pool.join()
        self._stop.set()
        self._pool_running = False

    def stopped(self):
        return self._stop.isSet()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


class EventDetector(EventDetectorBase):
    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self, config, log_queue,
                                   "watchdog_events")

        required_params = ["monitored_dir",
                           "fix_subdirs",
                           ["monitored_events", dict],
                           "time_till_closed",
                           "action_time"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            config,
                                                            self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {0}"
                          .format(config_reduced))

            self.config = config
            self.mon_dir = self.config["monitored_dir"]
            self.mon_subdirs = self.config["fix_subdirs"]

            self.paths = [os.path.normpath(os.path.join(self.mon_dir,
                                                        directory))
                          for directory in self.config["fix_subdirs"]]
            self.log.debug("paths: {0}".format(self.paths))

            self.time_till_closed = self.config["time_till_closed"]
            self.action_time = self.config["action_time"]

            self.observer_threads = []
            self.lock = threading.Lock()

            observer_id = 0
            for path in self.paths:
                observer = Observer()
                observer.schedule(
                    WatchdogEventHandler(observer_id, self.config, log_queue),
                    path, recursive=True)
                observer.start()
                self.log.info("Started observer for directory: {0}"
                              .format(path))

                self.observer_threads.append(observer)
                observer_id += 1

            self.checking_thread = CheckModTime(4, self.time_till_closed,
                                                self.mon_dir, self.action_time,
                                                self.lock, log_queue)
            self.checking_thread.start()

        else:
            self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    def get_new_event(self):
        global event_message_list

        self.lock.acquire()
        event_message_list_local = copy.deepcopy(event_message_list)
        # reset global list
        event_message_list = []
        self.lock.release()
        return event_message_list_local

    def stop(self):
        self.log.info("Stopping observer Threads")
        for observer in self.observer_threads:
            observer.stop()
            observer.join()

        # close the pool and wait for the work to finish
        self.checking_thread.stop()
        self.checking_thread.join()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    from shutil import copyfile
    from multiprocessing import Queue
    from logutils.queue import QueueHandler
    from __init__ import BASE_PATH
    import setproctitle

    logfile = os.path.join(BASE_PATH, "logs", "watchdogDetector.log")
    logsize = 10485760

    setproctitle.setproctitle("watchdog_events")

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True,
                                      onscreen_log_level="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = helpers.CustomQueueListener(log_queue, h1, h2)
    log_queue_listener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    config = {
        # TODO normpath to make insensitive to "/" at the end
        "monitored_dir": os.path.join(BASE_PATH, "data", "source"),
        "fix_subdirs": ["commissioning", "current", "local"],
        "monitored_events": {"IN_CLOSE_WRITE": [".tif", ".cbf"],
                             "IN_MOVED_TO": [".log"]},
        "time_till_closed": 1,  # s
        "action_time": 2  # s
    }

    source_file = os.path.join(BASE_PATH, "test_file.cbf")
    target_file_base = os.path.join(
        BASE_PATH, "data", "source", "local") + os.sep

    eventdetector = EventDetector(config, log_queue)

    copyFlag = False

    i = 100
    while i <= 105:
        try:
            event_list = eventdetector.get_new_event()
            if event_list:
                print("event_list:", event_list)
            if copyFlag:
                target_file = "{0}{1}.cbf".format(target_file_base, i)
                logging.debug("copy to {0}".format(target_file))
#                call(["cp", source_file, target_file])
                copyfile(source_file, target_file)
                i += 1
                copyFlag = False
            else:
                copyFlag = True

            time.sleep(0.5)
        except KeyboardInterrupt:
            break

    time.sleep(2)
    eventdetector.stop()
    for number in range(100, i):
        target_file = "{0}{1}.cbf".format(target_file_base, number)
        logging.debug("remove {0}".format(target_file))
        os.remove(target_file)

    log_queue.put_nowait(None)
    log_queue_listener.stop()
