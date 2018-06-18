from __future__ import print_function
from __future__ import unicode_literals
from six import iteritems

import os
from inotifyx import binding
# from inotifyx.distinfo import version as __version__
import collections
import threading
import time
import copy
import re

from eventdetectorbase import EventDetectorBase
import utils
from hidra import convert_suffix_list_to_regex

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


constants = {}
file_event_list = []

for name in dir(binding):
    if name.startswith("IN_"):
        globals()[name] = constants[name] = getattr(binding, name)


# Source: inotifyx library code example
class InotifyEvent (object):
    """
    InotifyEvent(wd, mask, cookie, name)

    A representation of the inotify_event structure.  See the inotify
    documentation for a description of these fields.
    """

    wd = None
    mask = None
    cookie = None
    name = None

    def __init__(self, wd, mask, cookie, name):
        self.wd = wd
        self.mask = mask
        self.cookie = cookie
        self.name = name

    def __str__(self):
        return "%s: %s" % (self.wd, self.get_mask_description())

    def __repr__(self):
        return "%s(%s, %s, %s, %s)" % (
            self.__class__.__name__,
            repr(self.wd),
            repr(self.mask),
            repr(self.cookie),
            repr(self.name),
        )

    def get_mask_description(self):
        """
        Return an ASCII string describing the mask field in terms of
        bitwise-or'd IN_* constants, or 0.  The result is valid Python code
        that could be eval'd to get the value of the mask field.  In other
        words, for a given event:

        >>> from inotifyx import *
        >>> assert (event.mask == eval(event.get_mask_description()))
        """

        parts = []
        for name, value in constants.items():
            if self.mask & value:
                parts.append(name)
        if parts:
            return "|".join(parts)
        return "0"


def get_event_message(path, filename, paths):

    parent_dir = path
    relative_path = ""
    event_message = {}

    # traverse the relative path till the original path is reached
    # e.g. created file: /source/dir1/dir2/test.tif
    while True:
        if parent_dir not in paths:
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
            relative_path = os.sep + rel_dir + relative_path
        else:
            # remove beginning "/"
            if relative_path.startswith(os.sep):
                relative_path = os.path.normpath(relative_path[1:])
            else:
                relative_path = os.path.normpath(relative_path)

            # the event for a file /tmp/test/source/local/file1.tif is of
            # the form:
            # {
            #   "source_path" : "/tmp/test/source"
            #   "relative_path": "local"
            #   "filename"   : "file1.tif"
            # }
            event_message = {
                "source_path": parent_dir,
                "relative_path": relative_path,
                "filename": filename
            }

            return event_message


class CleanUp (threading.Thread):
    def __init__(self, paths, mon_subdirs, mon_regex, cleanup_time,
                 action_time, lock, log_queue):

        self.log = utils.get_logger("CleanUp", log_queue, log_level="info")

        self.log.debug("init")
        self.paths = paths

        self.mon_subdirs = mon_subdirs
        self.mon_regex = mon_regex

        self.cleanup_time = cleanup_time
        self.action_time = action_time

        self.lock = lock
        self.run_loop = True

        self.log.debug("threading.Thread init")
        threading.Thread.__init__(self)

    def run(self):
        global file_event_list
        dirs_to_walk = [os.path.normpath(os.path.join(self.paths[0],
                                                      directory))
                        for directory in self.mon_subdirs]

        while self.run_loop:
            try:
                result = []
                for dirname in dirs_to_walk:
                    result += self.traverse_directory(dirname)

                with self.lock:
                    file_event_list += result
#                self.log.debug("file_event_list: {0}".format(file_event_list))
                time.sleep(self.action_time)
            except:
                self.log.error("Stopping loop due to error", exc_info=True)
                self.lock.release()
                break

    def traverse_directory(self, dirname):
        event_list = []

        for root, directories, files in os.walk(dirname):
            for filename in files:
                if self.mon_regex.match(filename) is None:
                    # self.log.debug("File ending not in monitored Suffixes: "
                    #               "{0}".format(filename))
                    continue

                filepath = os.path.join(root, filename)
                self.log.debug("filepath: {}".format(filepath))

                try:
                    time_last_modified = os.stat(filepath).st_mtime
                except:
                    self.log.error("Unable to get modification time for file: "
                                   "{}".format(filepath), exc_info=True)
                    continue

                try:
                    # get current time
                    time_current = time.time()
                except:
                    self.log.error("Unable to get current time for file: {}"
                                   .format(filepath), exc_info=True)
                    continue

                if time_current - time_last_modified >= self.cleanup_time:
                    self.log.debug("New closed file detected: {}"
                                   .format(filepath))
#                    self.log.debug("modTime: {}, currentTime: {}"
#                                   .format(time_last_modified, time_current))
#                    self.log.debug("time_current - time_last_modified: {}, "
#                                   "cleanup_time: {}"
#                                   .format(
#                                       (time_current - time_last_modified),
#                                       self.cleanup_time))
                    event_message = get_event_message(root, filename,
                                                      self.paths)
                    self.log.debug("event_message: {}".format(event_message))

                    # add to result list
                    event_list.append(event_message)

        return event_list

    def stop(self):
        self.run_loop = False


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self, config, log_queue,
                                   "inotifyx_events")

        required_params = ["monitored_dir",
                           "fix_subdirs",
                           ["monitored_events", dict],
                           # "event_timeout",
                           "history_size",
                           "use_cleanup"]

        # Check format of config
        check_passed, config_reduced = utils.check_config(required_params,
                                                          config,
                                                          self.log)

        if config["use_cleanup"]:
            required_params2 = ["time_till_closed", "action_time"]

            # Check format of config
            # the second set of parameters only has to be checked if cleanup
            # is enabled
            check_passed2, config_reduced2 = (
                utils.check_config(required_params2,
                                   config,
                                   self.log))
            if check_passed2:
                # To merge them the braces have to be removed
                config_reduced = (config_reduced[:-1]
                                  + ", "
                                  + config_reduced2[1:])

            self.get_remaining_events = self.get_events_from_cleanup
        else:
            check_passed2 = True
            self.get_remaining_events = self.get_no_events

        check_passed = check_passed and check_passed2

        self.wd_to_path = {}
        self.fd = binding.init()

        self.cleanup_thread = None

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {}"
                          .format(config_reduced))

            # TODO why is this necessary
            self.paths = [config["monitored_dir"]]
            self.mon_subdirs = config["fix_subdirs"]

            self.mon_regex_per_event = config["monitored_events"]
            self.log.debug("monitored_events={}"
                           .format(config["monitored_events"]))

            regexes = []
            for key, value in iteritems(config["monitored_events"]):
                self.mon_regex_per_event[key] = (
                    convert_suffix_list_to_regex(value,
                                                 compile_regex=False,
                                                 log=self.log))

                regexes.append(self.mon_regex_per_event[key])

                # cannot be compiled before because regexes needs to be a list
                # of string
                self.mon_regex_per_event[key] = (
                    re.compile(self.mon_regex_per_event[key]))

            self.log.debug("regexes={}".format(regexes))
            self.mon_regex = convert_suffix_list_to_regex(regexes,
                                                          suffix=False,
                                                          compile_regex=True,
                                                          log=self.log)

            # TODO decide if this should go into config
#            self.timeout = config["event_timeout"]
            self.timeout = 1

            self.history = collections.deque(maxlen=config["history_size"])

            self.lock = threading.Lock()

            self.add_watch()

            if config["use_cleanup"]:
                self.cleanup_time = config["time_till_closed"]
                self.action_time = config["action_time"]

                self.cleanup_thread = CleanUp(self.paths, self.mon_subdirs,
                                              self.mon_regex,
                                              self.cleanup_time,
                                              self.action_time,
                                              self.lock, log_queue)
                self.cleanup_thread.start()

        else:
            # self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    # Modification of the inotifyx example found inside inotifyx library
    # Copyright (c) 2005 Manuel Amador
    # Copyright (c) 2009-2011 Forest Bond
    def get_events(self, fd, *args):
        '''
        get_events(fd[, timeout])

        Return a list of InotifyEvent instances representing events read from
        inotify. If timeout is None, this will block forever until at least one
        event can be read.  Otherwise, timeout should be an integer or float
        specifying a timeout in seconds.  If get_events times out waiting for
        events, an empty list will be returned.  If timeout is zero, get_events
        will not block.
        '''
        return [
            InotifyEvent(wd, mask, cookie, name)
            for wd, mask, cookie, name in binding.get_events(fd, *args)
        ]

    def add_watch(self):
        try:
            for path in self.get_directory_structure():
                wd = binding.add_watch(self.fd, path)
                self.wd_to_path[wd] = path
                self.log.debug("Register watch for path: {}".format(path))
        except:
            self.log.error("Could not register watch for path: {}"
                           .format(path), exc_info=True)

    def get_directory_structure(self):
        # Add the default subdirs
        self.log.debug("paths: {}".format(self.paths))
        dirs_to_walk = [os.path.normpath(os.path.join(self.paths[0],
                                                      directory))
                        for directory in self.mon_subdirs]
        self.log.debug("dirs_to_walk: {}".format(dirs_to_walk))
        monitored_dirs = []

        # Walk the tree
        for directory in dirs_to_walk:
            if os.path.isdir(directory):
                monitored_dirs.append(directory)
                for root, directories, files in os.walk(directory):
                    # Add the found dirs to the list for the inotify-watch
                    if root not in monitored_dirs:
                        monitored_dirs.append(root)
                        self.log.info("Add directory to monitor: {}"
                                      .format(root))
            else:
                self.log.info("Dir does not exist: {}".format(directory))

        return monitored_dirs

    def get_no_events(self):
        return []

    def get_events_from_cleanup(self):
        global file_event_list

        event_message_list = []

        with self.lock:
            # get missed files
            event_message_list = copy.deepcopy(file_event_list)

        file_event_list = []

#        if event_message_list:
#            self.log.info("Added missed files: {0}"
#                          .format(event_message_list))

        return event_message_list

    def get_new_event(self):

        event_message_list = self.get_remaining_events()
        event_message = {}

        events = self.get_events(self.fd, self.timeout)
        removed_wd = None

        for event in events:

            if not event.name:
                continue

            try:
                path = self.wd_to_path[event.wd]
            except:
                path = removed_wd
            parts = event.get_mask_description()
            parts_array = parts.split("|")

            is_dir = ("IN_ISDIR" in parts_array)
            is_created = ("IN_CREATE" in parts_array)
            is_moved_from = ("IN_MOVED_FROM" in parts_array)
            is_moved_to = ("IN_MOVED_TO" in parts_array)

            current_mon_event = None
            for key, value in iteritems(self.mon_regex_per_event):
                if key in parts_array:
                    current_mon_event = key
                    current_mon_regex = self.mon_regex_per_event[key]

#            if not is_dir:
#                self.log.debug("{0} {1} {2}".format(path, event.name, parts))
#                self.log.debug("current_mon_event: {0}"
#                               .format(current_mon_event))
#            self.log.debug(event.name)
#            self.log.debug("is_dir: {0}".format(is_dir))
#            self.log.debug("is_created: {0}".format(is_created))
#            self.log.debug("is_moved_from: {0}".format(is_moved_from))
#            self.log.debug("is_moved_to: {0}".format(is_moved_to))

            # if a new directory is created or a directory is renamed inside
            # the monitored one, this one has to be monitored as well
            if is_dir and (is_created or is_moved_to):

                # self.log.debug("is_dir and is_created: {0} or is_moved_to: "
                #                "{1}".format(is_created, is_moved_to))
                # self.log.debug("{0} {1} {2}".format(path, event.name, parts)
                # self.log.debug(event.name)

                dirname = os.path.join(path, event.name)
                self.log.info("Directory event detected: {}, {}"
                              .format(dirname, parts))
                if dirname in self.paths:
                    self.log.debug("Directory already contained in path list:"
                                   " {}".format(dirname))
                else:
                    wd = binding.add_watch(self.fd, dirname)
                    self.wd_to_path[wd] = dirname
                    self.log.info("Added new directory to watch: {}"
                                  .format(dirname))

                    # because inotify misses subdirectory creations if they
                    # happen to fast, the newly created directory has to be
                    # walked to get catch this misses
                    # http://stackoverflow.com/questions/15806488/
                    #        inotify-missing-events
                    traversed_path = dirname
                    for root, directories, files in os.walk(dirname):
                        # Add the found dirs to the list for the inotify-watch
                        for dname in directories:
                            traversed_path = os.path.join(traversed_path,
                                                          dname)
                            wd = binding.add_watch(self.fd, traversed_path)
                            self.wd_to_path[wd] = traversed_path
                            self.log.info("Added new subdirectory to watch: "
                                          "{}".format(traversed_path))
                        self.log.debug("files: {}".format(files))
                        for filename in files:
                            # self.log.debug("filename: {0}".format(filename))
                            if self.mon_regex.match(filename) is None:
                                self.log.debug("File does not match monitored "
                                               "regex: {}"
                                               .format(filename))
                                self.log.debug("detected events were: {}"
                                               .format(parts))
                                continue

                            event_message = self.get_event_message(path,
                                                                   filename,
                                                                   self.paths)
                            self.log.debug("event_message: {}"
                                           .format(event_message))
                            event_message_list.append(event_message)
#                            self.log.debug("event_message_list: {0}"
#                                           .format(event_message_list))
                continue

            # if a directory is renamed the old watch has to be removed
            if is_dir and is_moved_from:

                # self.log.debug("is_dir and is_moved_from")
                # self.log.debug("{0} {1} {2}".format(path, event.name, parts)
                # self.log.debug(event.name)

                dirname = os.path.join(path, event.name)
                for watch, watchPath in iteritems(self.wd_to_path):
                    if watchPath == dirname:
                        found_watch = watch
                        break
                binding.rm_watch(self.fd, found_watch)
                self.log.info("Removed directory from watch: {}"
                              .format(dirname))
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

                # self.log.debug("not is_dir")
                # self.log.debug("current_mon_event: {0}"
                #                .format(current_mon_event))
                # self.log.debug("{0} {1} {2}".format(path, event.name, parts))
                # self.log.debug("filename: {0}".format(event.name))
                # self.log.debug("regex match: {0}".format(
                #                current_mon_regex.match(event.name)))

                # only files matching the regex specified with the current
                # event are monitored
                if current_mon_regex.match(event.name) is None:
                    # self.log.debug("File ending not in monitored Suffixes: "
                    #                "{0}".format(event.name))
                    # self.log.debug("detected events were: {0}".format(parts))
                    continue

                event_message = get_event_message(path, event.name, self.paths)
                self.log.debug("event_message {}".format(event_message))
                event_message_list.append(event_message)

                self.history.append([path, event.name])

        return event_message_list

    def stop(self):
        if self.cleanup_thread is not None:
            self.cleanup_thread.stop()

        try:
            for wd in self.wd_to_path:
                try:
                    binding.rm_watch(self.fd, wd)
                except:
                    self.log.error("Unable to remove watch: {}".format(wd),
                                   exc_info=True)
        finally:
            os.close(self.fd)

# testing was moved into test/unittests/event_detectors/test_inotifyx_events.py
