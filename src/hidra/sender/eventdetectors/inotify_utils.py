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
"""

# pylint: disable=broad-except
# pylint: disable=global-statement

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import threading
import time

import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

_file_event_list = []  # pylint: disable=invalid-name


def get_event_message(parent_dir, filename, paths):
    """
    Generates an event messages following the overall event detector schema
    e.g. input is:
        parent_dir = /my_home/source_dir/raw/subdir/test1
        filename = my_file.cbf
        paths = [/my_home/source_dir/raw,
                 /my_home/source_dir/scratch_bl]
    will result in
        {
           "source_path" : /my_home/source_dir/raw,
           "relative_path": subdir/test1,
           "filename"   : my_file.cbf
        }

    Args:
        parent_dir (str): the absolute path of the file
        filename (str): the name of the file
        paths (list): a list of source paths to break the parent_dir down to

    Returns:
        A dictionary of the form
        {
           "source_path" : ...
           "relative_path": ...
           "filename"   : ...
        }

    """

    for path in paths:
        if parent_dir.startswith(path):

            relative_path = os.path.relpath(parent_dir, path)

            event_message = {
                "source_path": path,
                "relative_path": relative_path,
                "filename": filename
            }

            return event_message

    raise Exception("Building event message failed")


def common_stop(config, log):
    """
    Execution of stopping operations common to all inotify event detector type:
    - Check if the monitored dir is empty and if not display the remaining
      file.

    Args:
        config (dict): The event detector configuration dictionary.
        log: log handler
    """

    log.debug("Checking for left over files in monitored_dir")

    remaining_files = utils.get_files_in_dir(
        dirs=[os.path.join(config["monitored_dir"], d)
              for d in config["fix_subdirs"]]
    )

    if remaining_files:
        log.warning("Left over files in monitored_dir: %s", remaining_files)
    else:
        log.info("No left over files in monitored_dir.")


class CleanUp(threading.Thread):
    """
    A threading finding left over files and generate events form them.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self,
                 paths,
                 mon_subdirs,
                 mon_regex,
                 cleanup_time,
                 action_time,
                 lock,
                 log_queue):

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
        # pylint: disable=invalid-name
        global _file_event_list

        dirs_to_walk = [os.path.normpath(os.path.join(self.paths[0],
                                                      directory))
                        for directory in self.mon_subdirs]

        while self.run_loop:
            try:
                result = []
                for dirname in dirs_to_walk:
                    result += self.traverse_directory(dirname)

                with self.lock:
                    _file_event_list += result
                time.sleep(self.action_time)
            except Exception:
                self.log.error("Stopping loop due to error", exc_info=True)
                break

    def traverse_directory(self, dirname):
        """
        Traverses the given directory and generate events for all files found
        which match the pattern and where not touched for some time.

        Args:
            dirname (str): the directory to traverse and check for files.

        Returns:
            A list of event messages.
        """

        event_list = []

        for root, _, files in os.walk(dirname):
            for filename in files:
                if self.mon_regex.match(filename) is None:
                    # self.log.debug("File ending not in monitored Suffixes: "
                    #               "{}".format(filename))
                    continue

                filepath = os.path.join(root, filename)
                self.log.debug("filepath: %s", filepath)

                try:
                    time_last_modified = os.stat(filepath).st_mtime
                except Exception:
                    self.log.error("Unable to get modification time for file: "
                                   "%s", filepath, exc_info=True)
                    continue

                try:
                    # get current time
                    time_current = time.time()
                except Exception:
                    self.log.error("Unable to get current time for file: %s",
                                   filepath, exc_info=True)
                    continue

                if time_current - time_last_modified >= self.cleanup_time:
                    self.log.debug("New closed file detected: %s",
                                   filepath)
#                    self.log.debug("modTime: %s, currentTime: %s",
#                                   time_last_modified, time_current)
#                    self.log.debug("time_current - time_last_modified: %s, "
#                                   "cleanup_time: %s",
#                                   (time_current - time_last_modified),
#                                   self.cleanup_time)
                    event_message = get_event_message(root,
                                                      filename,
                                                      self.paths)
                    self.log.debug("event_message: %s", event_message)

                    # add to result list
                    event_list.append(event_message)

        return event_list

    def stop(self):
        """Stops the clean up thread
        """

        self.log.debug("Stopping cleanup thread")
        self.run_loop = False
