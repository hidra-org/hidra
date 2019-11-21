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
This module implements an event detector based on ewmscp events.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import collections
import copy
import json
import os
import sys
import threading

from kafka import KafkaConsumer

try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))
SENDER_DIR = os.path.join(BASE_DIR, "src", "sender")

if SENDER_DIR not in sys.path:
    sys.path.insert(0, SENDER_DIR)

from eventdetectorbase import EventDetectorBase  # noqa E402
import hidra.utils as utils  # noqa E402

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

_synced_data = []  # pylint: disable=invalid-name


class Synchronizing(threading.Thread):
    """Synchronize all events belonging to one run.
    """

    def __init__(self,
                 log_queue,
                 lock,
                 config):
        threading.Thread.__init__(self)

        self.log = utils.get_logger("Synchronizing", log_queue)

        self.all_data = {}
        self.lock = lock
        self.server = config["kafka_server"]
        self.topic = config["kafka_topic"]
        self.operation = config["operation"]

        self.detids = config["detids"]
        self.n_detectors = config["n_detectors"]

        self.monitored_dir = pathlib.Path(config["monitored_dir"])
        self.fix_subdirs = config["fix_subdirs"]
        self.paths = None

        self.consumer = None

        self.sync_buffer = collections.deque(maxlen=config["buffer_size"])

        self.keep_running = True

        self._setup()

    def _setup(self):

        self.paths = [self.monitored_dir.joinpath(i) for i in self.fix_subdirs]

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.server,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def run(self):
        """Keep check for events."""
        # pylint: disable=global-variable-not-assigned

        while self.keep_running:

            try:
                message = self.consumer.poll(1000)

                if not message:
                    continue

                self._react_to_message(message)

            except KeyboardInterrupt:
                self.log.info("KeyboardInterrupt detected.")
                raise
            except Exception:
                self.log.info("Stopping thread.")
                raise

        self.log.info("Stopped while loop in synchronizing thread")

    def _react_to_message(self, message):
        global _synced_data  # pylint: disable=invalid-name

        # messages look like
        # {
        #   "finishTime": 1556031799.7914205,
        #   "inotifyTime": 1556031799.791173,
        #   "md5sum": "",
        #   "operation": "copy",
        #   "path": "/my_dir/my_subdir/file.test",
        #   "retries": 1,
        #   "size": 43008,
        #   "source": "./my_subdir/file.test"
        # }

        topic_partition = list(message.keys())[0]

        for msg in message[topic_partition]:

            if msg.value["operation"] != self.operation:
                self.log.debug("Event %s is not supported")
                continue

            msg_path = pathlib.Path(msg.value["path"])
            self.log.debug("msg_path=%s", msg_path)

            # check if in one of the fix_subdirs
            path_found = None
            for i in self.paths:
                try:
                    msg_path.relative_to(i)
                    path_found = i
                except ValueError:
                    pass

            if path_found is None:
                # not located in fix_subdirs
                continue

            # determine to which detector the message belongs to
            found_detector = None
            for detid in self.detids:
                if isinstance(detid, dict):
                    d_id = detid["id"]
                    pattern = detid["pattern"].format(d_id)
                else:
                    pattern = "*{}*".format(detid)
                    d_id = detid

                if msg_path.match(pattern):
                    found_detector = d_id
                    break

            if found_detector is None:
                self.log.debug("No match found for %s", msg_path)
                continue

            if str(msg_path).count(found_detector) == 1:
                path_tmpl = str(msg_path).replace(found_detector, "{}")
            else:
                self.log.error("Multiple occurrences of detector id "
                               "inside path is not supported.")
                self.log.debug("msg_path=%s", msg_path)
                continue

            # check for duplicates
            if any([i["detid"] == found_detector
                    and i["path_tmpl"] == path_tmpl
                    for i in self.sync_buffer]):
                self.log.debug("Detid already found in sync buffer")
            else:
                self.sync_buffer.append(
                    {
                        "path_tmpl": path_tmpl,
                        "detid": found_detector,
                        "file_path": str(msg_path),
                        "ewmscp_info": copy.deepcopy(msg.value)
                    }
                )

            # set full?
            # remember position in sync_buffer for easier removal later
            file_set = tuple(
                (i, f) for i, f in enumerate(self.sync_buffer)
                if f["path_tmpl"] == path_tmpl
            )

            if len(file_set) == self.n_detectors:
                self.log.debug("Full image detected: %s", path_tmpl)

                msg_data = [i for _, i in file_set]

                with self.lock:
                    _synced_data.append((path_tmpl, msg_data))

                # remove elements backwards to keep indices correct
                for i in sorted(file_set, reverse=True):
                    del self.sync_buffer[i[0]]

    def stop(self):
        """Notify the run method to stop."""
        self.keep_running = False

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.stop()

    def __del__(self):
        self.stop()


class EventDetector(EventDetectorBase):
    """Implementation of the event detector for ewmscp events.
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

        self.sync_thread = None
        self.lock = None
        self.monitored_dir = None

        self.required_params = {
            "eventdetector": {
                self.ed_type: [
                    "monitored_dir",
                    "fix_subdirs",
                    "buffer_size",
                    "kafka_server",
                    "kafka_topic",
                    "operation",
                    "detids",
                    "n_detectors"
                ]
            }
        }

        self.setup()

    def setup(self):
        """Set up and start synchronization thread.
        """

        # check that the required_params are set inside of module specific
        # config
        self.check_config()

        self.lock = threading.Lock()
        self.monitored_dir = self.config["monitored_dir"]

        self.sync_thread = Synchronizing(
            log_queue=self.log_queue,
            lock=self.lock,
            config=self.config
        )
        self.sync_thread.start()

    def get_new_event(self):
        """Get new events from ewmscp.

        Returns:
            The newest event list.
        """
        global _synced_data  # pylint: disable=invalid-name

        if _synced_data:
            self.log.debug("Found synced data.")

            event_message_list = []

            with self.lock:
                for i, msg_data in _synced_data:
                    path = pathlib.Path(i)

                    rel_path = path.parent.relative_to(self.monitored_dir)
                    event_message = {
                        "source_path": self.monitored_dir,
                        "relative_path": rel_path.as_posix(),
                        "filename": path.name,
                        "additional_info": msg_data
                    }

                    event_message_list.append(event_message)

                _synced_data = []
        else:
            event_message_list = []

        return event_message_list

    def stop(self):
        """Stop and clean up.
        """
        if self.sync_thread is not None:
            self.sync_thread.stop()
            self.sync_thread = None

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.stop()

    def __del__(self):
        self.stop()
