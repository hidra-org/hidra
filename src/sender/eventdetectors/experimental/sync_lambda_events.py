# Copyright (C) 2019  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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
This module implements an event detector that get images from different
detectors and synchronizes them.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import collections
import multiprocessing
import os
import PyTango
import queue
import sys
import time
import threading
import zmq

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


class Connector(object):
    """Gets data from one detector and forwards it to be synchronized.
    """
    def __init__(self,
                 det_id,
                 device_name,
                 wait_time,
                 shutdown_event,
                 data_queue,
                 log_queue):

        self.log = utils.get_logger("Connecting-{}".format(det_id), log_queue)

        self.n_attempts = 20
        self.det_id = det_id
        self.device_name = device_name
        self.wait_time = wait_time
        self.shutdown_event = shutdown_event
        self.data_queue = data_queue

        self.device = None

        self._setup()

    def _setup(self):
        found = False

        self.log.info("Connecting to detector with: %s", self.device_name)
        for i in range(self.n_attempts):
            try:
                self.device = PyTango.DeviceProxy(self.device_name)
                time.sleep(0.3)
                if self.device.state() in [PyTango.DevState.ON,
                                           PyTango.DevState.MOVING]:
                    found = True
                    break

            except KeyboardInterrupt:
                self.log.debug("Stopping...")
                break
            except Exception:
                self.log.warning("Could not initiate device proxy (attempt %s)",
                                 i, exc_info=True)

        if not found:
            raise Exception("Device %s not found", self.device_name)

        # just for testing
        self.start_acquisition()

    # this does not belong in the event detector, just for testing
    def start_acquisition(self):
        self.log.debug("start acquisition")
        if self.device.state() == PyTango.DevState.ON:
            self.device.command_inout("StartAcq")

    # this does not belong in the event detector, just for testing
    def stop_acquisition(self):
        if self.device.state() != PyTango.DevState.ON:
            # stop acq
            self.device.command_inout("StopAcq")
            self.log.debug("stop acquisition")

    def run(self):
        try:
            while not self.shutdown_event.is_set():
                # get live frame no
                frame_no = self.device.LiveFrameNo

                # read out live image
                data = self.device.LiveLastImageData
                self.log.debug("get image %s", frame_no)

                self.data_queue.put({
                    "det_id": self.det_id,
                    "frame_no": frame_no,
                    "data": data
                })
                time.sleep(self.wait_time)

        except KeyboardInterrupt:
            pass
        except Exception:
            self.log.error("Error while getting images", exc_info=True)
        finally:
            self.stop()

    def stop(self):
        self.shutdown_event.set()

        # just for testing
        if self.device is not None:
            self.stop_acquisition()


def connecting(det_id,
               device_name,
               wait_time,
               shutdown_event,
               data_queue,
               log_queue):
    """Start the connection and run"""

    connector = Connector(
        det_id,
        device_name,
        wait_time,
        shutdown_event,
        data_queue,
        log_queue
    )
    connector.run()


class Synchronizing(threading.Thread):
    """Synchronize all events belonging to one run.
    """

    def __init__(self,
                 lock,
                 config,
                 data_queue,
                 shutdown_event,
                 log_queue):
        threading.Thread.__init__(self)

        self.log = utils.get_logger("Synchronizing", log_queue)

        self.lock = lock
        self.data_queue = data_queue
        self.shutdown_event = shutdown_event

        self.n_detectors = len(config["device_names"])
        self.timeout = 1

        self.sync_buffer = collections.deque(maxlen=config["buffer_size"])

    def run(self):
        """Keep check for events."""
        # pylint: disable=global-variable-not-assigned

        while not self.shutdown_event.is_set():

            try:
                try:
                    message = self.data_queue.get(timeout=self.timeout)
                except queue.Empty:
                    continue

                if message == "STOP":
                    self.log.debug("Synchronizing got stop signal. Stopping.")
                    break

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

        # --------------------------------------------------------------------
        # check for duplicates and append to ring buffer
        # --------------------------------------------------------------------
        if any([i["det_id"] == message["det_id"]
               and i["frame_no"] == message["frame_no"]
               for i in self.sync_buffer]):
            self.log.debug("det_id already found in sync buffer")
        else:
            self.sync_buffer.append(message)

        # set full?
        # remember position in sync_buffer for easier removal later
        image_set = tuple(
            (i, f) for i, f in enumerate(self.sync_buffer)
            if f["frame_no"] == message["frame_no"]
        )

        if len(image_set) == self.n_detectors:
            self.log.debug("Full image detected")

            msg_data = [i for _, i in image_set]

            with self.lock:
                _synced_data.append(msg_data)

            # remove elements backwards to keep indices correct
            for i in sorted(image_set, reverse=True):
                del self.sync_buffer[i[0]]

    def stop(self):
        """Notify the run method to stop."""
        self.shutdown_event.set()

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.stop()

    def __del__(self):
        self.stop()


class EventDetector(EventDetectorBase):
    """Implementation of the event detector for lambda events.
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
        self.data_queue = multiprocessing.Queue()
        self.shutdown_event = None

        self.connections = None
        self.internal_com_socket = None

        self.required_params = {
            "eventdetector": {
                self.ed_type: [
                    "buffer_size",
                    "wait_time",
                    "internal_com_endpoint",
                    "device_names",
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

        self.shutdown_event = threading.Event()
        self.lock = threading.Lock()
        self.shutdown_event = multiprocessing.Event()

        # Create zmq socket to get events
        self.internal_com_socket = self.start_socket(
            name="internal_com_socket",
            sock_type=zmq.PUSH,
            sock_con="bind",
            endpoint=self.config["internal_com_endpoint"],
        )

        self.connections = []
        for i, name in enumerate(self.config["device_names"]):
            p = multiprocessing.Process(
                target=connecting,
                kwargs=(dict(
                    det_id=i,
                    device_name=name,
                    wait_time=self.config["wait_time"],
                    shutdown_event=self.shutdown_event,
                    data_queue=self.data_queue,
                    log_queue=self.log_queue,
                ))
            )
            self.connections.append(p)

        self.sync_thread = Synchronizing(
            lock=self.lock,
            config=self.config,
            data_queue=self.data_queue,
            shutdown_event=self.shutdown_event,
            log_queue=self.log_queue,
        )
        self.sync_thread.start()

        for p in self.connections:
            p.start()

    def get_new_event(self):
        """Get new events from the lambda tango server.

        Returns:
            The newest event list.
        """
        global _synced_data  # pylint: disable=invalid-name

        if _synced_data:
            self.log.debug("Found synced data.")

            event_message_list = []

            with self.lock:
                for image_set in _synced_data:
                    event_message = {
                        "source_path": "",
                        "relative_path": "",
                        "filename": str(image_set[0]["frame_no"]),
                        "additional_info": [
                            {
                                "det_id": i["det_id"],
                                "dtype": str(i["data"].dtype),
                                "shape": i["data"].shape,
                            }
                            for i in image_set
                        ]
                    }
                    self.internal_com_socket.send_multipart(
                        [i["data"] for i in image_set]
                    )

                    event_message_list.append(event_message)

                _synced_data = []
        else:
            event_message_list = []

        return event_message_list

    def stop(self):
        """Stop and clean up.
        """
        self.shutdown_event.set()
        self.data_queue.put("STOP")

        self.stop_socket(name="internal_com_socket")

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.stop()

    def __del__(self):
        self.stop()
