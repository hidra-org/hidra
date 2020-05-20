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
This module implements the data fetcher base class from which all data fetchers
inherit from.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import abc
import json
import os
import sys
import time
import zmq

try:
    # only available for Python3
    from pathlib import Path
except ImportError:
    from pathlib2 import Path

from base_class import Base
import hidra.utils as utils

# source:
# pylint: disable=line-too-long
# http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5  # noqa E501
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC  # pylint: disable=no-member
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataHandlingError(Exception):
    """An exception class to be used when handling data.
    """
    pass


class DataFetcherBase(Base, ABC):
    """
    Implementation of the data fetcher base class.
    """

    def __init__(self, datafetcher_base_config, name):
        """Initial setup

        Checks if the required parameters are set in the configuration for
        the base setup.

        Args:
            datafetcher_base_config: A dictionary containing all needed
                parameters encapsulated into a dictionary to prevent the data
                fetcher modules to be affected by adding and removing of
                parameters.
                datafetcher_base_args should contain the following keys:
                    config (dict): A dictionary containing the configuration
                        parameters.
                    log_queue: The multiprocessing queue which is used for
                        logging.
                    fetcher_id (int): The ID of this datafetcher instance.
                    context: The ZMQ context to be used.
                    lock: A threading lock object to handle control signal
                        access.
                    stop_request: A threading event to notify the data fetcher
                        to stop.
            name (str): The name of the derived data fetcher module. This is
                        used for logging.
        """
        super().__init__()

        self.log_queue = datafetcher_base_config["log_queue"]
        self.config_all = datafetcher_base_config["config"]
        self.fetcher_id = datafetcher_base_config["fetcher_id"]
        self.context = datafetcher_base_config["context"]
        self.lock = datafetcher_base_config["lock"]
        self.stop_request = datafetcher_base_config["stop_request"]
        check_dep = datafetcher_base_config["check_dep"]
        logger_name = "{}-{}".format(name, self.fetcher_id)

        self.log = utils.get_logger(logger_name, self.log_queue)

        # base_parameters
        self.required_params_base = {
            "network": [
                "endpoints",
                "main_pid"
            ],
            "datafetcher": [
                "type",
                "chunksize",
                "local_target",
                "use_cleaner",
                ["remove_data", [True,
                                 False,
                                 "stop_on_error",
                                 "with_confirmation"]]
            ]
        }

        self.required_params_dep = {}
        self.config_reduced = {}
        self._base_check(module_class="datafetcher", check_dep=check_dep)

        self.config_df = self.config_all["datafetcher"]
        self.df_type = self.config_df["type"]
        if self.required_params_dep:
            self.config = self.config_df[self.df_type]
        else:
            self.config = {}

        self.cleaner_job_socket = None
        self.confirmation_topic = None

        self.source_file = None
        self.target_file = None

        self.control_signal = None
        self.keep_running = True

        self.required_params = []

        self.base_setup()

    def check_config(self, print_log=False, check_module_config=True):
        """Check that the configuration contains the necessary parameters.

        Args:
            print_log (boolean, optional): If a summary of the configured
                parameters should be logged.
            check_module_config (boolean, optional): If the module specific
                config should also be checked.
        Raises:
            WrongConfiguration: The configuration has missing or
                wrong parameters.
        """

        if self.required_params and isinstance(self.required_params, list):
            self.required_params = {
                "datafetcher": {self.df_type: self.required_params}
            }

        if check_module_config:
            required_params = [
                self.required_params_base,
                self.required_params_dep,
                self.required_params
            ]
        else:
            required_params = [
                self.required_params_base,
            ]

        config_reduced = self._check_config_base(
            config=self.config_all,
            required_params=required_params
        )

        self.config_reduced.update(config_reduced)

        if print_log:
            super().print_config(self.config_reduced)

    def base_setup(self):
        """Sets up the shared components needed by all datafetchers.

        Created a socket to communicate with the cleaner and sets the topic.
        """

        if self.config_df["use_cleaner"]:

            config_net = self.config_all["network"]

            # create socket
            self.cleaner_job_socket = self.start_socket(
                name="cleaner_job_socket",
                sock_type=zmq.PUSH,
                sock_con="connect",
                endpoint=config_net["endpoints"].cleaner_job_con
            )

            self.confirmation_topic = (
                utils.generate_sender_id(config_net["main_pid"])
            )

    def send_to_targets(self,
                        targets,
                        open_connections,
                        metadata,
                        payload,
                        chunk_number,
                        timeout=-1):
        """Send the data to targets.

        Args:
            targets: A list of targets where to send the data to.
                Each target is of the form:
                    - target: A ZMQ endpoint to send the data to.
                    - prio (int): With which priority this data should be sent:
                        - 0 is highest priority with blocking
                        - all other priorities are non-blocking but sorted
                            numerically.
                    - send_type: If the data (means payload and metadata) or
                        only the metadata should be sent.
            open_connections (dict): Containing all open sockets. If data was
                send to a target already the socket is kept open till the
                target disconnects.
            metadata: The metadata of this data block.
            payload: The data block to be sent.
            chunk_number: The chunk number of the payload to be processed.
            timeout (optional): How long to wait for the message to be received
                in s (default: -1, means wait forever)
        """
        timeout = 1
        self._check_control_signal()

        zmq_options_prio = dict(copy=False, track=True)
        zmp_options_non_prio = dict(flags=zmq.NOBLOCK)

        sending_failed = False

        for target, prio, send_type in targets:

            # socket not known
            if target not in open_connections:
                open_connections[target] = self._open_socket(
                    endpoint="tcp://{}".format(target)
                )

            message_suffix = ("message part %s from file '%s' to '%s' with "
                              "priority %s", chunk_number, self.source_file,
                              target, prio)

            # send data to the data stream to store it in the storage system
            if prio == 0:
                # send data
                try:
                    # retry sending if anything goes wrong
                    retry_sending = True
                    while retry_sending:
                        retry_sending = False

                        tracker = self._send_data(
                            send_type=send_type,
                            connection=open_connections[target],
                            metadata=metadata,
                            payload=payload,
                            zmq_options=zmq_options_prio,
                            message_suffix=message_suffix
                        )

                        if tracker is None:
                            continue

                        retry_sending = self._check_tracker(
                            tracker=tracker,
                            chunk_number=chunk_number,
                            timeout=timeout
                        )

                except Exception:
                    self.log.debug("Raising DataHandling error", exc_info=True)
                    raise DataHandlingError(
                        "Sending (metadata of) {} failed."
                        .format(message_suffix[0]), message_suffix[1:]
                    )

            else:
                try:
                    self._send_data(
                        send_type=send_type,
                        connection=open_connections[target],
                        metadata=metadata,
                        payload=payload,
                        zmq_options=zmp_options_non_prio,
                        message_suffix=message_suffix
                    )
                except Exception:
                    # remember that there was an exception but keep sending
                    # to other targets
                    self.log.error(
                        "Sending {} failed.".format(message_suffix[0]),
                        *message_suffix[1:], exc_info=True
                    )
                    sending_failed = True

        if sending_failed:
            raise DataHandlingError("Sending (metadata of) message part "
                                    "failed for one of the targets.")

    def _open_socket(self, endpoint):
        try:
            # start and register socket
            return self.start_socket(
                name="socket",
                sock_type=zmq.PUSH,
                sock_con="connect",
                endpoint=endpoint
            )
        except Exception:
            self.log.debug("Raising DataHandling error",
                           exc_info=True)
            msg = ("Failed to start socket (connect): '{}'"
                   .format(endpoint))
            raise DataHandlingError(msg)

    def _send_data(self,
                   send_type,
                   connection,
                   metadata,
                   payload,
                   zmq_options,
                   message_suffix):

        if send_type == "data":
            tracker = connection.send_multipart(payload, **zmq_options)
            self.log.info("Sending {}".format(message_suffix[0]),
                          *message_suffix[1:])

        elif send_type == "metadata":
            # json.dumps(None) is 'N.'
            send_msg = [json.dumps(metadata).encode("utf-8"),
                        json.dumps(None).encode("utf-8")]
            tracker = connection.send_multipart(send_msg, **zmq_options)
            self.log.info("Sending metadata of {}".format(message_suffix[0]),
                          *message_suffix[1:])
            self.log.debug("metadata=%s", metadata)
        else:
            self.log.error("send_type %s is not supported", send_type)
            return

        return tracker

    def _check_tracker(self, tracker, chunk_number, timeout):
        retry_sending = False

        if not tracker.done:
            self.log.debug("Message part %s from file '%s' has not been sent "
                           "yet, waiting...", chunk_number, self.source_file)

            while (not tracker.done
                   and self.keep_running
                   and not self.stop_request.is_set()):
                try:
                    tracker.wait(timeout)
                except zmq.error.NotDone:
                    pass

                # check for control signals set from outside
                if self._check_control_signal():
                    self.log.info("Retry sending message part %s from file "
                                  "'%s'.", chunk_number, self.source_file)
                    retry_sending = True
                    return retry_sending

            if not retry_sending:
                self.log.debug("Message part %s from file '%s' has not been "
                               "sent yet, waiting...done",
                               chunk_number, self.source_file)

            return retry_sending

    def _check_control_signal(self):
        """Check for control signal and react accordingly.
        """

        woke_up = False

        if self.control_signal is None:
            return

        if self.control_signal[0] == b"EXIT":
            self.log.debug("Received %s signal.", self.control_signal[0])
            self.keep_running = False

        elif self.control_signal[0] == b"CLOSE_SOCKETS":
            # do nothing
            pass

        elif self.control_signal[0] == b"SLEEP":
            self.log.debug("Received %s signal", self.control_signal[0])
            self._react_to_sleep_signal(message=None)
            # TODO reschedule file (part?)
            woke_up = True

        elif self.control_signal[0] == b"WAKEUP":
            self.log.debug("Received %s signal without sleeping",
                           self.control_signal[0])

        else:
            self.log.error("Unhandled control signal received: %s",
                           self.control_signal)

        try:
            self.lock.acquire()
            self.control_signal = None
        finally:
            self.lock.release()

        return woke_up

    def _react_to_sleep_signal(self, message):
        self.log.debug("Received sleep signal. Going to sleep.")

        sleep_time = 0.2

        # control loop with variable instead of break/continue commands to be
        # able to reset control_signal
        keep_checking_signal = True

        while (keep_checking_signal
               and self.keep_running
               and not self.stop_request.is_set()):

            if self.control_signal[0] == "SLEEP":
                # go to sleep, but check every once in
                # a while for new signals
                time.sleep(sleep_time)

                # do not reset control_signal to be able to check on it
                # resetting it would mean "no signal set -> sleep"
                continue

            elif self.control_signal[0] == "WAKEUP":
                self.log.debug("Waking up after sleeping.")
                keep_checking_signal = False

            elif self.control_signal[0] == "EXIT":
                self.log.debug("Received %s signal.", self.control_signal[0])
                self.keep_running = False
                keep_checking_signal = False

            elif self.control_signal[0] == b"CLOSE_SOCKETS":
                # do nothing
                pass

            else:
                self.log.debug("Received unknown control signal. "
                               "Ignoring it.")
                keep_checking_signal = False

            try:
                self.lock.acquire()
                self.control_signal = None
            finally:
                self.lock.release()

    # pylint: disable=no-self-use
    def generate_file_id(self, metadata):
        """Generates a file id consisting of relative path and file name

        Args:
            metadata (dict): The dictionary with the metadata of the file.
        """
        # generate file identifier
        if (metadata["relative_path"] == ""
                or metadata["relative_path"] is None):
            file_id = metadata["filename"]
        # if the relative path starts with a slash path.join will consider it
        # as absolute path
        elif metadata["relative_path"].startswith("/"):
            file_id = os.path.join(metadata["relative_path"][1:],
                                   metadata["filename"])
        else:
            file_id = os.path.join(metadata["relative_path"],
                                   metadata["filename"])
        # convert Windows paths
        file_id = Path(file_id).as_posix()
        return file_id

    @abc.abstractmethod
    def get_metadata(self, targets, metadata):
        """Extends the given metadata and generates paths

        Reads the metadata dictionary from the event detector and extends it
        with the mandatory entries:
            - filesize (int):  the total size of the logical file (before
                chunking)
            - file_mod_time (float, epoch time): modification time of the
                logical file in epoch
            - file_create_time (float, epoch time): creation time of the
                logical file in epoch
            - chunksize (int): the size of the chunks the logical message was
                split into

        Additionally it generates the absolute source path and if a local
        target is specified the absolute target path as well.

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>, <request_type>], ...]
                where
                    <request_type>: u'data' or u'metadata'
            metadata (dict): Dictionary created by the event detector
                containing:
                    - filename
                    - source_path
                    - relative_path

        Sets:
            source_file (str): the absolute path of the source file
            target_file (str): the absolute path for the target file

        """
        pass

    @abc.abstractmethod
    def send_data(self, targets, metadata, open_connections):
        """Reads data into buffer and sends it to all targets

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>, <request_type>], ...]
                where
                    <request_type>: u'data' or u'metadata'
            metadata (dict): extended metadata dictionary filled by function
                get_metadata
            open_connections (dict)

        Returns:
            Nothing
        """
        pass

    @abc.abstractmethod
    def finish(self, targets, metadata, open_connections):
        """

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>, <request_type>], ...]
                where
                    <request_type>: u'data' or u'metadata'
            metadata (dict)
            open_connections

        Returns:
            Nothing
        """
        pass

    def close_socket(self):
        """Close open sockets
        """
        self.stop_socket("cleaner_job_socket")

    def stop_base(self):
        """Stop datafetcher run loop and clean up sockets.
        """
        self.close_socket()
        self.keep_running = False

    @abc.abstractmethod
    def stop(self):
        """Stop and clean up.
        """
        pass

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop_base()
        self.stop()

    def __del__(self):
        self.stop_base()
        self.stop()
