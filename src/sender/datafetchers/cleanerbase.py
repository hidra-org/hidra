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
This module implements the cleaner base class from which all cleaner inherit
from.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import abc
import sys
import zmq

import hidra.utils as utils
from base_class import Base

# source:
# pylint: disable=line-too-long
# http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5  # noqa E501
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC  # pylint: disable=no-member
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class CleanerBase(Base, ABC):
    """
    Implementation of the cleaner base class.
    """

    def __init__(self,
                 config,
                 log_queue,
                 endpoints,
                 context=None):
        """Initial setup

        Args:
             config (dict): A dictionary containing the configuration
                            parameters.
             log_queue: The multiprocessing queue which is used for logging.
             endpoints: The ZMQ endpoints to use.
             context (optional): The ZMQ context to be used.

        """
        super().__init__()

        self.log = utils.get_logger("Cleaner", log_queue)

        self.config = config
        self.endpoints = endpoints

        self.job_socket = None
        self.confirmation_socket = None
        self.control_socket = None

        self.confirm_topic = None
        self.poller = None

        self.continue_run = True

        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.create_sockets()

        try:
            self.run()
        except KeyboardInterrupt:
            pass

    def create_sockets(self):
        """Sets up the ZMQ sockets.
        """

        # socket to get information about data to be removed after
        # confirmation is received
        self.job_socket = self.start_socket(
            name="job_socket",
            sock_type=zmq.PULL,
            sock_con="bind",
            endpoint=self.endpoints.cleaner_job_bind
        )

        # socket to receive confirmation that data can be removed/discarded
        self.confirmation_socket = self.start_socket(
            name="confirmation_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.confirm_con
        )

        self.confirm_topic = utils.generate_sender_id(
            self.config["network"]["main_pid"]
        )
        self.confirmation_socket.setsockopt(zmq.SUBSCRIBE, self.confirm_topic)

        # socket for control signals
        self.control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.control_sub_con
        )

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")

        # register sockets at poller
        self.poller = zmq.Poller()
        self.poller.register(self.job_socket, zmq.POLLIN)
        self.poller.register(self.confirmation_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)

    def run(self):
        """Doing the actual work.
        """

        confirmations = {}
        jobs = {}

        while self.continue_run:
            socks = dict(self.poller.poll())

            # ----------------------------------------------------------------
            # messages from receiver
            # ----------------------------------------------------------------
            if (self.confirmation_socket in socks
                    and socks[self.confirmation_socket] == zmq.POLLIN):

                self.log.debug("Waiting for confirmation")
                message = self.confirmation_socket.recv_multipart()

                topic = message[0]
                file_id = message[1].decode("utf-8")

                # backward compatibility with versions <= 4.0.7
                if len(message) > 2:
                    chunk_number = int(message[2].decode("utf-8"))
                else:
                    chunk_number = None

                self.log.debug("topic=%s", topic)
                self.log.debug("New confirmation received: %s", file_id)
                self.log.debug("chunk_number=%s", chunk_number)

                if file_id in confirmations and chunk_number != 0:
                    confirmations[file_id]["count"] += 1
                    confirmations[file_id]["chunks"].append(chunk_number)
                else:
                    # either new confirmation or
                    # file transfer was aborted and restarted -> reset chunks
                    confirmations[file_id] = {
                        "count": 1,
                        "chunks": [chunk_number]
                    }

                self.log.debug("jobs=%s", jobs)
#                self.log.debug("confirmations={}".format(confirmations))

                if file_id in jobs:
                    this_confirm = confirmations[file_id]
                    this_job = jobs[file_id]

                    if this_confirm["count"] >= this_job["n_chunks"]:

                        # check that all chunks were received:
                        received_chunks = list(set(this_confirm["chunks"]))
                        if (len(received_chunks) == this_job["n_chunks"]
                                # backward compatibility with versions <= 4.0.7
                                or received_chunks == [None]):
                            self.remove_element(this_job["base_path"], file_id)
                            del confirmations[file_id]
                            del jobs[file_id]
                        else:
                            # correct count
                            self.log.info("More confirmations received than "
                                          "chunks sent for file %s.", file_id)
                            self.log.debug("chunks received=%s",
                                           this_confirm["chunks"])

                            this_confirm["count"] = len(received_chunks)
                            this_confirm["chunks"] = received_chunks
                else:
                    self.log.debug("confirmations without job notification "
                                   "received: %s", file_id)

            # ----------------------------------------------------------------
            # messages from DataFetcher
            # ----------------------------------------------------------------
            if (self.job_socket in socks
                    and socks[self.job_socket] == zmq.POLLIN):

                self.log.debug("Waiting for job")
                message = self.job_socket.recv_multipart()
                self.log.debug("New job received: %s", message)

                base_path = message[0].decode("utf-8")
                file_id = message[1].decode("utf-8")
                n_chunks = int(message[2].decode("utf-8"))

                if (file_id in confirmations
                        and confirmations[file_id]["count"] >= n_chunks):

                    this_confirm = confirmations[file_id]

                    # check that all chunks were received:
                    received_chunks = list(set(this_confirm["chunks"]))
                    if (len(received_chunks) == n_chunks
                            # backward compatibility with versions <= 4.0.7
                            or received_chunks == [None]):
                        self.remove_element(base_path, file_id)
                        del confirmations[file_id]
                    else:
                        # correct count
                        self.log.info("More confirmations received than "
                                      "chunks sent for file %s.", file_id)
                        self.log.debug("chunks received=%s",
                                       this_confirm["chunks"])

                        this_confirm["count"] = len(received_chunks)
                        this_confirm["chunks"] = received_chunks
                else:
                    jobs[file_id] = {
                        "base_path": base_path,
                        "n_chunks": n_chunks
                    }

            # ----------------------------------------------------------------
            # control commands
            # ----------------------------------------------------------------
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):

                # the exit signal should become effective
                if self.check_control_signal():
                    break

    def _react_to_sleep_signal(self, message):
        """Overwrite the base class reaction method to sleep signal.
        """

        # Do not react on sleep signals.
        pass

    def _react_to_wakeup_signal(self, message):
        """Overwrite the base class reaction method to wakeup signal.
        """

        if len(message) == 2 and message[1] == "RECONNECT":
            # close the connection
            self.poller.unregister(self.confirmation_socket)
            self.stop_socket(name="confirmation_socket")

            # reopen it
            self.confirmation_socket = self.start_socket(
                name="confirmation_socket",
                sock_type=zmq.SUB,
                sock_con="connect",
                endpoint=self.endpoints.confirm_con
            )

            self.confirmation_socket.setsockopt(
                zmq.SUBSCRIBE, self.confirm_topic
            )

            # register sockets at poller
            self.poller.register(self.confirmation_socket,
                                 zmq.POLLIN)

    @abc.abstractmethod
    def remove_element(self, base_path, file_id):
        """How to remove a file from the source.

        Args:
            base_path:
            file_id:
        """
        pass

    def stop(self):
        """ Clean up sockets and zmq environment.
        """

        self.stop_socket(name="job_socket")
        self.stop_socket(name="confirmation_socket")
        self.stop_socket(name="control_socket")

        if not self.ext_context and self.context is not None:
            self.log.debug("Destroying context")
            self.context.destroy(0)
            self.context = None

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()

# testing was moved into test/unittests/datafetchers/test_cleanerbase.py
