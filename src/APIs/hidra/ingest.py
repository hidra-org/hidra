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
API to ingest data into a data transfer unit
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
import logging
import os
import platform
import socket
import tempfile
import zmq

# from ._version import __version__
from ._shared_utils import LoggingFunction, Base


def is_windows():
    """Determines if code is run on a windows system.

    Returns:
        True if on windows, False otherwise.
    """

    return platform.system() == "Windows"


class Ingest(Base):
    """Ingest data into a hidra instance.
    """

    def __init__(self, use_log=False, context=None):

        super(Ingest, self).__init__()

        self.log = None
        self.context = None
        self.ext_context = None

        self.current_pid = None

        self.localhost = None
        self.localhost_is_ipv6 = None

        self.ext_ip = None
        self.ipc_dir = None

        self.signal_host = None
        self.signal_port = None
        self.event_det_port = None
        self.data_fetch_port = None

        self.signal_endpoint = None
        self.eventdet_endpoint = None
        self.datafetch_endpoint = None

        self.file_op_socket = None
        self.eventdet_socket = None
        self.datafetch_socket = None

        self.poller = None

        self.filename = False
        self.filepart = None

        self.response_timeout = None

        self._setup(use_log, context)

    def _setup(self, use_log, context):

        # pylint: disable=redefined-variable-type

        # print messages of certain level to screen
        if use_log in ["debug", "info", "warning", "error", "critical"]:
            self.log = LoggingFunction(use_log)

        # use logging
        elif use_log:
            self.log = logging.getLogger("Ingest")

        # use no logging at all
        elif use_log is None:
            self.log = LoggingFunction(None)

        # print everything to screen
        else:
            self.log = LoggingFunction("debug")

        # ZMQ applications always start by creating a context,
        # and then using that for creating sockets
        # (source: ZeroMQ, Messaging for Many Applications by Pieter Hintjens)
        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.current_pid = os.getpid()

        self.localhost = "127.0.0.1"
#        self.localhost = socket.gethostbyaddr("localhost")[2][0]
        try:
            socket.inet_aton(self.localhost)
            self.log.info("IPv4 address detected for localhost: {}."
                          .format(self.localhost))
            self.localhost_is_ipv6 = False
        except socket.error:
            self.log.info("Address '{}' is not a IPv4 address, asume it is "
                          "an IPv6 address.".format(self.localhost))
            self.localhost_is_ipv6 = True

        self.ext_ip = "0.0.0.0"
        self.ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")

        self.signal_host = "zitpcx19282"
        self.signal_port = "50050"

        # has to be the same port as configured in the configuration file
        # as event_det_port
        self.event_det_port = "50003"
        # has to be the same port as configured in the configuration file
        # as ...
        self.data_fetch_port = "50010"

        self.signal_endpoint = "tcp://{}:{}".format(self.signal_host,
                                                    self.signal_port)

        if is_windows():
            self.log.info("Using tcp for internal communication.")
            self.eventdet_endpoint = "tcp://{}:{}".format(self.localhost,
                                                          self.event_det_port)
            self.datafetch_endpoint = ("tcp://{}:{}"
                                       .format(self.localhost,
                                               self.data_fetch_port))
        else:
            self.log.info("Using ipc for internal communication.")
            self.eventdet_endpoint = "ipc://{}/{}".format(self.ipc_dir,
                                                          "eventdet")
            self.datafetch_endpoint = "ipc://{}/{}".format(self.ipc_dir,
                                                           "datafetch")
#            self.eventdet_endpoint = ("ipc://{}/{}_{}"
#                                      .format(self.ipc_dir,
#                                              self.current_pid,
#                                              "eventdet"))
#            self.datafetch_endpoint = ("ipc://{}/{}_{}"
#                                       .format(self.ipc_dir,
#                                               self.current_pid,
#                                               "datafetch"))

        self.poller = zmq.Poller()
        self.response_timeout = 1000

        self._create_socket()

    def _create_socket(self):

        # To send file open and file close notification, a communication
        # socket is needed

        self.file_op_socket = self._start_socket(
            name="file_op_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint=self.signal_endpoint
        )

        # using a Poller to implement the file_op_socket timeout
        # (in older ZMQ version there is no option RCVTIMEO)
        self.poller.register(self.file_op_socket, zmq.POLLIN)

        if is_windows():
            is_ipv6 = self.localhost_is_ipv6
        else:
            is_ipv6 = False

        self.file_op_socket = self._start_socket(
            name="eventdet_socket",
            sock_type=zmq.PUSH,
            sock_con="connect",
            endpoint=self.eventdet_endpoint,
            is_ipv6=is_ipv6
        )

        self.datafetch_socket = self._start_socket(
            name="datafetch_socket",
            sock_type=zmq.PUSH,
            sock_con="connect",
            endpoint=self.datafetch_endpoint,
            is_ipv6=is_ipv6
        )

    def create_file(self, filename):
        """Notify hidra to open a file.

        Args:
            filename: The name of the file to create.
        """

        signal = b"OPEN_FILE"

        if self.filename and self.filename != filename:
            raise Exception("File {} already opened.".format(filename))

        # send notification to receiver
        self.file_op_socket.send_multipart([signal, filename])
        self.log.info("Sending signal to open a new file.")

        message = self.file_op_socket.recv_multipart()
        self.log.debug("Received responce: {}".format(message))

        if signal == message[0] and filename == message[1]:
            self.filename = filename
            self.filepart = 0
        else:
            self.log.debug("signal={} and filename={}"
                           .format(signal, filename))
            raise Exception("Wrong responce received: {}".format(message))

    def write(self, data):
        """Write data into the file.

        Args:
            data: The data to be written (as binary block).
        """
        # pylint: disable=redefined-variable-type

        # send event to eventdet
        message = {
            "filename": self.filename,
            "filepart": self.filepart,
            "chunksize": len(data)
        }
#        message = ('{ "filepart": {0}, "filename": "{1}" }'
#                   .format(self.filepart, self.filename))
        message = json.dumps(message).encode("utf-8")
        self.eventdet_socket.send_multipart([message])

        # send data to ZMQ-Queue
        self.datafetch_socket.send(data)
        self.filepart += 1
#        self.log.debug("write action sent: {0}".format(message))

    def close_file(self):
        """Close the file.
        """

        # send close-signal to signal socket
        send_message = [b"CLOSE_FILE", self.filename]
        try:
            self.file_op_socket.send_multipart(send_message)
            self.log.info("Sending signal to close the file to file_op_socket")
        except:
            raise Exception("Sending signal to close the file to "
                            "file_op_socket...failed")

        # send close-signal to event Detector
        try:
            self.eventdet_socket.send_multipart(send_message)
            self.log.debug("Sending signal to close the file to "
                           "eventdet_socket (send_message={})"
                           .format(send_message))
        except:
            raise Exception("Sending signal to close the file to "
                            "eventdet_socket...failed")

        try:
            socks = dict(self.poller.poll(10000))  # in ms
        except Exception:
            socks = None
            self.log.error("Could not poll for signal", exc_info=True)

        # if there was a response
        if (socks
                and self.file_op_socket in socks
                and socks[self.file_op_socket] == zmq.POLLIN):
            self.log.info("Received answer to signal...")
            #  Get the reply.
            recv_message = self.file_op_socket.recv_multipart()
            self.log.info("Received answer to signal: {}"
                          .format(recv_message))
        else:
            recv_message = None

        if recv_message != send_message:
            self.log.debug("received message: {}".format(recv_message))
            self.log.debug("send message: {}".format(send_message))
            raise Exception("Something went wrong while notifying to close "
                            "the file")

        self.filename = None
        self.filepart = None

    def stop(self):
        """
        Send signal that the displayer is quitting, close ZMQ connections,
        destoying context
        """

        try:
            self._stop_socket(name="file_op_socket")
            self._stop_socket(name="eventdet_socket")
            self._stop_socket(name="datafetch_socket")
        except Exception:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.ext_context and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except Exception:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()
