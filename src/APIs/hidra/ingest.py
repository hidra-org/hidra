# API to ingest data into a data transfer unit

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import os
import platform
import zmq
import logging
import json
import tempfile
import socket

# from ._version import __version__
from ._shared_helpers import LoggingFunction


def is_windows():
    if platform.system() == "Windows":
        return True
    else:
        return False


class Ingest():
    # return error code
    def __init__(self, use_log=False, context=None):

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
            self.log.info("IPv4 address detected for localhost: {0}."
                          .format(self.localhost))
            self.localhost_is_ipv6 = False
        except socket.error:
            self.log.info("Address '{0}' is not a IPv4 address, asume it is "
                          "an IPv6 address.".format(self.localhost))
            self.localhost_is_ipv6 = True

        self.ext_ip = "0.0.0.0"
        self.ipc_path = os.path.join(tempfile.gettempdir(), "hidra")

        self.signal_host = "zitpcx19282"
        self.signal_port = "50050"

        # has to be the same port as configured in the configuration file
        # as event_det_port
        self.event_det_port = "50003"
        # has to be the same port as configured in the configuration file
        # as ...
        self.data_fetch_port = "50010"

        self.signal_con_id = "tcp://{0}:{1}".format(self.signal_host,
                                                    self.signal_port)

        if is_windows():
            self.log.info("Using tcp for internal communication.")
            self.eventdet_con_id = "tcp://{0}:{1}".format(self.localhost,
                                                          self.event_det_port)
            self.datafetch_con_id = ("tcp://{0}:{1}"
                                     .format(self.localhost,
                                             self.data_fetch_port))
        else:
            self.log.info("Using ipc for internal communication.")
            self.eventdet_con_id = "ipc://{0}/{1}".format(self.ipc_path,
                                                          "eventDet")
            self.datafetch_con_id = "ipc://{0}/{1}".format(self.ipc_path,
                                                           "dataFetch")
#            self.eventdet_con_id = ("ipc://{0}/{1}_{2}"
#                                    .format(self.ipc_path,
#                                            self.current_pid,
#                                            "eventDet"))
#            self.datafetch_con_id = ("ipc://{0}/{1}_{2}"
#                                     .format(self.ipc_path,
#                                             self.current_pid,
#                                             "dataFetch"))

        self.file_op_socket = None
        self.eventdet_socket = None
        self.datafetch_socket = None

        self.poller = zmq.Poller()

        self.filename = False
        self.filepart = None

        self.response_timeout = 1000

        self.__create_socket()

    def __create_socket(self):

        # To send file open and file close notification, a communication
        # socket is needed
        self.file_op_socket = self.context.socket(zmq.REQ)

        # time to wait for the sender to give a confirmation of the signal
#        self.file_op_socket.RCVTIMEO = self.response_timeout
        try:
            self.file_op_socket.connect(self.signal_con_id)
            self.log.info("file_op_socket started (connect) for '{0}'"
                          .format(self.signal_con_id))
        except Exception:
            self.log.error("Failed to start file_op_socket (connect): '{0}'"
                           .format(self.signal_con_id), exc_info=True)
            raise

        # using a Poller to implement the file_op_socket timeout
        # (in older ZMQ version there is no option RCVTIMEO)
#        self.poller = zmq.Poller()
        self.poller.register(self.file_op_socket, zmq.POLLIN)

        self.eventdet_socket = self.context.socket(zmq.PUSH)
        self.datafetch_socket = self.context.socket(zmq.PUSH)

        if is_windows() and self.localhost_is_ipv6:
            self.eventdet_socket.ipv6 = True
            self.log.debug("Enabling IPv6 socket eventdet_socket")

            self.datafetch_socket.ipv6 = True
            self.log.debug("Enabling IPv6 socket datafetch_socket")

        try:
            self.eventdet_socket.connect(self.eventdet_con_id)
            self.log.info("eventdet_socket started (connect) for '{0}'"
                          .format(self.eventdet_con_id))
        except:
            self.log.error("Failed to start eventdet_socket (connect): '{0}'"
                           .format(self.eventdet_con_id), exc_info=True)
            raise

        try:
            self.datafetch_socket.connect(self.datafetch_con_id)
            self.log.info("datafetch_socket started (connect) for '{0}'"
                          .format(self.datafetch_con_id))
        except:
            self.log.error("Failed to start datafetch_socket (connect): '{0}'"
                           .format(self.datafetch_con_id), exc_info=True)
            raise

    # return error code
    def create_file(self, filename):
        signal = b"OPEN_FILE"

        if self.filename and self.filename != filename:
            raise Exception("File {0} already opened.".format(filename))

        # send notification to receiver
        self.file_op_socket.send_multipart([signal, filename])
        self.log.info("Sending signal to open a new file.")

        message = self.file_op_socket.recv_multipart()
        self.log.debug("Received responce: {0}".format(message))

        if signal == message[0] and filename == message[1]:
            self.filename = filename
            self.filepart = 0
        else:
            self.log.debug("signal={0} and filename={1}"
                           .format(signal, filename))
            raise Exception("Wrong responce received: {0}".format(message))

    def write(self, data):
        # send event to eventDet
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

    # return error code
    def close_file(self):
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
                           "eventdet_socket (send_message={0})"
                           .format(send_message))
        except:
            raise Exception("Sending signal to close the file to "
                            "eventdet_socket...failed")

        try:
            socks = dict(self.poller.poll(10000))  # in ms
        except:
            socks = None
            self.log.error("Could not poll for signal", exc_info=True)

        # if there was a response
        if (socks
                and self.file_op_socket in socks
                and socks[self.file_op_socket] == zmq.POLLIN):
            self.log.info("Received answer to signal...")
            #  Get the reply.
            recv_message = self.file_op_socket.recv_multipart()
            self.log.info("Received answer to signal: {0}"
                          .format(recv_message))
        else:
            recv_message = None

        if recv_message != send_message:
            self.log.debug("received message: {0}".format(recv_message))
            self.log.debug("send message: {0}".format(send_message))
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
            if self.file_op_socket:
                self.log.info("closing file_op_socket...")
                self.file_op_socket.close(linger=0)
                self.file_op_socket = None
            if self.eventdet_socket:
                self.log.info("closing eventdet_socket...")
                self.eventdet_socket.close(linger=0)
                self.eventdet_socket = None
            if self.datafetch_socket:
                self.log.info("closing datafetch_socket...")
                self.datafetch_socket.close(linger=0)
                self.datafetch_socket = None
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.ext_context and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
