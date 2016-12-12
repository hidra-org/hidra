# API to ingest data into a data transfer unit

from __future__ import print_function
from __future__ import unicode_literals

import os
import platform
import zmq
import logging
import traceback
import json
import tempfile
import socket

__version__ = '0.0.1'


class LoggingFunction:
    def out(self, x, exc_info=None):
        if exc_info:
            print (x, traceback.format_exc())
        else:
            print (x)

    def __init__(self):
        self.debug = lambda x, exc_info=None: self.out(x, exc_info)
        self.info = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning = lambda x, exc_info=None: self.out(x, exc_info)
        self.error = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


class NoLoggingFunction:
    def out(self, x, exc_info=None):
        pass

    def __init__(self):
        self.debug = lambda x, exc_info=None: self.out(x, exc_info)
        self.info = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning = lambda x, exc_info=None: self.out(x, exc_info)
        self.error = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


def is_windows():
    if platform.system() == "Windows":
        return True
    else:
        return False


class Ingest():
    # return error code
    def __init__(self, use_log=False, context=None):

        if use_log:
            self.log = logging.getLogger("Ingest")
        elif use_log is None:
            self.log = NoLoggingFunction()
        else:
            self.log = LoggingFunction()

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

        # has to be the same port as configured in dataManager.conf
        # as event_det_port
        self.event_det_port = "50003"
        # has to be the same port as configured in dataManager.conf
        # as ...
        self.data_fetch_port = "50010"

        self.signal_con_id = "tcp://{0}:{1}".format(self.signal_host,
                                                    self.signal_port)

        if is_windows():
            self.log.info("Using tcp for internal communication.")
            self.eventdet_con_id = "tcp://{0}:{1}".format(self.localhost,
                                                          self.event_det_port)
            self.datafetch_con_id = "tcp://{0}:{1}".format(self.localhost,
                                                           self.data_fetch_port)
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

        self.signal_socket = None
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
        self.signal_socket = self.context.socket(zmq.REQ)

        # time to wait for the sender to give a confirmation of the signal
#        self.signal_socket.RCVTIMEO = self.response_timeout
        try:
            self.signal_socket.connect(self.signal_con_id)
            self.log.info("signal_socket started (connect) for '{0}'"
                          .format(self.signal_con_id))
        except Exception:
            self.log.error("Failed to start signal_socket (connect): '{0}'"
                           .format(self.signal_con_id), exc_info=True)
            raise

        # using a Poller to implement the signal_socket timeout
        # (in older ZMQ version there is no option RCVTIMEO)
#        self.poller = zmq.Poller()
        self.poller.register(self.signal_socket, zmq.POLLIN)

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
        self.signal_socket.send_multipart([signal, filename])
        self.log.info("Sending signal to open a new file.")

        message = self.signal_socket.recv_multipart()
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
            self.signal_socket.send_multipart(send_message)
            self.log.info("Sending signal to close the file to signal_socket")
        except:
            raise Exception("Sending signal to close the file to signal_socket"
                            "...failed")

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
                and self.signal_socket in socks
                and socks[self.signal_socket] == zmq.POLLIN):
            self.log.info("Received answer to signal...")
            #  Get the reply.
            recv_message = self.signal_socket.recv_multipart()
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
            if self.signal_socket:
                self.log.info("closing signal_socket...")
                self.signal_socket.close(linger=0)
                self.signal_socket = None
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
