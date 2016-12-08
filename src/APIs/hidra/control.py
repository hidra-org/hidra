# API to communicate with a data transfer unit

from __future__ import print_function
#from __future__ import unicode_literals

import socket
import logging
import os
import sys
import traceback

__version__ = '0.0.4'


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


class NotSupported(Exception):
    pass


class UsageError(Exception):
    pass


class FormatError(Exception):
    pass


class ConnectionFailed(Exception):
    pass


class VersionError(Exception):
    pass


class AuthenticationFailed(Exception):
    pass


class CommunicationFailed(Exception):
    pass


connection_list = {
    "p00": {
        "host": "asap3-p00",
        "port": 51000
        },
    "p01": {
        "host": "asap3-bl-prx07",
        "port": 51001
        },
    "p02.1": {
        "host": "asap3-bl-prx07",
        "port": 51002
        },
    "p02.2": {
        "host": "asap3-bl-prx07",
        "port": 51003
        },
    "p03": {
        "host": "asap3-bl-prx07",
        "port": 51004
        },
    "p04": {
        "host": "asap3-bl-prx07",
        "port": 51005
        },
    "p05": {
        "host": "asap3-bl-prx07",
        "port": 51006
        },
    "p06": {
        "host": "asap3-bl-prx07",
        "port": 51007
        },
    "p07": {
        "host": "asap3-bl-prx07",
        "port": 51008
        },
    "p08": {
        "host": "asap3-bl-prx07",
        "port": 51009
        },
    "p09": {
        "host": "asap3-bl-prx07",
        "port": 51010
        },
    "p10": {
        "host": "asap3-bl-prx07",
        "port": 51011
        },
    "p11": {
        "host": "asap3-bl-prx07",
        "port": 51012
        },
    }


class Control():
    def __init__(self, beamline, use_log=False):
        global connection_list

        if use_log:
            self.log = logging.getLogger("Control")
        elif use_log is None:
            self.log = NoLoggingFunction()
        else:
            self.log = LoggingFunction()

        self.current_pid = os.getpid()

        try:
            self.signal_host = connection_list[beamline]["host"]
            self.signal_port = connection_list[beamline]["port"]
            self.log.info("Starting connection to {0} on port {1}"
                          .format(self.signal_host, self.signal_port))
        except:
            self.log.error("Beamline {0} not supported".format(beamline))

        self.signal_socket = None

        self.__create_sockets()

    def __create_sockets(self):
        self.signal_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.signal_socket.connect((self.signal_host, self.signal_port))
        except Exception:
            self.log.error("connect() failed", exc_info=True)
            self.signal_socket.close()
            sys.exit()

    def get(self, attribute, timeout=None):
        msg = 'get {0}'.format(attribute)

        self.signal_socket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signal_socket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len(reply), reply))

        return reply

    def set(self, attribute, *value):
        value = list(value)

        # flatten list if entry was a list (result: list of lists)
        if type(value[0]) == list:
            value = [item for sublist in value for item in sublist]

        if attribute == "whitelist":
            msg = 'set {0} {1}'.format(attribute, value)
        else:
            msg = 'set {0} {1}'.format(attribute, value[0])

        self.signal_socket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signal_socket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len(reply), reply))

        return reply

    def do(self, command, timeout=None):
        msg = 'do {0}'.format(command)

        self.signal_socket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signal_socket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len(reply), reply))

        return reply

    def stop(self):
        if self.signal_socket:
            self.log.info("Sending close signal")
            msg = 'bye'

            self.signal_socket.send(msg)
            self.log.debug("sent (len %2d): %s" % (len(msg), msg))

            reply = self.signal_socket.recv(1024)
            self.log.debug("recv (len %2d): %s " % (len(reply), reply))

            try:
                self.log.info("closing signal_socket...")
                self.signal_socket.close()
                self.signal_socket = None
            except:
                self.log.error("closing sockets...failed.", exc_info=True)

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
