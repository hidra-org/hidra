# API to communicate with a data transfer unit

__version__ = '0.0.3'

import socket
import logging
import os
import sys


class loggingFunction:
    def out (self, x, exc_info = None):
        if exc_info:
            print x, traceback.format_exc()
        else:
            print x
    def __init__ (self):
        self.debug    = lambda x, exc_info=None: self.out(x, exc_info)
        self.info     = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning  = lambda x, exc_info=None: self.out(x, exc_info)
        self.error    = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


class noLoggingFunction:
    def out (self, x, exc_info = None):
        pass
    def __init__ (self):
        self.debug    = lambda x, exc_info=None: self.out(x, exc_info)
        self.info     = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning  = lambda x, exc_info=None: self.out(x, exc_info)
        self.error    = lambda x, exc_info=None: self.out(x, exc_info)
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

connectionList = {
    "p00": {
        "host" : "asap3-bl-prx07",
        "port" : 51000 },
    "p01": {
        "host" : "asap3-bl-prx07",
        "port" : 51001 },
    "p02.1": {
        "host" : "asap3-bl-prx07",
        "port" : 51002 },
    "p02.2": {
        "host" : "asap3-bl-prx07",
        "port" : 51003 },
    "p03": {
        "host" : "asap3-bl-prx07",
        "port" : 51004 },
    "p04": {
        "host" : "asap3-bl-prx07",
        "port" : 51005 },
    "p05": {
        "host" : "asap3-bl-prx07",
        "port" : 51006 },
    "p06": {
        "host" : "asap3-bl-prx07",
        "port" : 51007 },
    "p07": {
        "host" : "asap3-bl-prx07",
        "port" : 51008 },
    "p08": {
        "host" : "asap3-bl-prx07",
        "port" : 51009 },
    "p09": {
        "host" : "asap3-bl-prx07",
        "port" : 51010 },
    "p10": {
        "host" : "asap3-bl-prx07",
        "port" : 51011 },
    "p11": {
        "host" : "asap3-bl-prx07",
        "port" : 51012 },
    }


class HiDRAControlAPI():
    def __init__ (self, beamline, useLog = False):
        global connectionList

        if useLog:
            self.log = logging.getLogger("HiDRAControlAPI")
        elif useLog == None:
            self.log = noLoggingFunction()
        else:
            self.log = loggingFunction()

        self.currentPID     = os.getpid()

        try:
            self.signalHost = connectionList[beamline]["host"]
            self.signalPort = connectionList[beamline]["port"]
            self.log.info("Starting connection to {h} on port {p}".format(h=self.signalHost, p=self.signalPort))
        except:
            self.log.error("Beamline {bl} not supported".format(bl=beamline))

        self.signalSocket   = None

        self.__createSockets()


    def __createSockets (self):
        self.signalSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.signalSocket.connect((self.signalHost, self.signalPort))
        except Exception, e:
            self.log.error("connect() failed", exc_info=True)
            self.signalSocket.close()
            sys.exit()


    def get (self, attribute, timeout=None):
        msg = 'get {a}'.format(a = attribute)

        self.signalSocket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signalSocket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len( reply), reply))

        return reply


    def set (self, attribute, *value):
        value = list(value)
        if len(value) == 1:
            msg = 'set {a} {v}'.format(a = attribute, v = value[0])
        else:
            msg = 'set {a} {v}'.format(a = attribute, v = value)

        self.signalSocket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signalSocket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len( reply), reply))

        return reply


    def do (self, command, timeout=None):
        msg = 'do {c}'.format(c = command)

        self.signalSocket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signalSocket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len( reply), reply))

        return reply


    def stop (self):
        if self.signalSocket:
            self.log.info("Sending close signal")
            msg = 'bye'

            self.signalSocket.send(msg)
            self.log.debug("sent (len %2d): %s" % (len(msg), msg))

            reply = self.signalSocket.recv(1024)
            self.log.debug("recv (len %2d): %s " % (len( reply), reply))

            try:
                self.log.info("closing signalSocket...")
                self.signalSocket.close()
                self.signalSocket = None
            except:
                self.log.error("closing sockets...failed.", exc_info=True)


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


