# API to communicate with a data transfer unit

__version__ = '0.0.1'

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

class TangoAPI():
    def __init__ (self, signalHost, signalPort = None, useLog = False):

        if useLog:
            self.log = logging.getLogger("TangoAPI")
        elif useLog == None:
            self.log = noLoggingFunction()
        else:
            self.log = loggingFunction()

        self.currentPID            = os.getpid()

        self.signalHost            = signalHost
        self.signalPort            = signalPort or "51000"

        self.signalSocket          = None

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


    def do (self, command, timeout=None):
        msg = 'do {c}'.format(c = command)

        self.signalSocket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signalSocket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len( reply), reply))


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


