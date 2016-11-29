# API to ingest data into a data transfer unit

from __future__ import print_function
from __future__ import unicode_literals

__version__ = '0.0.1'

import os
import platform
import zmq
import logging
import traceback
import json
import tempfile
import socket

class loggingFunction:
    def out (self, x, exc_info = None):
        if exc_info:
            print (x, traceback.format_exc())
        else:
            print (x)
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


def isWindows():
    returnValue = False
    windowsName = "Windows"
    platformName = platform.system()

    if platformName == windowsName:
        returnValue = True

    return returnValue


class dataIngest():
    # return error code
    def __init__ (self, useLog = False, context = None):

        if useLog:
            self.log = logging.getLogger("dataIngest")
        elif useLog == None:
            self.log = noLoggingFunction()
        else:
            self.log = loggingFunction()

        # ZMQ applications always start by creating a context,
        # and then using that for creating sockets
        # (source: ZeroMQ, Messaging for Many Applications by Pieter Hintjens)
        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False

        self.currentPID      = os.getpid()

        self.localhost = socket.gethostbyaddr("localhost")[2][0]
        try:
            socket.inet_aton(self.localhost)
            self.log.info("IPv4 address detected for localhost: {0}.".format(self.localhost))
            self.localhost_isIPv6 = False
        except socket.error:
            self.log.info("Address '{0}' is not a IPv4 address, asume it is an IPv6 address.".format(self.localhost))
            self.localhost_isIPv6 = True

        self.extIp           = "0.0.0.0"
        self.ipcPath         = os.path.join(tempfile.gettempdir(), "hidra")

        self.signalHost      = "zitpcx19282"
        self.signalPort      = "50050"

        # has to be the same port as configured in dataManager.conf as eventDetPort
        self.eventDetPort    = "50003"
        # has to be the same port as configured in dataManager.conf as ...
        self.dataFetchPort   = "50010"

        self.signalConId     = "tcp://{ip}:{port}".format(ip=self.signalHost, port=self.signalPort)

        if isWindows():
            self.log.info("Using tcp for internal communication.")
            self.eventDetConId  = "tcp://{ip}:{port}".format(ip=self.localhost, port=self.eventDetPort)
            self.dataFetchConId = "tcp://{ip}:{port}".format(ip=self.localhost, port=self.dataFetchPort)
        else:
            self.log.info("Using ipc for internal communication.")
            self.eventDetConId  = "ipc://{path}/{id}".format(path=self.ipcPath, id="eventDet")
            self.dataFetchConId = "ipc://{path}/{id}".format(path=self.ipcPath, id="dataFetch")
#            self.eventDetConId   = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="eventDet")
#            self.dataFetchConId  = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="dataFetch")

        self.signalSocket    = None
        self.eventDetSocket  = None
        self.dataFetchSocket = None

        self.poller              = zmq.Poller()

        self.filename            = False
        self.filePart            = None

        self.responseTimeout     = 1000

        self.__createSocket()


    def __createSocket (self):

        # To send file open and file close notification, a communication socket is needed
        self.signalSocket = self.context.socket(zmq.REQ)

        # time to wait for the sender to give a confirmation of the signal
#        self.signalSocket.RCVTIMEO = self.responseTimeout
        try:
            self.signalSocket.connect(self.signalConId)
            self.log.info("signalSocket started (connect) for '{0}'".format(self.signalConId))
        except Exception as e:
            self.log.error("Failed to start signalSocket (connect): '{0}'".format(self.signalConId), exc_info=True)
            raise

        # using a Poller to implement the signalSocket timeout (in older ZMQ version there is no option RCVTIMEO)
#        self.poller = zmq.Poller()
        self.poller.register(self.signalSocket, zmq.POLLIN)


        self.eventDetSocket = self.context.socket(zmq.PUSH)
        self.dataFetchSocket  = self.context.socket(zmq.PUSH)

        if isWindows() and self.localhost_isIPv6:
            self.eventDetSocket.ipv6 = True
            self.log.debug("Enabling IPv6 socket eventDetSocket")

            self.dataFetchSocket.ipv6 = True
            self.log.debug("Enabling IPv6 socket dataFetchSocket")

        try:
            self.eventDetSocket.connect(self.eventDetConId)
            self.log.info("eventDetSocket started (connect) for '{0}'".format(self.eventDetConId))
        except:
            self.log.error("Failed to start eventDetSocket (connect): '{0}'".format(self.eventDetConId), exc_info=True)
            raise

        try:
            self.dataFetchSocket.connect(self.dataFetchConId)
            self.log.info("dataFetchSocket started (connect) for '{0}'".format(self.dataFetchConId))
        except:
            self.log.error("Failed to start dataFetchSocket (connect): '{0}'".format(self.dataFetchConId), exc_info=True)
            raise


    # return error code
    def createFile (self, filename):
        signal = b"OPEN_FILE"

        if self.filename and self.filename != filename:
            raise Exception("File {0} already opened.".format(filename))

        # send notification to receiver
        self.signalSocket.send_multipart([signal, filename])
        self.log.info("Sending signal to open a new file.")

        message = self.signalSocket.recv_multipart()
        self.log.debug("Received responce: {0}".format(message))

        if signal == message[0] and filename == message[1]:
            self.filename = filename
            self.filePart = 0
        else:
            self.log.debug("signal={s} and filename={f}".format(s=signal, f=filename))
            raise Exception("Wrong responce received: {0}".format(message))


    def write (self, data):
        # send event to eventDet
        message = {
                "filename" : self.filename,
                "filePart" : self.filePart,
                "chunkSize": len(data)
                }
#        message = '{ "filePart": {p}, "filename": "{n}" }'.format(p=self.filePart, n=self.filename)
        message = json.dumps(message).encode("utf-8")
        self.eventDetSocket.send_multipart([message])

        # send data to ZMQ-Queue
        self.dataFetchSocket.send(data)
        self.filePart += 1


    # return error code
    def closeFile (self):
        # send close-signal to signal socket
        sendMessage = [b"CLOSE_FILE", self.filename]
        try:
            self.signalSocket.send_multipart(sendMessage)
            self.log.info("Sending signal to close the file to signalSocket.")
        except:
            raise Exception("Sending signal to close the file to signalSocket...failed.")

        # send close-signal to event Detector
        try:
            self.eventDetSocket.send_multipart(sendMessage)
            self.log.debug("Sending signal to close the file to eventDetSocket. (sendMessage={0})".format(sendMessage))
        except:
            raise Exception("Sending signal to close the file to eventDetSocket...failed.")

        try:
            socks = dict(self.poller.poll(10000)) # in ms
        except:
            socks = None
            self.log.error("Could not poll for signal", exc_info=True)

        # if there was a response
        if socks and self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:
            self.log.info("Received answer to signal...")
            #  Get the reply.
            recvMessage = self.signalSocket.recv_multipart()
            self.log.info("Received answer to signal: {0}".format(recvMessage) )
        else:
            recvMessage = None

        if recvMessage != sendMessage:
            self.log.debug("recieved message: {0}".format(recvMessage))
            self.log.debug("send message: {0}".format(sendMessage))
            raise Exception("Something went wrong while notifying to close the file")

        self.filename = None
        self.filePart = None


    ##
    #
    # Send signal that the displayer is quitting, close ZMQ connections, destoying context
    #
    ##
    def stop (self):
        try:
            if self.signalSocket:
                self.log.info("closing signalSocket...")
                self.signalSocket.close(linger=0)
                self.signalSocket = None
            if self.eventDetSocket:
                self.log.info("closing eventDetSocket...")
                self.eventDetSocket.close(linger=0)
                self.eventDetSocket = None
            if self.dataFetchSocket:
                self.log.info("closing dataFetchSocket...")
                self.dataFetchSocket.close(linger=0)
                self.dataFetchSocket = None
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.extContext and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


