# API to ingest data into a data transfer unit

__version__ = '0.0.1'

import zmq
import socket
import logging
import json
import errno
import os
import cPickle
import traceback


class dataIngest():
    # return error code
    def __init__(self, useLog = False, context = None):

        if useLog:
            self.log = logging.getLogger("dataIngestAPI")
        else:
            class loggingFunction:
                def out(self, x, exc_info = None):
                    if exc_info:
                        print x, traceback.format_exc()
                    else:
                        print x
                def __init__(self):
                    self.debug    = lambda x, exc_info=None: self.out(x, exc_info)
                    self.info     = lambda x, exc_info=None: self.out(x, exc_info)
                    self.warning  = lambda x, exc_info=None: self.out(x, exc_info)
                    self.error    = lambda x, exc_info=None: self.out(x, exc_info)
                    self.critical = lambda x, exc_info=None: self.out(x, exc_info)

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


        self.signalHost  = "zitpcx19282"
        self.signalPort  = "6000"

        self.eventPort   = "6001"
        self.dataPort    = "6002"

        self.eventSocket = None
        self.dataSocket  = None

        self.openFile    = False
        self.filePart    = None

        self.responseTimeout = 1000

        self.__createSocket()


    def __createSocket(self):

        # To send file open and file close notification, a communication socket is needed
        self.signalSocket = self.context.socket(zmq.REQ)

        # time to wait for the sender to give a confirmation of the signal
#        self.signalSocket.RCVTIMEO = self.responseTimeout
        connectionStr = "tcp://" + str(self.signalHost) + ":" + str(self.signalPort)
        try:
            self.signalSocket.connect(connectionStr)
            self.log.info("signalSocket started (connect) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start signalSocket (connect): '" + connectionStr + "'", exc_info=True)
            raise

        # using a Poller to implement the signalSocket timeout (in older ZMQ version there is no option RCVTIMEO)
        self.poller = zmq.Poller()
        self.poller.register(self.signalSocket, zmq.POLLIN)


        self.eventSocket = self.context.socket(zmq.PUSH)
        connectionStr = "tcp://localhost:" + str(self.eventPort)
        try:
            self.eventSocket.connect(connectionStr)
            self.log.info("eventSocket started (connect) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start eventSocket (connect): '" + connectionStr + "'", exc_info=True)
            raise

        self.dataSocket  = self.context.socket(zmq.PUSH)
        connectionStr = "tcp://localhost:" + str(self.dataPort)
        try:
            self.dataSocket.connect(connectionStr)
            self.log.info("dataSocket started (connect) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start dataSocket (connect): '" + connectionStr + "'", exc_info=True)
            raise


    # return error code
    def createFile(self, filename):
        if self.openFile and self.openFile != filename:
            raise Exception("File " + str(filename) + " already opened.")

        # send notification to receiver
        self.signalSocket.send("openFile")
        self.log.info("Sending signal to open a new file.")

        message = self.signalSocket.recv()
        self.log.debug("Received responce: " + str(message))

        self.filename = filename
        self.filePart = 0


    def write(self, data):
        # send event to eventDetector
        message = {
                "filename" : self.filename,
                "filePart" : self.filePart
                }
        self.eventSocket.send(cPickle.dumps(message))

        # send data to ZMQ-Queue
        self.dataSocket.send(data)
        self.filePart += 1


    # return error code
    def closeFile(self):
        # send close-signal to signal socket
        sendMessage = "closeFile"
        self.signalSocket.send(sendMessage)
        self.log.info("Sending signal to close the file.")

        recvMessage = self.signalSocket.recv()

        if recvMessage != sendMessage:
            self.log.debug("recieved message: " + str(recvMessage))
            self.log.debug("send message: " + str(sendMessage))
            raise Exception("Something went wrong while notifying to close the file")

        # send close-signal to event Detector
        self.eventSocket.send(sendMessage)
        self.log.debug("Sending signal to close the file.")

        self.openFile = None
        self.filePart = None


    ##
    #
    # Send signal that the displayer is quitting, close ZMQ connections, destoying context
    #
    ##
    def stop(self):
        try:
            if self.signalSocket:
                self.log.info("closing eventSocket...")
                self.signalSocket.close(linger=0)
                self.signalSocket = None
            if self.eventSocket:
                self.log.info("closing eventSocket...")
                self.eventSocket.close(linger=0)
                self.eventSocket = None
            if self.dataSocket:
                self.log.info("closing dataSocket...")
                self.dataSocket.close(linger=0)
                self.dataSocket = None
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.extContext and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy()
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


