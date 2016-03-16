# API to receive NeXus files

__version__ = '0.0.1'

import zmq
import socket
import logging
import json
import errno
import os
import cPickle
import traceback


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


class nexusTransfer():
    def __init__(self, signalHost = None, useLog = False, context = None):

        if useLog:
            self.log = logging.getLogger("nexusTransferAPI")
        else:
            self.log = loggingFunction()

        # ZMQ applications always start by creating a context,
        # and then using that for creating sockets
        # (source: ZeroMQ, Messaging for Many Applications by Pieter Hintjens)
        if context:
            self.context         = context
            self.externalContext = True
        else:
            self.context         = zmq.Context()
            self.externalContext = False


        self.extHost      = "0.0.0.0"

        self.signalPort   = "50050"
        self.dataPort     = "50100"

        self.signalSocket = None
        self.dataSocket   = None


        self.__createSockets()


    def __createSockets(self):

        self.signalSocket = self.context.socket(zmq.REP)
        connectionStr     = "tcp://" + str(self.extHost) + ":" + str(self.signalPort)
        try:
            self.signalSocket.bind(connectionStr)
            logging.info("signalSocket started (bind) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start signalSocket (bind): '" + connectionStr + "'", exc_info=True)

        self.dataSocket   = self.context.socket(zmq.PULL)
        connectionStr     = "tcp://" + str(self.extHost) + ":" + str(self.dataPort)
        try:
            self.dataSocket.bind(connectionStr)
            self.log.info("dataSocket started (bind) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start dataSocket (bind): '" + connectionStr + "'", exc_info=True)

        self.poller = zmq.Poller()
        self.poller.register(self.signalSocket, zmq.POLLIN)
        self.poller.register(self.dataSocket, zmq.POLLIN)


    def read(self):

        socks = dict(self.poller.poll())

        if self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:

            message = self.signalSocket.recv()
            logging.debug("signalSocket recv: " + message)

            self.signalSocket.send(message)
            logging.debug("signalSocket send: " + message)

            return message

        if self.dataSocket in socks and socks[self.dataSocket] == zmq.POLLIN:

            try:
                return self.__getMultipartMessage()
            except KeyboardInterrupt:
                self.log.debug("Keyboard interrupt detected. Stopping to receive.")
                raise
            except:
                self.log.error("Unknown error while receiving files. Need to abort.", exc_info=True)
                return None, None



    def __getMultipartMessage(self):

        #save all chunks to file
        multipartMessage = self.dataSocket.recv_multipart()

        if len(multipartMessage) < 2:
            self.log.error("Received mutipart-message is too short. Either config or file content is missing.")
            self.log.debug("multipartMessage=" + str(mutipartMessage))

        #extract multipart message
        try:
            metadata = cPickle.loads(multipartMessage[0])
        except:
            self.log.error("Could not extract metadata from the multipart-message.", exc_info=True)
            metadata = None

        #TODO validate multipartMessage (like correct dict-values for metadata)

        try:
            payload = multipartMessage[1:]
        except:
            self.log.warning("An empty file was received within the multipart-message", exc_info=True)
            payload = None

        return [metadata, payload]


    ##
    #
    # Send signal that the displayer is quitting, close ZMQ connections, destoying context
    #
    ##
    def stop(self):

        try:
            if self.signalSocket:
                logging.info("closing signalSocket...")
                self.signalSocket.close(linger=0)
                self.signalSocket = None
            if self.dataSocket:
                logging.info("closing dataSocket...")
                self.dataSocket.close(linger=0)
                self.dataSocket = None
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.externalContext and self.context:
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


