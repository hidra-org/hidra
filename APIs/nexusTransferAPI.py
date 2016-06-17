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
import multiprocessing
import time


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


class StopPolling():
    def __init__(self, controlPort, useLog = False, context = None):

        if useLog:
            self.log = logging.getLogger("nexusTransferAPI")
        elif useLog == None:
            self.log = noLoggingFunction()
        else:
            self.log = loggingFunction()

        self.extHost         = "0.0.0.0"
        self.localhost       = "localhost"
        self.controlPort     = controlPort

        if context:
            self.context         = context
            self.externalContext = True
        else:
            self.context         = zmq.Context()
            self.externalContext = False

        self.controlSend  = self.context.socket(zmq.PUSH)
#        connectionStr     = "inproc://control"
        connectionStr     = "tcp://" + str(self.localhost) + ":" + str(self.controlPort)
        try:
            self.controlSend.connect(connectionStr)
            self.log.info("contolSend started (connect) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start controlSend (conntect): '" + connectionStr + "'", exc_info=True)

        self.run()


    def run(self):
        try:
            while True:
                time.sleep(2)
        except:
            self.log.debug("Exception detected")
        finally:
            self.log.debug("Sending stop control signal")
            self.controlSend.send("STOP")
            self.log.debug("Sending stop control signal..done")
            self.stop()


    def stop(self):
        try:
            if self.controlSend:
                self.log.info("closing controlSend socket...")
                self.controlSend.close(linger=1)
                self.controlSend = None
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



class nexusTransfer():
    def __init__(self, signalHost = None, useLog = False, context = None):

        if useLog:
            self.log = logging.getLogger("nexusTransferAPI")
        elif useLog == None:
            self.log = noLoggingFunction()
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


        self.extHost         = "0.0.0.0"
        self.localhost       = "localhost"

        self.fileSignalPort  = "50050"
        self.dataPort        = "50100"

        self.fileSignalSocket = None
        self.dataSocket      = None

        self.numberOfStreams = None
        self.recvdCloseFrom  = []
        self.replyToSignal   = False
        self.allCloseRecvd   = False

        self.controlPort     = "50200"

        self.fileOpened      = False
        self.callbackParams  = None
        self.openCallback    = None
        self.readCallback    = None
        self.closeCallback   = None

#        self.StopPollingThread = multiprocessing.Process (target = StopPolling, args = (self.controlPort, useLog, context))
#        self.StopPollingThread.start()

        self.__createSockets()


    def __createSockets(self):

        self.fileSignalSocket = self.context.socket(zmq.REP)
        connectionStr     = "tcp://" + str(self.extHost) + ":" + str(self.fileSignalPort)
        try:
            self.fileSignalSocket.bind(connectionStr)
            self.log.info("fileSignalSocket started (bind) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start fileSignalSocket (bind): '" + connectionStr + "'", exc_info=True)

        self.dataSocket   = self.context.socket(zmq.PULL)
        connectionStr     = "tcp://" + str(self.extHost) + ":" + str(self.dataPort)
        try:
            self.dataSocket.bind(connectionStr)
            self.log.info("dataSocket started (bind) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start dataSocket (bind): '" + connectionStr + "'", exc_info=True)

        self.controlRecv   = self.context.socket(zmq.PULL)
#        connectionStr     = "inproc://control"
        connectionStr     = "tcp://" + str(self.extHost) + ":" + str(self.controlPort)
        try:
            self.controlRecv.bind(connectionStr)
            self.log.info("contolRecv started (bind) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start controlRecv (bind): '" + connectionStr + "'", exc_info=True)

        self.poller = zmq.Poller()
        self.poller.register(self.fileSignalSocket, zmq.POLLIN)
        self.poller.register(self.dataSocket, zmq.POLLIN)
        self.poller.register(self.controlRecv, zmq.POLLIN)


    def read(self, callbackParams, openCallback, readCallback, closeCallback):
        self.callbackParams = callbackParams
        self.openCallback   = openCallback
        self.readCallback   = readCallback
        self.closeCallback  = closeCallback

        while True:
            self.log.debug("polling")
            socks = dict(self.poller.poll())

            if self.fileSignalSocket in socks and socks[self.fileSignalSocket] == zmq.POLLIN:
                self.log.debug("fileSignalSocket is polling")

                message = self.fileSignalSocket.recv()
                self.log.debug("fileSignalSocket recv: " + message)

                if message == b"CLOSE_FILE":
                    if self.allCloseRecvd:
                        self.fileSignalSocket.send(message)
                        logging.debug("fileSignalSocket send: " + message)
                        self.allCloseRecvd = False
                        break
                    else:
                        self.replyToSignal = message
                elif message == b"OPEN_FILE":
                    self.fileSignalSocket.send(message)
                    self.log.debug("fileSignalSocket send: " + message)

                    self.openCallback(self.callbackParams, message)
                    self.fileOpened = True
#                    return message
                else:
                    self.fileSignalSocket.send("ERROR")
                    self.log.debug("fileSignalSocket send: " + message)

            if self.dataSocket in socks and socks[self.dataSocket] == zmq.POLLIN:
                self.log.debug("dataSocket is polling")

                try:
                    multipartMessage = self.dataSocket.recv_multipart()
                    self.log.debug("multipartMessage=" + str(multipartMessage))
                except:
                    self.log.error("Could not receive data due to unknown error.", exc_info=True)

                if multipartMessage[0] == b"ALIVE_TEST":
                    continue

                if len(multipartMessage) < 2:
                    self.log.error("Received mutipart-message is too short. Either config or file content is missing.")
                    self.log.debug("multipartMessage=" + str(multipartMessage))
                    #TODO return errorcode

                try:
                    self.__reactOnMessage(multipartMessage)
                except KeyboardInterrupt:
                    self.log.debug("Keyboard interrupt detected. Stopping to receive.")
                    raise
                except:
                    self.log.error("Unknown error while receiving files. Need to abort.", exc_info=True)
                    return None, None

            if self.controlRecv in socks and socks[self.controlRecv] == zmq.POLLIN:
                self.log.debug("controlRecv is polling")
                self.controlRecv.recv()
#                self.log.debug("Control signal received. Stopping.")
                raise Exception("Control signal received. Stopping.")


    def __reactOnMessage(self, multipartMessage):

        if multipartMessage[0] == b"CLOSE_FILE":
            id = multipartMessage[1]
            self.recvdCloseFrom.append(id)
            self.log.debug("Received close-file-signal from DataDispatcher-" + id)

            # get number of signals to wait for
            if not self.numberOfStreams:
                self.numberOfStreams = int(id.split("/")[1])

            # have all signals arrived?
            self.log.debug("self.recvdCloseFrom=" + str(self.recvdCloseFrom) + ", self.numberOfStreams=" + str(self.numberOfStreams))
            if len(self.recvdCloseFrom) == self.numberOfStreams:
                self.log.info("All close-file-signals arrived")
                self.allCloseRecvd = True
                if self.replyToSignal:
                    self.fileSignalSocket.send(self.replyToSignal)
                    self.log.debug("fileSignalSocket send: " + self.replyToSignal)
                    self.replyToSignal = False
                    self.recvdCloseFrom = []
                else:
                    pass

                self.closeCallback(self.callbackParams, multipartMessage)
            else:
                self.log.info("self.recvdCloseFrom=" + str(self.recvdCloseFrom) + ", self.numberOfStreams=" + str(self.numberOfStreams))

        else:
            #extract multipart message
            try:
                #TODO exchange cPickle with json
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

            self.readCallback(self.callbackParams, [metadata, payload])


    ##
    #
    # Send signal that the displayer is quitting, close ZMQ connections, destoying context
    #
    ##
    def stop(self):
#        self.StopPollingThread.join()

        self.log.debug("closing sockets...")
        try:
            if self.fileSignalSocket:
                self.log.info("closing fileSignalSocket...")
                self.fileSignalSocket.close(linger=0)
                self.fileSignalSocket = None
            if self.dataSocket:
                self.log.info("closing dataSocket...")
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


