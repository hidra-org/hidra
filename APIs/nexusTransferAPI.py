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


class loggingFunction():
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


class StopPolling():
    def __init__(self, controlPort, useLog = False, context = None):

        if useLog:
            self.log = logging.getLogger("nexusTransferAPI")
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

        self.signalPort      = "50050"
        self.dataPort        = "50100"

        self.signalSocket    = None
        self.dataSocket      = None

        self.numberOfStreams = None
        self.recvdCloseFrom  = []
        self.replyToSignal   = False
        self.allCloseRecvd   = False

        self.controlPort     = "50200"

#        self.StopPollingThread = multiprocessing.Process (target = StopPolling, args = (self.controlPort, useLog, context))
#        self.StopPollingThread.start()

        self.__createSockets()


    def __createSockets(self):

        self.signalSocket = self.context.socket(zmq.REP)
        connectionStr     = "tcp://" + str(self.extHost) + ":" + str(self.signalPort)
        try:
            self.signalSocket.bind(connectionStr)
            self.log.info("signalSocket started (bind) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start signalSocket (bind): '" + connectionStr + "'", exc_info=True)

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
        self.poller.register(self.signalSocket, zmq.POLLIN)
        self.poller.register(self.dataSocket, zmq.POLLIN)
        self.poller.register(self.controlRecv, zmq.POLLIN)


    def read(self):
        while True:
            self.log.debug("polling")
            socks = dict(self.poller.poll())

            if self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:
                self.log.debug("signalSocket is polling")

                message = self.signalSocket.recv()
                self.log.debug("signalSocket recv: " + message)

                if message == b"CLOSE_FILE":
                    if self.allCloseRecvd:
                        self.signalSocket.send(message)
                        logging.debug("signalSocket send: " + message)
                        break
                    else:
                        self.replyToSignal = message
                else:
                    self.signalSocket.send(message)
                    self.log.debug("signalSocket send: " + message)

                    return message

            if self.dataSocket in socks and socks[self.dataSocket] == zmq.POLLIN:
                self.log.debug("dataSocket is polling")

                try:
                    return self.__getMultipartMessage()
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


    def __getMultipartMessage(self):

        try:
            multipartMessage = self.dataSocket.recv_multipart()
            self.log.debug("multipartMessage=" + str(multipartMessage))
        except:
            self.log.error("Could not receive data due to unknown error.", exc_info=True)


        if len(multipartMessage) < 2:
            self.log.error("Received mutipart-message is too short. Either config or file content is missing.")
            self.log.debug("multipartMessage=" + str(mutipartMessage))
            #TODO return errorcode

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
                    self.signalSocket.send(self.replyToSignal)
                    self.log.debug("signalSocket send: " + self.replyToSignal)
                    self.replyToSignal = False
                else:
                    pass

                return "CLOSE_FILE"
            else:
                self.log.info("self.recvdCloseFrom=" + str(self.recvdCloseFrom) + ", self.numberOfStreams=" + str(self.numberOfStreams))



        else:
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
#        self.StopPollingThread.join()

        self.log.debug("closing sockets...")
        try:
            if self.signalSocket:
                self.log.info("closing signalSocket...")
                self.signalSocket.close(linger=0)
                self.signalSocket = None
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


