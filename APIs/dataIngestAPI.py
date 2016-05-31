# API to ingest data into a data transfer unit

__version__ = '0.0.1'

import os
import platform
import zmq
import logging
import cPickle
import traceback

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
            self.log = logging.getLogger("dataIngestAPI")
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

        self.localhost       = "127.0.0.1"
        self.extIp           = "0.0.0.0"
        self.ipcPath         = "/tmp/zeromq-data-transfer"

        self.signalHost      = "zitpcx19282"
        self.signalPort      = "50050"

        # has to be the same port as configured in dataManager.conf as dataFetchPort
        self.eventDetPort    = "50003"
        #TODO add port in config
        # has to be the same port as configured in dataManager.conf as ...
        self.dataFetchPort   = "50010"

        self.signalConId     = "tcp://{ip}:{port}".format(ip=self.signalHost, port=self.signalPort)

        if isWindows():
            self.log.info("Using tcp for internal communication.")
            self.eventDetConId  = "tcp://{ip}:{port}".format(ip=self.localhost, port=self.dataFetchPort)
            self.dataFetchConId = "tcp://{ip}:{port}".format(ip=self.localhost, port=self.dataFetchPort)
        else:
            self.log.info("Using ipc for internal communication.")
            self.eventDetConId  = "ipc://{path}/{id}".format(path=self.ipcPath, id="eventDetConId")
            self.dataFetchConId = "ipc://{path}/{id}".format(path=self.ipcPath, id="dataFetchConId")
#            self.eventDetConId   = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="eventDetConId")
#            self.dataFetchConId  = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="dataFetchConId")

        self.signalSocket    = None
        self.eventDetSocket  = None
        self.dataFetchSocket = None

        self.poller              = zmq.Poller()

        self.filename            = False
        self.openFile            = False
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
            self.log.info("signalSocket started (connect) for '" + self.signalConId + "'")
        except Exception as e:
            self.log.error("Failed to start signalSocket (connect): '" + self.signalConId + "'", exc_info=True)
            raise

        # using a Poller to implement the signalSocket timeout (in older ZMQ version there is no option RCVTIMEO)
#        self.poller = zmq.Poller()
        self.poller.register(self.signalSocket, zmq.POLLIN)


        self.eventDetSocket = self.context.socket(zmq.PUSH)
        try:
            self.eventDetSocket.connect(self.eventDetConId)
            self.log.info("eventDetSocket started (connect) for '" + self.eventDetConId + "'")
        except:
            self.log.error("Failed to start eventDetSocket (connect): '" + self.eventDetConId + "'", exc_info=True)
            raise

        self.dataFetchSocket  = self.context.socket(zmq.PUSH)
        try:
            self.dataFetchSocket.connect(self.dataFetchConId)
            self.log.info("dataFetchSocket started (connect) for '" + self.dataFetchConId + "'")
        except:
            self.log.error("Failed to start dataFetchSocket (connect): '" + self.dataFetchConId + "'", exc_info=True)
            raise


    # return error code
    def createFile (self, filename):
        if self.openFile and self.openFile != filename:
            raise Exception("File " + str(filename) + " already opened.")

        # send notification to receiver
        self.signalSocket.send("OPEN_FILE")
        self.log.info("Sending signal to open a new file.")

        message = self.signalSocket.recv()
        self.log.debug("Received responce: " + str(message))

        self.filename = filename
        self.filePart = 0


    def write (self, data):
        # send event to eventDet
#        message = {
#                "filename" : self.filename,
#                "filePart" : self.filePart
#                }
#        message = "{ 'filename': " + self.filename + ", 'filePart': " + self.filePart + "}"
        message = '{ "filePart": ' + str(self.filePart) + ', "filename": "' + self.filename + '" }'
        self.eventDetSocket.send(message)

        # send data to ZMQ-Queue
        self.dataFetchSocket.send(data)
        self.filePart += 1


    # return error code
    def closeFile (self):
        # send close-signal to signal socket
        sendMessage = "CLOSE_FILE"
        try:
            self.signalSocket.send(sendMessage)
            self.log.info("Sending signal to close the file to signalSocket.")
        except:
            raise Exception("Sending signal to close the file to signalSocket...failed.")

        # send close-signal to event Detector
        try:
            self.eventDetSocket.send(sendMessage)
            self.log.debug("Sending signal to close the file to eventDetSocket. (sendMessage=" + sendMessage + ")")
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
            recvMessage = self.signalSocket.recv()
            self.log.info("Received answer to signal: " + str(recvMessage) )
        else:
            recvMessage = None

        if recvMessage != sendMessage:
            self.log.debug("recieved message: " + str(recvMessage))
            self.log.debug("send message: " + str(sendMessage))
            raise Exception("Something went wrong while notifying to close the file")

        self.openFile = None
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


