__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import zmq
import sys
import logging
import errno
import os

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH
del BASE_PATH

from dataTransferAPI import dataTransfer


#
#  --------------------------  class: FileReceiver  --------------------------------------
#
class FileReceiver:
    context               = None
    externalContext       = None         # if the context was created outside this class or not
    outputDir             = None
    dataStreamPort        = None
    lvCommunicatorIp      = None
    lvCommunicatorPort    = None
    signalIp              = None         # ip for socket to communicate with the sender
    socketResponseTimeout = None         # time in milliseconds to wait for the sender to answer to a signal
    log                   = None

    # sockets
    dataStreamSocket      = None         # socket to receive the data from
    lvCommunicatorSocket  = None         # socket to communicate with LiveViewCommunicator class
    signalSocket          = None         # socket to communicate with sender


    def __init__(self, outputDir,
                 signalIp, dataStreamPort,
                 lvCommunicatorPort, senderResponseTimeout = 1000,
                 context = None):

        self.outputDir             = os.path.normpath(outputDir)
        self.lvCommunicatorIp      = "127.0.0.1"
        self.lvCommunicatorPort    = lvCommunicatorPort
        self.signalIp              = signalIp
        self.socketResponseTimeout = senderResponseTimeout

        if context:
            self.context      = context
            self.externalContext = True
        else:
            self.context      = zmq.Context()
            self.externalContext = False

        #self.context = context or zmq.Context()

        self.log = self.getLogger()
        self.log.debug("Init")

        self.dataTransferObject = dataTransfer(signalIp, dataStreamPort, useLog = True, context = self.context)

        # create sockets
        self.createSockets()


    def getLogger(self):
        logger = logging.getLogger("fileReceiver")
        return logger


    def createSockets(self):
        # create socket to communicate with LiveViewCommunicator
        self.lvCommunicatorSocket = self.context.socket(zmq.PAIR)
        connectionStr = "tcp://{ip}:{port}".format(ip=self.lvCommunicatorIp, port=self.lvCommunicatorPort)
        try:
            self.lvCommunicatorSocket.connect(connectionStr)
            self.log.debug("lvCommunicatorSocket started (connect) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start lvCommunicatorSocket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))


    def process(self):

        try:
            self.dataTransferObject.start("stream")

            continueReceiving = True #receiving will stop if value gets False
            self.log.debug("Waiting for new messages...")
        except Exception as e:
            self.log.error("could not initiate stream")
            self.log.debug("Error was: " + str(e))
            continueReceiving = False

        #run loop, and wait for incoming messages
        while continueReceiving:

            try:
                self.combineMessage()
            except KeyboardInterrupt:
                self.log.debug("Keyboard interrupt detected. Stop receiving.")
                break
            except Exception as e:
                self.log.error("receive message...failed.")
                self.log.error("Error was: " + str(e))
                break

        self.log.info("shutting down receiver...")
        self.stop()


    def combineMessage(self):

        try:
            [payloadMetadata, payload] = self.dataTransferObject.get()
        except Exception as e:
            self.log.error("Getting data failed.")
            self.log.debug("Error was: " + str(e))
            raise

        self.dataTransferObject.store(self.outputDir, [payloadMetadata, payload] )

        filename            = self.dataTransferObject.generateTargetFilepath(self.outputDir, payloadMetadata)
        fileModTime         = payloadMetadata["fileModificationTime"]

        # send the file to the LiveViewCommunicator to add it to the ring buffer
        message = "AddFile" + str(filename) + ", " + str(fileModTime)
        self.log.debug("Send file to LiveViewCommunicator: " + message )
        self.lvCommunicatorSocket.send(message)


    def stop(self):


        if self.lvCommunicatorSocket:
            try:
                self.log.debug("Sending exit signal to LiveViewCommunicator...")
                self.lvCommunicatorSocket.send("Exit")
            except Exception as e:
                self.log.error("Sending exit signal to LiveViewCommunicator...failed")
                self.log.debug("Error was: " + str(e))

            try:
                self.log.debug("Closing communication socket...")
                # give signal time to arrive
                self.lvCommunicatorSocket.close(0.2)
                self.lvCommunicatorSocket = None
                self.log.debug("Closing communication socket...done")
            except Exception as e:
                self.log.error("Closing communication socket...failed")
                self.log.debug("Error was: " + str(e))


        self.dataTransferObject.stop()

        if not self.externalContext:
            try:
                if self.context:
                    self.log.info("Destroying ZMQ context...")
                    self.context.destroy()
                    self.context = None
                    self.log.debug("Destroying ZMQ context...done.")
            except:
                self.log.error("Destroying ZMQ context...failed.")
                self.log.debug("Error was: " + str(e))


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()
