__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import time
import zmq
import sys
import json
import logging
import errno
import os
import traceback
import socket       # needed to get hostname

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
print "BASE_PATH", BASE_PATH
API_PATH    = BASE_PATH + os.sep + "APIs"
print "API_PATH", API_PATH

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

    hostname              = socket.gethostname()
#    print socket.gethostbyname(socket.gethostname())


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
                continueReceiving = False
                break
            except:
                self.log.error("receive message...failed.")
                self.log.error(sys.exc_info())
                continueReceiving = False
                break

        self.log.info("shutting down receiver...")
        self.stop()


    def combineMessage(self):
        receivingMessages = True
        #save all chunks to file
        while receivingMessages:

            try:
                [payloadMetadataDict, payload] = self.dataTransferObject.get()
            except Exception as e:
                self.log.error("Getting data failes.")
                self.log.debug("Error was: " + str(e))
                break

            #append to file
            try:
                self.log.debug("append to file based on multipart-message...")
                #TODO: save message to file using a thread (avoids blocking)
                #TODO: instead of open/close file for each chunk recyle the file-descriptor for all chunks opened
                self.appendChunksToFileFromMultipartMessage(payloadMetadataDict, payload)
                self.log.debug("append to file based on multipart-message...success.")
            except KeyboardInterrupt:
                errorMessage = "KeyboardInterrupt detected. Unable to append multipart-content to file."
                self.log.info(errorMessage)
                break
            except Exception, e:
                errorMessage = "Unable to append multipart-content to file."
                self.log.error(errorMessage)
                self.log.debug("Error was: " + str(e))
                self.log.debug("append to file based on multipart-message...failed.")

            if len(payload) < payloadMetadataDict["chunkSize"] :
                #indicated end of file. closing file and leave loop
                self.log.debug("last file-chunk received. stop appending.")
                break

        filename            = self.generateTargetFilepath(payloadMetadataDict)
        fileModTime         = payloadMetadataDict["fileModificationTime"]
        self.log.info("New file with modification time " + str(fileModTime) + " received and saved: " + str(filename))

        # send the file to the LiveViewCommunicator to add it to the ring buffer
        message = "AddFile" + str(filename) + ", " + str(fileModTime)
        self.log.debug("Send file to LiveViewCommunicator: " + message )
        self.lvCommunicatorSocket.send(message)


    def generateTargetFilepath(self,configDict):
        """
        generates full path where target file will saved to.

        """
        targetFilename     = configDict["filename"]
        targetRelativePath = configDict["relativePath"]

        if targetRelativePath is '' or targetRelativePath is None:
            targetPath = self.outputDir
        else:
            targetPath = os.path.normpath(self.outputDir + os.sep + targetRelativePath)

        targetFilepath =  os.path.join(targetPath, targetFilename)

        return targetFilepath


    def generateTargetPath(self,configDict):
        """
        generates path where target file will saved to.

        """
        targetRelativePath = configDict["relativePath"]
        # if the relative path starts with a slash path.join will consider it as absolute path
        if targetRelativePath.startswith("/"):
            targetRelativePath = targetRelativePath[1:]

        targetPath = os.path.join(self.outputDir, targetRelativePath)

        return targetPath


    def appendChunksToFileFromMultipartMessage(self, configDict, payload):

        chunkCount = len(payload)

        #generate target filepath
        targetFilepath = self.generateTargetFilepath(configDict)
        self.log.debug("new file is going to be created at: " + targetFilepath)


        #append payload to file
        try:
            newFile = open(targetFilepath, "a")
        except IOError, e:
            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:
                #TODO create subdirectory first, then try to open the file again
                try:
                    targetPath = self.generateTargetPath(configDict)
                    os.makedirs(targetPath)
                    newFile = open(targetFilepath, "w")
                    self.log.info("New target directory created: " + str(targetPath))
                except Exception, f:
                    errorMessage = "unable to save payload to file: '" + targetFilepath + "'"
                    self.log.error(errorMessage)
                    self.log.debug("Error was: " + str(f))
                    self.log.debug("targetPath="+str(targetPath))
                    raise Exception(errorMessage)
            else:
                self.log.error("failed to append payload to file: '" + targetFilepath + "'")
                self.log.debug("Error was: " + str(e))
        except Exception, e:
            self.log.error("failed to append payload to file: '" + targetFilepath + "'")
            self.log.debug("Error was: " + str(e))
            self.log.debug("ErrorTyp: " + str(type(e)))
            self.log.debug("e.errno = " + str(e.errno) + "        errno.EEXIST==" + str(errno.EEXIST))

        #only write data if a payload exist
        try:
            if payload != None:
                for chunk in payload:
                    newFile.write(chunk)
            newFile.close()
            self.log.info("received file: " + str(targetFilepath))
        except Exception, e:
            errorMessage = "unable to append data to file."
            self.log.error(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(errorMessage)


    def stop(self):


        if self.lvCommunicatorSocket:
            try:
                self.log.debug("Sending exit signal to LiveViewCommunicator...")
                self.lvCommunicatorSocket.send("Exit")
            except Exception as e:
                self.log.error("Sending exit signal to LiveViewCommunicator...failed")
                self.log.debug("Error was: " + str(e))

            # give signal time to arrive
            time.sleep(0.2)

            try:
                self.log.debug("Closing communication socket...")
                self.lvCommunicatorSocket.close(0)
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
                self.log.debug(sys.exc_info())


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()
