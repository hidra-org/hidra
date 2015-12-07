__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import time
import zmq
import sys
import json
import logging
import errno
import os
import traceback
import threading


#
#  --------------------------  class: FileReceiver  --------------------------------------
#
class FileReceiver:
    zmqContext       = None
    outputDir        = None
    dataStreamIp     = None
    dataStreamPort   = None
    log              = None

    # sockets
    dataStreamSocket = None         # socket to receive the data from

    def __init__(self, outputDir, dataStreamIp, dataStreamPort, context = None):

        self.outputDir          = os.path.normpath(outputDir)
        self.dataStreamIp       = dataStreamIp
        self.dataStreamPort     = dataStreamPort

#        if context:
#            assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext = context or zmq.Context()

        self.log = self.getLogger()
        self.log.debug("Init")

        self.createSockets()

        try:
            self.log.info("Start receiving new files")
            self.startReceiving()
            self.log.info("Stopped receiving.")
        except Exception, e:
            self.log.error("Unknown error while receiving files. Need to abort.")
            self.log.debug("Error was: " + str(e))
        except:
            trace = traceback.format_exc()
            self.log.info("Unkown error state. Shutting down...")
            self.log.debug("Error was: " + str(trace))
            self.zmqContext.destroy()

        self.log.info("Quitting.")


    def getLogger(self):
        logger = logging.getLogger("fileReceiver")
        return logger


    def createSockets(self):
        # create pull socket
        self.dataStreamSocket = self.zmqContext.socket(zmq.PULL)
        connectionStr         = "tcp://{ip}:{port}".format(ip=self.dataStreamIp, port=self.dataStreamPort)
        try:
            self.dataStreamSocket.bind(connectionStr)
            self.log.info("dataStreamSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start dataStreamSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))


    def combineMessage(self, dataStreamSocket):
        receivingMessages = True
        #save all chunks to file
        while receivingMessages:
            multipartMessage = dataStreamSocket.recv_multipart()

            #extract multipart message
            try:
                #TODO is string conversion needed here?
                payloadMetadata = str(multipartMessage[0])
            except:
                self.log.error("an empty config was transferred for multipartMessage")


            #TODO validate multipartMessage (like correct dict-values for metadata)
            self.log.debug("multipartMessage.metadata = " + str(payloadMetadata))

            #extraction metadata from multipart-message
            payloadMetadataDict = json.loads(payloadMetadata)

            #append to file
            try:
                self.log.debug("append to file based on multipart-message...")
                #TODO: save message to file using a thread (avoids blocking)
                #TODO: instead of open/close file for each chunk recyle the file-descriptor for all chunks opened
                self.appendChunksToFileFromMultipartMessage(payloadMetadataDict, multipartMessage)
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

            if len(multipartMessage[1]) < payloadMetadataDict["chunkSize"] :
                #indicated end of file. closing file and leave loop
                self.log.debug("last file-chunk received. stop appending.")
                break
        filename            = self.generateTargetFilepath(payloadMetadataDict)
        fileModTime         = payloadMetadataDict["fileModificationTime"]
        self.log.info("New file with modification time " + str(fileModTime) + " received and saved: " + str(filename))


    def startReceiving(self):
        #run loop, and wait for incoming messages
        continueReceiving = True #receiving will stop if value gets False
        self.log.debug("Waiting for new messages...")

        while continueReceiving:
            try:
                self.combineMessage(self.dataStreamSocket)
            except KeyboardInterrupt:
                self.log.debug("Keyboard interrupt detected. Stop receiving.")
                continueReceiving = False
                break
            except:
                self.log.error("receive message...failed.")
                self.log.error(sys.exc_info())
                continueReceiving = False

        self.log.info("shutting down receiver...")
        try:
            self.stopReceiving(self.dataStreamSocket, self.zmqContext)
            self.log.debug("shutting down receiver...done.")
        except:
            self.log.error(sys.exc_info())
            self.log.error("shutting down receiver...failed.")


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
        outputDir = self.outputDir

        targetPath = os.path.join(outputDir, targetRelativePath)

        return targetPath


    def appendChunksToFileFromMultipartMessage(self, configDict, multipartMessage):

        try:
            chunkCount = len(multipartMessage) - 1 #-1 as the first element keeps the dictionary/metadata
            payload = multipartMessage[1:]
        except:
            self.log.warning("an empty file was received within the multipart-message")
            payload = None


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
#            print "received file: ", targetFilepath
        except Exception, e:
            errorMessage = "unable to append data to file."
            self.log.error(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(errorMessage)


    def stopReceiving(self, dataStreamSocket, zmqContext, sendToSender = True):

        self.log.debug("stopReceiving...")
        try:
            dataStreamSocket.close(0)
            self.log.debug("closing dataStreamSocket...done.")
        except:
            self.log.error("closing dataStreamSocket...failed.")
            self.log.error(sys.exc_info())

        try:
            zmqContext.destroy()
            self.log.debug("closing zmqContext...done.")
        except:
            self.log.error("closing zmqContext...failed.")
            self.log.error(sys.exc_info())

