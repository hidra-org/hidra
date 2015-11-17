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


#
#  --------------------------  class: FileReceiver  --------------------------------------
#
class FileReceiver:
    zmqContext            = None
    externalContext       = None         # if the context was created outside this class or not
    outputDir             = None
    dataStreamIp          = None
    dataStreamPort        = None
    lvCommunicatorIp      = None
    lvCommunicatorPort    = None
    senderComIp           = None         # ip for socket to communicate with the sender
    senderComPort         = None         # port for socket to communicate with the sender
    socketResponseTimeout = None         # time in milliseconds to wait for the sender to answer to a signal
    log                   = None

    # sockets
    dataStreamSocket      = None         # socket to receive the data from
    lvCommunicatorSocket  = None         # socket to communicate with LiveViewCommunicator class
    senderComSocket       = None         # socket to communicate with sender

    hostname              = socket.gethostname()
#    print socket.gethostbyname(socket.gethostname())


    def __init__(self, outputDir,
                 senderComIp, senderComPort,
                 dataStreamIp, dataStreamPort,
                 lvCommunicatorPort, senderResponseTimeout = 1000,
                 context = None):

        self.outputDir             = os.path.normpath(outputDir)
        self.dataStreamIp          = dataStreamIp
        self.dataStreamPort        = dataStreamPort
        self.lvCommunicatorIp      = "127.0.0.1"
        self.lvCommunicatorPort    = lvCommunicatorPort
        self.senderComIp           = senderComIp
        self.senderComPort         = senderComPort
        self.socketResponseTimeout = senderResponseTimeout

#        if context:
#            assert isinstance(context, zmq.sugar.context.Context)

        if context:
            self.zmqContext      = context
            self.externalContext = True
        else:
            self.zmqContext      = zmq.Context()
            self.externalContext = False

        #self.zmqContext = context or zmq.Context()

        self.log = self.getLogger()
        self.log.debug("Init")

        # create sockets
        self.createSockets()


    def getLogger(self):
        logger = logging.getLogger("fileReceiver")
        return logger


    def createSockets(self):
        # create socket to exchange signals with Sender
        self.senderComSocket = self.zmqContext.socket(zmq.REQ)
        # time to wait for the sender to give a confirmation of the signal
#        self.senderComSocket.RCVTIMEO = self.socketResponseTimeout
        connectionStr = "tcp://{ip}:{port}".format(ip=self.senderComIp, port=self.senderComPort)
        try:
            self.senderComSocket.connect(connectionStr)
            self.log.info("senderComSocket started (connect) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start senderComSocket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # using a Poller to implement the senderComSocket timeout (in older ZMQ version there is no option RCVTIMEO)
        self.poller = zmq.Poller()
        self.poller.register(self.senderComSocket, zmq.POLLIN)

        # create socket to communicate with LiveViewCommunicator
        self.lvCommunicatorSocket = self.zmqContext.socket(zmq.PAIR)
        connectionStr = "tcp://{ip}:{port}".format(ip=self.lvCommunicatorIp, port=self.lvCommunicatorPort)
        try:
            self.lvCommunicatorSocket.connect(connectionStr)
            self.log.debug("lvCommunicatorSocket started (connect) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start lvCommunicatorSocket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # create sockets to retrieve data from Sender
        self.dataStreamSocket = self.zmqContext.socket(zmq.PULL)
        connectionStr = "tcp://{ip}:{port}".format(ip=self.dataStreamIp, port=self.dataStreamPort)
        try:
            self.dataStreamSocket.bind(connectionStr)
            self.log.info("dataStreamSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start dataStreamSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))


    def process(self):
        # Starting live viewer
        message = "START_LIVE_VIEWER," + str(self.hostname) + "," + str(self.dataStreamPort)
        self.log.info("Sending start signal to sender...")
        self.log.debug("Sending start signal to sender, message: " + message)
        self.senderComSocket.send(str(message))

        senderMessage = None

        # wait for response of sender till timeout is reached
        try:
            socks = dict(self.poller.poll(self.socketResponseTimeout))

            # if there was a response
            if self.senderComSocket in socks and socks[self.senderComSocket] == zmq.POLLIN:
                try:
                    senderMessage = self.senderComSocket.recv()
                    self.log.info("Received message from sender: " + str(senderMessage) )
                except KeyboardInterrupt:
                    self.log.error("KeyboardInterrupt: No message received from sender")
                    self.stopReceiving(sendToSender = False)
                    sys.exit(1)
                except Exception as e:
                    self.log.error("No message received from sender")
                    self.log.debug("Error was: " + str(e))
                    self.stopReceiving(sendToSender = False)
                    sys.exit(1)

            # if the response was correct: start data retrieving
            if senderMessage and senderMessage.startswith("START_LIVE_VIEWER"):
                self.log.info("Received confirmation from sender...start receiving files")
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
            # if there was no response or the response was of the wrong format, the receiver should be shut down
            else:
                self.log.info("Sending start signal to sender...failed.")
                self.stopReceiving(sendToSender = False)

        except KeyboardInterrupt:
            self.log.info("Message could not be received due to KeyboardInterrupt during polling.")
            self.stopReceiving(sendToSender = False)

        except Exception as e:
            self.log.error("Message could not be received due to unknown error during polling.")
            self.log.debug("Error was: " + str(e))
            self.stopReceiving(sendToSender = False)

        self.log.info("Quitting FileReceiver.")


    def startReceiving(self):
        #run loop, and wait for incoming messages
        loopCounter       = 0    #counter of total received messages
        continueReceiving = True #receiving will stop if value gets False
        self.log.debug("Waiting for new messages...")
        while continueReceiving:
            try:
                self.combineMessage()
                loopCounter+=1
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
            self.stopReceiving()
            self.log.debug("shutting down receiver...done.")
        except:
            self.log.error(sys.exc_info())
            self.log.error("shutting down receiver...failed.")


    def combineMessage(self):
        receivingMessages = True
        #save all chunks to file
        while receivingMessages:
            multipartMessage = self.dataStreamSocket.recv_multipart()

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
            self.log.info("received file: " + str(targetFilepath))
        except Exception, e:
            errorMessage = "unable to append data to file."
            self.log.error(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(errorMessage)


    def stopReceiving(self, sendToSender = True):

        self.log.debug("stopReceiving...")
        try:
            self.dataStreamSocket.close(0)
            self.log.debug("closing dataStreamSocket...done.")
        except:
            self.log.error("closing dataStreamSocket...failed.")
            self.log.error(sys.exc_info())

        self.log.debug("sending exit signal to LiveViewCommunicator...")
        self.lvCommunicatorSocket.send("Exit")

        if sendToSender:
            message = "STOP_LIVE_VIEWER,"+ str(self.hostname)
            self.log.info("sending stop signal to sender...")
            self.log.debug("Sending stop signal to sender, message: " + message)
            self.senderComSocket.send(str(message), zmq.NOBLOCK)

            socks = dict(self.poller.poll(self.socketResponseTimeout))
            if self.senderComSocket in socks and socks[self.senderComSocket] == zmq.POLLIN:
                try:
                    senderMessage = self.senderComSocket.recv()
                    self.log.info("Received message from sender: " + str(senderMessage) )

                    if senderMessage == "STOP_LIVE_VIEWER":
                        self.log.info("Received confirmation from sender...")
                    else:
                        self.log.error("Received confirmation from sender...failed")
                except KeyboardInterrupt:
                    self.log.error("KeyboardInterrupt: No message received from sender")
                except Exception as e:
                    self.log.error("sending stop signal to sender...failed.")
                    self.log.debug("Error was: " + str(e))

        # give the signal time to arrive
        time.sleep(0.1)
        self.log.debug("closing signal communication sockets...")
        self.lvCommunicatorSocket.close(0)
        self.senderComSocket.close(0)
        self.log.debug("closing signal communication sockets...done")

        if not self.externalContext:
            self.log.debug("Destroying context")
            try:
                self.zmqContext.destroy()
                self.log.debug("closing zmqContext...done.")
            except:
                self.log.error("closing zmqContext...failed.")
                self.log.error(sys.exc_info())

