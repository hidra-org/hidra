__author__ = 'Manuela Kuhn <marnuel.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import time
import zmq
import sys
import random
import json
import argparse
import logging
import errno
import os
import traceback
from stat import S_ISREG, ST_MTIME, ST_MODE
import threading
import socket       # needed to get hostname
import helperScript
from RingBuffer import RingBuffer

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname (  os.path.realpath ( __file__ ) ) ) )
CONFIG_PATH = BASE_PATH + os.sep + "conf"

sys.path.append ( CONFIG_PATH )

from config import defaultConfigReceiver as defaultConfig


#
#  --------------------------  class: FileReceiver  --------------------------------------
#
class FileReceiver:
    zmqContext               = None
    outputDir                = None
    zmqDataStreamIp          = None
    zmqDataStreamPort        = None
    zmqLiveViewerIp          = None
    zmqLiveViewerPort        = None
    exchangeIp               = "127.0.0.1"
    exchangePort             = "6072"
    senderComIp              = None         # ip for socket to communicate with receiver
    senderComPort            = None         # port for socket to communicate receiver
    socketResponseTimeout    = None         # time in milliseconds to wait for the sender to answer to a signal

    log                      = None

    # sockets
    zmqDataStreamSocket      = None         # socket to receive the data from
    exchangeSocket           = None         # socket to communicate with Coordinator class
    senderComSocket          = None         # socket to communicate with sender

    hostname                 = socket.gethostname()
#    print socket.gethostbyname(socket.gethostname())


    def __init__(self, outputDir, zmqDataStreamPort, zmqDataStreamIp, zmqLiveViewerPort, zmqLiveViewerIp, senderComPort,
                 maxRingBuffersize, senderResponseTimeout = 1000, context = None):

        self.outputDir             = outputDir
        self.zmqDataStreamIp       = zmqDataStreamIp
        self.zmqDataStreamPort     = zmqDataStreamPort
        self.zmqLiveViewerIp       = zmqLiveViewerIp
        self.zmqLiveViewerPort     = zmqLiveViewerPort
        self.senderComIp           = zmqDataStreamIp        # ip for socket to communicate with sender; is the same ip as the data stream ip
        self.senderComPort         = senderComPort
        self.socketResponseTimeout = senderResponseTimeout

        if context:
            assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext = context or zmq.Context()

        self.log = self.getLogger()
        self.log.debug("Init")

        # start file receiver
        self.receiverThread = threading.Thread(target=Coordinator, args=(self.outputDir, self.zmqDataStreamPort, self.zmqDataStreamIp, self.zmqLiveViewerPort, self.zmqLiveViewerIp, maxRingBuffersize))
        self.receiverThread.start()

        # create pull socket
        self.zmqDataStreamSocket = self.zmqContext.socket(zmq.PULL)
        connectionStrDataStreamSocket = "tcp://{ip}:{port}".format(ip=self.zmqDataStreamIp, port=self.zmqDataStreamPort)
        print "connectionStrDataStreamSocket", connectionStrDataStreamSocket
        self.zmqDataStreamSocket.connect(connectionStrDataStreamSocket)
        self.log.debug("zmqDataStreamSocket started (connect) for '" + connectionStrDataStreamSocket + "'")

        self.exchangeSocket = self.zmqContext.socket(zmq.PAIR)
        connectionStrExchangeSocket = "tcp://{ip}:{port}".format(ip=self.exchangeIp, port=self.exchangePort)
        self.exchangeSocket.connect(connectionStrExchangeSocket)
        self.log.debug("exchangeSocket started (connect) for '" + connectionStrExchangeSocket + "'")

        self.senderComSocket = self.zmqContext.socket(zmq.REQ)
        # time to wait for the sender to give a confirmation of the signal
        self.senderComSocket.RCVTIMEO = self.socketResponseTimeout
        connectionStrSenderComSocket = "tcp://{ip}:{port}".format(ip=self.senderComIp, port=self.senderComPort)
        print "connectionStrSenderComSocket", connectionStrSenderComSocket
        self.senderComSocket.connect(connectionStrSenderComSocket)
        self.log.debug("senderComSocket started (connect) for '" + connectionStrSenderComSocket + "'")

        message = "START_LIVE_VIEWER," + str(self.hostname)
        self.log.info("Sending start signal to sender...")
        self.log.debug("Sending start signal to sender, message: " + message)
        print "sending message ", message
        self.senderComSocket.send(str(message))
#        self.senderComSocket.send("START_LIVE_VIEWER")

        senderMessage = None
        try:
            senderMessage = self.senderComSocket.recv()
            print "answer to start live viewer: ", senderMessage
            self.log.debug("Received message from sender: " + str(senderMessage) )
        except KeyboardInterrupt:
            self.log.error("KeyboardInterrupt: No message received from sender")
            self.stopReceiving(self.zmqDataStreamSocket, self.zmqContext, sendToSender = False)
            sys.exit(1)
        except Exception as e:
            self.log.error("No message received from sender")
            self.log.debug("Error was: " + str(e))
            self.stopReceiving(self.zmqDataStreamSocket, self.zmqContext, sendToSender = False)
            sys.exit(1)

        if senderMessage == "START_LIVE_VIEWER":
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
        else:
            print "Sending start signal to sender...failed."
            self.log.info("Sending start signal to sender...failed.")
            self.stopReceiving(self.zmqDataStreamSocket, self.zmqContext, sendToSender = False)


        self.log.info("Quitting.")


    def getLogger(self):
        logger = logging.getLogger("fileReceiver")
        return logger


    def combineMessage(self, zmqDataStreamSocket):
        receivingMessages = True
        #save all chunks to file
        while receivingMessages:
            multipartMessage = zmqDataStreamSocket.recv_multipart()

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
        print "receiving multipart message from data pipe: ", filename
        self.log.info("New file with modification time " + str(fileModTime) + " received and saved: " + str(filename))

        # send the file to the coordinator to add it to the ring buffer
        message = "AddFile" + str(filename) + ", " + str(fileModTime)
        self.log.debug("Send file to coordinator: " + message )
        self.exchangeSocket.send(message)


    def startReceiving(self):
        #run loop, and wait for incoming messages
        loopCounter       = 0    #counter of total received messages
        continueReceiving = True #receiving will stop if value gets False
        self.log.debug("Waiting for new messages...")
        while continueReceiving:
            try:
                self.combineMessage(self.zmqDataStreamSocket)
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
            self.stopReceiving(self.zmqDataStreamSocket, self.zmqContext)
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
            targetPath = self.getOutputDir()
        else:
            targetPath = os.path.normpath(self.getOutputDir() + os.sep + targetRelativePath)

        targetFilepath =  os.path.join(targetPath, targetFilename)

        return targetFilepath


    def getOutputDir(self):
        return self.outputDir


    def generateTargetPath(self,configDict):
        """
        generates path where target file will saved to.

        """
        targetRelativePath = configDict["relativePath"]
        outputDir = self.getOutputDir()

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


    def stopReceiving(self, zmqDataStreamSocket, zmqContext, sendToSender = True):

        self.log.debug("stopReceiving...")
        try:
            zmqDataStreamSocket.close(0)
            self.log.debug("closing zmqDataStreamSocket...done.")
        except:
            self.log.error("closing zmqDataStreamSocket...failed.")
            self.log.error(sys.exc_info())

        self.log.debug("sending exit signal to coordinator...")
        self.exchangeSocket.send("Exit")

        if sendToSender:
            self.log.debug("sending stop signal to sender...")

            message = "STOP_LIVE_VIEWER,"+ str(self.hostname)
            print "sending message ", message
            self.senderComSocket.send(str(message), zmq.NOBLOCK)

            try:
                senderMessage = self.senderComSocket.recv()
                print "answer to stop live viewer: ", senderMessage
                self.log.debug("Received message from sender: " + str(senderMessage) )

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
        self.exchangeSocket.close(0)
        self.senderComSocket.close(0)
        self.log.debug("closing signal communication sockets...done")

        try:
            zmqContext.destroy()
            self.log.debug("closing zmqContext...done.")
        except:
            self.log.error("closing zmqContext...failed.")
            self.log.error(sys.exc_info())


#
#  --------------------------  class: Coordinator  --------------------------------------
#
class Coordinator:
    zmqContext               = None
    liveViewerZmqContext     = None
    outputDir                = None
    zmqDataStreamIp          = None
    zmqDataStreamPort        = None
    zmqLiveViewerIp          = None
    zmqLiveViewerPort        = None
    receiverExchangeIp       = "127.0.0.1"
    receiverExchangePort     = "6072"

    ringBuffer               = []
    maxRingBufferSize        = None

    log                      = None

    receiverThread           = None
    liveViewerThread         = None

    # sockets
    receiverExchangeSocket   = None         # socket to communicate with FileReceiver class
    zmqliveViewerSocket      = None         # socket to communicate with live viewer


    def __init__(self, outputDir, zmqDataStreamPort, zmqDataStreamIp, zmqLiveViewerPort, zmqLiveViewerIp, maxRingBufferSize, context = None):
        self.outputDir          = outputDir
        self.zmqDataStreamIp    = zmqDataStreamIp
        self.zmqDataStreamPort  = zmqDataStreamPort
        self.zmqLiveViewerIp    = zmqLiveViewerIp
        self.zmqLiveViewerPort  = zmqLiveViewerPort

        self.maxRingBufferSize  = maxRingBufferSize
#        # TODO remove outputDir from ringBuffer?
#        self.ringBuffer         = RingBuffer(self.maxRingBufferSize, self.outputDir)
        self.ringBuffer         = RingBuffer(self.maxRingBufferSize)

        self.log = self.getLogger()
        self.log.debug("Init")

        if context:
            assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext = context or zmq.Context()

        # create sockets
        self.receiverExchangeSocket         = self.zmqContext.socket(zmq.PAIR)
        connectionStrReceiverExchangeSocket = "tcp://" + self.receiverExchangeIp + ":%s" % self.receiverExchangePort
        self.receiverExchangeSocket.bind(connectionStrReceiverExchangeSocket)
        self.log.debug("receiverExchangeSocket started (bind) for '" + connectionStrReceiverExchangeSocket + "'")

        # create socket for live viewer
        self.zmqliveViewerSocket         = self.zmqContext.socket(zmq.REP)
        connectionStrLiveViewerSocket    = "tcp://" + self.zmqLiveViewerIp + ":%s" % self.zmqLiveViewerPort
        self.zmqliveViewerSocket.bind(connectionStrLiveViewerSocket)
        self.log.debug("zmqLiveViewerSocket started (bind) for '" + connectionStrLiveViewerSocket + "'")

        self.poller = zmq.Poller()
        self.poller.register(self.receiverExchangeSocket, zmq.POLLIN)
        self.poller.register(self.zmqliveViewerSocket, zmq.POLLIN)

        try:
            self.log.info("Start communication")
            self.communicate()
            self.log.info("Stopped communication.")
        except Exception, e:
            trace = traceback.format_exc()
            self.log.info("Unkown error state. Shutting down...")
            self.log.debug("Error was: " + str(e))


        self.log.info("Quitting.")


    def getLogger(self):
        logger = logging.getLogger("coordinator")
        return logger


    def communicate(self):
        should_continue = True

        while should_continue:
            socks = dict(self.poller.poll())

            if self.receiverExchangeSocket in socks and socks[self.receiverExchangeSocket] == zmq.POLLIN:
                message = self.receiverExchangeSocket.recv()
                self.log.debug("Recieved control command: %s" % message )
                if message == "Exit":
                    self.log.debug("Received exit command, coordinator thread will stop recieving messages")
                    should_continue = False
                    # TODO why sending signal to live viewer?
#                    self.zmqliveViewerSocket.send("Exit", zmq.NOBLOCK)
                    break
                elif message.startswith("AddFile"):
                    self.log.debug("Received AddFile command")
                    # add file to ring buffer
                    splittedMessage = message[7:].split(", ")
                    filename        = splittedMessage[0]
                    fileModTime     = splittedMessage[1]
                    self.log.debug("Add new file to ring buffer: " + str(filename) + ", " + str(fileModTime))
                    self.ringBuffer.add(filename, fileModTime)

            if self.zmqliveViewerSocket in socks and socks[self.zmqliveViewerSocket] == zmq.POLLIN:
                message = self.zmqliveViewerSocket.recv()
                self.log.debug("Call for next file... " + message)
                # send newest element in ring buffer to live viewer
                answer = self.ringBuffer.getNewestFile()
                print answer
                try:
                    self.zmqliveViewerSocket.send(answer)
                except zmq.error.ContextTerminated:
                    break

        self.log.debug("Closing socket")
        self.receiverExchangeSocket.close(0)
        self.zmqliveViewerSocket.close(0)


def argumentParsing():
    defConf = defaultConfig()

    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"          , type=str, default=defConf.logfilePath          , help="path where logfile will be created (default=" + str(defConf.logfilePath) + ")")
    parser.add_argument("--logfileName"          , type=str, default=defConf.logfileName          , help="filename used for logging (default=" + str(defConf.logfileName) + ")")
    parser.add_argument("--targetDir"            , type=str, default=defConf.targetDir            , help="where incoming data will be stored to (default=" + str(defConf.targetDir) + ")")
    parser.add_argument("--dataStreamIp"         , type=str, default=defConf.dataStreamIp         , help="ip of dataStream-socket to pull new files from (default=" + str(defConf.dataStreamIp) + ")")
    parser.add_argument("--dataStreamPort"       , type=str, default=defConf.dataStreamPort       , help="port number of dataStream-socket to pull new files from (default=" + str(defConf.dataStreamPort) + ")")
    parser.add_argument("--liveViewerIp"         , type=str, default=defConf.liveViewerIp         , help="local ip to bind LiveViewer to (default=" + str(defConf.liveViewerIp) + ")")
    parser.add_argument("--liveViewerPort"       , type=str, default=defConf.liveViewerPort       , help="tcp port of live viewer (default=" + str(defConf.liveViewerPort) + ")")
    parser.add_argument("--senderComPort"        , type=str, default=defConf.senderComPort        , help="port number of dataStream-socket to send signals back to the sender (default=" + str(defConf.senderComPort) + ")")
    parser.add_argument("--maxRingBufferSize"    , type=int, default=defConf.maxRingBufferSize    , help="size of the ring buffer for the live viewer (default=" + str(defConf.maxRingBufferSize) + ")")
    parser.add_argument("--senderResponseTimeout", type=int, default=defConf.senderResponseTimeout, help=argparse.SUPPRESS)
    parser.add_argument("--verbose"              ,           action="store_true"                  , help="more verbose output")

    arguments = parser.parse_args()

    targetDir = str(arguments.targetDir)

    # check target directory for existance
    helperScript.checkFolderExistance(targetDir)

    return arguments


class Receiver():
    logfilePath           = None
    logfileName           = None
    logfileFullPath       = None
    verbose               = None

    targetDir             = None
    zmqDataStreamIp       = None
    zmqDataStreamPort     = None

    zmqLiveViewerIp       = None
    zmqLiveViewerPort     = None
    senderComPort         = None
    maxRingBufferSize     = None
    senderResponseTimeout = None

    def __init__(self, verbose):
        defConf = defaultConfig()

        self.logfilePath           = defConf.logfilePath
        self.logfileName           = defConf.logfileName
        self.logfileFullPath       = os.path.join(self.logfilePath, self.logfileName)
        self.verbose               = verbose

        self.targetDir             = defConf.targetDir
        self.zmqDataStreamIp       = defConf.dataStreamIp
        self.zmqDataStreamPort     = defConf.dataStreamPort

        self.zmqLiveViewerIp       = defConf.liveViewerIp
        self.zmqLiveViewerPort     = defConf.liveViewerPort
        self.senderComPort         = defConf.senderComPort
        self.maxRingBufferSize     = defConf.maxRingBufferSize
        self.senderResponseTimeout = defConf.senderResponseTimeout


        #enable logging
        helperScript.initLogging(self.logfileFullPath, self.verbose)


        #start file receiver
        myWorker = FileReceiver(self.targetDir, self.zmqDataStreamPort, self.zmqDataStreamIp, self.zmqLiveViewerPort, self.zmqLiveViewerIp, self.senderComPort, self.maxRingBufferSize, self.senderResponseTimeout)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true", help="more verbose output")
    arguments = parser.parse_args()

    receiver = Receiver(arguments.verbose)
