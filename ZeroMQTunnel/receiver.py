__author__ = 'Marco Strutz <marco.strutz@desy.de>', 'Manuela Kuhn <marnuel.kuhn@desy.de>'


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


class Coordinator:
    zmqContext               = None
    liveViewerZmqContext     = None
    outputDir                = None
    zqmDataStreamIp          = None
    zmqDataStreamPort        = None
    zmqLiveViewerIp          = None
    zmqLiveViewerPort        = None
    receiverExchangeIp       = "127.0.0.1"
    receiverExchangePort     = "6072"
    liveViewerExchangeIp     = "127.0.0.1"
    liveViewerExchangePort   = "6073"
    ringBuffer               = []
    maxRingBufferSize        = 200
    timeToWaitForRingBuffer  = 2

    log                      = None

    receiverThread           = None
    liveViewerThread         = None

    # sockets
    receiverExchangeSocket   = None
    liveViewerExchangeSocket = None
    zmqliveViewerSocket      = None


    def __init__(self, outputDir, zmqDataStreamPort, zmqDataStreamIp, zmqLiveViewerPort, zmqLiveViewerIp, context = None):
        self.outputDir          = outputDir
        self.zmqDataStreamIp    = zmqDataStreamIp
        self.zmqDataStreamPort  = zmqDataStreamPort
        self.zmqLiveViewerIp    = zmqLiveViewerIp
        self.zmqLiveViewerPort  = zmqLiveViewerPort

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

        # TODO use this to communicate with live viewer class
        self.liveViewerExchangeSocket         = self.zmqContext.socket(zmq.PAIR)
        connectionStrLiveViewerExchangeSocket = "tcp://" + self.liveViewerExchangeIp + ":%s" % self.liveViewerExchangePort
        self.liveViewerExchangeSocket.bind(connectionStrLiveViewerExchangeSocket)
        self.log.debug("liveViewerExchangeSocket started (bind) for '" + connectionStrLiveViewerExchangeSocket + "'")

        # create socket for live viewer
        self.zmqliveViewerSocket         = self.zmqContext.socket(zmq.REP)
        connectionStrLiveViewerSocket    = "tcp://" + self.zmqLiveViewerIp + ":%s" % self.zmqLiveViewerPort
        self.zmqliveViewerSocket.bind(connectionStrLiveViewerSocket)
        self.log.debug("zmqLiveViewerSocket started (bind) for '" + connectionStrLiveViewerSocket + "'")

        self.poller = zmq.Poller()
        self.poller.register(self.receiverExchangeSocket, zmq.POLLIN)
#        self.poller.register(self.liveViewerExchangeSocket, zmq.POLLIN)
        self.poller.register(self.zmqliveViewerSocket, zmq.POLLIN)


        # thread to communicate with live viewer
#        self.liveViewerThread = threading.Thread(target=LiveViewer)
#        self.liveViewerThread.start()


        # initialize ring buffer
        # get all entries in the directory
        # TODO empty target dir -> ringBuffer = []
        self.ringBuffer = (os.path.join(self.outputDir, fn) for fn in os.listdir(self.outputDir))
        # get the corresponding stats
        self.ringBuffer = ((os.stat(path), path) for path in self.ringBuffer)
        # leave only regular files, insert modification date
        self.ringBuffer = [[stat[ST_MTIME], path]
                for stat, path in self.ringBuffer if S_ISREG(stat[ST_MODE])]

        # sort the ring buffer in descending order (new to old files)
        self.ringBuffer = sorted(self.ringBuffer, reverse=True)
        self.log.debug("Init ring buffer")


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
                    self.log.debug("Recieved exit command, coordinator thread will stop recieving messages")
                    should_continue = False
                    self.liveViewerSocket.send("Exit")
                    break
                elif message.startswith("AddFile"):
                    self.log.debug("Received AddFile command")
                    # add file to ring buffer
                    splittedMessage = message[7:].split(", ")
                    filename        = splittedMessage[0]
                    fileModTime     = splittedMessage[1]
                    self.log.debug("Send new file to ring buffer: " + str(filename) + ", " + str(fileModTime))
                    self.addFileToRingBuffer(filename, fileModTime)

#            if self.liveViewerExchangeSocket in socks and socks[self.liveViewerExchangeSocket] == zmq.POLLIN:
#                message = self.liveViewerExchangeSocket.recv()
            if self.zmqliveViewerSocket in socks and socks[self.zmqliveViewerSocket] == zmq.POLLIN:
                message = self.zmqliveViewer.recv()
                self.log.debug("Call for next file... " + message)
                # send first element in ring buffer to live viewer (the path of this file is the second entry)
                if self.ringBuffer:
                    answer = self.ringBuffer[0][1]
                else:
                    answer = "None"

                print answer
                try:
#                    self.liveViewerExchangeSocket.send(answer)
                    self.zmqliveViewerSocket.send(answer)
                except zmq.error.ContextTerminated:
                    break

        self.log.debug("sending exit signal to thread...")
#        self.liveViewerExchangeSocket.send("Exit")
        self.zmqliveViewerSocket.send("Exit")
        # give the signal time to arrive
        time.sleep(0.1)

        self.log.debug("Closing socket")
        self.receiverExchangeSocket.close()
        self.liveViewerExchangeSocket.close()
        self.zmqliveViewerSocket.close()


    def addFileToRingBuffer(self, filename, fileModTime):
        # prepend file to ring buffer and restore order
        self.ringBuffer[:0] = [[fileModTime, filename]]
        self.ringBuffer = sorted(self.ringBuffer, reverse=True)

        # if the maximal size is exceeded: remove the oldest files
        if len(self.ringBuffer) > self.maxRingBufferSize:
            for mod_time, path in self.ringBuffer[self.maxRingBufferSize:]:
                if float(time.time()) - mod_time > self.timeToWaitForRingBuffer:
                    os.remove(path)
                    self.ringBuffer.remove([mod_time, path])



class FileReceiver:
    zmqContext               = None
    outputDir                = None
    zqmDataStreamIp          = None
    zmqDataStreamPort        = None
    zmqLiveViewerIp          = None
    zmqLiveViewerPort        = None
    exchangeIp               = "127.0.0.1"
    exchangePort             = "6072"

    log                      = None

    # sockets
    zmqSocket                = None
    exchangeSocket           = None


    def __init__(self, outputDir, zmqDataStreamPort, zmqDataStreamIp, zmqLiveViewerPort, zmqLiveViewerIp, context = None):
        self.outputDir          = outputDir
        self.zmqDataStreamIp    = zmqDataStreamIp
        self.zmqDataStreamPort  = zmqDataStreamPort
        self.zmqLiveViewerIp    = zmqLiveViewerIp
        self.zmqLiveViewerPort  = zmqLiveViewerPort

        if context:
            assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext = context or zmq.Context()

        self.log = self.getLogger()
        self.log.debug("Init")

        # start file receiver
        self.receiverThread = threading.Thread(target=Coordinator, args=(self.outputDir, self.zmqDataStreamPort, self.zmqDataStreamIp, self.zmqLiveViewerPort, self.zmqLiveViewerIp))
        self.receiverThread.start()

        # create pull socket
        self.zmqSocket         = self.zmqContext.socket(zmq.PULL)
        connectionStrZmqSocket = "tcp://" + self.zmqDataStreamIp + ":%s" % self.zmqDataStreamPort
        self.zmqSocket.bind(connectionStrZmqSocket)
        self.log.debug("zmqSocket started (bind) for '" + connectionStrZmqSocket + "'")

        self.exchangeSocket = self.zmqContext.socket(zmq.PAIR)
        connectionStrExchangeSocket = "tcp://" + self.exchangeIp + ":%s" % self.exchangePort
        self.exchangeSocket.connect(connectionStrExchangeSocket)
        self.log.debug("exchangeSocket started (connect) for '" + connectionStrExchangeSocket + "'")

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


    def combineMessage(self, zmqSocket):
        receivingMessages = True
        #save all chunks to file
        while receivingMessages:
            multipartMessage = zmqSocket.recv_multipart()

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
            except Exception, e:
                errorMessage = "Unable to append multipart-content to file."
                self.log.error(errorMessage)
                self.log.debug("Error was: " + str(e))
                self.log.debug("append to file based on multipart-message...failed.")
            except:
                errorMessage = "Unable to append multipart-content to file. Unknown Error."
                self.log.error(errorMessage)
                self.log.debug("append to file based on multipart-message...failed.")
            if len(multipartMessage[1]) < payloadMetadataDict["chunkSize"] :
                #indicated end of file. closing file and leave loop
                self.log.debug("last file-chunk received. stop appending.")
                break
        filename            = self.generateTargetFilepath(payloadMetadataDict)
        fileModTime         = payloadMetadataDict["fileModificationTime"]
        self.log.info("New file with modification time " + str(fileModTime) + " received and saved: " + str(filename))

        # send the file to the coordinator to add it to the ring buffer
        message = "AddFile" + str(filename) + ", " + str(fileModTime)
        self.log.debug("Send file to coordinator: " + message )
        self.exchangeSocket.send(message)



    def startReceiving(self):
        #run loop, and wait for incoming messages
        continueStreaming = True
        loopCounter       = 0    #counter of total received messages
        continueReceiving = True #receiving will stop if value gets False
        self.log.debug("Waiting for new messages...")
        while continueReceiving:
            try:
                self.combineMessage(self.zmqSocket)
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
            self.stopReceiving(self.zmqSocket, self.zmqContext)
            self.log.debug("shutting down receiver...done.")
        except:
            self.log.error(sys.exc_info())
            self.log.error("shutting down receiver...failed.")


    def generateTargetFilepath(self,configDict):
        """
        generates full path where target file will saved to.

        """
        targetFilename     = configDict["filename"]
        targetRelativePath = configDict["relativeParent"]

        if targetRelativePath is '' or targetRelativePath is None:
            targetPath = self.getOutputDir()
        else:
            targetPath = os.path.join(self.getOutputDir(), targetRelativePath)

        targetFilepath =  os.path.join(targetPath, targetFilename)

        return targetFilepath


    def getOutputDir(self):
        return self.outputDir


    def generateTargetPath(self,configDict):
        """
        generates path where target file will saved to.

        """
        targetRelativePath = configDict["relativeParent"]
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
        except Exception, e:
            errorMessage = "unable to append data to file."
            self.log.error(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(errorMessage)


    def stopReceiving(self, zmqSocket, zmqContext):

        self.log.debug("stopReceiving...")
        try:
            zmqSocket.close()
            self.log.debug("closing zmqSocket...done.")
        except:
            self.log.error("closing zmqSocket...failed.")
            self.log.error(sys.exc_info())

        self.log.debug("sending exit signal to thread...")
        self.exchangeSocket.send("Exit")
        # give the signal time to arrive
        time.sleep(0.1)
        self.exchangeSocket.close()
        self.log.debug("sending exit signal to thread...done")

        try:
            zmqContext.destroy()
            self.log.debug("closing zmqContext...done.")
        except:
            self.log.error("closing zmqContext...failed.")
            self.log.error(sys.exc_info())


class LiveViewer():
    zmqContext               = None
    liveViewerIp             = None
    liveViewerPort           = None
    exchangeIp               = "127.0.0.1"
    exchangePort             = "6072"

    log                      = None

    # sockets
    liveViewerSocket         = None
    exchangeSocket           = None

    poller                   = None


    def __init__(self, liveViewerIp = "127.0.0.1", liveViewerPort = "6071", context = None):
        self.liveViewerIp    = liveViewerIp
        self.liveViewerPort  = liveViewerPort

        if context:
            assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext = context or zmq.Context()

        self.log = self.getLogger()
        self.log.debug("Init")

        # create socket for live viewer
        self.liveViewerSocket         = self.zmqContext.socket(zmq.REP)
        connectionStrLiveViewerSocket = "tcp://" + self.liveViewerIp + ":%s" % self.liveViewerPort
        self.liveViewerSocket.bind(connectionStrLiveViewerSocket)
        self.log.debug("zmqLiveViewerSocket started (bind) for '" + connectionStrLiveViewerSocket + "'")

        # create socket for message exchange
        self.exchangeSocket         = self.zmqContext.socket(zmq.PAIR)
        connectionStrExchangeSocket = "tcp://" + self.exchangeIp + ":%s" % self.exchangePort
        self.exchangeSocket.connect(connectionStrExchangeSocket)
        self.log.debug("exchangeSocket started (connect) for '" + connectionStrExchangeSocket + "'")

        self.poller = zmq.Poller()
        self.poller.register(self.liveViewerSocket, zmq.POLLIN)
        self.poller.register(self.exchangeSocket, zmq.POLLIN)

        self.sendFileToLiveViewer()


    def getLogger(self):
        logger = logging.getLogger("liveViewer")
        return logger


    def sendFileToLiveViewer(self):
        should_continue = True

        while should_continue:
            socks = dict(self.poller.poll())
            if self.exchangeSocket in socks and socks[self.exchangeSocket] == zmq.POLLIN:
                message = self.exchangeSocket.recv()
                self.log.debug("Recieved control command: %s" % message )
                if message == "Exit":
                    self.log.debug("Recieved exit command, liveViewer thread will stop recieving messages")
                    should_continue = False
                    break

            if self.liveViewerSocket in socks and socks[self.liveViewerSocket] == zmq.POLLIN:
                message = self.liveViewerSocket.recv()
                self.log.debug("Call for next file... ")
                # send first element in ring buffer to live viewer (the path of this file is the second entry)
                if self.ringBuffer:
                    answer = self.ringBuffer[0][1]
                else:
                    answer = "None"

                print answer
                try:
                    self.liveViewerSocket.send(answer)
                except zmq.error.ContextTerminated:
                    break

        self.log.debug("LiveViewerThread: closing socket")
        self.liveViewerSocket.close()
        self.exchangeSocket.close()





def argumentParsing():

    parser = argparse.ArgumentParser()
    parser.add_argument("--outputDir"             , type=str, help="where incoming data will be stored to", default="/tmp/watchdog/data_mirror/")
    parser.add_argument("--tcpPortDataStream"     , type=int, help="tcp port of data pipe", default=6061)
    parser.add_argument("--tcpPortLiveViewer"     , type=int, help="tcp port of live viewer", default=6071)
    parser.add_argument("--logfile"               , type=str, help="file used for logging", default="/tmp/watchdog/fileReceiver.log")
    parser.add_argument("--bindingIpForDataStream", type=str, help="local ip to bind dataStream to", default="127.0.0.1")
    parser.add_argument("--bindingIpForLiveViewer", type=str, help="local ip to bind LiveViewer to", default="127.0.0.1")
    parser.add_argument("--verbose"       ,           help="more verbose output", action="store_true")

    arguments = parser.parse_args()

    # TODO: check folder-directory for existance

    return arguments


def initLogging(filenameFullPath, verbose):
    #@see https://docs.python.org/2/howto/logging-cookbook.html


    #more detailed logging if verbose-option has been set
    loggingLevel = logging.INFO
    if verbose:
        loggingLevel = logging.DEBUG

    #log everything to file
    logging.basicConfig(level=loggingLevel,
                        format='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s:%(lineno)d] [%(name)s] [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d_%H:%M:%S',
                        filename=filenameFullPath,
                        filemode="a")

    #log info to stdout, display messages with different format than the file output
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    formatter = logging.Formatter("%(asctime)s >  %(message)s")
    console.setFormatter(formatter)

    logging.getLogger("").addHandler(console)


if __name__ == "__main__":


    #argument parsing
    arguments         = argumentParsing()
    outputDir         = arguments.outputDir
    verbose           = arguments.verbose
    zmqDataStreamIp   = str(arguments.bindingIpForDataStream)
    zmqDataStreamPort = str(arguments.tcpPortDataStream)
    zmqLiveViewerIp   = str(arguments.bindingIpForLiveViewer)
    zmqLiveViewerPort = str(arguments.tcpPortLiveViewer)
    logFile           = arguments.logfile
    logfileFilePath   = arguments.logfile


    #enable logging
    initLogging(logfileFilePath, verbose)


    #start file receiver
    myWorker = FileReceiver(outputDir, zmqDataStreamPort, zmqDataStreamIp, zmqLiveViewerPort, zmqLiveViewerIp)
