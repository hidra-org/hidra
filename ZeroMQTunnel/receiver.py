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



class FileReceiver:
    zmqContext               = None
    liveViewerZmqContext     = None
    outputDir                = None
    zqmDataStreamIp          = None
    zmqDataStreamPort        = None
    zmqLiveViewerIp          = None
    zmqLiveViewerPort        = None
    ringBuffer               = []
    maxRingBufferSize        = 200
    timeToWaitForRingBuffer  = 2

    # sockets
    zmqSocket                = None


    def __init__(self, outputDir, zmqDataStreamPort, zmqDataStreamIp, zmqLiveViewerPort, zmqLiveViewerIp, context = None):
        self.outputDir          = outputDir
        self.zmqDataStreamIp    = zmqDataStreamIp
        self.zmqDataStreamPort  = zmqDataStreamPort
        self.zmqLiveViewerIp    = zmqLiveViewerIp
        self.zmqLiveViewerPort  = zmqLiveViewerPort

        if context:
            assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext = context or zmq.Context()

        # create pull socket
        self.zmqSocket         = self.zmqContext.socket(zmq.PULL)
        connectionStrZmqSocket = "tcp://" + self.zmqDataStreamIp + ":%s" % self.zmqDataStreamPort
        self.zmqSocket.bind(connectionStrZmqSocket)
        logging.debug("zmqSocket started for '" + connectionStrZmqSocket + "'")

        # thread to communicate with live viewer
        self.liveViewerThread = threading.Thread(target=self.sendFileToLiveViewer)
        self.liveViewerThread.start()

        # initialize ring buffer
        # get all entries in the directory
        # TODO empty target dir -> ringBuffer = []
        ringBuffer = (os.path.join(self.outputDir, fn) for fn in os.listdir(self.outputDir))
        # get the corresponding stats
        ringBuffer = ((os.stat(path), path) for path in ringBuffer)

        # leave only regular files, insert modification date
        ringBuffer = [[stat[ST_MTIME], path]
                  for stat, path in ringBuffer if S_ISREG(stat[ST_MODE])]

        # sort the ring buffer in descending order (new to old files)
        ringBuffer = sorted(ringBuffer, reverse=True)

        try:
            logging.info("Start receiving new files")
            self.startReceiving()
            logging.info("Stopped receiving.")
        except Exception, e:
            logging.error("Unknown error while receiving files. Need to abort.")
            logging.debug("Error was: " + str(e))
        except:
            trace = traceback.format_exc()
            logging.info("Unkown error state. Shutting down...")
            logging.debug("Error was: " + str(trace))
            self.zmqContext.destroy()

        logging.info("Quitting.")


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


    def sendFileToLiveViewer(self):

        # create socket for live viewer
        zmqLiveViewerSocket         = self.zmqContext.socket(zmq.REP)
        connectionStrZmqLiveViewerSocket = "tcp://" + self.zmqLiveViewerIp + ":%s" % self.zmqLiveViewerPort
        zmqLiveViewerSocket.bind(connectionStrZmqLiveViewerSocket)
        logging.debug("zmqLiveViewerSocket started for '" + connectionStrZmqLiveViewerSocket + "'")


        # if there is a request of the live viewer:
        while True:
            #  Wait for next request from client
            try:
                message = zmqLiveViewerSocket.recv()
            except zmq.error.ContextTerminated:
                break
            print "Received request: ", message
            time.sleep (1)
            # send first element in ring buffer to live viewer (the path of this file is the second entry)
            if self.ringBuffer:
                try:
                    zmqLiveViewerSocket.send(self.ringBuffer[0][1])
                    print self.ringBuffer[0][1]
                except zmq.error.ContextTerminated:
                    break
            else:
                try:
                    zmqLiveViewerSocket.send("None")
                    print self.ringBuffer
                except zmq.error.ContextTerminated:
                    break

        zmqLiveViewerSocket.close()


    def combineMessage(self, zmqSocket):
        #save all chunks to file
        while True:
            multipartMessage = zmqSocket.recv_multipart()
            #append to file
            try:
                logging.debug("append to file based on multipart-message...")
                #TODO: save message to file using a thread (avoids blocking)
                #TODO: instead of open/close file for each chunk recyle the file-descriptor for all chunks opened
                self.appendChunksToFileFromMultipartMessage(multipartMessage)
                logging.debug("append to file based on multipart-message...success.")
            except Exception, e:
                errorMessage = "Unable to append multipart-content to file."
                logging.error(errorMessage)
                logging.debug("Error was: " + str(e))
                logging.debug("append to file based on multipart-message...failed.")
            except:
                errorMessage = "Unable to append multipart-content to file. Unknown Error."
                logging.error(errorMessage)
                logging.debug("append to file based on multipart-message...failed.")
            if len(multipartMessage[1]) == 0:
                #indicated end of file. closing file and leave loop
                logging.debug("last file-chunk received. stop appending.")
                break
        payloadMetadata     = str(multipartMessage[0])
        payloadMetadataDict = json.loads(payloadMetadata)
        filename            = self.generateTargetFilepath(payloadMetadataDict)
        fileModTime         = payloadMetadataDict["fileModificationTime"]
        logging.info("New file with modification time " + str(fileModTime) + " received and saved: " + str(filename))

        # logging.debug("message-type  : " + str(type(multipartMessage)))
        # logging.debug("message-length: " + str(len(multipartMessage)))

        # add to ring buffer
        self.addFileToRingBuffer(str(filename), fileModTime)



    def startReceiving(self):
        #run loop, and wait for incoming messages
        continueStreaming = True
        loopCounter       = 0    #counter of total received messages
        continueReceiving = True #receiving will stop if value gets False
        logging.debug("Waiting for new messages...")
        while continueReceiving:
            try:
                self.combineMessage(self.zmqSocket)
                loopCounter+=1
            except KeyboardInterrupt:
                logging.debug("Keyboard interrupt detected. Stop receiving.")
                break
            except:
                logging.error("receive message...failed.")
                logging.error(sys.exc_info())
                continueReceiving = False

        logging.info("shutting down receiver...")
        try:
            self.stopReceiving(self.zmqSocket, self.zmqContext)
            logging.debug("shutting down receiver...done.")
        except:
            logging.error(sys.exc_info())
            logging.error("shutting down receiver...failed.")


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


    def appendChunksToFileFromMultipartMessage(self, multipartMessage):

        #extract multipart message
        try:
            configDictJson = multipartMessage[0]
        except:
            logging.error("an empty config was transferred for multipartMessage")
        try:
            chunkCount = len(multipartMessage) - 1 #-1 as the first element keeps the dictionary/metadata
            payload = multipartMessage[1:]
        except:
            logging.warning("an empty file was received within the multipart-message")
            payload = None

        #TODO validate multipartMessage (like correct dict-values for metadata)
        logging.debug("multipartMessage.metadata = " + str(configDictJson))

        #extraction metadata from multipart-message
        configDict         = json.loads(configDictJson)


        #generate target filepath
        targetFilepath = self.generateTargetFilepath(configDict)
        logging.debug("new file is going to be created at: " + targetFilepath)


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
                    logging.info("New target directory created: " + str(targetPath))
                except Exception, f:
                    errorMessage = "unable to save payload to file: '" + targetFilepath + "'"
                    logging.error(errorMessage)
                    logging.debug("Error was: " + str(f))
                    logging.debug("targetPath="+str(targetPath))
                    raise Exception(errorMessage)
        except Exception, e:
            logging.error("failed to append payload to file: '" + targetFilepath + "'")
            logging.debug("Error was: " + str(e))
            logging.debug("ErrorTyp: " + str(type(e)))
            logging.debug("e.errno = " + str(e.errno) + "        errno.EEXIST==" + str(errno.EEXIST))
        #only write data if a payload exist
        try:
            if payload != None:
                for chunk in payload:
                    newFile.write(chunk)
            newFile.close()
        except Exception, e:
            errorMessage = "unable to append data to file."
            logging.error(errorMessage)
            logging.debug("Error was: " + str(e))
            raise Exception(errorMessage)



    def stopReceiving(self, zmqSocket, zmqContext):

        logging.debug("stopReceiving...")
        try:
            zmqSocket.close()
            logging.debug("closing zmqSocket...done.")
        except:
            logging.error("closing zmqSocket...failed.")
            logging.error(sys.exc_info())

        try:
            zmqContext.destroy()
            logging.debug("closing zmqContext...done.")
        except:
            logging.error("closing zmqContext...failed.")
            logging.error(sys.exc_info())





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
