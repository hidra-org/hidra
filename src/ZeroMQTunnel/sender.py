from __builtin__ import open, type

__author__ = 'Marco Strutz <marco.strutz@desy.de>', 'Manuela Kuhn <manuela.kuhn@desy.de>'


import time
import argparse
import zmq
import os
import logging
import sys
import traceback
from multiprocessing import Process, freeze_support
import subprocess
import json
import shutil
import helperScript
import socket       # needed to get hostname
from watcher import DirectoryWatcher
from Cleaner import Cleaner

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname (  os.path.realpath ( __file__ ) ) ) )
CONFIG_PATH = BASE_PATH + os.sep + "conf"

sys.path.append ( CONFIG_PATH )

from config import defaultConfigSender

#
#  --------------------------  class: WorkerProcess  --------------------------------------
#
class WorkerProcess():
    id                   = None
    dataStreamIp         = None
    dataStreamPort       = None
    zmqContextForWorker  = None
    externalContext      = None    # if the context was created outside this class or not
    zmqMessageChunkSize  = None
    zmqCleanerIp         = None         # responsable to delete/move files
    zmqCleanerPort       = None         # responsable to delete/move files

    zmqDataStreamSocket  = None
    routerSocket         = None
    cleanerSocket        = None

    useLiveViewer        = False        # boolian to inform if the receiver to show the files in the live viewer is running

    # to get the logging only handling this class
    log                  = None

    def __init__(self, id, dataStreamIp, dataStreamPort, chunkSize, zmqCleanerIp, zmqCleanerPort,
                 context = None):
        self.id                   = id
        self.dataStreamIp         = dataStreamIp
        self.dataStreamPort       = dataStreamPort
        self.zmqMessageChunkSize  = chunkSize
        self.zmqCleanerIp         = zmqCleanerIp
        self.zmqCleanerPort       = zmqCleanerPort

        #initialize router
        if context:
            self.zmqContextForWorker = context
            self.externalContext     = True
        else:
            self.zmqContextForWorker = zmq.Context()
            self.externalContext     = False

        self.log = self.getLogger()


        self.log.debug("new workerProcess started. id=" + str(self.id))

        self.zmqDataStreamSocket      = self.zmqContextForWorker.socket(zmq.PUSH)
        connectionStrDataStreamSocket = "tcp://{ip}:{port}".format(ip=self.dataStreamIp, port=self.dataStreamPort)
        print "connectionStrDataStreamSocket", connectionStrDataStreamSocket
        self.zmqDataStreamSocket.bind(connectionStrDataStreamSocket)
        self.log.debug("zmqDataStreamSocket started (bind) for '" + connectionStrDataStreamSocket + "'")

        # initialize sockets
        routerIp   = "127.0.0.1"
        routerPort = "50000"

        self.routerSocket             = self.zmqContextForWorker.socket(zmq.REQ)
        self.routerSocket.identity    = u"worker-{ID}".format(ID=self.id).encode("ascii")
        connectionStrRouterSocket     = "tcp://{ip}:{port}".format(ip=routerIp, port=routerPort)
        self.routerSocket.connect(connectionStrRouterSocket)
        self.log.debug("routerSocket started (connect) for '" + connectionStrRouterSocket + "'")

        #init Cleaner message-pipe
        self.cleanerSocket            = self.zmqContextForWorker.socket(zmq.PUSH)
        connectionStrCleanerSocket    = "tcp://{ip}:{port}".format(ip=self.zmqCleanerIp, port=self.zmqCleanerPort)
        self.cleanerSocket.connect(connectionStrCleanerSocket)
        self.log.debug("cleanerSocket started (connect) for '" + connectionStrCleanerSocket + "'")

        try:
            self.process()
        except KeyboardInterrupt:
            # trace = traceback.format_exc()
            self.log.debug("KeyboardInterrupt detected. Shutting down workerProcess " + str(self.id) + ".")
        except:
            trace = traceback.format_exc()
            self.log.error("Stopping workerProcess due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))
        finally:
            self.stop()


    def process(self):
        """
          sends a 'ready' to a broker and receives a 'job' to process.
          The 'job' will be to pass the file of an fileEvent to the
          dataPipe.

          Why?
          -> the simulated "onClosed" event waits for a file for being
           not modified within a certain period of time.
           Instead of processing file after file the work will be
           spreaded to many workerProcesses. So each process can wait
           individual periods of time for a file without blocking
           new file events - as new file events will be handled by
           another workerProcess.
        """

        """
          takes the fileEventMessage, reading and passing the new file to
          a separate data-messagePipe. Afterwards the original file
          will be removed.
        """

        processingJobs = True
        jobCount = 0

        while processingJobs:
            #sending a "ready"-signal to the router.
            #the reply will contain the actual job/task.
            self.log.debug("worker-"+str(self.id)+": sending ready signal")

            self.routerSocket.send(b"READY")

            # Get workload from router, until finished
            self.log.debug("worker-"+str(self.id)+": waiting for new job")
            workload = self.routerSocket.recv()
            self.log.debug("worker-"+str(self.id)+": new job received")

            finished = workload == b"END"
            if finished:
                processingJobs = False
                self.log.debug("router requested to shutdown worker-process. Worker processed: %d files" % jobCount)
                break
            jobCount += 1

            # the live viewer is turned on
            startLV = workload == b"START_LIVE_VIEWER"
            if startLV:
                self.log.info("worker-"+str(self.id)+": Received live viewer start command...starting live viewer")
                self.useLiveViewer = True
                continue

            # the live viewer is turned of
            stopLV = workload == b"STOP_LIVE_VIEWER"
            if stopLV:
                self.log.info("worker-"+str(self.id)+": Received live viewer stop command...stopping live viewer")
                self.useLiveViewer = False
                continue

            if self.useLiveViewer:
                #convert fileEventMessage back to a dictionary
                fileEventMessageDict = None
                try:
                    fileEventMessageDict = json.loads(str(workload))
                    self.log.debug("str(messageDict) = " + str(fileEventMessageDict) + "  type(messageDict) = " + str(type(fileEventMessageDict)))
                except Exception, e:
                    errorMessage = "Unable to convert message into a dictionary."
                    self.log.error(errorMessage)
                    self.log.debug("Error was: " + str(e))


                #extract fileEvent metadata
                try:
                    #TODO validate fileEventMessageDict dict
                    filename     = fileEventMessageDict["filename"]
                    sourcePath   = fileEventMessageDict["sourcePath"]
                    relativePath = fileEventMessageDict["relativePath"]
                except Exception, e:
                    self.log.error("Invalid fileEvent message received.")
                    self.log.debug("Error was: " + str(e))
                    self.log.debug("fileEventMessageDict=" + str(fileEventMessageDict))
                    #skip all further instructions and continue with next iteration
                    continue

                #passing file to data-messagPipe
                try:
                    self.log.debug("worker-" + str(self.id) + ": passing new file to data-messagePipe...")
                    self.passFileToDataStream(filename, sourcePath, relativePath)
                    self.log.debug("worker-" + str(self.id) + ": passing new file to data-messagePipe...success.")
                except Exception, e:
                    errorMessage = "Unable to pass new file to data-messagePipe."
                    self.log.error(errorMessage)
                    self.log.error("Error was: " + str(e))
                    self.log.debug("worker-"+str(id) + ": passing new file to data-messagePipe...failed.")
                    #skip all further instructions and continue with next iteration
                    continue
            else:
                print "worker-"+str(self.id)+": no data sent"


            #send remove-request to message pipe
            try:
                #sending to pipe
                self.log.debug("send file-event for file " + str(sourcePath) + str(relativePath) + str(filename) + " to cleaner-pipe...")
                self.cleanerSocket.send(workload)
                self.log.debug("send file-event for file " + str(sourcePath) + str(relativePath) + str(filename) + " to cleaner-pipe...success.")

                #TODO: remember workload. append to list?
                # can be used to verify files which have been processed twice or more
            except Exception, e:
                errorMessage = "Unable to notify Cleaner-pipe to delete file: " + str(filename)
                self.log.error(errorMessage)
                self.log.debug("fileEventMessageDict=" + str(fileEventMessageDict))


    def getLogger(self):
        logger = logging.getLogger("workerProcess")
        return logger


    def passFileToDataStream(self, filename, sourcePath, relativePath):
        """filesizeRequested == filesize submitted by file-event. In theory it can differ to real file size"""

        # filename = "img.tiff"
        # filepath = "C:\dir"
        #
        # -->  sourceFilePathFull = 'C:\\dir\img.tiff'
        sourceFilePath     = os.path.normpath(sourcePath + os.sep + relativePath)
        sourceFilePathFull = os.path.join(sourceFilePath, filename)

        #reading source file into memory
        try:
            #for quick testing set filesize of file as chunksize
            self.log.debug("get filesize for '" + str(sourceFilePathFull) + "'...")
            filesize             = os.path.getsize(sourceFilePathFull)
            fileModificationTime = os.stat(sourceFilePathFull).st_mtime
            chunksize            = filesize    #can be used later on to split multipart message
            self.log.debug("filesize(%s) = %s" % (sourceFilePathFull, str(filesize)))
            self.log.debug("fileModificationTime(%s) = %s" % (sourceFilePathFull, str(fileModificationTime)))

        except Exception, e:
            errorMessage = "Unable to get file metadata for '" + str(sourceFilePathFull) + "'."
            self.log.error(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(e)

        try:
            self.log.debug("opening '" + str(sourceFilePathFull) + "'...")
            fileDescriptor = open(str(sourceFilePathFull), "rb")

        except Exception, e:
            errorMessage = "Unable to read source file '" + str(sourceFilePathFull) + "'."
            self.log.error(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(e)


        #build payload for message-pipe by putting source-file into a message
        try:
            payloadMetadata = self.buildPayloadMetadata(filename, filesize, fileModificationTime, sourcePath, relativePath)
        except Exception, e:
            self.log.error("Unable to assemble multi-part message.")
            self.log.debug("Error was: " + str(e))
            raise Exception(e)


        #send message
        try:
            self.log.info("Passing multipart-message for file " + str(sourceFilePathFull) + "...")
            print "sending file: ", sourceFilePathFull
            chunkNumber = 0
            stillChunksToRead = True
            while stillChunksToRead:
                chunkNumber += 1

                #read next chunk from file
                fileContentAsByteObject = fileDescriptor.read(self.getChunkSize())

                #detect if end of file has been reached
                if not fileContentAsByteObject:
                    stillChunksToRead = False

                    #as chunk is empty decrease chunck-counter
                    chunkNumber -= 1
                    break

                #assemble metadata for zmq-message
                chunkPayloadMetadata = payloadMetadata.copy()
                chunkPayloadMetadata["chunkNumber"] = chunkNumber
                chunkPayloadMetadataJson = json.dumps(chunkPayloadMetadata)
                chunkPayload = []
                chunkPayload.append(chunkPayloadMetadataJson)
                chunkPayload.append(fileContentAsByteObject)

                #send to zmq pipe
                self.zmqDataStreamSocket.send_multipart(chunkPayload, zmq.NOBLOCK)

            #close file
            fileDescriptor.close()
            print "sending file: ", sourceFilePathFull, "done"

            # self.zmqDataStreamSocket.send_multipart(multipartMessage)
            self.log.info("Passing multipart-message for file " + str(sourceFilePathFull) + "...done.")
        except zmq.error.Again:
            self.log.error("unable to send multiplart-message for file " + str(sourceFilePathFull))
            self.log.error("Receiver has disconnected").
        except Exception, e:
            self.log.error("Unable to send multipart-message for file " + str(sourceFilePathFull))
            self.log.debug("Error was: " + str(e))
            self.log.info("Passing multipart-message...failed.")
#            raise Exception(e)


    def appendFileChunksToPayload(self, payload, sourceFilePathFull, fileDescriptor, chunkSize):
        try:
            # chunksize = 16777216 #16MB
            self.log.debug("reading file '" + str(sourceFilePathFull)+ "' to memory")

            # FIXME: chunk is read-out as str. why not as bin? will probably add to much overhead to zmq-message
            fileContentAsByteObject = fileDescriptor.read(chunkSize)

            while fileContentAsByteObject != "":
                payload.append(fileContentAsByteObject)
                fileContentAsByteObject = fileDescriptor.read(chunkSize)
        except Exception, e:
            raise Exception(str(e))



    def buildPayloadMetadata(self, filename, filesize, fileModificationTime, sourcePath, relativePath):
        """
        builds metadata for zmq-multipart-message. should be used as first element for payload.
        :param filename:
        :param filesize:
        :param fileModificationTime:
        :param sourcePath:
        :param relativePath:
        :return:
        """

        #add metadata to multipart
        self.log.debug("create metadata for source file...")
        metadataDict = {
                         "filename"             : filename,
                         "filesize"             : filesize,
                         "fileModificationTime" : fileModificationTime,
                         "sourcePath"           : sourcePath,
                         "relativePath"         : relativePath,
                         "chunkSize"            : self.getChunkSize()}

        self.log.debug("metadataDict = " + str(metadataDict))

        return metadataDict

    def getChunkSize(self):
        return self.zmqMessageChunkSize


    def showFilesystemStatistics(self, vfsPath):
        statvfs = os.statvfs(vfsPath)
        totalSize                 = statvfs.f_frsize * statvfs.f_blocks
        freeBytes                 = statvfs.f_frsize * statvfs.f_bfree
        freeSpaceAvailableForUser = statvfs.f_frsize * statvfs.f_bavail  #in bytes
        freeSpaceAvailableForUser_gigabytes = freeSpaceAvailableForUser / 1024 / 1024 / 1024
        freeUserSpaceLeft_percent = ( float(freeBytes) / float(totalSize) ) * 100

        # print "{number:.{digits}f}".format(number=freeUserSpaceLeft_percent, digits=0)
        # print int(freeUserSpaceLeft_percent)

        self.log.debug("vfsstat: freeSpaceAvailableForUser=" + str(freeSpaceAvailableForUser_gigabytes)+ " Gigabytes "
                       + " (" + str(int(freeUserSpaceLeft_percent)) + "% free disk space left)")

        #warn if disk space is running low
        highWaterMark = 85
        if int(freeUserSpaceLeft_percent) >= int(highWaterMark):
            self.log.warning("Running low in disk space! " + str(int(freeUserSpaceLeft_percent)) + "% free disk space left.")


    def stop(self):
        self.log.debug("Sending stop signal to cleaner from worker-" + str(self.id))
        self.cleanerSocket.send("STOP")
        self.log.info("Closing sockets for worker " + str(self.id))
        if self.zmqDataStreamSocket:
            self.zmqDataStreamSocket.close(0)
        self.routerSocket.close(0)
        self.cleanerSocket.close(0)
        if not self.externalContext:
            self.log.debug("Destroying context")
            self.zmqContextForWorker.destroy()



#
#  --------------------------  class: FileMover  --------------------------------------
#
class FileMover():
    zmqContext          = None
    fileEventIp         = None      # serverIp for incoming messages
    fileEventPort       = None
    dataStreamIp        = None      # ip of dataStream-socket to push new files to
    dataStreamPort      = None      # port number of dataStream-socket to push new files to
    zmqCleanerIp        = None      # zmq pull endpoint, responsable to delete/move files
    zmqCleanerPort      = None      # zmq pull endpoint, responsable to delete/move files
    receiverComIp       = None      # ip for socket to communicate with receiver
    receiverComPort     = None      # port for socket to communicate receiver
    receiverWhiteList   = None
    parallelDataStreams = None
    chunkSize           = None

    # sockets
    fileEventSocket     = None      # to receive fileMove-jobs as json-encoded dictionary
    receiverComSocket   = None      # to exchange messages with the receiver
    routerSocket        = None

    useLiveViewer       = False     # boolian to inform if the receiver to show the files in the live viewer is running

    # to get the logging only handling this class
    log                   = None


    def __init__(self, fileEventIp, fileEventPort, dataStreamIp, dataStreamPort,
                 receiverComPort, receiverWhiteList,
                 parallelDataStreams, chunkSize,
                 zmqCleanerIp, zmqCleanerPort,
                 context = None):

        assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext          = context or zmq.Context()
        self.fileEventIp         = fileEventIp
        self.fileEventPort       = fileEventPort
        self.dataStreamIp        = dataStreamIp
        self.dataStreamPort      = dataStreamPort
        self.zmqCleanerIp        = zmqCleanerIp
        self.zmqCleanerPort      = zmqCleanerPort
        self.receiverComIp       = dataStreamIp         # ip for socket to communicate with receiver; is the same ip as the data stream ip
        self.receiverComPort     = receiverComPort
        self.receiverWhiteList   = receiverWhiteList
        self.parallelDataStreams = parallelDataStreams
        self.chunkSize           = chunkSize


        self.log = self.getLogger()
        self.log.debug("Init")

        # create zmq socket for incoming file events
        self.fileEventSocket         = self.zmqContext.socket(zmq.PULL)
        connectionStrFileEventSocket = "tcp://{ip}:{port}".format(ip=self.fileEventIp, port=self.fileEventPort)
        self.fileEventSocket.bind(connectionStrFileEventSocket)
        self.log.debug("fileEventSocket started (bind) for '" + connectionStrFileEventSocket + "'")


        # create zmq socket for communitation with receiver
        self.receiverComSocket         = self.zmqContext.socket(zmq.REP)
        connectionStrReceiverComSocket = "tcp://{ip}:{port}".format(ip=self.receiverComIp, port=self.receiverComPort)
        print "connectionStrReceiverComSocket", connectionStrReceiverComSocket
        self.receiverComSocket.bind(connectionStrReceiverComSocket)
        self.log.debug("receiverComSocket started (bind) for '" + connectionStrReceiverComSocket + "'")

        # Poller to get either messages from the watcher or communication messages to stop sending data to the live viewer
        self.poller = zmq.Poller()
        self.poller.register(self.fileEventSocket, zmq.POLLIN)
        self.poller.register(self.receiverComSocket, zmq.POLLIN)

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        routerIp   = "127.0.0.1"
        routerPort = "50000"

        self.routerSocket         = self.zmqContext.socket(zmq.ROUTER)
        connectionStrRouterSocket = "tcp://{ip}:{port}".format(ip=routerIp, port=routerPort)
        self.routerSocket.bind(connectionStrRouterSocket)
        self.log.debug("routerSocket started (bind) for '" + connectionStrRouterSocket + "'")


    def process(self):
        try:
            self.startReceiving()
        except zmq.error.ZMQError as e:
            self.log.error("ZMQError: "+ str(e))
            self.log.debug("Shutting down workerProcess.")
        except:
            trace = traceback.format_exc()
            self.log.info("Stopping fileMover due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))



    def getLogger(self):
        logger = logging.getLogger("fileMover")
        return logger


    def startReceiving(self):

        self.log.debug("new message-socket crated for: new file events.")

        incomingMessageCounter = 0

        #start worker-processes. each will have its own PushSocket.
        workerProcessList      = list()
        numberOfWorkerProcesses = int(self.parallelDataStreams)
        for processNumber in range(numberOfWorkerProcesses):
            self.log.debug("instantiate new workerProcess (nr " + str(processNumber) + " )")
            newWorkerProcess = Process(target=WorkerProcess, args=(processNumber,
                                                                  self.dataStreamIp,
                                                                  self.dataStreamPort,
                                                                  self.chunkSize,
                                                                  self.zmqCleanerIp,
                                                                  self.zmqCleanerPort))
            workerProcessList.append(newWorkerProcess)

            self.log.debug("start worker process nr " + str(processNumber))
            newWorkerProcess.start()

        #run loop, and wait for incoming messages
        continueReceiving = True
        self.log.debug("waiting for new fileEvent-messages")
        try:
            while continueReceiving:
                socks = dict(self.poller.poll())

                if self.fileEventSocket in socks and socks[self.fileEventSocket] == zmq.POLLIN:
                    try:
                        incomingMessage = self.fileEventSocket.recv()
                        self.log.debug("new fileEvent-message received.")
                        self.log.debug("message content: " + str(incomingMessage))
                        incomingMessageCounter += 1

                        self.log.debug("processFileEvent..." + str(incomingMessageCounter))
                        self.processFileEvent(incomingMessage)  #TODO refactor as separate process to emphasize unblocking
                        self.log.debug("processFileEvent...done")
                    except Exception, e:
                        self.log.error("Failed to receive new fileEvent-message.")
                        self.log.error(sys.exc_info())

                        #TODO might using a error-count and threshold when to stop receiving, e.g. after 100 misses?
                        # continueReceiving = False
                    continue

                if self.receiverComSocket in socks and socks[self.receiverComSocket] == zmq.POLLIN:
                    # signals are of the form [signal, hostname]
                    incomingMessage = self.receiverComSocket.recv()
                    self.log.debug("Recieved control command: %s" % incomingMessage )

                    signal         = None
                    signalHostname = None

                    try:
                        incomingMessage = incomingMessage.split(',')
                        signal          = incomingMessage[0]
                        signalHostname  = incomingMessage[1]
                    except Exception as e:
                        self.log.info("Received live viewer signal from host " + str(signalHostname) + " is of the wrong format")
                        self.receiverComSocket.send("NO_VALID_SIGNAL", zmq.NOBLOCK)
                        continue

                    self.log.debug("Check if signal sending host is in WhiteList...")
                    if signalHostname in self.receiverWhiteList:
                        self.log.info("Check if signal sending host is in WhiteList...Host " + str(signalHostname) + " is allowed to connect.")
                    else:
                        self.log.info("Check if signal sending host is in WhiteList...Host " + str(signalHostname) + " is not allowed to connect.")
                        self.log.debug("Signal from host " + str(signalHostname) + " is discarded.")
                        print "Signal from host " + str(signalHostname) + " is discarded."
                        self.receiverComSocket.send("NO_VALID_HOST", zmq.NOBLOCK)
                        continue


                    if signal == "STOP_LIVE_VIEWER":
                        self.log.info("Received live viewer stop signal from host " + str(signalHostname) + "...stopping live viewer")
                        print "Received live viewer stop signal from host " + signalHostname + "...stopping live viewer"
                        self.useLiveViewer = False
                        self.sendLiveViewerSignal(signal)
                        continue
                    elif signal == "START_LIVE_VIEWER":
                        self.log.info("Received live viewer start signal from host " + str(signalHostname) + "...starting live viewer")
                        print "Received live viewer start signal from host " + str(signalHostname) + "...starting live viewer"
                        self.useLiveViewer = True
                        self.sendLiveViewerSignal(signal)
                        continue
                    else:
                        self.log.info("Received live viewer signal from host " + str(signalHostname) + " unkown: " + str(signal))
                        self.receiverComSocket.send("NO_VALID_SIGNAL", zmq.NOBLOCK)

        except KeyboardInterrupt:
            self.log.info("Keyboard interuption detected. Stop receiving")



    def processFileEvent(self, fileEventMessage):
        self.log.debug("waiting for available workerProcess.")

        # address == "worker-0"
        # empty   == b''                   # as delimiter
        # ready   == b'READY'
        address, empty, ready = self.routerSocket.recv_multipart()
        self.log.debug("available workerProcess detected.")

        self.log.debug("passing job to workerProcess...")
        self.routerSocket.send_multipart([
                                     address,
                                     b'',
                                     fileEventMessage,
                                    ])

        self.log.debug("passing job to workerProcess...done.")


    def sendLiveViewerSignal(self, signal):
        numberOfWorkerProcesses = int(self.parallelDataStreams)
        for processNumber in range(numberOfWorkerProcesses):
            self.log.debug("send live viewer signal " + str(signal) + " to workerProcess (nr " + str(processNumber) + " )")

            address, empty, ready = self.routerSocket.recv_multipart()
            self.log.debug("available workerProcess detected.")

            # address == "worker-0"
            # empty   == b''                   # as delimiter
            # signal  == b'START_LIVE_VIEWER'
            self.routerSocket.send_multipart([
                                         address,
                                         b'',
                                         signal,
                                        ])
            self.receiverComSocket.send(signal, zmq.NOBLOCK)


    def stop(self):
        self.log.debug("Closing sockets")
        self.fileEventSocket.close(0)
        self.receiverComSocket.close(0)
        self.routerSocket.close(0)


def argumentParsing():
    defConf = defaultConfigSender()

    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"        , type=str , default=defConf.logfilePath        , help="path where logfile will be created (default=" + str(defConf.logfilePath) + ")")
    parser.add_argument("--logfileName"        , type=str , default=defConf.logfileName        , help="filename used for logging (default=" + str(defConf.logfileName) + ")")
    parser.add_argument("--verbose"            ,           action="store_true"                , help="more verbose output")

    parser.add_argument("--watchFolder"        , type=str , default=defConf.watchFolder        , help="folder you want to monitor for changes")
    parser.add_argument("--fileEventIp"        , type=str , default=defConf.fileEventIp        , help="zmq endpoint (IP-address) to send file events to (default=" + str(defConf.fileEventIp) + ")")
    parser.add_argument("--fileEventPort"      , type=str , default=defConf.fileEventPort      , help="zmq endpoint (port) to send file events to (default=" + str(defConf.fileEventPort) + ")")

    parser.add_argument("--dataStreamIp"       , type=str , default=defConf.dataStreamIp       , help="ip of dataStream-socket to push new files to (default=" + str(defConf.dataStreamIp) + ")")
    parser.add_argument("--dataStreamPort"     , type=str , default=defConf.dataStreamPort     , help="port number of dataStream-socket to push new files to (default=" + str(defConf.dataStreamPort) + ")")
    parser.add_argument("--cleanerTargetPath"  , type=str , default=defConf.cleanerTargetPath  , help="Target to move the files into (default=" + str(defConf.cleanerTargetPath) + ")")
    parser.add_argument("--zmqCleanerIp"       , type=str , default=defConf.zmqCleanerIp       , help="zmq-pull-socket ip which deletes/moves given files (default=" + str(defConf.zmqCleanerIp) + ")")
    parser.add_argument("--zmqCleanerPort"     , type=str , default=defConf.zmqCleanerPort     , help="zmq-pull-socket port which deletes/moves given files (default=" + str(defConf.zmqCleanerPort) + ")")
    parser.add_argument("--receiverComPort"    , type=str , default=defConf.receiverComPort    , help="port number of dataStream-socket to receive signals from the receiver (default=" + str(defConf.receiverComPort) + ")")
    parser.add_argument("--receiverWhiteList"  , nargs='+', default=defConf.receiverWhiteList  , help="names of the hosts allowed to receive data (default=" + str(defConf.receiverWhiteList) + ")")

    parser.add_argument("--parallelDataStreams", type=int , default=defConf.parallelDataStreams, help="number of parallel data streams (default=" + str(defConf.parallelDataStreams) + ")")
    parser.add_argument("--chunkSize"          , type=int , default=defConf.chunkSize          , help="chunk size of file-parts getting send via zmq (default=" + str(defConf.chunkSize) + ")")

    arguments = parser.parse_args()

    watchFolder = str(arguments.watchFolder)
    logfilePath = str(arguments.logfilePath)
    logfileName = str(arguments.logfileName)

    #check watchFolder for existance
    helperScript.checkFolderExistance(watchFolder)

    #check logfile-path for existance
    helperScript.checkFolderExistance(logfilePath)

    #error if logfile cannot be written
    helperScript.checkLogFileWritable(logfilePath, logfileName)

    return arguments



if __name__ == '__main__':
    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows
    arguments = argumentParsing()

    logfilePath         = str(arguments.logfilePath)
    logfileName         = str(arguments.logfileName)
    logfileFullPath     = os.path.join(logfilePath, logfileName)
    verbose             = arguments.verbose

    watchFolder         = str(arguments.watchFolder)
    fileEventIp         = str(arguments.fileEventIp)
    fileEventPort       = str(arguments.fileEventPort)

    dataStreamIp        = str(arguments.dataStreamIp)
    dataStreamPort      = str(arguments.dataStreamPort)
    cleanerTargetPath   = str(arguments.cleanerTargetPath)
    zmqCleanerIp        = str(arguments.zmqCleanerIp)
    zmqCleanerPort      = str(arguments.zmqCleanerPort)
    receiverComPort     = str(arguments.receiverComPort)
    receiverWhiteList   = arguments.receiverWhiteList

    parallelDataStreams = str(arguments.parallelDataStreams)
    chunkSize           = int(arguments.chunkSize)


    #enable logging
    helperScript.initLogging(logfileFullPath, verbose)


    #create zmq context
    # there should be only one context in one process
    zmqContext = zmq.Context.instance()
    logging.info("registering zmq global context")

    logging.debug("start watcher process...")
    watcherProcess = Process(target=DirectoryWatcher, args=(fileEventIp, watchFolder, fileEventPort, zmqContext))
    logging.debug("watcher process registered")
    watcherProcess.start()
    logging.debug("start watcher process...done")

    logging.debug("start cleaner process...")
    cleanerProcess = Process(target=Cleaner, args=(cleanerTargetPath, zmqCleanerIp, zmqCleanerPort, zmqContext))
    logging.debug("cleaner process registered")
    cleanerProcess.start()
    logging.debug("start cleaner process...done")

    #start new fileMover
    fileMover = FileMover(fileEventIp, fileEventPort, dataStreamIp, dataStreamPort,
                          receiverComPort, receiverWhiteList,
                          parallelDataStreams, chunkSize,
                          zmqCleanerIp, zmqCleanerPort,
                          zmqContext)
    try:
        fileMover.process()
    except KeyboardInterrupt:
        logging.info("Keyboard interruption detected. Shutting down")
    # except Exception, e:
    #     print "unknown exception detected."


    logging.debug("shutting down zeromq...")
    try:
        fileMover.stop()
        logging.debug("shutting down zeromq...done.")
    except:
        logging.error(sys.exc_info())
        logging.error("shutting down zeromq...failed.")

    # give the other processes time to close the sockets
    time.sleep(0.1)
    try:
        logging.debug("closing zmqContext...")
        zmqContext.destroy()
        logging.debug("closing zmqContext...done.")
    except:
        logging.debug("closing zmqContext...failed.")
        logging.error(sys.exc_info())

