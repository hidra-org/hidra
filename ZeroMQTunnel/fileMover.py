from __builtin__ import open, type

__author__ = 'Marco Strutz <marco.strutz@desy.de>', 'Manuela Kuhn <manuela.kuhn@desy.de>'


import time
import argparse
import zmq
import os
import logging
import sys
import json
import traceback
from multiprocessing import Process, freeze_support
import subprocess
import json
import shutil

DEFAULT_CHUNK_SIZE = 1048576
#
#  --------------------------  class: WorkerProcess  --------------------------------------
#
class WorkerProcess():
    id                   = None
    dataStreamIp         = None
    dataStreamPort       = None
    logfileFullPath      = None
    zmqContextForWorker  = None
    zmqMessageChunkSize  = None
    fileWaitTime_inMs    = None
    fileMaxWaitTime_InMs = None
    zmqCleanerIp        = None              # responsable to delete files
    zmqCleanerPort      = None              # responsable to delete files
    zmqDataStreamSocket  = None

    def __init__(self, id, dataStreamIp, dataStreamPort, logfileFullPath, chunkSize, zmqCleanerIp, zmqCleanerPort,
                 fileWaitTimeInMs=2000.0, fileMaxWaitTimeInMs=10000.0):
        self.id                   = id
        self.dataStreamIp         = dataStreamIp
        self.dataStreamPort       = dataStreamPort
        self.logfileFullPath      = logfileFullPath
        self.zmqMessageChunkSize  = chunkSize
        self.zmqCleanerIp         = zmqCleanerIp
        self.zmqCleanerPort       = zmqCleanerPort
        self.fileWaitTime_inMs    = fileWaitTimeInMs
        self.fileMaxWaitTime_InMs = fileMaxWaitTimeInMs

        self.initLogging(logfileFullPath)

        try:
            self.process()
        except KeyboardInterrupt:
            # trace = traceback.format_exc()
            logging.debug("KeyboardInterrupt detected. Shutting down workerProcess.")
#            self.zmqDataStreamSocket.close()
#            self.zmqContextForWorker.destroy()
        else:
            trace = traceback.format_exc()
            logging.error("Stopping workerProcess due to unknown error condition.")
            logging.debug("Error was: " + str(trace))


    def process(self):
        """
          sends a 'ready' to a broker and receives a 'job' to process.
          The 'job' will be to pass the file of an fileEvent to the
          dataPipe.

          Why?
          -> the simulated "onClosed" event waits for a file for being
           not modified within a certain period of time.
           Instead of processing file after file the work will be
           spreaded to many workerThreads. So each thread can wait
           individual periods of time for a file without blocking
           new file events - as new file events will be handled by
           another workerThread.
        """

        """
          takes the fileEventMessage, reading and passing the new file to
          a separate data-messagePipe. Afterwards the original file
          will be removed.
        """
        id             = self.id
        dataStreamIp   = self.dataStreamIp
        dataStreamPort = self.dataStreamPort

        logging.debug("new workerThread started. id=" + str(id))

        #initialize router
        zmqContextForWorker = zmq.Context()
        self.zmqContextForWorker = zmqContextForWorker

        self.zmqDataStreamSocket = zmqContextForWorker.socket(zmq.PUSH)
        connectionStrDataStreamSocket = "tcp://{ip}:{port}".format(ip=dataStreamIp, port=dataStreamPort)
        self.zmqDataStreamSocket.connect(connectionStrDataStreamSocket)

        routerSocket = zmqContextForWorker.socket(zmq.REQ)
        routerSocket.identity = u"worker-{ID}".format(ID=id).encode("ascii")
        connectionStrRouterSocket = "tcp://{ip}:{port}".format(ip="127.0.0.1", port="50000")
        routerSocket.connect(connectionStrRouterSocket)
        processingJobs = True
        jobCount = 0

        #init Cleaner message-pipe
        cleanerSocket = zmqContextForWorker.socket(zmq.PUSH)
        connectionStringCleanerSocket = "tcp://{ip}:{port}".format(ip=self.zmqCleanerIp, port=self.zmqCleanerPort)
        cleanerSocket.connect(connectionStringCleanerSocket)


        while processingJobs:
            #sending a "ready"-signal to the router.
            #the reply will contain the actual job/task.
            logging.debug("worker-"+str(id)+": sending ready signal")

            routerSocket.send(b"READY")

            # Get workload from router, until finished
            logging.debug("worker-"+str(id)+": waiting for new job")
            workload = routerSocket.recv()
            logging.debug("worker-"+str(id)+": new job received")
            finished = workload == b"END"
            if finished:
                processingJobs = False
                logging.debug("router requested to shutdown worker-thread. Worker processed: %d files" % jobCount)
                break
            jobCount += 1

            #convert fileEventMessage back to a dictionary
            fileEventMessageDict = None
            try:
                fileEventMessageDict = json.loads(str(workload))
                logging.debug("str(messageDict) = " + str(fileEventMessageDict) + "  type(messageDict) = " + str(type(fileEventMessageDict)))
            except Exception, e:
                errorMessage = "Unable to convert message into a dictionary."
                logging.error(errorMessage)
                logging.debug("Error was: " + str(e))


            #extract fileEvent metadata
            try:
                #TODO validate fileEventMessageDict dict
                filename       = fileEventMessageDict["filename"]
                sourcePath     = fileEventMessageDict["sourcePath"]
                relativeParent = fileEventMessageDict["relativeParent"]
            except Exception, e:
                errorMessage   = "Invalid fileEvent message received."
                logging.error(errorMessage)
                logging.debug("Error was: " + str(e))
                logging.debug("fileEventMessageDict=" + str(fileEventMessageDict))
                #skip all further instructions and continue with next iteration
                continue

            #passing file to data-messagPipe
            try:
                logging.debug("worker-" + str(id) + ": passing new file to data-messagePipe...")
                self.passFileToDataStream(filename, sourcePath, relativeParent)
                logging.debug("worker-" + str(id) + ": passing new file to data-messagePipe...success.")
            except Exception, e:
                errorMessage = "Unable to pass new file to data-messagePipe."
                logging.error(errorMessage)
                logging.error("Error was: " + str(e))
                logging.debug("worker-"+str(id) + ": passing new file to data-messagePipe...failed.")
                #skip all further instructions and continue with next iteration
                continue


            #send remove-request to message pipe
            try:
                #sending to pipe
                logging.debug("send file-event to cleaner-pipe...")
                cleanerSocket.send(workload)
                logging.debug("send file-event to cleaner-pipe...success.")

                #TODO: remember workload. append to list?
                # can be used to verify files which have been processed twice or more
            except Exception, e:
                errorMessage = "Unable to notify Cleaner-pipe to delete file: " + str(filename)
                logging.error(errorMessage)
                logging.debug("fileEventMessageDict=" + str(fileEventMessageDict))




    def getFileWaitTimeInMs(self):
        waitTime = 2000.0
        return waitTime


    def getFileMaxWaitTimeInMs(self):
        maxWaitTime = 10000.0
        return maxWaitTime


    def passFileToDataStream(self, filename, sourcePath, relativeParent):
        """filesizeRequested == filesize submitted by file-event. In theory it can differ to real file size"""

        # filename = "img.tiff"
        # filepath = "C:\dir"
        #
        # -->  sourceFilePathFull = 'C:\\dir\img.tiff'
        sourceFilePathFull = os.path.join(sourcePath, filename)

        #reading source file into memory
        try:
            #wait x seconds if file was modified within past y seconds
            fileWaitTimeInMs    = self.getFileWaitTimeInMs()
            fileMaxWaitTimeInMs = self.getFileMaxWaitTimeInMs()
            fileIsStillInUse = True #true == still being written to file by a process
            timeStartWaiting = time.time()
            while fileIsStillInUse:
                #skip waiting periode if waiting to long for file to get closed
                if time.time() - timeStartWaiting >= (fileMaxWaitTimeInMs / 1000):
                    logging.debug("waited to long for file getting closed. aborting")
                    break

                #wait for other process to finish file access
                #grabs time when file was modified last
                statInfo = os.stat(sourceFilePathFull)
                fileLastModified = statInfo.st_mtime
                logging.debug("'" + str(sourceFilePathFull) + "' modified last: " + str(fileLastModified))
                timeNow = time.time()
                timeDiff = timeNow - fileLastModified
                logging.debug("timeNow=" + str(timeNow) + "  timeDiff=" + str(timeDiff))
                waitTimeInSeconds = fileWaitTimeInMs/1000
                if timeDiff >= waitTimeInSeconds:
                    fileIsStillInUse = False
                    logging.debug("File was not modified within past " + str(fileWaitTimeInMs) + "ms.")
                else:
                    logging.debug("still waiting for file to get closed...")
                    time.sleep(fileWaitTimeInMs / 1000 )

            #for quick testing set filesize of file as chunksize
            logging.debug("get filesize for '" + str(sourceFilePathFull) + "'...")
            filesize             = os.path.getsize(sourceFilePathFull)
            fileModificationTime = os.stat(sourceFilePathFull).st_mtime
            chunksize            = filesize    #can be used later on to split multipart message
            logging.debug("filesize(%s) = %s" % (sourceFilePathFull, str(filesize)))
            logging.debug("fileModificationTime(%s) = %s" % (sourceFilePathFull, str(fileModificationTime)))

        except Exception, e:
            errorMessage = "Unable to get file metadata for '" + str(sourceFilePathFull) + "'."
            logging.error(errorMessage)
            logging.debug("Error was: " + str(e))
            raise Exception(e)

        try:
            logging.debug("opening '" + str(sourceFilePathFull) + "'...")
            fileDescriptor = open(str(sourceFilePathFull), "rb")

        except Exception, e:
            errorMessage = "Unable to read source file '" + str(sourceFilePathFull) + "'."
            logging.error(errorMessage)
            logging.debug("Error was: " + str(e))
            raise Exception(e)


        #build payload for message-pipe by putting source-file into a message
        try:
            payloadMetadata = self.buildPayloadMetadata(filename, filesize, fileModificationTime, sourcePath, relativeParent)
        except Exception, e:
            errorMessage = "Unable to assemble multi-part message."
            logging.error(errorMessage)
            logging.debug("Error was: " + str(e))
            raise Exception(e)


        #send message
        try:
            logging.debug("Passing multipart-message...")
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

                #assemble metadata for zmq-message
                chunkPayloadMetadata = payloadMetadata.copy()
                chunkPayloadMetadata["chunkNumber"] = chunkNumber
                chunkPayloadMetadataJson = json.dumps(chunkPayloadMetadata)
                chunkPayload = []
                chunkPayload.append(chunkPayloadMetadataJson)
                chunkPayload.append(fileContentAsByteObject)

                #send to zmq pipe
                self.zmqDataStreamSocket.send_multipart(chunkPayload)

            #close file
            fileDescriptor.close()

            # self.zmqDataStreamSocket.send_multipart(multipartMessage)
            logging.debug("Passing multipart-message...done.")
        except Exception, e:
            logging.error("Unable to send multipart-message")
            logging.debug("Error was: " + str(e))
            logging.info("Passing multipart-message...failed.")
            raise Exception(e)



    def appendFileChunksToPayload(self, payload, sourceFilePathFull, fileDescriptor, chunkSize):
        try:
            # chunksize = 16777216 #16MB
            logging.debug("reading file '" + str(sourceFilePathFull)+ "' to memory")

            # FIXME: chunk is read-out as str. why not as bin? will probably add to much overhead to zmq-message
            fileContentAsByteObject = fileDescriptor.read(chunkSize)

            while fileContentAsByteObject != "":
                payload.append(fileContentAsByteObject)
                fileContentAsByteObject = fileDescriptor.read(chunkSize)
        except Exception, e:
            raise Exception(str(e))



    def buildPayloadMetadata(self, filename, filesize, fileModificationTime, sourcePath, relativeParent):
        """
        builds metadata for zmq-multipart-message. should be used as first element for payload.
        :param filename:
        :param filesize:
        :param fileModificationTime:
        :param sourcePath:
        :param relativeParent:
        :return:
        """

        #add metadata to multipart
        logging.debug("create metadata for source file...")
        metadataDict = {
                         "filename"             : filename,
                         "filesize"             : filesize,
                         "fileModificationTime" : fileModificationTime,
                         "sourcePath"           : sourcePath,
                         "relativeParent"       : relativeParent,
                         "chunkSize"            : self.getChunkSize()}

        logging.debug("metadataDict = " + str(metadataDict))


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

        logging.debug("vfsstat: freeSpaceAvailableForUser=" + str(freeSpaceAvailableForUser_gigabytes)+ " Gigabytes "
                       + " (" + str(int(freeUserSpaceLeft_percent)) + "% free disk space left)")

        #warn if disk space is running low
        highWaterMark = 85
        if int(freeUserSpaceLeft_percent) >= int(highWaterMark):
            logging.warning("Running low in disk space! " + str(int(freeUserSpaceLeft_percent)) + "% free disk space left.")


    def initLogging(self, filenameFullPath):
        #@see https://docs.python.org/2/howto/logging-cookbook.html

        #log everything to file
        logging.basicConfig(level=logging.DEBUG,
                            format='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s] [%(name)s] [%(levelname)s] %(message)s',
                            datefmt='%Y-%m-%d_%H:%M',
                            filename=filenameFullPath,
                            filemode="a")

        #log info to stdout, display messages with different format than the file output
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s >  %(message)s")
        console.setFormatter(formatter)
        logging.getLogger("").addHandler(console)



#
#  --------------------------  class: FileMover  --------------------------------------
#
class FileMover():
    patterns              = ["*"]
    fileList_newFiles     = list()
    fileCount_newFiles    = 0
    zmqContext            = None
    messageSocket         = None         # to receiver fileMove-jobs as json-encoded dictionary
    dataSocket            = None         # to send fileObject as multipart message
    bindingIpForSocket    = None
    zqmFileEventServerIp  = "127.0.0.1"  # serverIp for incoming messages
    tcpPort_messageStream = "6060"
    dataStreamIp          = "127.0.0.1"  # ip of dataStream-socket to push new files to
    dataStreamPort        = "6061"       # port number of dataStream-socket to push new files to
    zmqCleanerIp          = "127.0.0.1"  # zmq pull endpoint, responsable to delete files
    zmqCleanerPort        = "6062"       # zmq pull endpoint, responsable to delete files
    fileWaitTimeInMs      = None
    fileMaxWaitTimeInMs   = None
    pipe_name             = "/tmp/zeromqllpipe_resp"

    currentZmqDataStreamSocketListIndex = None # Index-Number of a socket used to send datafiles to
    logfileFullPath = None
    chunkSize = None


    def process(self):
        try:
            self.startReceiving()
        except zmq.error.ZMQError as e:
            logging.error("ZMQError: "+ str(e))
            log.debug("Shutting down workerProcess.")
        except KeyboardInterrupt:
            logging.debug("KeyboardInterrupt detected. Shutting down fileMover.")
            logging.info("Shutting down fileMover as KeyboardInterrupt was detected.")
            self.zmqContext.destroy()
        except:
            trace = traceback.format_exc()
            logging.info("Stopping fileMover due to unknown error condition.")
            logging.debug("Error was: " + str(trace))



    def __init__(self, bindingIpForSocket, bindingPortForSocket, dataStreamIp, dataStreamPort, parallelDataStreams,
                 logfileFullPath, chunkSize, zmqCleanerIp, zmqCleanerPort,
                 fileWaitTimeInMs, fileMaxWaitTimeInMs):
        logging.info("registering zmq global context")

        #create zmq context
        zmqContext = zmq.Context()

        self.zmqContext            = zmqContext
        self.bindingIpForSocket    = bindingIpForSocket
        self.tcpPort_messageStream = bindingPortForSocket
        self.dataStreamIp          = dataStreamIp
        self.dataStreamPort        = dataStreamPort
        self.parallelDataStreams   = parallelDataStreams
        self.logfileFullPath       = logfileFullPath
        self.chunkSize             = chunkSize
        self.zmqCleanerIp          = zmqCleanerIp
        self.zmqCleanerPort        = zmqCleanerPort
        self.fileWaitTimeInMs      = fileWaitTimeInMs
        self.fileMaxWaitTimeInMs   = fileMaxWaitTimeInMs


        #create zmq sockets. one for incoming file events, one for passing fileObjects to
        self.messageSocket = self.getZmqSocket_Pull(self.zmqContext)
        self.dataSocket    = self.getZmqSocket_Push(self.zmqContext)


    def getFileWaitTimeInMs(self):
        return self.fileWaitTimeInMs


    def getFileMaxWaitTimeInMs(self):
        return self.fileMaxWaitTimeInMs


    def startReceiving(self):
        #create socket
        zmqContext = self.zmqContext
        zmqSocketForNewFileEvents  = self.createPullSocket()
        logging.debug("new message-socket crated for: new file events.")
        parallelDataStreams = int(self.parallelDataStreams)

        logging.debug("new message-socket crated for: passing file objects.")

        incomingMessageCounter = 0


        #setting up router for load-balancing worker-threads.
        #each worker-thread will handle a file event
        routerSocket = self.zmqContext.socket(zmq.ROUTER)
        routerSocket.bind("tcp://127.0.0.1:50000")
        logging.debug("routerSocket started for 'tcp://127.0.0.1:50000'")


        #start worker-threads. each will have its own PushSocket.
        workerThreadList      = list()
        numberOfWorkerThreads = parallelDataStreams
        fileWaitTimeInMs      = self.getFileWaitTimeInMs()
        fileMaxWaitTimeInMs   = self.getFileMaxWaitTimeInMs()
        for threadNumber in range(numberOfWorkerThreads):
            logging.debug("instantiate new workerProcess (nr " + str(threadNumber))
            newWorkerThread = Process(target=WorkerProcess, args=(threadNumber,
                                                                  self.dataStreamIp,
                                                                  self.dataStreamPort,
                                                                  logfileFullPath,
                                                                  self.chunkSize,
                                                                  zmqCleanerIp,
                                                                  zmqCleanerPort,
                                                                  fileWaitTimeInMs,
                                                                  fileMaxWaitTimeInMs))
            workerThreadList.append(newWorkerThread)

            logging.debug("start worker process nr " + str(threadNumber))
            newWorkerThread.start()

        #run loop, and wait for incoming messages
        continueReceiving = True
        logging.debug("waiting for new fileEvent-messages")
        while continueReceiving:
            try:
                incomingMessage = zmqSocketForNewFileEvents.recv()
                logging.debug("new fileEvent-message received.")
                logging.debug("message content: " + str(incomingMessage))
                incomingMessageCounter += 1

                logging.debug("processFileEvent...")
                self.processFileEvent(incomingMessage, routerSocket)  #TODO refactor as separate process to emphasize unblocking
                logging.debug("processFileEvent...done")
            except Exception, e:
                print "exception"
                logging.error("Failed to receive new fileEvent-message.")
                logging.error(sys.exc_info())

                #TODO might using a error-count and threshold when to stop receiving, e.g. after 100 misses?
                # continueReceiving = False


        print "shutting down fileEvent-receiver..."
        try:
            logging.debug("shutting down zeromq...")
            self.stopReceiving(zmqSocketForNewFileEvents, zmqContext)
            logging.debug("shutting down zeromq...done.")
        except:
            logging.error(sys.exc_info())
            logging.error("shutting down zeromq...failed.")



    def routeFileEventToWorkerThread(self, fileEventMessage, routerSocket):
        # LRU worker is next waiting in the queue
        logging.debug("waiting for available workerThread.")

        # address == "worker-0"
        # empty   == ""
        # ready   == "READY"
        address, empty, ready = routerSocket.recv_multipart()
        logging.debug("available workerThread detected.")

        logging.debug("passing job to workerThread...")
        routerSocket.send_multipart([
                                     address,
                                     b'',
                                     fileEventMessage,
                                    ])

        # inform lsyncd wrapper that the file can be moved

#        if not os.path.exists(self.pipe_name):
#                os.mkfifo(self.pipe_name)


#        messageToPipe = json.loads ( fileEventMessage )
#        messageToPipe = messageToPipe["sourcePath"] + os.sep + messageToPipe["filename"]
#        my_cmd = 'echo "' + messageToPipe + '"  > ' + self.pipe_name
#        p = subprocess.Popen ( my_cmd, shell=True )
#        p.communicate()

        logging.debug("passing job to workerThread...done.")


    def processFileEvent(self, fileEventMessage, routerSocket):
        self.routeFileEventToWorkerThread(fileEventMessage, routerSocket)


    def stopReceiving(self, zmqSocket, msgContext):
        try:
            logging.debug("closing zmqSocket...")
            zmqSocket.close()
            logging.debug("closing zmqSocket...done.")
        except:
            logging.debug("closing zmqSocket...failed.")
            logging.error(sys.exc_info())

        try:
            logging.debug("closing zmqContext...")
            msgContext.destroy()
            logging.debug("closing zmqContext...done.")
        except:
            logging.debug("closing zmqContext...failed.")
            logging.error(sys.exc_info())


    def getZmqSocket_Pull(self, context):
        pattern_pull = zmq.PULL
        assert isinstance(context, zmq.sugar.context.Context)
        socket = context.socket(pattern_pull)

        return socket


    def getZmqSocket_Push(self, context):
        pattern = zmq.PUSH
        assert isinstance(context, zmq.sugar.context.Context)
        socket = context.socket(pattern)

        return socket



    def createPullSocket(self):
        #get default message-socket
        socket = self.messageSocket

        logging.info("binding to message socket: tcp://" + self.bindingIpForSocket + ":%s" % self.tcpPort_messageStream)
        socket.bind('tcp://' + self.bindingIpForSocket + ':%s' % self.tcpPort_messageStream)
        return socket



class Cleaner():
    """
    * received cleaning jobs via zeromq,
      such as removing a file
    * Does regular checks on the watched directory,
    such as
      - deleting files which have been successfully send
        to target but still remain in the watched directory
      - poll the watched directory and reissue new files
        to fileMover which have not been detected yet
    """
    logfileFullPath      = None
    bindingPortForSocket = None
    bindingIpForSocket   = None
    zmqContextForCleaner = None

    def __init__(self, logfilePath, context, bindingIp="127.0.0.1", bindingPort="6062", verbose=False):
        self.bindingPortForSocket = bindingPort
        self.bindingIpForSocket   = bindingIp
        self.initLogging(logfilePath, verbose)
        log = self.getLogger()
        try:
            self.process()
        except zmq.error.ZMQError:
            logging.error("ZMQError: "+ str(e))
            log.debug("Shutting down workerProcess.")
            self.zmqContextForCleaner.destroy()
        except KeyboardInterrupt:
            log.debug("KeyboardInterrupt detected. Shutting down workerProcess.")
            self.zmqContextForCleaner.destroy()
        except:
            trace = traceback.format_exc()
            log.error("Stopping cleanerProcess due to unknown error condition.")
            log.debug("Error was: " + str(trace))


    def getLogger(self):
        logger = logging.getLogger("cleaner")
        return logger

    def initLogging(self, logfilePath, verbose):
        #@see https://docs.python.org/2/howto/logging-cookbook.html

        logfilePathFull = os.path.join(logfilePath, "cleaner.log")
        logger = logging.getLogger("cleaner")

        #more detailed logging if verbose-option has been set
        loggingLevel = logging.INFO
        if verbose:
            loggingLevel = logging.DEBUG


        #log everything to file
        fileHandler = logging.FileHandler(filename=logfilePathFull,
                                          mode="a")
        fileHandlerFormat = logging.Formatter(datefmt='%Y-%m-%d_%H:%M:%S',
                                              fmt='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s] [%(name)s] [%(levelname)s] %(message)s')
        fileHandler.setFormatter(fileHandlerFormat)
        fileHandler.setLevel(loggingLevel)
        logger.addHandler(fileHandler)



    def process(self):
        processingJobs = True
        log = self.getLogger()

        #create zmq context
        zmqContextForCleaner = zmq.Context()
        self.zmqContextForCleaner = zmqContextForCleaner

        #bind to local port
        zmqJobSocket = zmqContextForCleaner.socket(zmq.PULL)
        zmqJobSocket.bind('tcp://' + self.bindingIpForSocket + ':%s' % self.bindingPortForSocket)

        #processing messaging
        while processingJobs:
            #waiting for new jobs
            try:
                workload = zmqJobSocket.recv()
            except Exception as e:
                logging.error("Error in receiving job: " + str(e))


            #transform to dictionary
            try:
                workloadDict = json.loads(str(workload))
            except:
                errorMessage = "invalid job received. skipping job"
                log.error(errorMessage)
                log.debug("workload=" + str(workload))
                continue

            #extract fileEvent metadata
            try:
                #TODO validate fileEventMessageDict dict
                filename       = workloadDict["filename"]
                sourcePath     = workloadDict["sourcePath"]
                relativeParent = workloadDict["relativeParent"]
                targetPath     = workloadDict["targetPath"]
                # filesize       = workloadDict["filesize"]
            except Exception, e:
                errorMessage   = "Invalid fileEvent message received."
                log.error(errorMessage)
                log.debug("Error was: " + str(e))
                log.debug("workloadDict=" + str(workloadDict))
                #skip all further instructions and continue with next iteration
                continue

            #moving source file
            sourceFilepath = None
            try:
                logging.debug("removing source file...")
                #generate target filepath
#                sourceFilepath = os.path.join(sourcePath,filename)
                self.moveFile(sourceFilepath)
                self.moveFile(sourcePath, filename, target)

                # #show filesystem statistics
                # try:
                #     self.showFilesystemStatistics(sourcePath)
                # except Exception, f:
                #     logging.warning("Unable to get filesystem statistics")
                #     logging.debug("Error was: " + str(f))
                log.debug("file removed: " + str(sourceFilepath))
                log.debug("removing source file...success.")

            except Exception, e:
                errorMessage = "Unable to remove source file."
                log.error(errorMessage)
                trace = traceback.format_exc()
                log.error("Error was: " + str(trace))
                log.debug("sourceFilepath="+str(sourceFilepath))
                log.debug("removing source file...failed.")
                #skip all further instructions and continue with next iteration
                continue



    def moveFile(self, source, filename, target):
        log = self.getLogger()
        maxAttemptsToRemoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        log.info("Moving file '" + str(filename) + "' from '" +  str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
        fileWasMoved = False

        while iterationCount <= maxAttemptsToRemoveFile and not fileWasMoved:
            iterationCount+=1
            try:
                # check if the directory exists before moving the file
                if not os.path.exists(target):
                    try:
                        os.makedirs(target)
                    except OSError:
                        pass
                # moving the file
                os.remove(filepath)
                shutil.move(source + os.sep + filename, target + os.sep + filename)
                fileWasMoved = True
                log.debug("Moving file '" + str(filename) + "' from '" + str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to move file {FILE}.".format(FILE=str(source) + str(filename))
                log.warning(warningMessage)
                log.debug("trace=" + str(trace))
                log.warning("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))


        if not fileWasMoved:
            log.error("Moving file '" + str(filename) + " from " + str(source) + " to " + str(target) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToMoveFile reached (value={ATTEMPT}). Unable to move file '{FILE}'.".format(ATTEMPT=str(iterationCount),
                                                                                                                            FILE=filepath))


    def removeFile(self, filepath):
        log = self.getLogger()
        maxAttemptsToRemoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        log.info("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...")
        fileWasRemoved = False

        while iterationCount <= maxAttemptsToRemoveFile and not fileWasRemoved:
            iterationCount+=1
            try:
                os.remove(filepath)
                fileWasRemoved = True
                log.debug("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...success.")
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to remove file {FILE}.".format(FILE=str(filepath))
                log.warning(warningMessage)
                log.debug("trace=" + str(trace))
                log.warning("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))


        if not fileWasRemoved:
            log.error("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToRemoveFile reached (value={ATTEMPT}). Unable to remove file '{FILE}'.".format(ATTEMPT=str(iterationCount),
                                                                                                                            FILE=filepath))


def argumentParsing():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"   , type=str, help="path where logfile will be created", default="/tmp/log/")
    parser.add_argument("--logfileName"   , type=str, help="filename used for logging", default="fileMover.log")
    parser.add_argument("--bindingIpForSocket"  , type=str, help="local ip to bind to", default="127.0.0.1")
    parser.add_argument("--bindingPortForSocket", type=str, help="local port to bind to", default="6060")
    parser.add_argument("--dataStreamIp"  , type=str, help="ip of dataStream-socket to push new files to", default="127.0.0.1")
    parser.add_argument("--dataStreamPort", type=str, help="port number of dataStream-socket to push new files to", default="6061")
    parser.add_argument("--zmqCleanerIp"  , type=str, help="zmq-pull-socket which deletes given files", default="127.0.0.1")
    parser.add_argument("--zmqCleanerPort", type=str, help="zmq-pull-socket which deletes given files", default="6063")
    parser.add_argument("--parallelDataStreams", type=int, help="number of parallel data streams. default is 1", default="1")
    parser.add_argument("--chunkSize",      type=int, help="chunk size of file-parts getting send via zmq", default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--verbose"       ,           help="more verbose output", action="store_true")

    parser.add_argument("--fileWaitTimeInMs",    type=int, help=argparse.SUPPRESS, default=2000)
    parser.add_argument("--fileMaxWaitTimeInMs", type=int, help=argparse.SUPPRESS, default=10000)


    arguments = parser.parse_args()

    return arguments




def checkFolderForExistance(watchFolderPath):
    """
    abort if watch-folder does not exist

    :return:
    """

    #check folder path for existance. exits if it does not exist
    if not os.path.exists(watchFolderPath):
        logging.error("WatchFolder '%s' does not exist. Abort." % str(watchFolderPath))
        sys.exit(1)



def checkLogfileFolder(logfilePath):
    """
    abort if watch-folder does not exist

    :return:
    """

    #check folder path for existance. exits if it does not exist
    if not os.path.exists(logfilePath):
        logging.error("LogfileFilder '%s' does not exist. Abort." % str(logfilePath))
        sys.exit(1)


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


if __name__ == '__main__':
    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows
    arguments = argumentParsing()
    logFile = str(arguments.logfilePath) + "/" + str(arguments.logfileName)

    bindingIpForSocket   = str(arguments.bindingIpForSocket)
    bindingPortForSocket = str(arguments.bindingPortForSocket)
    dataStreamIp         = str(arguments.dataStreamIp)
    dataStreamPort       = str(arguments.dataStreamPort)
    logfilePath          = str(arguments.logfilePath)
    logfileName          = str(arguments.logfileName)
    parallelDataStreams  = str(arguments.parallelDataStreams)
    zmqCleanerIp         = str(arguments.zmqCleanerIp)
    zmqCleanerPort       = str(arguments.zmqCleanerPort)
    chunkSize            = arguments.chunkSize
    verbose              = arguments.verbose
    logfileFullPath      = os.path.join(logfilePath, logfileName)
    fileWaitTimeInMs     = float(arguments.fileWaitTimeInMs)
    fileMaxWaitTimeInMs  = float(arguments.fileMaxWaitTimeInMs)


    cleanerThread = Process(target=Cleaner, args=(logfilePath, zmqCleanerIp, zmqCleanerPort))
    logging.debug("cleaner thread started")
    cleanerThread.start()

    #enable logging
    initLogging(logfileFullPath, verbose)


    #start new fileMover
    # try:
    fileMover = FileMover(bindingIpForSocket, bindingPortForSocket, dataStreamIp, dataStreamPort,
                          parallelDataStreams, logfileFullPath, chunkSize,
                          zmqCleanerIp, zmqCleanerPort,
                          fileWaitTimeInMs, fileMaxWaitTimeInMs)
    fileMover.process()
    # except KeyboardInterrupt, ke:
    #     print "keyboardInterrupt detected."
    # except Exception, e:
    #     print "unknown exception detected."



