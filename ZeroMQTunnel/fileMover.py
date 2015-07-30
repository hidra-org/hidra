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
import helperScript


DEFAULT_CHUNK_SIZE = 1048576


#
#  --------------------------  class: WorkerProcess  --------------------------------------
#
class WorkerProcess():
    id                   = None
    dataStreamIp         = None
    dataStreamPort       = None
    zmqContextForWorker  = None
    zmqMessageChunkSize  = None
    fileWaitTime_inMs    = None
    fileMaxWaitTime_InMs = None
    zmqCleanerIp         = None              # responsable to delete files
    zmqCleanerPort       = None              # responsable to delete files
    zmqDataStreamSocket  = None
    routerSocket         = None
    cleanerSocket        = None

    # to get the logging only handling this class
    log                   = None

    def __init__(self, id, dataStreamIp, dataStreamPort, chunkSize, zmqCleanerIp, zmqCleanerPort,
                 fileWaitTimeInMs=2000.0, fileMaxWaitTimeInMs=10000.0,
                 context = None):
        self.id                   = id
        self.dataStreamIp         = dataStreamIp
        self.dataStreamPort       = dataStreamPort
        self.zmqMessageChunkSize  = chunkSize
        self.zmqCleanerIp         = zmqCleanerIp
        self.zmqCleanerPort       = zmqCleanerPort
        self.fileWaitTime_inMs    = fileWaitTimeInMs
        self.fileMaxWaitTime_InMs = fileMaxWaitTimeInMs

        #initialize router
        self.zmqContextForWorker = context or zmq.Context()

        self.log = self.getLogger()


        dataStreamIp   = self.dataStreamIp
        dataStreamPort = self.dataStreamPort

        self.log.debug("new workerThread started. id=" + str(self.id))

        # initialize sockets
        self.zmqDataStreamSocket      = self.zmqContextForWorker.socket(zmq.PUSH)
        connectionStrDataStreamSocket = "tcp://{ip}:{port}".format(ip=dataStreamIp, port=self.dataStreamPort)
        self.zmqDataStreamSocket.connect(connectionStrDataStreamSocket)
        self.log.debug("zmqDataStreamSocket started for '" + connectionStrDataStreamSocket + "'")

        self.routerSocket             = self.zmqContextForWorker.socket(zmq.REQ)
        self.routerSocket.identity    = u"worker-{ID}".format(ID=self.id).encode("ascii")
        connectionStrRouterSocket     = "tcp://{ip}:{port}".format(ip="127.0.0.1", port="50000")
        self.routerSocket.connect(connectionStrRouterSocket)
        self.log.debug("routerSocket started for '" + connectionStrRouterSocket + "'")

        #init Cleaner message-pipe
        self.cleanerSocket            = self.zmqContextForWorker.socket(zmq.PUSH)
        connectionStrCleanerSocket    = "tcp://{ip}:{port}".format(ip=self.zmqCleanerIp, port=self.zmqCleanerPort)
        self.cleanerSocket.connect(connectionStrCleanerSocket)

        try:
            self.process()
        except KeyboardInterrupt:
            # trace = traceback.format_exc()
            self.log.debug("KeyboardInterrupt detected. Shutting down workerProcess.")
        else:
            trace = traceback.format_exc()
            self.log.error("Stopping workerProcess due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))

        self.log.info("Closing sockets")
        self.zmqDataStreamSocket.close()
        self.routerSocket.close()
        self.cleanerSocket.close()
        self.zmqContextForWorker.destroy()


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
                self.log.debug("router requested to shutdown worker-thread. Worker processed: %d files" % jobCount)
                break
            jobCount += 1

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
                filename       = fileEventMessageDict["filename"]
                sourcePath     = fileEventMessageDict["sourcePath"]
                relativeParent = fileEventMessageDict["relativeParent"]
            except Exception, e:
                errorMessage   = "Invalid fileEvent message received."
                self.log.error(errorMessage)
                self.log.debug("Error was: " + str(e))
                self.log.debug("fileEventMessageDict=" + str(fileEventMessageDict))
                #skip all further instructions and continue with next iteration
                continue

            #passing file to data-messagPipe
            try:
                self.log.debug("worker-" + str(id) + ": passing new file to data-messagePipe...")
                self.passFileToDataStream(filename, sourcePath, relativeParent)
                self.log.debug("worker-" + str(id) + ": passing new file to data-messagePipe...success.")
            except Exception, e:
                errorMessage = "Unable to pass new file to data-messagePipe."
                self.log.error(errorMessage)
                self.log.error("Error was: " + str(e))
                self.log.debug("worker-"+str(id) + ": passing new file to data-messagePipe...failed.")
                #skip all further instructions and continue with next iteration
                continue


            #send remove-request to message pipe
            try:
                #sending to pipe
                self.log.debug("send file-event to cleaner-pipe...")
                self.cleanerSocket.send(workload)
                self.log.debug("send file-event to cleaner-pipe...success.")

                #TODO: remember workload. append to list?
                # can be used to verify files which have been processed twice or more
            except Exception, e:
                errorMessage = "Unable to notify Cleaner-pipe to delete file: " + str(filename)
                self.log.error(errorMessage)
                self.log.debug("fileEventMessageDict=" + str(fileEventMessageDict))


    def getLogger(self):
        logger = logging.getLogger("workerProcess")
        return logger


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
                    self.log.debug("waited to long for file getting closed. aborting")
                    break

                #wait for other process to finish file access
                #grabs time when file was modified last
                statInfo = os.stat(sourceFilePathFull)
                fileLastModified = statInfo.st_mtime
                self.log.debug("'" + str(sourceFilePathFull) + "' modified last: " + str(fileLastModified))
                timeNow = time.time()
                timeDiff = timeNow - fileLastModified
                self.log.debug("timeNow=" + str(timeNow) + "  timeDiff=" + str(timeDiff))
                waitTimeInSeconds = fileWaitTimeInMs/1000
                if timeDiff >= waitTimeInSeconds:
                    fileIsStillInUse = False
                    self.log.debug("File was not modified within past " + str(fileWaitTimeInMs) + "ms.")
                else:
                    self.log.debug("still waiting for file to get closed...")
                    time.sleep(fileWaitTimeInMs / 1000 )

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
            payloadMetadata = self.buildPayloadMetadata(filename, filesize, fileModificationTime, sourcePath, relativeParent)
        except Exception, e:
            errorMessage = "Unable to assemble multi-part message."
            self.log.error(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(e)


        #send message
        try:
            self.log.debug("Passing multipart-message...")
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
                self.zmqDataStreamSocket.send_multipart(chunkPayload)

            #close file
            fileDescriptor.close()

            # self.zmqDataStreamSocket.send_multipart(multipartMessage)
            self.log.debug("Passing multipart-message...done.")
        except Exception, e:
            self.log.error("Unable to send multipart-message")
            self.log.debug("Error was: " + str(e))
            self.log.info("Passing multipart-message...failed.")
            raise Exception(e)


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
        self.log.debug("create metadata for source file...")
        metadataDict = {
                         "filename"             : filename,
                         "filesize"             : filesize,
                         "fileModificationTime" : fileModificationTime,
                         "sourcePath"           : sourcePath,
                         "relativeParent"       : relativeParent,
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



#
#  --------------------------  class: FileMover  --------------------------------------
#
class FileMover():
    zmqContext            = None
    bindingIpForSocket    = None
    zqmFileEventServerIp  = "127.0.0.1"  # serverIp for incoming messages
    tcpPort_messageStream = "6060"
    dataStreamIp          = "127.0.0.1"  # ip of dataStream-socket to push new files to
    dataStreamPort        = "6061"       # port number of dataStream-socket to push new files to
    zmqCleanerIp          = "127.0.0.1"  # zmq pull endpoint, responsable to delete files
    zmqCleanerPort        = "6062"       # zmq pull endpoint, responsable to delete files
    fileWaitTimeInMs      = None
    fileMaxWaitTimeInMs   = None
    parallelDataStreams   = None
    chunkSize             = None

    # sockets
    messageSocket         = None         # to receiver fileMove-jobs as json-encoded dictionary
    routerSocket          = None

    # to get the logging only handling this class
    log                   = None


    def __init__(self, bindingIpForSocket, bindingPortForSocket, dataStreamIp, dataStreamPort, parallelDataStreams,
                 chunkSize, zmqCleanerIp, zmqCleanerPort,
                 fileWaitTimeInMs, fileMaxWaitTimeInMs,
                 context = None):

        assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext            = context or zmq.Context()
        self.bindingIpForSocket    = bindingIpForSocket
        self.tcpPort_messageStream = bindingPortForSocket
        self.dataStreamIp          = dataStreamIp
        self.dataStreamPort        = dataStreamPort
        self.parallelDataStreams   = parallelDataStreams
        self.chunkSize             = chunkSize
        self.zmqCleanerIp          = zmqCleanerIp
        self.zmqCleanerPort        = zmqCleanerPort
        self.fileWaitTimeInMs      = fileWaitTimeInMs
        self.fileMaxWaitTimeInMs   = fileMaxWaitTimeInMs


        self.log = self.getLogger()
        self.log.debug("Init")

        #create zmq sockets. one for incoming file events, one for passing fileObjects to
        self.messageSocket         = self.zmqContext.socket(zmq.PULL)
        connectionStrMessageSocket = "tcp://" + self.bindingIpForSocket + ":%s" % self.tcpPort_messageStream
        self.messageSocket.bind(connectionStrMessageSocket)
        self.log.debug("messageSocket started for '" + connectionStrMessageSocket + "'")

        #setting up router for load-balancing worker-threads.
        #each worker-thread will handle a file event
        self.routerSocket         = self.zmqContext.socket(zmq.ROUTER)
        connectionStrRouterSocket = "tcp://127.0.0.1:50000"
        self.routerSocket.bind(connectionStrRouterSocket)
        self.log.debug("routerSocket started for '" + connectionStrRouterSocket + "'")


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


    def getFileWaitTimeInMs(self):
        return self.fileWaitTimeInMs


    def getFileMaxWaitTimeInMs(self):
        return self.fileMaxWaitTimeInMs


    def startReceiving(self):
        self.log.debug("new message-socket crated for: new file events.")
        parallelDataStreams = int(self.parallelDataStreams)

        self.log.debug("new message-socket crated for: passing file objects.")

        incomingMessageCounter = 0

        #start worker-threads. each will have its own PushSocket.
        workerThreadList      = list()
        numberOfWorkerThreads = parallelDataStreams
        fileWaitTimeInMs      = self.getFileWaitTimeInMs()
        fileMaxWaitTimeInMs   = self.getFileMaxWaitTimeInMs()
        for threadNumber in range(numberOfWorkerThreads):
            self.log.debug("instantiate new workerProcess (nr " + str(threadNumber) + " )")
            newWorkerThread = Process(target=WorkerProcess, args=(threadNumber,
                                                                  self.dataStreamIp,
                                                                  self.dataStreamPort,
                                                                  self.chunkSize,
                                                                  self.zmqCleanerIp,
                                                                  self.zmqCleanerPort,
                                                                  fileWaitTimeInMs,
                                                                  fileMaxWaitTimeInMs))
            workerThreadList.append(newWorkerThread)

            self.log.debug("start worker process nr " + str(threadNumber))
            newWorkerThread.start()

        #run loop, and wait for incoming messages
        continueReceiving = True
        self.log.debug("waiting for new fileEvent-messages")
        try:
            while continueReceiving:
                try:
                    incomingMessage = self.messageSocket.recv()
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
        except KeyboardInterrupt:
            self.log.info("Keyboard interuption detected. Stop receiving")



    def processFileEvent(self, fileEventMessage):
        # LRU worker is next waiting in the queue
        self.log.debug("waiting for available workerThread.")

        # address == "worker-0"
        # empty   == ""
        # ready   == "READY"
        address, empty, ready = self.routerSocket.recv_multipart()
        self.log.debug("available workerThread detected.")

        self.log.debug("passing job to workerThread...")
        self.routerSocket.send_multipart([
                                     address,
                                     b'',
                                     fileEventMessage,
                                    ])

        self.log.debug("passing job to workerThread...done.")


    def stop(self):
        self.messageSocket.close()
        self.routerSocket.close()



#
#  --------------------------  class: Cleaner  --------------------------------------
#
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
    bindingPortForSocket = None
    bindingIpForSocket   = None
    zmqContextForCleaner = None
    zmqCleanerSocket     = None

    # to get the logging only handling this class
    log                  = None

    def __init__(self, bindingIp="127.0.0.1", bindingPort="6062", context = None, verbose=False):
        self.bindingPortForSocket = bindingPort
        self.bindingIpForSocket   = bindingIp
        self.zmqContextForCleaner = context or zmq.Context()

        self.log = self.getLogger()
        self.log.debug("Init")

        #bind to local port
        self.zmqCleanerSocket      = self.zmqContextForCleaner.socket(zmq.PULL)
        connectionStrCleanerSocket = "tcp://" + self.bindingIpForSocket + ":%s" % self.bindingPortForSocket
        self.zmqCleanerSocket.bind(connectionStrCleanerSocket)
        self.log.debug("zmqCleanerSocket started for '" + connectionStrCleanerSocket + "'")

        try:
            self.process()
        except zmq.error.ZMQError:
            self.log.error("ZMQError: "+ str(e))
            self.log.debug("Shutting down cleaner.")
        except KeyboardInterrupt:
            self.log.info("KeyboardInterrupt detected. Shutting down cleaner.")
        except:
            trace = traceback.format_exc()
            self.log.error("Stopping cleanerProcess due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))

        self.zmqCleanerSocket.close()
        self.zmqContextForCleaner.destroy()


    def getLogger(self):
        logger = logging.getLogger("cleaner")
        return logger


    def process(self):
        #processing messaging
        while True:
            #waiting for new jobs
            self.log.debug("Waiting for new jobs")
            try:
                workload = self.zmqCleanerSocket.recv()
            except Exception as e:
                self.log.error("Error in receiving job: " + str(e))


            #transform to dictionary
            try:
                workloadDict = json.loads(str(workload))
            except:
                errorMessage = "invalid job received. skipping job"
                self.log.error(errorMessage)
                self.log.debug("workload=" + str(workload))
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
                self.log.error(errorMessage)
                self.log.debug("Error was: " + str(e))
                self.log.debug("workloadDict=" + str(workloadDict))
                #skip all further instructions and continue with next iteration
                continue

            #moving source file
            sourceFilepath = None
            try:
                self.log.debug("removing source file...")
                #generate target filepath
                sourceFilepath = os.path.join(sourcePath,filename)
#                self.removeFile(sourceFilepath)
                self.log.debug ("sourcePath: " + str (sourcePath))
                self.log.debug ("filename: " + str (filename))
                self.log.debug ("targetPath: " + str (targetPath))
                self.moveFile(sourcePath, filename, targetPath)

                # #show filesystem statistics
                # try:
                #     self.showFilesystemStatistics(sourcePath)
                # except Exception, f:
                #     logging.warning("Unable to get filesystem statistics")
                #     logging.debug("Error was: " + str(f))
                self.log.debug("file removed: " + str(sourcePath))
                self.log.debug("removing source file...success.")

            except Exception, e:
                errorMessage = "Unable to remove source file: " + str (sourcePath)
                self.log.error(errorMessage)
                trace = traceback.format_exc()
                self.log.error("Error was: " + str(trace))
                self.log.debug("sourceFilepath="+str(sourceFilepath))
                self.log.debug("removing source file...failed.")
                #skip all further instructions and continue with next iteration
                continue



    def moveFile(self, source, filename, target):
        maxAttemptsToRemoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        self.log.info("Moving file '" + str(filename) + "' from '" +  str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
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
                sourceFile = source + os.sep + filename
                targetFile = target + os.sep + filename
                self.log.debug("sourceFile: " + str(sourceFile))
                self.log.debug("targetFile: " + str(targetFile))
                shutil.move(sourceFile, targetFile)
                fileWasMoved = True
                self.log.debug("Moving file '" + str(filename) + "' from '" + str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
            except IOError:
                self.log.debug ("IOError: " + str(filename))
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to move file {FILE}.".format(FILE=str(source) + str(filename))
                self.log.warning(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.warning("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))


        if not fileWasMoved:
            self.log.error("Moving file '" + str(filename) + " from " + str(source) + " to " + str(target) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToMoveFile reached (value={ATTEMPT}). Unable to move file '{FILE}'.".format(ATTEMPT=str(iterationCount),
                                                                                                                            FILE=filename))


    def removeFile(self, filepath):
        maxAttemptsToRemoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        self.log.info("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...")
        fileWasRemoved = False

        while iterationCount <= maxAttemptsToRemoveFile and not fileWasRemoved:
            iterationCount+=1
            try:
                os.remove(filepath)
                fileWasRemoved = True
                self.log.debug("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...success.")
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to remove file {FILE}.".format(FILE=str(filepath))
                self.log.warning(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.warning("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))


        if not fileWasRemoved:
            self.log.error("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...FAILED.")
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


    #enable logging
    helperScript.initLogging(logfileFullPath, verbose)


    #create zmq context
    # there should be only one context in one process
    zmqContext = zmq.Context.instance()
    logging.info("registering zmq global context")


    cleanerThread = Process(target=Cleaner, args=(zmqCleanerIp, zmqCleanerPort, zmqContext))
    cleanerThread.start()
    logging.debug("cleaner thread started")

    #start new fileMover
    fileMover = FileMover(bindingIpForSocket, bindingPortForSocket, dataStreamIp, dataStreamPort,
                          parallelDataStreams, chunkSize,
                          zmqCleanerIp, zmqCleanerPort,
                          fileWaitTimeInMs, fileMaxWaitTimeInMs,
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

    try:
        logging.debug("closing zmqContext...")
        zmqContext.destroy()
        logging.debug("closing zmqContext...done.")
    except:
        logging.debug("closing zmqContext...failed.")
        logging.error(sys.exc_info())

