from __builtin__ import open, type

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'

import zmq
import os
import logging
import traceback
import json
from fabio.cbfimage import cbfimage

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
    cleanerIp            = None         # responsable to delete/move files
    cleanerPort          = None         # responsable to delete/move files

    zmqDataStreamSocket  = None
    routerSocket         = None
    cleanerSocket        = None

    useLiveViewer        = False        # boolian to inform if the receiver for the live viewer is running
    useRealTimeAnalysis  = False        # boolian to inform if the receiver for realtime-analysis is running

    # to get the logging only handling this class
    log                  = None

    def __init__(self, id, dataStreamIp, dataStreamPort, chunkSize, cleanerIp, cleanerPort,
                 context = None):
        self.id                   = id
        self.dataStreamIp         = dataStreamIp
        self.dataStreamPort       = dataStreamPort
        self.zmqMessageChunkSize  = chunkSize
        self.cleanerIp            = cleanerIp
        self.cleanerPort          = cleanerPort

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
        connectionStrCleanerSocket    = "tcp://{ip}:{port}".format(ip=self.cleanerIp, port=self.cleanerPort)
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

            # the live viewer is turned off
            stopLV = workload == b"STOP_LIVE_VIEWER"
            if stopLV:
                self.log.info("worker-"+str(self.id)+": Received live viewer stop command...stopping live viewer")
                self.useLiveViewer = False
                continue

            # the realtime-analysis is turned on
            startRTA = workload == b"START_REALTIME_ANALYSIS"
            if startRTA:
                self.log.info("worker-"+str(self.id)+": Received realtime-analysis start command...starting live viewer")
                self.useRealTimeAnalysis = True
                continue

            # the realtime-analysis is turned off
            stopRTA = workload == b"STOP_REALTIME_ANALYSIS"
            if stopRTA:
                self.log.info("worker-"+str(self.id)+": Received realtime-analysis stop command...stopping live viewer")
                self.useRealTimeAnalysis = False
                continue

            if self.useLiveViewer or self.useRealTimeAnalysis:
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

            if self.useLiveViewer:
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

            if self.useRealTimeAnalysis:
                # -->  sourceFilePathFull = 'C:\\dir\img.tiff'
                sourceFilePath     = os.path.normpath(sourcePath + os.sep + relativePath)
                sourceFilePathFull = os.path.join(sourceFilePath, filename)

                #reading source file into memory
                try:
                    #for quick testing set filesize of file as chunksize
                    self.log.debug("get filesize for '" + str(sourceFilePathFull) + "'...")
                    filesize             = os.path.getsize(sourceFilePathFull)
                    fileModificationTime = os.stat(sourceFilePathFull).st_mtime
                    self.log.debug("filesize(%s) = %s" % (sourceFilePathFull, str(filesize)))
                    self.log.debug("fileModificationTime(%s) = %s" % (sourceFilePathFull, str(fileModificationTime)))
                    fileFormat           = filename.rsplit(".", 2)[1]
                    self.log.debug("fileFormat(%s) = %s" % (filename, str(fileFormat)))
                except Exception, e:
                    self.log.error("Unable to get file metadata for '" + str(sourceFilePathFull) + "'." )
                    self.log.debug("Error was: " + str(e))
#                    raise Exception(e)

                try:
                    fileContent          = self.getFileContent(sourceFilePathFull, fileFormat)
                except Exception, e:
                    self.log.error("Unable to get file content for '" + str(sourceFilePathFull) + "'." )
                    self.log.debug("Error was: " + str(e))

                #build payload for message-pipe by putting source-file into a message
                try:
#                    dataToSend                = self.buildPayloadMetadata(filename, filesize, fileModificationTime, sourcePath, relativePath)
                    payloadMetadata            = self.buildPayloadMetadata(filename, filesize, fileModificationTime, sourcePath, relativePath, fileFormat)
                    # append the data to store in the ringbuffer
                    payloadMetadata            = json.dumps(payloadMetadata)
                    dataToSend = payloadMetadata + "|||" + str( fileContent)
                    print dataToSend
                except Exception, e:
                    self.log.error("Unable to assemble multi-part message.")
                    self.log.debug("Error was: " + str(e))
#                    raise Exception(e)


            if self.useRealTimeAnalysis:
                #send remove-request to message pipe
                self.log.debug("send file-event for file to cleaner-pipe...")
                try:
                    #sending to pipe
                    self.log.debug("dataToSend = " + str(dataToSend))
                    self.cleanerSocket.send(dataToSend)
                    self.log.debug("send file-event for file to cleaner-pipe...success.")
                except Exception, e:
                    self.log.error("Unable to notify Cleaner-pipe to hanlde file: " + str(workload))
#                    self.log.debug("dataToSend=" + str(dataToSend))
                    self.log.debug("Error was: " + str(e))

            else:
                #send remove-request to message pipe
                try:
                    #sending to pipe
                    self.log.debug("send file-event for file to cleaner-pipe...")
                    self.log.debug("workload = " + str(workload))
                    self.cleanerSocket.send(workload)
                    self.log.debug("send file-event for file to cleaner-pipe...success.")

                    #TODO: remember workload. append to list?
                    # can be used to verify files which have been processed twice or more
                except Exception, e:
                    errorMessage = "Unable to notify Cleaner-pipe to delete file: " + str(workload)
                    self.log.error(errorMessage)
                    self.log.debug("fileEventMessageDict=" + str(fileEventMessageDict))


    def getLogger(self):
        logger = logging.getLogger("workerProcess")
        return logger


    def getFileContent(self, filePath, fileFormat):

        if fileFormat == "cbf":
            # initialize a cbfimage opject
            cbfFile = cbfimage()
            try:
                # load the cbf file
                cbfFile.read(filePath)

                # add the data to the metadata JSON
                content = cbfFile.header
                content[u"data"] = cbfFile.data
            except Exception as e:
                self.log.error("Unable to read cbf-file")
                self.log.debug("Error was: " + str(e))

        else:
            content = "None"

        return content


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
            self.log.error("Receiver has disconnected")
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



    def buildPayloadMetadata(self, filename, filesize, fileModificationTime, sourcePath, relativePath, fileFormat = None):
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
                         "chunkSize"            : self.getChunkSize()
                         }
        if fileFormat:
            metadataDict["fileFormat"] = fileFormat

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


