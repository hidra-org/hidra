from __builtin__ import open, type

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'

import zmq
import os
import sys
import logging
import traceback
import json

#path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SHARED_PATH = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helperScript

#
#  --------------------------  class: WorkerProcess  --------------------------------------
#
class WorkerProcess():
    id                   = None
    dataStreamIp         = None
    dataStreamPort       = None
    zmqContextForWorker  = None
    externalContext      = None         # if the context was created outside this class or not
    zmqMessageChunkSize  = None
    cleanerIp            = None         # responsable to delete/move files
    cleanerPort          = None         # responsable to delete/move files
    liveViewerIp         = None
    liveViewerPort       = None
    ondaIp               = None
    ondaPort             = None

    routerSocket         = None
    cleanerSocket        = None
    dataStreamSocket     = None
    liveViewerSocket     = None
    ondaComSocket        = None

    useDataStream        = False        # boolian to inform if the data should be send to the data stream pipe (to the storage system)
    openConnections      = None         # list of all open hosts and ports to which a data stream is opened
    useRealTimeAnalysis  = False        # boolian to inform if the receiver for realtime-analysis is running

    requestFromOnda      = False

    # to get the logging only handling this class
    log                  = None

    def __init__(self, id, dataStreamIp, dataStreamPort, chunkSize, cleanerIp, cleanerPort, ondaIp, ondaPort,
                 useDataStream, context = None):
        self.id                   = id
        self.dataStreamIp         = dataStreamIp
        self.dataStreamPort       = dataStreamPort
        self.zmqMessageChunkSize  = chunkSize
        self.cleanerIp            = cleanerIp
        self.cleanerPort          = cleanerPort
        self.ondaIp               = ondaIp
        self.ondaPort             = ondaPort

        self.useDataStream        = useDataStream

        self.openConnections = {
                "streams"     : [] ,
                "queryNewest" : []
                }

        if context:
            self.zmqContextForWorker = context
            self.externalContext     = True
        else:
            self.zmqContextForWorker = zmq.Context()
            self.externalContext     = False

        self.log = self.getLogger()

        self.log.debug("new workerProcess started. id=" + str(self.id))

        self.createSockets()

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


    def createSockets(self):
        if self.useDataStream:
            self.dataStreamSocket = self.zmqContextForWorker.socket(zmq.PUSH)
            connectionStr         = "tcp://{ip}:{port}".format(ip=self.dataStreamIp, port=self.dataStreamPort)
            self.dataStreamSocket.connect(connectionStr)
            self.log.info("dataStreamSocket started (connect) for '" + connectionStr + "'")

        # initialize sockets
        routerIp   = "127.0.0.1"
        routerPort = "50002"

        self.routerSocket             = self.zmqContextForWorker.socket(zmq.REQ)
        self.routerSocket.identity    = u"worker-{ID}".format(ID=self.id).encode("ascii")
        connectionStr                 = "tcp://{ip}:{port}".format(ip=routerIp, port=routerPort)
        try:
            self.routerSocket.connect(connectionStr)
            self.log.debug("routerSocket started (connect) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start routerSocket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        #init Cleaner message-pipe
        self.cleanerSocket            = self.zmqContextForWorker.socket(zmq.PUSH)
        connectionStrCleanerSocket    = "tcp://{ip}:{port}".format(ip=self.cleanerIp, port=self.cleanerPort)
        try:
            self.cleanerSocket.connect(connectionStrCleanerSocket)
            self.log.debug("cleanerSocket started (connect) for '" + connectionStrCleanerSocket + "'")
        except Exception as e:
            self.log.error("Failed to start cleanerSocket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # Poller to get either messages from the watcher or communication messages to stop sending data to the live viewer
        self.poller = zmq.Poller()
        #TODO do I need to register the routerSocket in here?
        self.poller.register(self.routerSocket, zmq.POLLIN)


    def getLogger(self):
        logger = logging.getLogger("workerProcess")
        return logger


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


            # after the signal processing
            if self.checkForSignals(workload):
                continue



            # get metadata of the file
            if self.useDataStream or self.openConnections["streams"] or self.openConnections["queryNewest"] or self.useRealTimeAnalysis:
                try:
                    self.log.debug("building MetadataDict")
                    sourcePathFull, metadataDict = self.buildMetadataDict(workload)
                except Exception as e:
                    self.log.error("Building of metadata dictionary failed for file: " + str(sourcePathFull) + ".")
                    self.log.debug("Error was: " + str(e))
                    #skip all further instructions and continue with next iteration
                    continue
            else:
                try:
                    self.log.debug("building MetadataDict")
                    sourcePathFull, metadataDict = self.buildMetadataDict(workload, reduced = True)
                except Exception as e:
                    self.log.error("Building of metadata dictionary failed for file: " + str(sourcePathFull) + ".")
                    self.log.debug("Error was: " + str(e))
                    #skip all further instructions and continue with next iteration
                    continue

            # dict with all sockets to send data to (additionally to the dataStreamSocket)
            socketListToSendData = dict()

            if self.openConnections["streams"]:
                socketListToSendData["liveViewer"] = self.openConnections["streams"][0]["socket"] #TODO
                self.log.debug("worker-" + str(self.id) + ": Add socket from host " + str(self.openConnections["streams"][0]["host"])
                        + " on port " + str(self.openConnections["streams"][0]["port"]) + " to sockets to send data to.")

            if self.useRealTimeAnalysis or self.openConnections["queryNewest"]:
                socks = dict(self.poller.poll(0))

                queryCandidate = self.openConnections["queryNewest"][0]

                if queryCandidate["socket"] in socks and socks[queryCandidate["socket"]] == zmq.POLLIN:

                    request = queryCandidate["socket"].recv()
                    self.log.debug("worker-"+str(self.id)+": received new request for newest File from "
                            + str(queryCandidate["host"]) + " on port " + str(queryCandidate["port"]))

                    if request == b"NEXT_FILE":
                        queryCandidate["request"] = True
                        self.log.debug("worker-" + str(self.id) + ": mark socket as requested...")
                        #skip all further instructions and continue with next iteration
#                        continue

                elif self.ondaComSocket in socks and socks[self.ondaComSocket] == zmq.POLLIN:
                    ondaWorkload = self.ondaComSocket.recv()
                    self.log.debug("worker-"+str(self.id)+": received new request from onda")
                    request = ondaWorkload == b"NEXT_FILE"
                    if request:
                        self.requestFromOnda = True
                        #passing file to data-messagPipe
                        try:
                            self.log.debug("worker-" + str(self.id) + ": passing new file to data-messagePipe...")
                            socketListToSendData["onda"] = self.ondaComSocket
                            self.log.debug("worker-" + str(self.id) + ": passing new file to data-messagePipe...success.")
                        except Exception as e:
                            self.log.error("Unable to pass new file to data-messagePipe.")
                            self.log.error("Error was: " + str(e))
                            self.log.debug("worker-"+str(self.id) + ": passing new file to data-messagePipe...failed.")
                            #skip all further instructions and continue with next iteration
#                            continue

                elif self.requestFromOnda:
                    #passing file to data-messagPipe
                    try:
                        self.log.debug("worker-" + str(self.id) + ": passing new file to data-messagePipe...")
                        socketListToSendData["onda"] = self.ondaComSocket
                        self.log.debug("worker-" + str(self.id) + ": passing new file to data-messagePipe...success.")
                    except Exception as e:
                        self.log.error("Unable to pass new file to data-messagePipe.")
                        self.log.error("Error was: " + str(e))
                        self.log.debug("worker-"+str(self.id) + ": passing new file to data-messagePipe...failed.")
                        #skip all further instructions and continue with next iteration
#                        continue


            if self.useDataStream or socketListToSendData:
                self.log.debug("passing file to dataStream")
                try:
                    self.passFileToDataStream(sourcePathFull, metadataDict, socketListToSendData)
                except Exception as e:
                    self.log.debug("worker-"+str(self.id) + ": passing new file to dataStream...failed.")
                    self.log.debug("Error was: " + str(e))

            #send file to cleaner pipe
            try:
                #sending to pipe
                self.log.debug("send file-event for file to cleaner-pipe...")
#                self.log.debug("workload = " + str(workload))
#                self.cleanerSocket.send(workload)
                self.log.debug("metadataDict = " + str(metadataDict))
                self.cleanerSocket.send(json.dumps(metadataDict))
                self.log.debug("send file-event for file to cleaner-pipe...success.")

                #TODO: remember workload. append to list?
                # can be used to verify files which have been processed twice or more
            except Exception, e:
                self.log.error("Unable to notify Cleaner-pipe to handle file: " + str(workload))


    def checkForSignals(self, workload):

        signal, host, port = helperScript.extractSignal(workload, self.log)

        # a data stream is turned on
        if signal == "START_LIVE_VIEWER":
            self.log.info("worker-"+str(self.id)+": Received signal to start data stream...")

            # parent process has already checked for streams on this host and port: there is none running
            # create the socket to send data to the live viewer
            socket        = self.zmqContextForWorker.socket(zmq.PUSH)
            connectionStr = "tcp://" + str(host) + ":" + str(port)
            try:
                socket.connect(connectionStr)
                self.log.info("liveViewerSocket started (connect) for '" + connectionStr + "'")

                self.openConnections["streams"].append({
                            "host"   : host,
                            "port"   : port,
                            "socket" : socket
                            })
            except:
                self.log.info("liveViewerSocket could not be started for '" + connectionStr + "'")

            return True

        # a data stream is turned off
        elif signal == "STOP_LIVE_VIEWER":
            self.log.info("worker-"+str(self.id)+": Received signal to stop data stream...")

            # parent process has already checked for streams on this host and port: there is one running
            # close the socket to send data to the live viewer
            for i in range(len(self.openConnections["streams"])):
                connection = self.openConnections["streams"][i]

                if connection["host"] == host and connection["port"] == port:
                    connection["socket"].close(0)
                    self.openConnections["streams"].pop(i)
                    self.log.info("liveViewerSocket closed")
                    break

            return True

        # the realtime-analysis is turned on
        elif signal == "START_QUERY_NEWEST":
            self.log.info("worker-"+str(self.id)+": Received signal to start a query for the newest file...")

            # create the socket to send data to the realtime analysis
            socket        = self.zmqContextForWorker.socket(zmq.REP)
            connectionStr = "tcp://" + str(host) + ":" + str(port)
            try:
                socket.connect(connectionStr)
                self.log.info("queryNewestSocket started (connect) for '" + connectionStr + "'")

                self.poller.register(socket, zmq.POLLIN)

                self.openConnections["queryNewest"].append({
                            "host"    : host,
                            "port"    : port,
                            "socket"  : socket,
                            "request" : False
                            })
            except:
                self.log.info("queryNewestSocket could not be started for '" + connectionStr + "'")

            return True

        # the realtime-analysis is turned off
        elif signal == "STOP_QUERY_NEWEST":
            self.log.info("worker-"+str(self.id)+": Received signal to stop to query for newest file...")

            # parent process has already checked for streams on this host and port: there is one running
            # close the socket to send data to the realtime analysis
            for i in range(len(self.openConnections["queryNewest"])):
                connection = self.openConnections["queryNewest"][i]

                if connection["host"] == host and connection["port"] == port:
                    connection["socket"].close(0)
                    self.openConnections["queryNewest"].pop(i)
                    self.log.info("queryNewestSocket closed")
                    break

            return True

        # the realtime-analysis is turned on
        elif signal == "START_REALTIME_ANALYSIS":
            self.log.info("worker-"+str(self.id)+": Received realtime-analysis start command...")
            self.useRealTimeAnalysis = True

            # close the socket to send data to the realtime analysis
            if self.ondaComSocket:
                self.ondaComSocket.close(0)
                #TODO unbind?
                self.ondaComSocket = None
                self.log.debug("ondaComSocket refreshed")

            # create the socket to send data to the realtime analysis
            self.ondaComSocket    = self.zmqContextForWorker.socket(zmq.REP)
            connectionStr         = "tcp://{ip}:{port}".format(ip=self.ondaIp, port=self.ondaPort)
            self.ondaComSocket.bind(connectionStr)
            self.log.info("ondaSocket started (bind) for '" + connectionStr + "'")

            self.poller.register(self.ondaComSocket, zmq.POLLIN)

            return True

        # the realtime-analysis is turned off
        elif signal == "STOP_REALTIME_ANALYSIS":
            self.log.info("worker-"+str(self.id)+": Received realtime-analysis stop command...stopping realtime analysis")
            self.useRealTimeAnalysis = False

            # close the socket to send data to the realtime analysis
            if self.ondaComSocket:
                self.ondaComSocket.close(0)
                self.log.info("ondaComSocket closed")

            return True

        return False


    def buildMetadataDict(self, workload, reduced = False):

        #convert fileEventMessage back to a dictionary
        metadataDict = None
        try:
            metadataDict = json.loads(str(workload))
            self.log.debug("str(messageDict) = " + str(metadataDict) + "  type(messageDict) = " + str(type(metadataDict)))
        except Exception as e:
            errorMessage = "Unable to convert message into a dictionary."
            self.log.info(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(e)


        #extract fileEvent metadata
        try:
            #TODO validate metadataDict dict
            filename     = metadataDict["filename"]
            sourcePath   = metadataDict["sourcePath"]
            relativePath = metadataDict["relativePath"]
        except Exception as e:
            self.log.info("Invalid fileEvent message received.")
            self.log.debug("Error was: " + str(e))
            self.log.debug("metadataDict=" + str(metadataDict))
            #skip all further instructions and continue with next iteration
            raise Exception(e)

        # filename = "img.tiff"
        # filepath = "C:\dir"
        #
        # -->  sourceFilePathFull = 'C:\\dir\img.tiff'
        sourceFilePath     = os.path.normpath(sourcePath + os.sep + relativePath)
        sourceFilePathFull = os.path.join(sourceFilePath, filename)

        if reduced:
            try:
                fileModificationTime = os.stat(sourceFilePathFull).st_mtime
                self.log.debug("fileModificationTime(%s) = %s" % (sourceFilePathFull, str(fileModificationTime)))
            except Exception as e:
                self.log.info("Unable to get file metadata for '" + str(sourceFilePathFull) + "'.")
                self.log.debug("Error was: " + str(e))
                raise Exception(e)

            #build payload for message-pipe by putting source-file into a message
            try:
                self.log.debug("create (reduced) metadata for source file...")
                #metadataDict = {
                #                 "filename"             : filename,
                #                 "sourcePath"           : sourcePath,
                #                 "relativePath"         : relativePath,
                #                 "fileModificationTime" : fileModificationTime,
                #                 }
                metadataDict [ "fileModificationTime" ] = fileModificationTime

                self.log.debug("metadataDict = " + str(metadataDict))
            except Exception as e:
                self.log.info("Unable to create metadata dictionary.")
                self.log.debug("Error was: " + str(e))
                raise Exception(e)
        else:
            try:
                #for quick testing set filesize of file as chunksize
                self.log.debug("get filesize for '" + str(sourceFilePathFull) + "'...")
                filesize             = os.path.getsize(sourceFilePathFull)
                fileModificationTime = os.stat(sourceFilePathFull).st_mtime
                chunksize            = filesize    #can be used later on to split multipart message
                self.log.debug("filesize(%s) = %s" % (sourceFilePathFull, str(filesize)))
                self.log.debug("fileModificationTime(%s) = %s" % (sourceFilePathFull, str(fileModificationTime)))

            except Exception as e:
                self.log.info("Unable to create metadata dictionary.")
                self.log.debug("Error was: " + str(e))
                raise Exception(e)

            #build payload for message-pipe by putting source-file into a message
            try:
                self.log.debug("create metadata for source file...")
                #metadataDict = {
                #                 "filename"             : filename,
                #                 "sourcePath"           : sourcePath,
                #                 "relativePath"         : relativePath,
                #                 "filesize"             : filesize,
                #                 "fileModificationTime" : fileModificationTime,
                #                 "chunkSize"            : self.zmqMessageChunkSize
                #                 }
                metadataDict [ "filesize"             ] = filesize
                metadataDict [ "fileModificationTime" ] = fileModificationTime
                metadataDict [ "chunkSize"            ] = self.zmqMessageChunkSize

                self.log.debug("metadataDict = " + str(metadataDict))
            except Exception as e:
                self.log.info("Unable to assemble multi-part message.")
                self.log.debug("Error was: " + str(e))
                raise Exception(e)

        return sourceFilePathFull, metadataDict


    def passFileToDataStream(self, sourceFilePathFull, payloadMetadata, socketDict):

        #reading source file into memory
        try:
            self.log.debug("opening '" + str(sourceFilePathFull) + "'...")
            fileDescriptor = open(str(sourceFilePathFull), "rb")
        except Exception as e:
            errorMessage = "Unable to read source file '" + str(sourceFilePathFull) + "'."
            self.log.error(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(e)

        #send message
        try:
            self.log.debug("Passing multipart-message for file " + str(sourceFilePathFull) + "...")
            chunkNumber = 0
            stillChunksToRead = True
            payloadAll = [json.dumps(payloadMetadata.copy())]
            while stillChunksToRead:
                chunkNumber += 1

                #read next chunk from file
                fileContentAsByteObject = fileDescriptor.read(self.zmqMessageChunkSize)

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
                if socketDict.has_key("onda"):
                    payloadAll.append(fileContentAsByteObject)

                # send data to the data stream to store it in the storage system
                if self.useDataStream:

                    tracker = self.dataStreamSocket.send_multipart(chunkPayload, copy=False, track=True)

                    if not tracker.done:
                        self.log.info("Message part from file " + str(sourceFilePathFull) + " has not been sent yet, waiting...")
                        tracker.wait()
                        self.log.info("Message part from file " + str(sourceFilePathFull) + " has not been sent yet, waiting...done")

                #send data to the live viewer
                if socketDict.has_key("liveViewer"):
                    socketDict["liveViewer"].send_multipart(chunkPayload, zmq.NOBLOCK)
                    self.log.info("Sending message part from file " + str(sourceFilePathFull) + " to LiveViewer")

            # send data to onda
            if socketDict.has_key("onda"):
                socketDict["onda"].send_multipart(payloadAll, zmq.NOBLOCK)
                self.log.info("Sending from file " + str(sourceFilePathFull) + " to OnDA")
                self.requestFromOnda = False

            #close file
            fileDescriptor.close()

            # self.liveViewerSocket.send_multipart(multipartMessage)
            self.log.debug("Passing multipart-message for file " + str(sourceFilePathFull) + "...done.")
#            except zmq.error.Again:
#                self.log.error("unable to send multiplart-message for file " + str(sourceFilePathFull))
#                self.log.error("Receiver has disconnected")
        except Exception, e:
            self.log.error("Unable to send multipart-message for file " + str(sourceFilePathFull))
#                self.log.debug("Error was: " + str(e))
            trace = traceback.format_exc()
            self.log.debug("Error was: " + str(trace))
            self.log.debug("Passing multipart-message...failed.")



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
             self.log("Error was: " + str(e))



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
        self.cleanerSocket.send("STOP")        #no communication needed because cleaner detects KeyboardInterrupt signals
        self.log.debug("Closing sockets for worker " + str(self.id))
        if self.dataStreamSocket:
            self.dataStreamSocket.close(0)
        if self.liveViewerSocket:
            self.liveViewerSocket.close(0)
        if self.ondaComSocket:
            self.ondaComSocket.close(0)
        self.routerSocket.close(0)
        self.cleanerSocket.close(0)
        if not self.externalContext:
            self.log.debug("Destroying context")
            self.zmqContextForWorker.destroy()


