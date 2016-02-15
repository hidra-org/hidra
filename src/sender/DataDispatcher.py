from __builtin__ import open, type

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

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
#  --------------------------  class: DataDispatcher  --------------------------------------
#
class DataDispatcher():

    def __init__(self, id, dataStreamPort, chunkSize, cleanerPort,
                 useDataStream, context = None):

        self.log = self.getLogger()
        self.log.debug("DataDispatcher Nr. " + str(self.id) + " started.")

        self.id          = id
        self.localhost   = "127.0.0.1"
        self.extIp       = "0.0.0.0"
        self.routerPort  = routerPort
        self.chunkSize   = chunkSize

        self.routerSocket = None

        # dict with informations of all open sockets to which a data stream is opened (host, port,...)
        self.openConnections = dict()

        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False

        self.createSockets()

        try:
            self.process()
        except KeyboardInterrupt:
            self.log.debug("KeyboardInterrupt detected. Shutting down DataDispatcher Nr. " + str(self.id) + ".")
        except:
            trace = traceback.format_exc()
            self.log.error("Stopping DataDispatcher Nr " + str(self.id) + " due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))


    def createSockets(self):
        self.routerSocket = self.context.socket(zmq.PULL)
        connectionStr  = "tcp://{ip}:{port}".format( ip=self.localhost, port=self.routerPort )
        routerSocket.connect(connectionStr)
        self.log.info("Start routerSocket (connect): '" + str(connectionStr) + "'")


    def getLogger(self):
        logger = logging.getLogger("DataDispatcher")
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

        while True:
            # Get workload from router, until finished
            self.log.debug("DataDispatcher-" + str(self.id) + ": waiting for new job")
            message = self.routerSocket.recv()
            self.log.debug("DataDispatcher-" + str(self.id) + ": new job received")

            finished = workload == b"EXIT"
            if finished:
                self.log.debug("Router requested to shutdown DataDispatcher-"+ str(self.id) + ".")
                break

            workload = message[0]
            targets  = message[1:]

            # get metadata of the file
            try:
                self.log.debug("Getting file metadata")
                sourcePathFull, metadata = self.getMetadata(workload)
            except Exception as e:
                self.log.error("Building of metadata dictionary failed for file: " + str(sourcePathFull) + ".")
                self.log.debug("Error was: " + str(e))
                #skip all further instructions and continue with next iteration
                continue

            # send data
            try:
                self.sendData(targets, sourcePathFull, metadataDict)
            except Exception as e:
                self.log.debug("DataDispatcher-"+str(self.id) + ": Passing new file to data Stream...failed.")
                self.log.debug("Error was: " + str(e))



            # send file to cleaner pipe
            try:
                #sending to pipe
                self.log.debug("send file-event for file to cleaner-pipe...")
                self.log.debug("metadataDict = " + str(metadataDict))
                self.cleanerSocket.send(json.dumps(metadataDict))
                self.log.debug("send file-event for file to cleaner-pipe...success.")

                #TODO: remember workload. append to list?
                # can be used to verify files which have been processed twice or more
            except Exception, e:
                self.log.error("Unable to notify Cleaner-pipe to handle file: " + str(workload))


    def getMetadata(self, workload):

        #convert fileEventMessage back to a dictionary
        metadata = None
        try:
            metadata = json.loads(str(workload))
            self.log.debug("str(metadata) = " + str(metadata) + "  type(metadata) = " + str(type(metadata)))
        except Exception as e:
            self.log.info("Unable to convert message into a dictionary.")
            self.log.debug("Error was: " + str(e))
            raise Exception(e)


        #extract fileEvent metadata
        try:
            #TODO validate metadata dict
            filename     = metadata["filename"]
            sourcePath   = metadata["sourcePath"]
            relativePath = metadata["relativePath"]
        except Exception as e:
            self.log.info("Invalid fileEvent message received.")
            self.log.debug("Error was: " + str(e))
            self.log.debug("metadata=" + str(metadata))
            #skip all further instructions and continue with next iteration
            raise Exception(e)

        # filename = "img.tiff"
        # filepath = "C:\dir"
        #
        # -->  sourceFilePathFull = 'C:\\dir\img.tiff'
        sourceFilePath     = os.path.normpath(sourcePath + os.sep + relativePath)
        sourceFilePathFull = os.path.join(sourceFilePath, filename)

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
            #metadata = {
            #                 "filename"             : filename,
            #                 "sourcePath"           : sourcePath,
            #                 "relativePath"         : relativePath,
            #                 "filesize"             : filesize,
            #                 "fileModificationTime" : fileModificationTime,
            #                 "chunkSize"            : self.zmqMessageChunkSize
            #                 }
            metadata[ "filesize"             ] = filesize
            metadata[ "fileModificationTime" ] = fileModificationTime
            metadata[ "chunkSize"            ] = self.zmqMessageChunkSize

            self.log.debug("metadata = " + str(metadata))
        except Exception as e:
            self.log.info("Unable to assemble multi-part message.")
            self.log.debug("Error was: " + str(e))
            raise Exception(e)

        return sourceFilePathFull, metadata


    def sendData(self, targets, sourceFilepath, metadata):
        #reading source file into memory
        try:
            self.log.debug("Opening '" + str(sourceFilepath) + "'...")
            fileDescriptor = open(str(sourceFilepath), "rb")
        except Exception as e:
            self.log.error("Unable to read source file '" + str(sourceFilepath) + "'.")
            self.log.debug("Error was: " + str(e))
            raise Exception(e)

        #send message
        try:
            self.log.debug("Passing multipart-message for file " + str(sourceFilepath) + "...")
            chunkNumber = 0
            stillChunksToRead = True
            payloadAll = [json.dumps(payloadMetadata.copy())]
            while stillChunksToRead:
                chunkNumber += 1

                #read next chunk from file
                fileContent = fileDescriptor.read(self.zmqMessageChunkSize)

                #detect if end of file has been reached
                if not fileContent:
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
                chunkPayload.append(fileContent)

                # send data to the data stream to store it in the storage system
#                if self.useDataStream:
#
#                    tracker = self.dataStreamSocket.send_multipart(chunkPayload, copy=False, track=True)
#
#                    if not tracker.done:
#                        self.log.info("Message part from file " + str(sourceFilepath) + " has not been sent yet, waiting...")
#                        tracker.wait()
#                        self.log.info("Message part from file " + str(sourceFilepath) + " has not been sent yet, waiting...done")

                # streaming data
                #TODO priority
                for target in targets:
                    # socket already known
                    if target in self.openConnections:
                        # send data
                        self.connections[target].send_multipart(chunkPayload, zmq.NOBLOCK)
                        self.log.info("Sending message part from file " + str(sourceFilepath) + " to " + target)
                    # socket not known
                    else:
                        # open socket
                        socket        = self.zmqContextForWorker.socket(zmq.PUSH)
                        connectionStr = "tcp://" + str(target)

                        # register socket
                        self.openConnections[target] = socket

                        # send data
                        self.connections[target].send_multipart(chunkPayload, zmq.NOBLOCK)
                        self.log.info("Sending message part from file " + str(sourceFilepath) + " to " + target)

            #close file
            fileDescriptor.close()
            self.log.debug("Passing multipart-message for file " + str(sourceFilepath) + "...done.")

        except Exception, e:
            self.log.error("Unable to send multipart-message for file " + str(sourceFilepath))
            trace = traceback.format_exc()
            self.log.debug("Error was: " + str(trace))
            self.log.debug("Passing multipart-message...failed.")


    def appendFileChunksToPayload(self, payload, sourceFilePathFull, fileDescriptor, chunkSize):
        try:
            # chunksize = 16777216 #16MB
            self.log.debug("reading file '" + str(sourceFilePathFull)+ "' to memory")

            # FIXME: chunk is read-out as str. why not as bin? will probably add to much overhead to zmq-message
            fileContent = fileDescriptor.read(chunkSize)

            while fileContent != "":
                payload.append(fileContent)
                fileContent = fileDescriptor.read(chunkSize)
        except Exception, e:
             self.log("Error was: " + str(e))



    def stop(self):
        self.log.debug("Closing sockets for DataDispatcher-" + str(self.id))
        for connection in self.openConnections:
            if self.openConnections[connection]:
                self.openConnections[connection].close(0)
                self.openConnections[connection] = None
        self.routerSocket.close(0)
        self.routerSocket = None
        if not self.externalContext:
            self.log.debug("Destroying context")
            self.zmqContextForWorker.destroy()


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()
