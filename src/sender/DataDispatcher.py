from __builtin__ import open, type

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import zmq
import os
import sys
import logging
import traceback
import cPickle
import shutil

#path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
#BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
SHARED_PATH  = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

#
#  --------------------------  class: DataDispatcher  --------------------------------------
#
class DataDispatcher():

    def __init__(self, id, routerPort, chunkSize, fixedStreamId, localTarget = None, context = None):

        self.log          = self.getLogger()
        self.id           = id

        self.log.debug("DataDispatcher Nr. " + str(self.id) + " started.")

        self.localhost    = "127.0.0.1"
        self.extIp        = "0.0.0.0"
        self.routerPort   = routerPort
        self.chunkSize    = chunkSize

        self.routerSocket = None

        self.fixedStreamId = fixedStreamId
        self.localTarget = localTarget

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
        self.routerSocket.connect(connectionStr)
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
            message = self.routerSocket.recv_multipart()
            self.log.debug("DataDispatcher-" + str(self.id) + ": new job received")
            self.log.debug("message = " + str(message))


            if len(message) >= 2:
                workload = cPickle.loads(message[0])
                targets  = cPickle.loads(message[1])
                if self.fixedStreamId:
                    targets.insert(0,[self.fixedStreamId, 0])
                # sort the target list by the priority
                targets = sorted(targets, key=lambda target: target[1])
            else:
                finished = message[0] == b"EXIT"
                if finished:
                    self.log.debug("Router requested to shutdown DataDispatcher-"+ str(self.id) + ".")
                    break

                workload = cPickle.loads(message[0])
                if self.fixedStreamId:
                    targets = [[self.fixedStreamId, 0]]
                else:
                    targets = None

            # get metadata of the file
            try:
                self.log.debug("Getting file metadata")
                sourceFile, targetFile, metadata = self.getMetadata(workload)
            except Exception as e:
                self.log.error("Building of metadata dictionary failed for workload: " + str(workload) + ".")
                self.log.debug("Error was: " + str(e))
                #skip all further instructions and continue with next iteration
                continue

            # send data
            if targets:
                try:
                    self.sendData(targets, sourceFile, metadata)
                except Exception as e:
                    self.log.debug("DataDispatcher-"+str(self.id) + ": Passing new file to data Stream...failed.")
                    self.log.debug("Error was: " + str(e))

                # remove file
                try:
                    os.remove(sourceFile)
                    self.log.info("Removing file '" + str(sourceFile) + "' ...success.")
                except IOError:
                    self.log.debug ("IOError: " + str(sourceFile))
                except Exception, e:
                    trace = traceback.format_exc()
                    self.log.debug("Unable to remove file {FILE}.".format(FILE=str(sourceFile)))
                    self.log.debug("trace=" + str(trace))
            else:
                # move file
                try:
                    shutil.move(sourceFile, targetFile)
                    self.log.info("Moving file '" + str(sourceFile) + "' ...success.")
                except IOError:
                    self.log.debug ("IOError: " + str(sourceFile))
                except Exception, e:
                    trace = traceback.format_exc()
                    self.log.debug("Unable to move file {FILE}.".format(FILE=str(sourceFile)))
                    self.log.debug("trace=" + str(trace))


            # send file to cleaner pipe
#            try:
#                #sending to pipe
#                self.log.debug("send file-event for file to cleaner-pipe...")
#                self.log.debug("metadata = " + str(metadata))
#                self.cleanerSocket.send(cPickle.dumps(metadata))
#                self.log.debug("send file-event for file to cleaner-pipe...success.")
#
#                #TODO: remember workload. append to list?
#                # can be used to verify files which have been processed twice or more
#            except Exception, e:
#                self.log.error("Unable to notify Cleaner-pipe to handle file: " + str(workload))
#

    def getMetadata(self, metadata):

        #extract fileEvent metadata
        try:
            #TODO validate metadata dict
            filename     = metadata["filename"]
            sourcePath   = metadata["sourcePath"]
            relativePath = metadata["relativePath"]
        except Exception as e:
            self.log.info("Invalid fileEvent message received.")
            trace = traceback.format_exc()
            self.log.debug("Error was: " + str(trace))
            self.log.debug("metadata=" + str(metadata))
            #skip all further instructions and continue with next iteration
            raise Exception(e)

        # filename = "img.tiff"
        # filepath = "C:\dir"
        #
        # -->  sourceFilePathFull = 'C:\\dir\img.tiff'
        sourceFilePath     = os.path.normpath(sourcePath + os.sep + relativePath)
        sourceFilePathFull = os.path.join(sourceFilePath, filename)

        #TODO combine better with sourceFile... (for efficiency)
        if self.localTarget:
            targetFilePath     = os.path.normpath(self.localTarget + os.sep + relativePath)
            targetFilePathFull = os.path.join(targetFilePath, filename)
        else:
            targetFilePathFull = None

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
            metadata[ "chunkSize"            ] = self.chunkSize

            self.log.debug("metadata = " + str(metadata))
        except Exception as e:
            self.log.info("Unable to assemble multi-part message.")
            self.log.debug("Error was: " + str(e))
            raise Exception(e)

        return sourceFilePathFull, targetFilePathFull, metadata


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
            while stillChunksToRead:
                chunkNumber += 1

                #read next chunk from file
                fileContent = fileDescriptor.read(self.chunkSize)

                #detect if end of file has been reached
                if not fileContent:
                    stillChunksToRead = False

                    #as chunk is empty decrease chunck-counter
                    chunkNumber -= 1
                    break

                #assemble metadata for zmq-message
                chunkPayloadMetadata = metadata.copy()
                chunkPayloadMetadata["chunkNumber"] = chunkNumber
                chunkPayloadMetadataJson = cPickle.dumps(chunkPayloadMetadata)
                chunkPayload = []
                chunkPayload.append(chunkPayloadMetadataJson)
                chunkPayload.append(fileContent)

                # streaming data

                print "targets", targets
                for target, prio in targets:

                    # send data to the data stream to store it in the storage system
                    if prio == 0:
                        # socket already known
                        if target in self.openConnections:
                            tracker = self.openConnections[target].send_multipart(chunkPayload, copy=False, track=True)
                            self.log.info("Sending message part from file " + str(sourceFilepath) + " to '" + target + "' with priority " + str(prio) )
                        else:
                            # open socket
                            socket        = self.context.socket(zmq.PUSH)
                            connectionStr = "tcp://" + str(target)

                            socket.connect(connectionStr)
                            self.log.info("Start socket (connect): '" + str(connectionStr) + "'")

                            # register socket
                            self.openConnections[target] = socket

                            # send data
                            tracker = self.openConnections[target].send_multipart(chunkPayload, copy=False, track=True)
                            self.log.info("Sending message part from file " + str(sourceFilepath) + " to '" + target + "' with priority " + str(prio) )

                        # socket not known
                        if not tracker.done:
                            self.log.info("Message part from file " + str(sourceFilepath) + " has not been sent yet, waiting...")
                            tracker.wait()
                            self.log.info("Message part from file " + str(sourceFilepath) + " has not been sent yet, waiting...done")

                    else:
                        # socket already known
                        if target in self.openConnections:
                            # send data
                            self.openConnections[target].send_multipart(chunkPayload, zmq.NOBLOCK)
                            self.log.info("Sending message part from file " + str(sourceFilepath) + " to " + target)
                        # socket not known
                        else:
                            # open socket
                            socket        = self.context.socket(zmq.PUSH)
                            connectionStr = "tcp://" + str(target)

                            socket.connect(connectionStr)
                            self.log.info("Start socket (connect): '" + str(connectionStr) + "'")

                            # register socket
                            self.openConnections[target] = socket

                            # send data
                            self.openConnections[target].send_multipart(chunkPayload, zmq.NOBLOCK)
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
        if not self.extContext:
            self.log.debug("Destroying context")
            self.context.destroy()


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()

if __name__ == '__main__':
    from multiprocessing import Process
    import time
    from shutil import copyfile

    logfile = BASE_PATH + os.sep + "logs" + os.sep + "dataDispatcher.log"

    #enable logging
    helpers.initLogging(logfile, verbose=True, onScreenLogLevel="debug")

    sourceFile = BASE_PATH + os.sep + "test_file.cbf"
    targetFile = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep + "100.cbf"


    copyfile(sourceFile, targetFile)
    time.sleep(0.5)


    routerPort    = "7000"
    receivingPort = "6005"
    receivingPort2 = "6006"
    chunkSize     = 10485760 ; # = 1024*1024*10 = 10 MiB

    localTarget   = BASE_PATH + os.sep + "data" + os.sep + "target"
    fixedStreamId = False
    fixedStreamId = "zitpcx19282:6006"

    dataDispatcherPr = Process ( target = DataDispatcher, args = ( 1, routerPort, chunkSize, fixedStreamId, localTarget) )
    dataDispatcherPr.start()

    context       = zmq.Context.instance()

    routerSocket  = context.socket(zmq.PUSH)
    connectionStr = "tcp://127.0.0.1:" + routerPort
    routerSocket.bind(connectionStr)
    logging.info("=== routerSocket connected to " + connectionStr)

    receivingSocket = context.socket(zmq.PULL)
    connectionStr   = "tcp://0.0.0.0:" + receivingPort
    receivingSocket.bind(connectionStr)
    logging.info("=== receivingSocket connected to " + connectionStr)

    receivingSocket2 = context.socket(zmq.PULL)
    connectionStr   = "tcp://0.0.0.0:" + receivingPort2
    receivingSocket2.bind(connectionStr)
    logging.info("=== receivingSocket2 connected to " + connectionStr)


    metadata = {
            "sourcePath"  : BASE_PATH + os.sep +"data" + os.sep + "source",
            "relativePath": os.sep + "local" + os.sep + "raw",
            "filename"    : "100.cbf"
            }
    targets = [['zitpcx19282:6005', 1], ['zitpcx19282:6006', 0]]

    message = [ cPickle.dumps(metadata), cPickle.dumps(targets) ]
#    message = [ cPickle.dumps(metadata)]

    time.sleep(1)

    workload = routerSocket.send_multipart(message)
    logging.info("=== send message")

    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: " + str(cPickle.loads(recv_message[0])))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: " + str(cPickle.loads(recv_message[0])))
    except KeyboardInterrupt:
        pass
    finally:
        dataDispatcherPr.terminate()

        routerSocket.close(0)
        receivingSocket.close(0)
        receivingSocket2.close(0)
        context.destroy()
