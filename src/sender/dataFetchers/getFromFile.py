__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import zmq
import os
import sys
import logging
import traceback
import cPickle
import shutil


def setup (log, dataFetcherProp):
    #TODO
    # check if dataFetcherProp has correct format
    return


def getMetadata (log, metadata, chunkSize, localTarget = None):

    #extract fileEvent metadata
    try:
        #TODO validate metadata dict
        filename     = metadata["filename"]
        sourcePath   = metadata["sourcePath"]
        relativePath = metadata["relativePath"]
    except:
        log.error("Invalid fileEvent message received.", exc_info=True)
        log.debug("metadata=" + str(metadata))
        #skip all further instructions and continue with next iteration
        raise

    # filename = "img.tiff"
    # filepath = "C:\dir"
    #
    # -->  sourceFilePathFull = 'C:\\dir\img.tiff'
    sourceFilePath = os.path.normpath(sourcePath + os.sep + relativePath)
    sourceFile     = os.path.join(sourceFilePath, filename)

    #TODO combine better with sourceFile... (for efficiency)
    if localTarget:
        targetFilePath = os.path.normpath(localTarget + os.sep + relativePath)
        targetFile     = os.path.join(targetFilePath, filename)
    else:
        targetFile     = None

    try:
        # For quick testing set filesize of file as chunksize
        log.debug("get filesize for '" + str(sourceFile) + "'...")
        filesize    = os.path.getsize(sourceFile)
        fileModTime = os.stat(sourceFile).st_mtime
        chunksize   = filesize    #can be used later on to split multipart message
        log.debug("filesize(%s) = %s" % (sourceFile, str(filesize)))
        log.debug("fileModTime(%s) = %s" % (sourceFile, str(fileModTime)))

    except:
        log.error("Unable to create metadata dictionary.", exc_info=True)
        raise

    try:
        log.debug("create metadata for source file...")
        #metadata = {
        #        "filename"     : filename,
        #        "sourcePath"   : sourcePath,
        #        "relativePath" : relativePath,
        #        "filesize"     : filesize,
        #        "fileModTime"  : fileModTime,
        #        "chunkSize"    : self.zmqMessageChunkSize
        #        }
        metadata[ "filesize"    ] = filesize
        metadata[ "fileModTime" ] = fileModTime
        metadata[ "chunkSize"   ] = chunkSize

        log.debug("metadata = " + str(metadata))
    except:
        log.error("Unable to assemble multi-part message.", exc_info=True)
        raise

    return sourceFile, targetFile, metadata


def sendData (log, targets, sourceFile, metadata, openConnections, context, properties):

    if not targets:
        properties["removeFlag"] = False
        return

    #reading source file into memory
    try:
        log.debug("Opening '" + str(sourceFile) + "'...")
        fileDescriptor = open(str(sourceFile), "rb")
    except:
        log.error("Unable to read source file '" + str(sourceFile) + "'.", exc_info=True)
        raise

    chunkSize = metadata[ "chunkSize"   ]

    #send message
    try:
        log.debug("Passing multipart-message for file " + str(sourceFile) + "...")
        chunkNumber = 0
        stillChunksToRead = True
        while stillChunksToRead:
            chunkNumber += 1

            #read next chunk from file
            fileContent = fileDescriptor.read(chunkSize)

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

            for target, prio in targets:

                # send data to the data stream to store it in the storage system
                if prio == 0:
                    # socket already known
                    if target in openConnections:
                        tracker = openConnections[target].send_multipart(chunkPayload, copy=False, track=True)
                        log.info("Sending message part from file " + str(sourceFile) + " to '" + target + "' with priority " + str(prio) )
                    else:
                        # open socket
                        socket        = context.socket(zmq.PUSH)
                        connectionStr = "tcp://" + str(target)

                        socket.connect(connectionStr)
                        log.info("Start socket (connect): '" + str(connectionStr) + "'")

                        # register socket
                        openConnections[target] = socket

                        # send data
                        tracker = openConnections[target].send_multipart(chunkPayload, copy=False, track=True)
                        log.info("Sending message part from file " + str(sourceFile) + " to '" + target + "' with priority " + str(prio) )

                    # socket not known
                    if not tracker.done:
                        log.info("Message part from file " + str(sourceFile) + " has not been sent yet, waiting...")
                        tracker.wait()
                        log.info("Message part from file " + str(sourceFile) + " has not been sent yet, waiting...done")

                else:
                    # socket already known
                    if target in openConnections:
                        # send data
                        openConnections[target].send_multipart(chunkPayload, zmq.NOBLOCK)
                        log.info("Sending message part from file " + str(sourceFile) + " to " + target)
                    # socket not known
                    else:
                        # open socket
                        socket        = context.socket(zmq.PUSH)
                        connectionStr = "tcp://" + str(target)

                        socket.connect(connectionStr)
                        log.info("Start socket (connect): '" + str(connectionStr) + "'")

                        # register socket
                        openConnections[target] = socket

                        # send data
                        openConnections[target].send_multipart(chunkPayload, zmq.NOBLOCK)
                        log.info("Sending message part from file " + str(sourceFile) + " to " + target)

        #close file
        fileDescriptor.close()
        log.debug("Passing multipart-message for file " + str(sourceFile) + "...done.")

        properties["removeFlag"] = True

    except:
        log.error("Unable to send multipart-message for file " + str(sourceFile), exc_info=True)


def finishDataHandling (log, sourceFile, targetFile, prop):

    if prop["removeFlag"]:
        # remove file
        try:
            os.remove(sourceFile)
            log.info("Removing file '" + str(sourceFile) + "' ...success.")
        except:
            log.error("Unable to remove file " + str(sourceFile), exc_info=True)

        prop["removeFlag"] = False
    else:
        # move file
        try:
            shutil.move(sourceFile, targetFile)
            log.info("Moving file '" + str(sourceFile) + "' ...success.")
        except:
            log.error("Unable to move file " + str(sourceFile), exc_info=True)

#    # send file to cleaner pipe
#    try:
#        #sending to pipe
#        self.log.debug("send file-event for file to cleaner-pipe...")
#        self.log.debug("metadata = " + str(metadata))
#        self.cleanerSocket.send(cPickle.dumps(metadata))
#        self.log.debug("send file-event for file to cleaner-pipe...success.")
#
#        #TODO: remember workload. append to list?
#        # can be used to verify files which have been processed twice or more
#    except:
#        self.log.error("Unable to notify Cleaner-pipe to handle file: " + str(workload), exc_info=True)


def clean (properties):
    pass


if __name__ == '__main__':
    import time
    from shutil import copyfile

    try:
        BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
    except:
        BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) ))))
    print "BASE_PATH", BASE_PATH
    SHARED_PATH  = BASE_PATH + os.sep + "src" + os.sep + "shared"

    if not SHARED_PATH in sys.path:
        sys.path.append ( SHARED_PATH )
    del SHARED_PATH

    import helpers

    logfile = BASE_PATH + os.sep + "logs" + os.sep + "getFromFile.log"
    logsize = 10485760

    # Get the log Configuration for the lisener
    h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose=True, onScreenLogLevel="debug")

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG) # Log level = DEBUG
    root.addHandler(h1)
    root.addHandler(h2)

    receivingPort    = "6005"
    receivingPort2   = "6006"
    extIp            = "0.0.0.0"

    context          = zmq.Context.instance()

    receivingSocket  = context.socket(zmq.PULL)
    connectionStr    = "tcp://{ip}:{port}".format( ip=extIp, port=receivingPort )
    receivingSocket.bind(connectionStr)
    logging.info("=== receivingSocket connected to " + connectionStr)

    receivingSocket2 = context.socket(zmq.PULL)
    connectionStr    = "tcp://{ip}:{port}".format( ip=extIp, port=receivingPort2 )
    receivingSocket2.bind(connectionStr)
    logging.info("=== receivingSocket2 connected to " + connectionStr)


    prework_sourceFile = BASE_PATH + os.sep + "test_file.cbf"
    prework_targetFile = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep + "100.cbf"

    copyfile(prework_sourceFile, prework_targetFile)
    time.sleep(0.5)

    workload = {
            "sourcePath"  : BASE_PATH + os.sep +"data" + os.sep + "source",
            "relativePath": os.sep + "local" + os.sep + "raw",
            "filename"    : "100.cbf"
            }
    targets = [['localhost:' + receivingPort, 1], ['localhost:' + receivingPort2, 0]]

    chunkSize       = 10485760 ; # = 1024*1024*10 = 10 MiB
    localTarget     = BASE_PATH + os.sep + "data" + os.sep + "target"
    openConnections = dict()

    dataFetcherProp = {
            "type"       : "getFromFile",
            "removeFlag" : False
            }

    logging.debug("openConnections before function call: " + str(openConnections))

    setup(logging, dataFetcherProp)

    sourceFile, targetFile, metadata = getMetadata (logging, workload, chunkSize, localTarget = None)
    sendData(logging, targets, sourceFile, metadata, openConnections, context, dataFetcherProp)

    finishDataHandling(logging, sourceFile, targetFile, dataFetcherProp)

    logging.debug("openConnections after function call: " + str(openConnections))


    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: " + str(cPickle.loads(recv_message[0])))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: " + str(cPickle.loads(recv_message[0])))
    except KeyboardInterrupt:
        pass
    finally:
        receivingSocket.close(0)
        receivingSocket2.close(0)
        clean(dataFetcherProp)
        context.destroy()

