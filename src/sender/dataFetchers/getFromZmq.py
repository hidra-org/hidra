__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import zmq
import os
import sys
import logging
import traceback
import cPickle
import shutil
import time

from send_helpers import __sendToTargets

try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( '__file__' ) ))))
#    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) ))))
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers


def setup (log, prop):

    if ( not prop.has_key("context") or
        not prop.has_key("dataFetchConStr") ):

        log.error ("Configuration of wrong format")
        log.debug ("dataFetcherProp="+ str(prop))
        return False

    else:

        # Create zmq socket
        try:
            socket        = prop["context"].socket(zmq.PULL)
            socket.bind(prop["dataFetchConStr"])
            log.info("Start socket (bind): '" + str(prop["dataFetchConStr"]) + "'")
        except:
            log.error("Failed to start comSocket (bind): '" + prop["dataFetchConStr"] + "'", exc_info=True)
            raise

        # register socket
        prop["socket"] = socket

        return True


def getMetadata (log, prop, targets, metadata, chunkSize, localTarget = None):

    #extract fileEvent metadata
    try:
        #TODO validate metadata dict
        sourceFile = metadata["filename"]
    except:
        log.error("Invalid fileEvent message received.", exc_info=True)
        log.debug("metadata=" + str(metadata))
        #skip all further instructions and continue with next iteration
        raise

    #TODO combine better with sourceFile... (for efficiency)
    if localTarget:
        targetFile     = os.path.join(localTarget, sourceFile)
    else:
        targetFile     = None

    if targets:
        try:
            log.debug("create metadata for source file...")
            #metadata = {
            #        "filename"       : ...,
            #        "fileModTime"    : ...,
            #        "fileCreateTime" : ...,
            #        "chunkSize"      : ...
            #        }
            metadata[ "filesize"    ]   = None
            metadata[ "fileModTime" ]   = time.time()
            metadata[ "fileCreateTime"] = time.time()
            metadata[ "chunkSize"   ]   = chunkSize

            log.debug("metadata = " + str(metadata))
        except:
            log.error("Unable to assemble multi-part message.", exc_info=True)
            raise

        return sourceFile, targetFile, metadata
    else:
        return sourceFile, targetFile, dict()


def sendData (log, targets, sourceFile, targetFile, metadata, openConnections, context, prop):

    if not targets:
        return

    #reading source file into memory
    try:
        log.debug("Getting data out of queue for file '" + str(sourceFile) + "'...")
        data = prop["socket"].recv()
    except:
        log.error("Unable to get data out of queue for file '" + str(sourceFile) + "'", exc_info=True)
        raise

    try:
        chunkSize = metadata[ "chunkSize" ]
    except:
        log.error("Unable to get chunkSize", exc_info=True)

    try:
        log.debug("Packing multipart-message for file " + str(sourceFile) + "...")
        chunkNumber = 0

        #assemble metadata for zmq-message
        metadataExtended = metadata.copy()
        metadataExtended["chunkNumber"] = chunkNumber

        payload = []
        payload.append(cPickle.dumps(metadataExtended))
        payload.append(data)
    except:
        log.error("Unable to pack multipart-message for file " + str(sourceFile), exc_info=True)

    #send message
    try:
        __sendToTargets(log, targets, sourceFile, targetFile, openConnections, metadataExtended, payload, context)
        log.debug("Passing multipart-message for file " + str(sourceFile) + "...done.")
    except:
        log.error("Unable to send multipart-message for file " + str(sourceFile), exc_info=True)


def finishDataHandling (log, targets, sourceFile, targetFile, metadata, openConnections, context, prop):
    pass


def clean (prop):
    # Close zmq socket
    if prop["socket"]:
        prop["socket"].close(0)
        prop["socket"] = None


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
    dataFetchConStr  = "ipc://{path}/{id}".format(path="/tmp/zeromq-data-transfer", id="dataFetchConId")

    context          = zmq.Context.instance()

    dataFwSocket     = context.socket(zmq.PUSH)
    dataFwSocket.connect(dataFetchConStr)
    logging.info("=== Start dataFwsocket (connect): '" + str(dataFetchConStr) + "'")

    receivingSocket  = context.socket(zmq.PULL)
    connectionStr    = "tcp://{ip}:{port}".format( ip=extIp, port=receivingPort )
    receivingSocket.bind(connectionStr)
    logging.info("=== receivingSocket connected to " + connectionStr)

    receivingSocket2 = context.socket(zmq.PULL)
    connectionStr    = "tcp://{ip}:{port}".format( ip=extIp, port=receivingPort2 )
    receivingSocket2.bind(connectionStr)
    logging.info("=== receivingSocket2 connected to " + connectionStr)


    prework_sourceFile = BASE_PATH + os.sep + "test_file.cbf"

    #read file to send it in data pipe
    fileDescriptor = open(prework_sourceFile, "rb")
    fileContent = fileDescriptor.read()
    logging.debug("=== File read")
    fileDescriptor.close()

    dataFwSocket.send(fileContent)
    logging.debug("=== File send")

    workload = {
            "sourcePath"  : BASE_PATH + os.sep +"data" + os.sep + "source",
            "relativePath": os.sep + "local" + os.sep + "raw",
            "filename"    : "100.cbf"
            }
    targets = [['localhost:' + receivingPort, 1, [".cbf", ".tif"], "data"], ['localhost:' + receivingPort2, 0, [".cbf", ".tif"], "data"]]

    chunkSize       = 10485760 ; # = 1024*1024*10 = 10 MiB
    localTarget     = BASE_PATH + os.sep + "data" + os.sep + "target"
    openConnections = dict()

    dataFetcherProp = {
            "type"            : "getFromZmq",
            "context"         : context,
            "dataFetchConStr" : dataFetchConStr
            }

    logging.debug("openConnections before function call: " + str(openConnections))

    setup(logging, dataFetcherProp)

    sourceFile, targetFile, metadata = getMetadata (logging, dataFetcherProp, targets, workload, chunkSize, localTarget = None)
    sendData(logging, targets, sourceFile, targetFile, metadata, openConnections, context, dataFetcherProp)

    finishDataHandling(logging, targets, sourceFile, targetFile, metadata, openConnections, context, dataFetcherProp)

    logging.debug("openConnections after function call: " + str(openConnections))


    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: " + str(cPickle.loads(recv_message[0])))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: " + str(cPickle.loads(recv_message[0])))
    except KeyboardInterrupt:
        pass
    finally:
        dataFwSocket.close(0)
        receivingSocket.close(0)
        receivingSocket2.close(0)
        clean(dataFetcherProp)
        context.destroy()

