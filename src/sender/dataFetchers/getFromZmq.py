from __future__ import print_function
from __future__ import unicode_literals

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import zmq
import os
import sys
import logging
import traceback
import json
import shutil
import time

from send_helpers import __sendToTargets

try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( '__file__' ) ))))
#    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) ))))
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers


def setup (log, prop):

    if ( not prop.has_key("context") or
        not prop.has_key("dataFetchConStr") ):

        log.error ("Configuration of wrong format")
        log.debug ("dataFetcherProp={p}".format(p=prop))
        return False

    else:

        # Create zmq socket
        try:
            socket        = prop["context"].socket(zmq.PULL)
            socket.bind(prop["dataFetchConStr"])
            log.info("Start socket (bind): '{p}'".format(p=prop["dataFetchConStr"]))
        except:
            log.error("Failed to start comSocket (bind): '{p}'".format(p=prop["dataFetchConStr"]), exc_info=True)
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
        log.debug("metadata={m}".format(m=metadata))
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
            # chunkSize is coming from ZMQDetector

            log.debug("metadata = {m}".format(m=metadata))
        except:
            log.error("Unable to assemble multi-part message.", exc_info=True)
            raise

    return sourceFile, targetFile, metadata


def sendData (log, targets, sourceFile, targetFile, metadata, openConnections, context, prop):

    if not targets:
        return

    #reading source file into memory
    try:
        log.debug("Getting data out of queue for file '{f}'...".format(f=sourceFile))
        data = prop["socket"].recv()
    except:
        log.error("Unable to get data out of queue for file '{f}'".format(f=sourceFile), exc_info=True)
        raise

    try:
        chunkSize = metadata[ "chunkSize" ]
    except:
        log.error("Unable to get chunkSize", exc_info=True)

    try:
        log.debug("Packing multipart-message for file {f}...".format(f=sourceFile))
        chunkNumber = 0

        #assemble metadata for zmq-message
        metadataExtended = metadata.copy()
        metadataExtended["chunkNumber"] = chunkNumber

        payload = []
        payload.append(json.dumps(metadataExtended).encode("utf-8"))
        payload.append(data)
    except:
        log.error("Unable to pack multipart-message for file '{f}'".format(f=sourceFile), exc_info=True)

    #send message
    try:
        __sendToTargets(log, targets, sourceFile, targetFile, openConnections, metadataExtended, payload, context)
        log.debug("Passing multipart-message for file '{f}'...done.".format(f=sourceFile))
    except:
        log.error("Unable to send multipart-message for file '{f}'".format(f=sourceFile), exc_info=True)


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
    import tempfile

    try:
        BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
    except:
        BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) ))))
    print ("BASE_PATH", BASE_PATH)
    SHARED_PATH  = os.path.join(BASE_PATH, "src", "shared")

    if not SHARED_PATH in sys.path:
        sys.path.append ( SHARED_PATH )
    del SHARED_PATH

    import helpers

    logfile = os.path.join(BASE_PATH, "logs", "getFromFile.log")
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
    dataFetchConStr  = "ipc://{path}/{id}".format(path= os.path.join(tempfile.gettempdir(), "hidra"), id="dataFetch")

    context          = zmq.Context.instance()

    dataFwSocket     = context.socket(zmq.PUSH)
    dataFwSocket.connect(dataFetchConStr)
    logging.info("=== Start dataFwsocket (connect): '{s}'".format(s=dataFetchConStr))

    receivingSocket  = context.socket(zmq.PULL)
    connectionStr    = "tcp://{ip}:{port}".format( ip=extIp, port=receivingPort )
    receivingSocket.bind(connectionStr)
    logging.info("=== receivingSocket connected to {s}".format(s=connectionStr))

    receivingSocket2 = context.socket(zmq.PULL)
    connectionStr    = "tcp://{ip}:{port}".format( ip=extIp, port=receivingPort2 )
    receivingSocket2.bind(connectionStr)
    logging.info("=== receivingSocket2 connected to {s}".format(s=connectionStr))


    prework_sourceFile = os.path.join(BASE_PATH, "test_file.cbf")

    #read file to send it in data pipe
    fileDescriptor = open(prework_sourceFile, "rb")
    fileContent = fileDescriptor.read()
    logging.debug("=== File read")
    fileDescriptor.close()

    dataFwSocket.send(fileContent)
    logging.debug("=== File send")

    workload = {
            "sourcePath"  : os.path.join(BASE_PATH, "data", "source"),
            "relativePath": os.sep + "local" + os.sep + "raw",
            "filename"    : "100.cbf"
            }
    targets = [['localhost:{p}'.format(p=receivingPort), 1, [".cbf", ".tif"], "data"], ['localhost:{p}'.format(p=receivingPort2), 0, [".cbf", ".tif"], "data"]]

    chunkSize       = 10485760 ; # = 1024*1024*10 = 10 MiB
    localTarget     = os.path.join(BASE_PATH, "data", "target")
    openConnections = dict()

    dataFetcherProp = {
            "type"            : "getFromZmq",
            "context"         : context,
            "dataFetchConStr" : dataFetchConStr
            }

    logging.debug("openConnections before function call: {c}".format(c=openConnections))

    setup(logging, dataFetcherProp)

    sourceFile, targetFile, metadata = getMetadata (logging, dataFetcherProp, targets, workload, chunkSize, localTarget = None)
    sendData(logging, targets, sourceFile, targetFile, metadata, openConnections, context, dataFetcherProp)

    finishDataHandling(logging, targets, sourceFile, targetFile, metadata, openConnections, context, dataFetcherProp)

    logging.debug("openConnections after function call: {c}".format(c=openConnections))


    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: {0}".format(json.loads(recv_message[0].decode("utf-8"))))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: {0}".format(json.loads(recv_message[0].decode("utf-8"))))
    except KeyboardInterrupt:
        pass
    finally:
        dataFwSocket.close(0)
        receivingSocket.close(0)
        receivingSocket2.close(0)
        clean(dataFetcherProp)
        context.destroy()

