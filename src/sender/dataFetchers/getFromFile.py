__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import zmq
import os
import sys
import logging
import traceback
import cPickle
import shutil
import errno

from send_helpers import __sendToTargets, DataHandlingError


def setup (log, prop):

    if ( not prop.has_key("fixSubdirs") or
        not prop.has_key("storeData")  or
        not prop.has_key("removeData") ):

        log.error ("Configuration of wrong format")
        log.debug ("dataFetcherProp="+ str(prop))
        return False

    else:
        prop["timeout"]    = -1 #10
        prop["removeFlag"] = False
        return True


def getMetadata (log, prop, targets, metadata, chunkSize, localTarget = None):

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

    if targets:
        try:
            # For quick testing set filesize of file as chunksize
            log.debug("get filesize for '" + str(sourceFile) + "'...")
            filesize       = os.path.getsize(sourceFile)
            fileModTime    = os.stat(sourceFile).st_mtime
            fileCreateTime = os.stat(sourceFile).st_ctime
            chunksize      = filesize    #can be used later on to split multipart message
            log.debug("filesize(%s) = %s" % (sourceFile, str(filesize)))
            log.debug("fileModTime(%s) = %s" % (sourceFile, str(fileModTime)))

        except:
            log.error("Unable to create metadata dictionary.")
            raise

        try:
            log.debug("create metadata for source file...")
            #metadata = {
            #        "filename"       : ...,
            #        "sourcePath"     : ...,
            #        "relativePath"   : ...,
            #        "filesize"       : ...,
            #        "fileModTime"    : ...,
            #        "fileCreateTime" : ...,
            #        "chunkSize"      : ...
            #        }
            metadata[ "filesize"    ]   = filesize
            metadata[ "fileModTime" ]   = fileModTime
            metadata[ "fileCreateTime"] = fileCreateTime
            metadata[ "chunkSize"   ]   = chunkSize

            log.debug("metadata = " + str(metadata))
        except:
            log.error("Unable to assemble multi-part message.")
            raise

    return sourceFile, targetFile, metadata


def sendData (log, targets, sourceFile, targetFile, metadata, openConnections, context, prop):

    if not targets:
        prop["removeFlag"] = True
        return

    targets_data       = [i for i in targets if i[3] == "data"]

    if not targets_data:
        prop["removeFlag"] = True
        return

    prop["removeFlag"] = False
    chunkSize          = metadata[ "chunkSize" ]

    chunkNumber        = 0
    sendError          = False

    #reading source file into memory
    try:
        log.debug("Opening '" + str(sourceFile) + "'...")
        fileDescriptor = open(str(sourceFile), "rb")
    except:
        log.error("Unable to read source file '" + str(sourceFile) + "'.", exc_info=True)
        raise

    log.debug("Passing multipart-message for file " + str(sourceFile) + "...")
    while True:

        #read next chunk from file
        fileContent = fileDescriptor.read(chunkSize)

        #detect if end of file has been reached
        if not fileContent:
            break

        try:
            #assemble metadata for zmq-message
            chunkMetadata = metadata.copy()
            chunkMetadata["chunkNumber"] = chunkNumber

            chunkPayload = []
            chunkPayload.append(cPickle.dumps(chunkMetadata))
            chunkPayload.append(fileContent)
        except:
            log.error("Unable to pack multipart-message for file " + str(sourceFile), exc_info=True)

        #send message to data targets
        try:
            __sendToTargets(log, targets_data, sourceFile, targetFile, openConnections, None, chunkPayload, context)
        except DataHandlingError:
            log.error("Unable to send multipart-message for file " + str(sourceFile) + " (chunk " + str(chunkNumber) + ")", exc_info=True)
            sendError = True
        except:
            log.error("Unable to send multipart-message for file " + str(sourceFile) + " (chunk " + str(chunkNumber) + ")", exc_info=True)

        chunkNumber += 1

    #close file
    try:
        log.debug("Closing '" + str(targetFile) + "'...")
        fileDescriptor.close()
    except:
        log.error("Unable to close target file '" + str(targetFile) + "'.", exc_info=True)
        raise

    if not sendError:
        prop["removeFlag"] = True


def __dataHandling(log, sourceFile, targetFile, actionFunction, metadata, prop):
    try:
        actionFunction(sourceFile, targetFile)
    except IOError as e:

        # errno.ENOENT == "No such file or directory"
        if e.errno == errno.ENOENT:
            subdir, tmp = os.path.split(metadata["relativePath"])

            if metadata["relativePath"] in prop["fixSubdirs"]:
                log.error("Unable to copy/move file '" + sourceFile + "' to '" + targetFile +
                          ": Directory " + metadata["relativePath"] + " is not available.", exc_info=True)
                raise
            elif subdir in prop["fixSubdirs"] :
                log.error("Unable to copy/move file '" + sourceFile + "' to '" + targetFile +
                          ": Directory " + subdir + " is not available.", exc_info=True)
                raise
            else:
                try:
                    targetPath, filename = os.path.split(targetFile)
                    os.makedirs(targetPath)
                    log.info("New target directory created: " + str(targetPath))
                    actionFunction(sourceFile, targetFile)
                except OSError, e:
                    log.info("Target directory creation failed, was already created in the meantime: " + targetPath)
                    actionFunction(sourceFile, targetFile)
                except:
                    log.error("Unable to copy/move file '" + sourceFile + "' to '" + targetFile, exc_info=True)
                    log.debug("targetPath:" + str(targetPath))
        else:
            log.error("Unable to copy/move file '" + sourceFile + "' to '" + targetFile, exc_info=True)
            raise
    except:
        log.error("Unable to copy/move file '" + sourceFile + "' to '" + targetFile, exc_info=True)
        raise


def finishDataHandling (log, targets, sourceFile, targetFile, metadata, openConnections, context, prop):

    targets_metadata = [i for i in targets if i[3] == "metadata"]

    if prop["storeData"] and prop["removeData"] and prop["removeFlag"]:

        # move file
        try:
            __dataHandling(log, sourceFile, targetFile, shutil.move, metadata, prop)
            log.info("Moving file '" + str(sourceFile) + "' ...success.")
        except:
            log.error("Could not move file {f} to {t}".format(f=sourceFile, t=targetFile), exc_info=True)
            return

    elif prop["storeData"]:

        # copy file
        # (does not preserve file owner, group or ACLs)
        try:
            __dataHandling(log, sourceFile, targetFile, shutil.copy, metadata, prop)
            log.info("Copying file '" + str(sourceFile) + "' ...success.")
        except:
            return

    elif prop["removeData"] and prop["removeFlag"]:
        # remove file
        try:
            os.remove(sourceFile)
            log.info("Removing file '" + str(sourceFile) + "' ...success.")
        except:
            log.error("Unable to remove file " + str(sourceFile), exc_info=True)

        prop["removeFlag"] = False

    #send message to metadata targets
    if targets_metadata:
        try:
            __sendToTargets(log, targets_metadata, sourceFile, targetFile, openConnections, metadata, None, context, prop["timeout"] )
            log.debug("Passing metadata multipart-message for file " + str(sourceFile) + "...done.")

        except:
            log.error("Unable to send metadata multipart-message for file " + str(sourceFile) + " to " + str(targets_metadata), exc_info=True)


def clean (prop):
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
    targets = [['localhost:' + receivingPort, 1, "data"], ['localhost:' + receivingPort2, 0, "data"]]

    chunkSize       = 10485760 ; # = 1024*1024*10 = 10 MiB
    localTarget     = BASE_PATH + os.sep + "data" + os.sep + "target"
    openConnections = dict()

    dataFetcherProp = {
            "type"       : "getFromFile",
            "fixSubdirs" : ["commissioning", "current", "local"],
            "storeData"  : False
            }

    logging.debug("openConnections before function call: " + str(openConnections))

    setup(logging, dataFetcherProp)

    sourceFile, targetFile, metadata = getMetadata (logging, workload, chunkSize, localTarget = None)
    sendData(logging, targets, sourceFile, targetFile, metadata, openConnections, context, dataFetcherProp)

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

