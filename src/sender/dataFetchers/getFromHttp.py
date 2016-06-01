__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Jan Garrevoet <jan,garrevoet@desy.de>'

import zmq
import os
import sys
import logging
import cPickle
import urllib
import requests
import re
import urllib2
import time
import errno

from send_helpers import __sendToTargets


def setup (log, prop):

    if ( not prop.has_key("session") or
        not prop.has_key("storeData") or
        not prop.has_key("removeData") or
        not prop.has_key("fixSubdirs")):

        log.error ("Configuration of wrong format")
        log.debug ("dataFetcherProp="+ str(prop))
        return False

    else:

        prop["session"] = requests.session()
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


    # no normpath used because that would transform http://... into http:/...
    sourceFilePath = sourcePath + os.sep + relativePath
    sourceFile     = os.path.join(sourceFilePath, filename)

    #TODO combine better with sourceFile... (for efficiency)
    if localTarget:
        targetFilePath = os.path.normpath(localTarget + os.sep + relativePath)
        targetFile     = os.path.join(targetFilePath, filename)
    else:
        targetFile = None

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
        metadata[ "fileModTime" ]   = time.time()
        metadata[ "fileCreateTime"] = time.time()
        metadata[ "chunkSize" ]     = chunkSize

        log.debug("metadata = " + str(metadata))
    except:
        log.error("Unable to assemble multi-part message.", exc_info=True)
        raise

    return sourceFile, targetFile, metadata


def sendData (log, targets, sourceFile, targetFile,  metadata, openConnections, context, prop):

    response = prop["session"].get(sourceFile)
    try:
        response.raise_for_status()
        log.debug("Initiating http get for file '" + sourceFile + "' succeded.")
    except:
        log.error("Initiating http get for file '" + sourceFile + "' failed.", exc_info=True)
        return

    try:
        chunkSize = metadata[ "chunkSize" ]
    except:
        log.error("Unable to get chunkSize", exc_info=True)

    fileOpened  = False
    fileWritten = True
    fileClosed  = False
    fileSend    = True

    if prop["storeData"]:
        try:
            log.debug("Opening '" + targetFile + "'...")
            fileDescriptor = open(targetFile, "wb")
            fileOpened = True
        except IOError, e:
            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:

                subdir, tmp = os.path.split(metadata["relativePath"])

                if metadata["relativePath"] in prop["fixSubdirs"]:
                    log.error("Unable to move file '" + sourceFile + "' to '" + targetFile +
                              ": Directory " + metadata["relativePath"] + " is not available.", exc_info=True)

                elif subdir in prop["fixSubdirs"] :
                    log.error("Unable to move file '" + sourceFile + "' to '" + targetFile +
                              ": Directory " + subdir + " is not available.", exc_info=True)
                else:
                    try:
                        targetPath, filename = os.path.split(targetFile)
                        os.makedirs(targetPath)
                        fileDescriptor = open(targetFile, "w")
                        log.info("New target directory created: " + targetPath)
                        fileOpened = True
                    except OSError, e:
                        log.info("Target directory creation failed, was already created in the meantime: " + targetPath)
                        fileDescriptor = open(targetFile, "w")
                        fileOpened = True
                    except:
                        log.error("Unable to open target file '" + targetFile + "'.", exc_info=True)
                        log.debug("targetPath:" + targetPath)
                        raise
            else:
                log.error("Unable to open target file '" + targetFile + "'.", exc_info=True)
        except:
            log.error("Unable to open target file '" + targetFile + "'.", exc_info=True)

    targets_data     = [i for i in targets if i[3] == "data"]
    targets_metadata = [i for i in targets if i[3] == "metadata"]
    chunkNumber      = 0

    log.debug("Getting data for file '" + str(sourceFile) + "'...")
    #reading source file into memory
    for data in response.iter_content(chunk_size=chunkSize):
        log.debug("Packing multipart-message for file " + str(sourceFile) + "...")

        try:
            #assemble metadata for zmq-message
            metadataExtended = metadata.copy()
            metadataExtended["chunkNumber"] = chunkNumber

            payload = []
            payload.append(cPickle.dumps(metadataExtended))
            payload.append(data)
        except:
            log.error("Unable to pack multipart-message for file " + sourceFile, exc_info=True)

        if prop["storeData"]:
            try:
                fileDescriptor.write(data)
            except:
                log.error("Unable write data for file " + sourceFile, exc_info=True)
                fileWritten = False


        #send message to data targets
        try:
            __sendToTargets(log, targets_data, sourceFile, targetFile, openConnections, metadataExtended, payload, context)
            log.debug("Passing multipart-message for file " + sourceFile + "...done.")

        except:
            log.error("Unable to send multipart-message for file " + sourceFile, exc_info=True)
            fileSend = False

        chunkNumber += 1

    if prop["storeData"]:
        try:
            log.debug("Closing '" + targetFile + "'...")
            fileDescriptor.close()
            fileClosed = True
        except:
            log.error("Unable to close target file '" + targetFile + "'.", exc_info=True)
            raise

        # update the creation and modification time
        metadataExtended[ "fileModTime" ]   = os.stat(targetFile).st_mtime
        metadataExtended[ "fileCreateTime"] = os.stat(targetFile).st_ctime

        #send message to metadata targets
        try:
            __sendToTargets(log, targets_metadata, sourceFile, targetFile,
                            openConnections, metadataExtended, payload,
                            context)
            log.debug("Passing metadata multipart-message for file " + sourceFile + "...done.")

        except:
            log.error("Unable to send metadata multipart-message for file " + sourceFile, exc_info=True)

        prop["removeFlag"] = fileOpened and fileWritten and fileClosed
    else:
        prop["removeFlag"] = fileSend



def finishDataHandling (log, targets, sourceFile, targetFile, metadata,
                        openConnections, context, prop):

    if prop["removeData"] and prop["removeFlag"]:
        responce = requests.delete(sourceFile)

        try:
	    responce.raise_for_status()
	    log.debug("Deleting file " + sourceFile + " succeded.")
        except:
            log.error("Deleting file " + sourceFile + " failed.", exc_info=True)


def clean (prop):
    pass


if __name__ == '__main__':
    from shutil import copyfile
    import subprocess

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

    logfile = BASE_PATH + os.sep + "logs" + os.sep + "getFromHttp.log"
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
    dataFwPort       = "50010"

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
    localTarget        = BASE_PATH + os.sep + "data" + os.sep + "target"

    #read file to send it in data pipe
    logging.debug("=== copy file to lsdma-lab04")
#    os.system('scp "%s" "%s:%s"' % (localfile, remotehost, remotefile) )
    subprocess.call("scp " + prework_sourceFile + " root@lsdma-lab04:/var/www/html/test_httpget/data", shell=True)

    workload = {
            "sourcePath"  : "http://192.168.138.37/data",
            "relativePath": "testp06",
            "filename"    : "35_data_000170.h5"
            }
#    workload = {
#            "sourcePath"  : "http://131.169.55.170/test_httpget/data",
#            "relativePath": "",
#            "filename"    : "test_file.cbf"
#            }
    targets = [['localhost:' + receivingPort, 1, "data"], ['localhost:' + receivingPort2, 1, "data"]]

    chunkSize       = 10485760 ; # = 1024*1024*10 = 10 MiB
    localTarget     = BASE_PATH + os.sep + "data" + os.sep + "target"
    openConnections = dict()

    dataFetcherProp = {
            "type"       : "getFromHttp",
            "session"    : None,
            "storeData"  : True,
            "removeData" : False
            }

    setup(logging, dataFetcherProp)

    sourceFile, targetFile, metadata = getMetadata (logging, workload, chunkSize, localTarget)
#    sourceFile = "http://131.169.55.170/test_httpget/data/test_file.cbf"

    sendData(logging, targets, sourceFile, targetFile, metadata, openConnections, context, dataFetcherProp)

    finishDataHandling(logging, sourceFile, targetFile, dataFetcherProp)

    logging.debug("openConnections after function call: " + str(openConnections))


    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: " + str(cPickle.loads(recv_message[0])))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: " + str(cPickle.loads(recv_message[0])) + "\ndata-part: " + str(len(recv_message[1])))
    except KeyboardInterrupt:
        pass
    finally:
        receivingSocket.close(0)
        receivingSocket2.close(0)
        clean(dataFetcherProp)
        context.destroy()

