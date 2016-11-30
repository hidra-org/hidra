from __future__ import print_function
from __future__ import unicode_literals

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Jan Garrevoet <jan,garrevoet@desy.de>'

import zmq
import os
import sys
import logging
import json
import urllib
import requests
import re
import urllib2
import time
import errno

from send_helpers import __send_to_targets


def setup (log, prop):

    if ( not prop.has_key("session") or
        not prop.has_key("storeData") or
        not prop.has_key("removeData") or
        not prop.has_key("fixSubdirs")):

        log.error ("Configuration of wrong format")
        log.debug ("dataFetcherProp={0}".format(prop))
        return False

    else:

        prop["session"] = requests.session()
        prop["removeFlag"] = False

        return True


def get_metadata (log, prop, targets, metadata, chunkSize, localTarget = None):

    #extract fileEvent metadata
    try:
        #TODO validate metadata dict
        filename     = metadata["filename"]
        sourcePath   = metadata["sourcePath"]
        relativePath = metadata["relativePath"]
    except:
        log.error("Invalid fileEvent message received.", exc_info=True)
        log.debug("metadata={0}".format(metadata))
        #skip all further instructions and continue with next iteration
        raise


    # no normpath used because that would transform http://... into http:/...
    sourceFilePath = os.path.join(sourcePath, relativePath)
    sourceFile     = os.path.join(sourceFilePath, filename)

    #TODO combine better with sourceFile... (for efficiency)
    if localTarget:
        targetFilePath = os.path.normpath(os.path.join(localTarget, relativePath))
        targetFile     = os.path.join(targetFilePath, filename)
    else:
        targetFile = None

    metadata[ "chunkSize" ]     = chunkSize

    if targets:
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

            log.debug("metadata = {0}".format(metadata))
        except:
            log.error("Unable to assemble multi-part message.", exc_info=True)
            raise

    return sourceFile, targetFile, metadata


def send_data (log, targets, sourceFile, targetFile,  metadata, openConnections, context, prop):

    response = prop["session"].get(sourceFile)
    try:
        response.raise_for_status()
        log.debug("Initiating http get for file '{0}' succeeded.".format(sourceFile))
    except:
        log.error("Initiating http get for file '{0}' failed.".format(sourceFile), exc_info=True)
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
            log.debug("Opening '{0}'...".format(targetFile))
            fileDescriptor = open(targetFile, "wb")
            fileOpened = True
        except IOError as e:
            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:

                subdir, tmp = os.path.split(metadata["relativePath"])

                if metadata["relativePath"] in prop["fixSubdirs"]:
                    log.error("Unable to move file '{0}' to '{1}': Directory {2} is not available.".format(sourceFile, targetFile, metadata["relativePath"]), exc_info=True)

                elif subdir in prop["fixSubdirs"] :
                    log.error("Unable to move file '{0}' to '{1}': Directory {2} is not available.".format(sourceFile, targetFile, subdir), exc_info=True)
                else:
                    try:
                        targetPath, filename = os.path.split(targetFile)
                        os.makedirs(targetPath)
                        fileDescriptor = open(targetFile, "wb")
                        log.info("New target directory created: {0}".format(targetPath))
                        fileOpened = True
                    except OSError as e:
                        log.info("Target directory creation failed, was already created in the meantime: {0}".format(targetPath))
                        fileDescriptor = open(targetFile, "wb")
                        fileOpened = True
                    except:
                        log.error("Unable to open target file '{0}'.".format(targetFile), exc_info=True)
                        log.debug("targetPath: {0}".format(targetPath))
                        raise
            else:
                log.error("Unable to open target file '{0}'.".format(targetFile), exc_info=True)
        except:
            log.error("Unable to open target file '{0}'.".format(targetFile), exc_info=True)

    targets_data     = [i for i in targets if i[3] == "data"]
    targets_metadata = [i for i in targets if i[3] == "metadata"]
    chunkNumber      = 0

    log.debug("Getting data for file '{0}'...".format(sourceFile))
    #reading source file into memory
    for data in response.iter_content(chunk_size=chunkSize):
        log.debug("Packing multipart-message for file '{0}'...".format(sourceFile))

        try:
            #assemble metadata for zmq-message
            metadataExtended = metadata.copy()
            metadataExtended["chunkNumber"] = chunkNumber

            payload = []
            payload.append(json.dumps(metadataExtended).encode("utf-8"))
            payload.append(data)
        except:
            log.error("Unable to pack multipart-message for file '{0}'".format(sourceFile), exc_info=True)

        if prop["storeData"]:
            try:
                fileDescriptor.write(data)
            except:
                log.error("Unable write data for file '{0}'".format(sourceFile), exc_info=True)
                fileWritten = False


        #send message to data targets
        try:
            __send_to_targets(log, targets_data, sourceFile, targetFile, openConnections, metadataExtended, payload, context)
            log.debug("Passing multipart-message for file {0}...done.".format(sourceFile))

        except:
            log.error("Unable to send multipart-message for file {0}".format(sourceFile), exc_info=True)
            fileSend = False

        chunkNumber += 1

    if prop["storeData"]:
        try:
            log.debug("Closing '{0}'...".format(targetFile))
            fileDescriptor.close()
            fileClosed = True
        except:
            log.error("Unable to close target file '{0}'.".format(targetFile), exc_info=True)
            raise

        # update the creation and modification time
        metadataExtended[ "fileModTime" ]   = os.stat(targetFile).st_mtime
        metadataExtended[ "fileCreateTime"] = os.stat(targetFile).st_ctime

        #send message to metadata targets
        try:
            __send_to_targets(log, targets_metadata, sourceFile, targetFile,
                            openConnections, metadataExtended, payload,
                            context)
            log.debug("Passing metadata multipart-message for file '{0}'...done.".format(sourceFile))

        except:
            log.error("Unable to send metadata multipart-message for file '{0}'".format(sourceFile), exc_info=True)

        prop["removeFlag"] = fileOpened and fileWritten and fileClosed
    else:
        prop["removeFlag"] = fileSend



def finish_data_handling (log, targets, sourceFile, targetFile, metadata,
                        openConnections, context, prop):

    if prop["removeData"] and prop["removeFlag"]:
        responce = requests.delete(sourceFile)

        try:
	    responce.raise_for_status()
	    log.debug("Deleting file '{0}' succeeded.".format(sourceFile))
        except:
            log.error("Deleting file '{0}' failed.".format(sourceFile), exc_info=True)


def clean (prop):
    pass


if __name__ == '__main__':
    from shutil import copyfile
    import subprocess

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

    logfile = os.path.join(BASE_PATH, "logs", "getFromHttp.log")
    logsize = 10485760

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True, onScreenLogLevel="debug")

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
    logging.info("=== receivingSocket connected to {0}".format(connectionStr))

    receivingSocket2 = context.socket(zmq.PULL)
    connectionStr    = "tcp://{ip}:{port}".format( ip=extIp, port=receivingPort2 )
    receivingSocket2.bind(connectionStr)
    logging.info("=== receivingSocket2 connected to {0}".format(connectionStr))


    prework_sourceFile = os.path.join(BASE_PATH, "test_file.cbf")
    localTarget        = os.path.join(BASE_PATH, "data", "target")

    #read file to send it in data pipe
    logging.debug("=== copy file to lsdma-lab04")
#    os.system('scp "%s" "%s:%s"' % (localfile, remotehost, remotefile) )
    subprocess.call("scp {0} root@lsdma-lab04:/var/www/html/test_httpget/data".format(prework_sourceFile), shell=True)

#    workload = {
#            "sourcePath"  : "http://192.168.138.37/data",
#            "relativePath": "",
#            "filename"    : "35_data_000170.h5"
#            }
    workload = {
            "sourcePath"  : "http://131.169.55.170/test_httpget/data",
            "relativePath": "",
            "filename"    : "test_file.cbf"
            }
    targets = [['localhost:{0}'.format(receivingPort), 1, [".cbf", ".tif"], "data"],
            ['localhost:{0}'.format(receivingPort2), 1, [".cbf", ".tif"], "data"]]

    chunkSize       = 10485760 ; # = 1024*1024*10 = 10 MiB
    localTarget     = os.path.join(BASE_PATH, "data", "target")
    openConnections = dict()

    dataFetcherProp = {
            "type"       : "getFromHttp",
            "session"    : None,
            "fixSubdirs" : ["commissioning", "current", "local"],
            "storeData"  : True,
            "removeData" : False
            }

    setup(logging, dataFetcherProp)

    sourceFile, targetFile, metadata = get_metadata (logging, dataFetcherProp, targets, workload, chunkSize, localTarget)
#    sourceFile = "http://131.169.55.170/test_httpget/data/test_file.cbf"

    send_data(logging, targets, sourceFile, targetFile, metadata, openConnections, context, dataFetcherProp)

    finish_data_handling(logging, targets, sourceFile, targetFile, metadata, openConnections, context, dataFetcherProp)

    logging.debug("openConnections after function call: {0}".format(openConnections))


    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: {0}".format(json.loads(recv_message[0].decode("utf-8"))))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: {0}\ndata-part: {0}".format(json.loads(recv_message[0].decode("utf-8")), len(recv_message[1])))
    except KeyboardInterrupt:
        pass
    finally:
        receivingSocket.close(0)
        receivingSocket2.close(0)
        clean(dataFetcherProp)
        context.destroy()

