from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import sys
import logging
import json
import shutil
import errno

from send_helpers import __send_to_targets, DataHandlingError

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def setup(log, prop):

    if ("fixSubdirs" not in prop
            or "storeData" not in prop
            or "removeData" not in prop):

        log.error("Configuration of wrong format")
        log.debug("dataFetcherProp={0}".format(prop))
        return False

    else:
        prop["timeout"] = -1  # 10
        prop["removeFlag"] = False
        return True


def get_metadata(log, prop, targets, metadata, chunkSize, localTarget=None):

    # extract fileEvent metadata
    try:
        # TODO validate metadata dict
        filename = metadata["filename"]
        sourcePath = metadata["sourcePath"]
        relativePath = metadata["relativePath"]
    except:
        log.error("Invalid fileEvent message received.", exc_info=True)
        log.debug("metadata={0}".format(metadata))
        # skip all further instructions and continue with next iteration
        raise

    # filename = "img.tiff"
    # filepath = "C:\dir"
    #
    # -->  sourceFilePathFull = 'C:\\dir\img.tiff'
    sourceFilePath = os.path.normpath(os.path.join(sourcePath, relativePath))
    sourceFile = os.path.join(sourceFilePath, filename)

    # TODO combine better with sourceFile... (for efficiency)
    if localTarget:
        targetFilePath = os.path.normpath(os.path.join(localTarget,
                                                       relativePath))
        targetFile = os.path.join(targetFilePath, filename)
    else:
        targetFile = None

    if targets:
        try:
            log.debug("get filesize for '{0}'...".format(sourceFile))
            filesize = os.path.getsize(sourceFile)
            fileModTime = os.stat(sourceFile).st_mtime
            fileCreateTime = os.stat(sourceFile).st_ctime
            # For quick testing set filesize of file as chunksize
            # chunksize can be used later on to split multipart message
#            chunksize = filesize
            log.debug("filesize({0}) = {1}".format(sourceFile, filesize))
            log.debug("fileModTime({0}) = {1}".format(sourceFile, fileModTime))

        except:
            log.error("Unable to create metadata dictionary.")
            raise

        try:
            log.debug("create metadata for source file...")
            # metadata = {
            #        "filename"       : ...,
            #        "sourcePath"     : ...,
            #        "relativePath"   : ...,
            #        "filesize"       : ...,
            #        "fileModTime"    : ...,
            #        "fileCreateTime" : ...,
            #        "chunkSize"      : ...
            #        }
            metadata["filesize"] = filesize
            metadata["fileModTime"] = fileModTime
            metadata["fileCreateTime"] = fileCreateTime
            metadata["chunkSize"] = chunkSize

            log.debug("metadata = {0}".format(metadata))
        except:
            log.error("Unable to assemble multi-part message.")
            raise

    return sourceFile, targetFile, metadata


def send_data(log, targets, sourceFile, targetFile, metadata, openConnections,
              context, prop):

    if not targets:
        prop["removeFlag"] = True
        return

    targets_data = [i for i in targets if i[3] == "data"]

    if not targets_data:
        prop["removeFlag"] = True
        return

    prop["removeFlag"] = False
    chunkSize = metadata["chunkSize"]

    chunkNumber = 0
    sendError = False

    # reading source file into memory
    try:
        log.debug("Opening '{0}'...".format(sourceFile))
        fileDescriptor = open(str(sourceFile), "rb")
    except:
        log.error("Unable to read source file '{0}'.".format(sourceFile),
                  exc_info=True)
        raise

    log.debug("Passing multipart-message for file '{0}'...".format(sourceFile))
    while True:

        # read next chunk from file
        fileContent = fileDescriptor.read(chunkSize)

        # detect if end of file has been reached
        if not fileContent:
            break

        try:
            # assemble metadata for zmq-message
            chunkMetadata = metadata.copy()
            chunkMetadata["chunkNumber"] = chunkNumber

            chunkPayload = []
            chunkPayload.append(json.dumps(chunkMetadata).encode("utf-8"))
            chunkPayload.append(fileContent)
        except:
            log.error("Unable to pack multipart-message for file '{0}'"
                      .format(sourceFile), exc_info=True)

        # send message to data targets
        try:
            __send_to_targets(log, targets_data, sourceFile, targetFile,
                              openConnections, None, chunkPayload, context)
        except DataHandlingError:
            log.error("Unable to send multipart-message for file '{0}' "
                      "(chunk {1})".format(sourceFile, chunkNumber),
                      exc_info=True)
            sendError = True
        except:
            log.error("Unable to send multipart-message for file '{0}' "
                      "(chunk {1})".format(sourceFile, chunkNumber),
                      exc_info=True)

        chunkNumber += 1

    # close file
    try:
        log.debug("Closing '{0}'...".format(sourceFile))
        fileDescriptor.close()
    except:
        log.error("Unable to close target file '{0}'.".format(sourceFile),
                  exc_info=True)
        raise

    if not sendError:
        prop["removeFlag"] = True


def __data_handling(log, sourceFile, targetFile, actionFunction, metadata,
                    prop):
    try:
        actionFunction(sourceFile, targetFile)
    except IOError as e:

        # errno.ENOENT == "No such file or directory"
        if e.errno == errno.ENOENT:
            subdir, tmp = os.path.split(metadata["relativePath"])
            targetBasePath = os.path.join(
                targetFile.split(subdir + os.sep)[0], subdir)

            if metadata["relativePath"] in prop["fixSubdirs"]:
                log.error("Unable to copy/move file '{0}' to '{1}': "
                          "Directory {2} is not available."
                          .format(sourceFile, targetFile,
                                  metadata["relativePath"]))
                raise
            elif (subdir in prop["fixSubdirs"]
                    and not os.path.isdir(targetBasePath)):
                log.error("Unable to copy/move file '{0}' to '{1}': "
                          "Directory {2} is not available."
                          .format(sourceFile, targetFile, subdir))
                raise
            else:
                try:
                    targetPath, filename = os.path.split(targetFile)
                    os.makedirs(targetPath)
                    log.info("New target directory created: {0}"
                             .format(targetPath))
                    actionFunction(sourceFile, targetFile)
                except OSError as e:
                    log.info("Target directory creation failed, was already "
                             "created in the meantime: {0}".format(targetPath))
                    actionFunction(sourceFile, targetFile)
                except:
                    log.error("Unable to copy/move file '{0}' to '{1}'"
                              .format(sourceFile, targetFile), exc_info=True)
                    log.debug("targetPath: {p}".format(p=targetPath))
        else:
            log.error("Unable to copy/move file '{0}' to '{1}'"
                      .format(sourceFile, targetFile), exc_info=True)
            raise
    except:
        log.error("Unable to copy/move file '{0}' to '{1}'"
                  .format(sourceFile, targetFile), exc_info=True)
        raise


def finish_data_handling(log, targets, sourceFile, targetFile, metadata,
                         openConnections, context, prop):

    targets_metadata = [i for i in targets if i[3] == "metadata"]

    if prop["storeData"] and prop["removeData"] and prop["removeFlag"]:

        # move file
        try:
            __data_handling(log, sourceFile, targetFile, shutil.move, metadata,
                            prop)
            log.info("Moving file '{0}' ...success.".format(sourceFile))
        except:
            log.error("Could not move file {0} to {1}"
                      .format(sourceFile, targetFile), exc_info=True)
            return

    elif prop["storeData"]:

        # copy file
        # (does not preserve file owner, group or ACLs)
        try:
            __data_handling(log, sourceFile, targetFile, shutil.copy,
                            metadata, prop)
            log.info("Copying file '{0}' ...success.".format(sourceFile))
        except:
            return

    elif prop["removeData"] and prop["removeFlag"]:
        # remove file
        try:
            os.remove(sourceFile)
            log.info("Removing file '{0}' ...success.".format(sourceFile))
        except:
            log.error("Unable to remove file {0}".format(sourceFile),
                      exc_info=True)

        prop["removeFlag"] = False

    # send message to metadata targets
    if targets_metadata:
        try:
            __send_to_targets(log, targets_metadata, sourceFile, targetFile,
                              openConnections, metadata, None, context,
                              prop["timeout"])
            log.debug("Passing metadata multipart-message for file {0}...done."
                      .format(sourceFile))

        except:
            log.error("Unable to send metadata multipart-message for file "
                      "'{0}' to '{1}'".format(sourceFile, targets_metadata),
                      exc_info=True)


def clean(prop):
    pass


if __name__ == '__main__':
    import time
    from shutil import copyfile

    try:
        BASE_PATH = os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.realpath(__file__)))))
    except:
        BASE_PATH = os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.abspath(sys.argv[0])))))
    print ("BASE_PATH", BASE_PATH)
    SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

    if SHARED_PATH not in sys.path:
        sys.path.append(SHARED_PATH)
    del SHARED_PATH

    import helpers

    logfile = os.path.join(BASE_PATH, "logs", "file_fetcher.log")
    logsize = 10485760

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True,
                                      onScreenLogLevel="debug")

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    root.addHandler(h1)
    root.addHandler(h2)

    receivingPort = "6005"
    receivingPort2 = "6006"
    extIp = "0.0.0.0"

    context = zmq.Context.instance()

    receivingSocket = context.socket(zmq.PULL)
    connectionStr = "tcp://{ip}:{port}".format(ip=extIp, port=receivingPort)
    receivingSocket.bind(connectionStr)
    logging.info("=== receivingSocket connected to {0}".format(connectionStr))

    receivingSocket2 = context.socket(zmq.PULL)
    connectionStr = "tcp://{ip}:{port}".format(ip=extIp, port=receivingPort2)
    receivingSocket2.bind(connectionStr)
    logging.info("=== receivingSocket2 connected to {0}".format(connectionStr))

    prework_sourceFile = os.path.join(BASE_PATH, "test_file.cbf")
    prework_targetFile = os.path.join(
        BASE_PATH, "data", "source", "local", "100.cbf")

    copyfile(prework_sourceFile, prework_targetFile)
    time.sleep(0.5)

    workload = {
        "sourcePath": os.path.join(BASE_PATH, "data", "source"),
        "relativePath": os.sep + "local",
        "filename": "100.cbf"
        }
    targets = [['localhost:{0}'.format(receivingPort), 1, [".cbf"], "data"],
               ['localhost:{0}'.format(receivingPort2), 0, [".cbf"],  "data"]]

    chunkSize = 10485760  # = 1024*1024*10 = 10 MiB
    localTarget = os.path.join(BASE_PATH, "data", "target")
    openConnections = dict()

    dataFetcherProp = {
        "type": "file_fetcher",
        "fixSubdirs": ["commissioning", "current", "local"],
        "storeData": False,
        "removeData": False
        }

    logging.debug("openConnections before function call: {0}"
                  .format(openConnections))

    setup(logging, dataFetcherProp)

    sourceFile, targetFile, metadata = get_metadata(logging, dataFetcherProp,
                                                    targets, workload,
                                                    chunkSize,
                                                    localTarget=None)
    send_data(logging, targets, sourceFile, targetFile, metadata,
              openConnections, context, dataFetcherProp)

    finish_data_handling(logging, targets, sourceFile, targetFile, metadata,
                         openConnections, context, dataFetcherProp)

    logging.debug("openConnections after function call: {0}"
                  .format(openConnections))

    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: {0}".format(json.loads(
            recv_message[0].decode("utf-8"))))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: {0}".format(json.loads(
            recv_message[0].decode("utf-8"))))
    except KeyboardInterrupt:
        pass
    finally:
        receivingSocket.close(0)
        receivingSocket2.close(0)
        clean(dataFetcherProp)
        context.destroy()
