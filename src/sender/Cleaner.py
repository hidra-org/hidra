from __builtin__ import open, type

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import time
import argparse
import zmq
import os
import logging
import sys
import json
import traceback
from multiprocessing import Process, freeze_support
import subprocess
import json
import shutil
import collections


DEFAULT_CHUNK_SIZE = 1048576


#
#  --------------------------  class: Cleaner  --------------------------------------
#
class Cleaner():
    """
    * received cleaning jobs via zeromq,
      such as removing a file
    * Does regular checks on the watched directory,
    such as
      - deleting files which have been successfully send
        to target but still remain in the watched directory
      - poll the watched directory and reissue new files
        to fileMover which have not been detected yet
    """
    newJobPort        = None
    newJobIp          = None
    senderComIp       = None

    contextForCleaner = None
    externalContext   = None    # if the context was created outside this class or not

    newJobSocket      = None
    lvComSocket       = None    # socket to communicate with Coordinator class

    useDataStream     = True    # boolian to inform if the data should be send to the data stream pipe (to the storage system)

    useRingbuffer     = False
    lvComIp           = None
    lvComPort         = None

    lastHandledFiles  = collections.deque(maxlen = 20)

    # to get the logging only handling this class
    log               = None

    def __init__(self, targetPath, newJobIp, newJobPort, useDataStream = True, lvComPort = None, context = None):
        self.newJobIp      = newJobIp
        self.newJobPort    = newJobPort
        self.senderComIp   = self.newJobIp

        self.targetPath    = os.path.normpath(targetPath)

        self.useDataStream = useDataStream

        if lvComPort:
            self.useRingbuffer = True
            self.lvComIp       = self.newJobIp
            self.lvComPort     = lvComPort

        if context:
            self.contextForCleaner = context
            self.externalContext   = True
        else:
            self.contextForCleaner = zmq.Context()
            self.externalContext   = False

        self.log = self.getLogger()
        self.log.debug("Init")

        # create Sockets
        self.createSockets()

        try:
            self.process()
        except KeyboardInterrupt:
            self.log.debug("KeyboardInterrupt detected. Shutting down cleaner.")
        except Exception as e:
            trace = traceback.format_exc()
            self.log.error("Stopping cleanerProcess due to unknown error condition.")
            self.log.debug("Error was: " + str(e))
            self.log.debug("Trace was: " + str(trace))
        finally:
            self.stop()


    def getLogger(self):
        logger = logging.getLogger("cleaner")
        return logger


    def createSockets(self):
        self.newJobSocket = self.contextForCleaner.socket(zmq.PULL)
        connectionStr = "tcp://" + self.newJobIp + ":%s" % self.newJobPort
        try:
            self.newJobSocket.bind(connectionStr)
            self.log.debug("newJobSocket started for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start newJobSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        if self.useRingbuffer:
            self.lvComSocket = self.contextForCleaner.socket(zmq.PAIR)
            connectionStr = "tcp://" + self.lvComIp + ":%s" % self.lvComPort
            try:
                self.lvComSocket.connect(connectionStr)
                self.log.debug("lvComSocket started (connect) for '" + connectionStr + "'")
            except Exception as e:
                self.log.error("Failed to start lvComSocket (connect): '" + connectionStr + "'")
                self.log.debug("Error was:" + str(e))


    def process(self):
        #processing messaging
        while True:
            #waiting for new jobs
            self.log.debug("Waiting for new jobs")

            try:
                workload = self.newJobSocket.recv()
            except Exception as e:
                self.log.error("Error in receiving job: " + str(e))

            if workload == "STOP":
                self.log.debug("Stopping cleaner")
                break

            # transform to dictionary
            # metadataDict = {
            #   "filename"             : filename,
            #   "filesize"             : filesize,
            #   "fileModificationTime" : fileModificationTime,
            #   "sourcePath"           : sourcePath,
            #   "relativePath"         : relativePath,
            #   "chunkSize"            : self.getChunkSize()
            #   }
            try:
                workloadDict = json.loads(str(workload))
            except:
                errorMessage = "invalid job received. skipping job"
                self.log.error(errorMessage)
                self.log.debug("workload=" + str(workload))
                continue

            #extract fileEvent metadata/data
            try:
                #TODO validate fileEventMessageDict dict
                filename       = workloadDict["filename"]
                sourcePath     = workloadDict["sourcePath"]
                relativePath   = workloadDict["relativePath"]
                fileModTime    = workloadDict["fileModificationTime"]
#                print "workloadDict:", workloadDict
            except Exception as e:
                errorMessage   = "Invalid fileEvent message received."
                self.log.error(errorMessage)
                self.log.debug("Error was: " + str(e))
                self.log.debug("workloadDict=" + str(workloadDict))
                #skip all further instructions and continue with next iteration
                continue

            #source file
            sourceFullpath = None
            try:
                #generate target filepath
                # use normpath here instead of join because relativePath is starting with a "/" and join would see that as absolut path
                sourcePath = os.path.normpath(sourcePath + os.sep + relativePath)
                sourceFullPath = os.path.join(sourcePath,filename)
                targetFullPath = os.path.normpath(self.targetPath + os.sep +  relativePath)
                self.log.debug("sourcePath: " + str (sourcePath))
                self.log.debug("filename: " + str (filename))
                self.log.debug("targetPath: " + str (targetFullPath))

            except Exception as e:
                self.log.error("Unable to generate file paths")
                self.log.error("Error was: " + str(e))
                #skip all further instructions and continue with next iteration
                continue

            if self.useRingbuffer:

                try:
                    self.copyFile(sourcePath, filename, targetFullPath)
                except Exception as e:
                    self.log.error("Unable to handle source file: " + str (sourceFullPath) )
                    trace = traceback.format_exc()
                    self.log.debug("Error was: " + str(e))
                    self.log.debug("Trace: " + str(trace))
                    #skip all further instructions and continue with next iteration
                    continue
                try:
                    # send the file to the coordinator to add it to the ring buffer
                    message = "AddFile" + str(sourceFullPath) + ", " + str(fileModTime)
                    self.log.debug("Send file to LvCommunicator: " + message )
                    self.lvComSocket.send(message)
                except Exception as e:
                    self.log.error("Unable to send source file to LvCommunicator: " + str (sourceFullPath) )
                    self.log.debug("Error was: " + str(e))
                    continue
            else:

                try:
                    if self.useDataStream:
                        self.removeFile(sourceFullPath)
                    else:
                        self.moveFile(sourcePath, filename, targetFullPath)
#                        self.copyFile(sourcePath, filename, targetFullPath)
                except Exception as e:
                    self.log.error("Unable to handle source file: " + str (sourceFullPath) )
                    trace = traceback.format_exc()
                    self.log.debug("Error was: " + str(e))
                    self.log.debug("Trace: " + str(trace))
                    #skip all further instructions and continue with next iteration
                    continue


    def copyFile(self, source, filename, target):
        maxAttemptsToCopyFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        fileWasCopied = False

        while iterationCount <= maxAttemptsToCopyFile and not fileWasCopied:
            iterationCount+=1
            try:
                # check if the directory exists before moving the file
                if not os.path.exists(target):
                    try:
                        os.makedirs(target)
                    except OSError:
                        pass
                # copying the file
                sourceFile = source + os.sep + filename
                targetFile = target + os.sep + filename
#                targetFile = "/gpfs/current/scratch_bl/test" + os.sep + filename
                self.log.debug("sourceFile: " + str(sourceFile))
                self.log.debug("targetFile: " + str(targetFile))
                try:
                    shutil.copyfile(sourceFile, targetFile)
                    self.lastHandledFiles.append(filename)
                    fileWasCopied = True
                    self.log.info("Copying file '" + str(filename) + "' from '" + str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
                except Exception, e:
                    self.log.debug ("Checking if file was already copied: " + str(filename))
                    self.log.debug ("Error was: " + str(e))
                    if filename in self.lastHandledFiles:
                       self.log.info("File was found in history.")
                       fileWasCopied = True
                    else:
                       self.log.info("File was not found in history.")

            except IOError:
                self.log.debug ("IOError: " + str(filename))
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to copy file {FILE}.".format(FILE=str(source) + str(filename))
                self.log.debug(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.debug("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasCopied:
            self.log.info("Copying file '" + str(filename) + " from " + str(source) + " to " + str(target) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToCopyFile reached (value={ATTEMPT}). Unable to move file '{FILE}'.".format(ATTEMPT=str(iterationCount), FILE=filename))


    def moveFile(self, source, filename, target):
        maxAttemptsToMoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        fileWasMoved = False

        while iterationCount <= maxAttemptsToMoveFile and not fileWasMoved:
            iterationCount+=1
            try:
                # check if the directory exists before moving the file
                if not os.path.exists(target):
                    try:
                        os.makedirs(target)
                    except OSError:
                        pass
                # moving the file
                sourceFile = source + os.sep + filename
                targetFile = target + os.sep + filename
#                targetFile = "/gpfs/current/scratch_bl/test" + os.sep + filename
                self.log.debug("sourceFile: " + str(sourceFile))
                self.log.debug("targetFile: " + str(targetFile))
                try:
                    shutil.move(sourceFile, targetFile)
                    self.lastHandledFiles.append(filename)
                    fileWasMoved = True
                    self.log.info("Moving file '" + str(filename) + "' from '" + str(sourceFile) + "' to '" + str(targetFile) + "' (attempt " + str(iterationCount) + ")...success.")
                except Exception, e:
                    self.log.debug ("Checking if file was already moved: " + str(filename))
                    self.log.debug ("Error was: " + str(e))
                    if filename in self.lastHandledFiles:
                       self.log.info("File was found in history.")
                       fileWasMoved = True
                    else:
                       self.log.info("File was not found in history.")

            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to move file {FILE}.".format(FILE=str(sourceFile))
                self.log.debug(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.debug("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasMoved:
            self.log.info("Moving file '" + str(filename) + " from " + str(sourceFile) + " to " + str(targetFile) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToMoveFile reached (value={ATTEMPT}). Unable to move file '{FILE}'.".format(ATTEMPT=str(iterationCount), FILE=filename))


    def removeFile(self, filepath):
        maxAttemptsToRemoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        self.log.debug("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...")
        fileWasRemoved = False

        while iterationCount <= maxAttemptsToRemoveFile and not fileWasRemoved:
            iterationCount+=1
            try:
                os.remove(filepath)
                fileWasRemoved = True
                self.log.info("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...success.")
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to remove file {FILE}.".format(FILE=str(filepath))
                self.log.debug(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.debug("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasRemoved:
            self.log.info("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToRemoveFile reached (value={ATTEMPT}). Unable to remove file '{FILE}'.".format(ATTEMPT=str(iterationCount), FILE=filepath))


    def stop(self):
        self.log.debug("Closing socket")
        self.newJobSocket.close(0)
        if self.lvComSocket:
            self.lvComSocket.close(0)
        if not self.externalContext:
            self.log.debug("Destroying context")
            self.contextForCleaner.destroy()
