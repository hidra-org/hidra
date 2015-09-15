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
import helperScript
from RingBuffer import RingBuffer


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
    bindingPortForSocket = None
    bindingIpForSocket   = None
    senderComPort        = None
    senderComIp          = None
    zmqContextForCleaner = None
    externalContext      = None    # if the context was created outside this class or not
    zmqCleanerSocket     = None
    senderComSocket      = None

    # to get the logging only handling this class
    log                  = None

    useRealTimeAnalysis  = True    # boolian to inform if the receiver for the realtime analysis is running
    maxRingBufferSize    = None
    ringBuffer           = None

    def __init__(self, targetPath, bindingIp="127.0.0.1", bindingPort="6062", senderComPort="6063", maxRingBufferSize = None, context = None, verbose=False):
        self.bindingPortForSocket = bindingPort
        self.bindingIpForSocket   = bindingIp
        self.senderComPort        = senderComPort
        self.senderComIp          = self.bindingIpForSocket
        self.targetPath           = targetPath

        if context:
            self.zmqContextForCleaner = context
            self.externalContext      = True
        else:
            self.zmqContextForCleaner = zmq.Context()
            self.externalContext      = False

        if maxRingBufferSize:
            self.maxRingBufferSize    = maxRingBufferSize
#            # TODO remove targetPath?
#            self.ringBuffer           = RingBuffer(self.maxRingBufferSize, self.targetPath)
            self.ringBuffer           = RingBuffer(self.maxRingBufferSize)


        self.log = self.getLogger()
        self.log.debug("Init")

        #bind to local port
        self.zmqCleanerSocket = self.zmqContextForCleaner.socket(zmq.PULL)
        connectionStrSocket   = "tcp://" + self.bindingIpForSocket + ":%s" % self.bindingPortForSocket
        self.zmqCleanerSocket.bind(connectionStrSocket)
        self.log.debug("zmqCleanerSocket started for '" + connectionStrSocket + "'")

        #bind to local port
        self.senderComSocket = self.zmqContextForCleaner.socket(zmq.REP)
        connectionStrSocket  = "tcp://" + self.senderComIp + ":%s" % self.senderComPort
        self.senderComSocket.bind(connectionStrSocket)
        self.log.debug("senderComSocket started for '" + connectionStrSocket + "'")

        # Poller to get either messages from the watcher or communication messages to stop sending data to the live viewer
        self.poller = zmq.Poller()
        self.poller.register(self.zmqCleanerSocket, zmq.POLLIN)
        self.poller.register(self.senderComSocket, zmq.POLLIN)

        try:
            self.process()
        except zmq.error.ZMQError:
            self.log.error("ZMQError: "+ str(e))
            self.log.debug("Shutting down cleaner.")
        except KeyboardInterrupt:
            self.log.info("KeyboardInterrupt detected. Shutting down cleaner.")
        except:
            trace = traceback.format_exc()
            self.log.error("Stopping cleanerProcess due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))

        self.stop()


    def getLogger(self):
        logger = logging.getLogger("cleaner")
        return logger


    def process(self):
        #processing messaging
        while True:
            socks = dict(self.poller.poll())
            #waiting for new jobs
            self.log.debug("Waiting for new jobs")

            if self.senderComSocket in socks and socks[self.senderComSocket] == zmq.POLLIN:
                try:
                    workload = self.senderComSocket.recv()
                except Exception as e:
                    self.log.error("Error in communication signal: " + str(e))

                if workload == "STOP":
                    self.log.info("Stopping cleaner")
                    self.stop()
                    break
                elif workload == "START_REALTIME_ANALYSIS":
                    self.useRealTimeAnalysis = True
                    self.log.info("Starting realtime analysis")
                    break
                elif workload == "STOP_REALTIME_ANALYSIS":
                    self.useRealTimeAnalysis = False
                    self.log.info("Stopping realtime analysis")
                    break
                elif workload == "NEXT_FILE":
                    newestFile = self.ringBuffergetNewestFile()
                    print newestFile

            if self.zmqCleanerSocket in socks and socks[self.zmqCleanerSocket] == zmq.POLLIN:
                try:
                    workload = self.zmqCleanerSocket.recv()
                except Exception as e:
                    self.log.error("Error in receiving job: " + str(e))

                if workload == "STOP":
                    self.log.info("Stopping cleaner")
                    self.stop()
                    break
                elif workload == "START_REALTIME_ANALYSIS":
                    self.useRealTimeAnalysis = True
                    self.log.info("Starting realtime analysis")
                    break
                elif workload == "STOP_REALTIME_ANALYSIS":
                    self.useRealTimeAnalysis = False
                    self.log.info("Stopping realtime analysis")
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

                #extract fileEvent metadata
                try:
                    #TODO validate fileEventMessageDict dict
                    filename       = workloadDict["filename"]
                    sourcePath     = workloadDict["sourcePath"]
                    relativePath   = workloadDict["relativePath"]
                    if self.useRealTimeAnalysis:
                        modTime        = workloadDict["fileModificationTime"]
                    # filesize       = workloadDict["filesize"]
                except Exception, e:
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
                    sourcePath = os.path.normpath(sourcePath + os.sep + relativePath)
                    sourceFullPath = os.path.join(sourcePath,filename)
                    targetFullPath = os.path.normpath(self.targetPath + relativePath)
                    self.log.debug("sourcePath: " + str (sourcePath))
                    self.log.debug("filename: " + str (filename))
                    self.log.debug("targetPath: " + str (targetFullPath))

                except Exception, e:
                    self.log.error("Unable to generate file paths")
                    trace = traceback.format_exc()
                    self.log.error("Error was: " + str(trace))
                    #skip all further instructions and continue with next iteration
                    continue

                if self.useRealTimeAnalysis:
                    # copy file
                    try:
                        self.log.debug("Copying source file...")
                        self.copyFile(sourcePath, filename, targetFullPath)
                        self.log.debug("File copied: " + str(sourceFullPath))
                        self.log.debug("Copying source file...success.")

                    except Exception, e:
                        self.log.error("Unable to copy source file: " + str (sourceFullPath) )
                        trace = traceback.format_exc()
                        self.log.error("Error was: " + str(trace))
                        self.log.debug("sourceFullpath="+str(sourceFullpath))
                        self.log.debug("Copying source file...failed.")
                        #skip all further instructions and continue with next iteration
                        continue

                    # add file to ring buffer
                    self.log.debug("Add new file to ring buffer: " + str(sourceFullPath) + ", " + str(modTime))
                    self.ringBuffer.add(sourceFullPath, modTime)
                else:
                    try:
                        self.log.debug("Moving source file...")
                        self.moveFile(sourcePath, filename, targetFullPath)
    #                    self.removeFile(sourceFullpath)

                        # #show filesystem statistics
                        # try:
                        #     self.showFilesystemStatistics(sourcePath)
                        # except Exception, f:
                        #     logging.warning("Unable to get filesystem statistics")
                        #     logging.debug("Error was: " + str(f))
                        self.log.debug("File moved: " + str(sourceFullPath))
                        self.log.debug("Moving source file...success.")

                    except Exception, e:
                        self.log.error("Unable to move source file: " + str (sourceFullPath) )
                        trace = traceback.format_exc()
                        self.log.error("Error was: " + str(trace))
                        self.log.debug("sourceFullpath="+str(sourceFullpath))
                        self.log.debug("Moving source file...failed.")
                        #skip all further instructions and continue with next iteration
                        continue


    def copyFile(self, source, filename, target):
        maxAttemptsToCopyFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        self.log.info("Copying file '" + str(filename) + "' from '" +  str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
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
                # moving the file
                sourceFile = source + os.sep + filename
                targetFile = target + os.sep + filename
                self.log.debug("sourceFile: " + str(sourceFile))
                self.log.debug("targetFile: " + str(targetFile))
                shutil.copyfile(sourceFile, targetFile)
                fileWasCopied = True
                self.log.debug("Copying file '" + str(filename) + "' from '" + str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
            except IOError:
                self.log.debug ("IOError: " + str(filename))
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to copy file {FILE}.".format(FILE=str(source) + str(filename))
                self.log.warning(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.warning("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasCopied:
            self.log.error("Copying file '" + str(filename) + " from " + str(source) + " to " + str(target) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToCopyFile reached (value={ATTEMPT}). Unable to move file '{FILE}'.".format(ATTEMPT=str(iterationCount), FILE=filename))


    def moveFile(self, source, filename, target):
        maxAttemptsToMoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        self.log.info("Moving file '" + str(filename) + "' from '" +  str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
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
                self.log.debug("sourceFile: " + str(sourceFile))
                self.log.debug("targetFile: " + str(targetFile))
                shutil.move(sourceFile, targetFile)
                fileWasMoved = True
                self.log.debug("Moving file '" + str(filename) + "' from '" + str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
            except IOError:
                self.log.debug ("IOError: " + str(filename))
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to move file {FILE}.".format(FILE=str(source) + str(filename))
                self.log.warning(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.warning("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasMoved:
            self.log.error("Moving file '" + str(filename) + " from " + str(source) + " to " + str(target) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToMoveFile reached (value={ATTEMPT}). Unable to move file '{FILE}'.".format(ATTEMPT=str(iterationCount), FILE=filename))


    def removeFile(self, filepath):
        maxAttemptsToRemoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        self.log.info("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...")
        fileWasRemoved = False

        while iterationCount <= maxAttemptsToRemoveFile and not fileWasRemoved:
            iterationCount+=1
            try:
                os.remove(filepath)
                fileWasRemoved = True
                self.log.debug("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...success.")
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to remove file {FILE}.".format(FILE=str(filepath))
                self.log.warning(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.warning("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasRemoved:
            self.log.error("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToRemoveFile reached (value={ATTEMPT}). Unable to remove file '{FILE}'.".format(ATTEMPT=str(iterationCount), FILE=filepath))


    def stop(self):
        self.log.debug("Closing socket")
        self.zmqCleanerSocket.close(0)
        if not self.externalContext:
            self.log.debug("Destroying context")
            self.zmqContextForCleaner.destroy()
