from __builtin__ import open, type

__author__ = 'Marco Strutz <marco.strutz@desy.de>', 'Manuela Kuhn <manuela.kuhn@desy.de>'


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
    zmqContextForCleaner = None
    zmqCleanerSocket     = None

    # to get the logging only handling this class
    log                  = None

    def __init__(self, bindingIp="127.0.0.1", bindingPort="6062", context = None, verbose=False):
        self.bindingPortForSocket = bindingPort
        self.bindingIpForSocket   = bindingIp
        self.zmqContextForCleaner = context or zmq.Context()

        self.log = self.getLogger()
        self.log.debug("Init")

        #bind to local port
        self.zmqCleanerSocket      = self.zmqContextForCleaner.socket(zmq.PULL)
        connectionStrCleanerSocket = "tcp://" + self.bindingIpForSocket + ":%s" % self.bindingPortForSocket
        self.zmqCleanerSocket.bind(connectionStrCleanerSocket)
        self.log.debug("zmqCleanerSocket started for '" + connectionStrCleanerSocket + "'")

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

        self.zmqCleanerSocket.close(0)
        self.zmqContextForCleaner.destroy()


    def getLogger(self):
        logger = logging.getLogger("cleaner")
        return logger


    def process(self):
        #processing messaging
        while True:
            #waiting for new jobs
            self.log.debug("Waiting for new jobs")
            try:
                workload = self.zmqCleanerSocket.recv()
            except Exception as e:
                self.log.error("Error in receiving job: " + str(e))


            #transform to dictionary
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
                relativeParent = workloadDict["relativeParent"]
                targetPath     = workloadDict["targetPath"]
                # filesize       = workloadDict["filesize"]
            except Exception, e:
                errorMessage   = "Invalid fileEvent message received."
                self.log.error(errorMessage)
                self.log.debug("Error was: " + str(e))
                self.log.debug("workloadDict=" + str(workloadDict))
                #skip all further instructions and continue with next iteration
                continue

            #moving source file
            sourceFilepath = None
            try:
                self.log.debug("removing source file...")
                #generate target filepath
                sourceFilepath = os.path.join(sourcePath,filename)
#                self.removeFile(sourceFilepath)
                self.log.debug ("sourcePath: " + str (sourcePath))
                self.log.debug ("filename: " + str (filename))
                self.log.debug ("targetPath: " + str (targetPath))
                self.moveFile(sourcePath, filename, targetPath)

                # #show filesystem statistics
                # try:
                #     self.showFilesystemStatistics(sourcePath)
                # except Exception, f:
                #     logging.warning("Unable to get filesystem statistics")
                #     logging.debug("Error was: " + str(f))
                self.log.debug("file removed: " + str(sourcePath))
                self.log.debug("removing source file...success.")

            except Exception, e:
                errorMessage = "Unable to remove source file: " + str (sourcePath)
                self.log.error(errorMessage)
                trace = traceback.format_exc()
                self.log.error("Error was: " + str(trace))
                self.log.debug("sourceFilepath="+str(sourceFilepath))
                self.log.debug("removing source file...failed.")
                #skip all further instructions and continue with next iteration
                continue



    def moveFile(self, source, filename, target):
        maxAttemptsToRemoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        self.log.info("Moving file '" + str(filename) + "' from '" +  str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
        fileWasMoved = False

        while iterationCount <= maxAttemptsToRemoveFile and not fileWasMoved:
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
            raise Exception("maxAttemptsToMoveFile reached (value={ATTEMPT}). Unable to move file '{FILE}'.".format(ATTEMPT=str(iterationCount),
                                                                                                                            FILE=filename))


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
            raise Exception("maxAttemptsToRemoveFile reached (value={ATTEMPT}). Unable to remove file '{FILE}'.".format(ATTEMPT=str(iterationCount),
                                                                                                                            FILE=filepath))

