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

                if not self.useDataStream:
                    try:
                        self.handleFile("copy", sourcePath, filename, targetFullPath)
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
                        self.handleFile("remove", sourcePath, filename, None)
                    else:
                        self.handleFile("move", sourcePath, filename, targetFullPath)
                except Exception as e:
                    self.log.error("Unable to handle source file: " + str (sourceFullPath) )
                    trace = traceback.format_exc()
                    self.log.debug("Error was: " + str(e))
                    self.log.debug("Trace: " + str(trace))
                    #skip all further instructions and continue with next iteration
                    continue


    def handleFile(self, fileOperation, source, filename, target):

        if not fileOperation in ["copy", "move", "remove"]:
            self.log.debug("Requested operation type: " + str(fileOperation) + "; Supported: copy, move, remove")
            raise Exception("Requested operation type " + str(fileOperation) + " is not supported. Unable to process file " + str(filename))

        maxAttemptsToHandleFile     = 2
        waitTimeBetweenAttemptsInMs = 500

        iterationCount = 0
        fileWasHandled = False

        while iterationCount <= maxAttemptsToHandleFile and not fileWasHandled:
            iterationCount+=1
            try:
                sourceFile = os.path.join(source,filename)
                self.log.debug("sourceFile: " + str(sourceFile))

                # check if the directory exists before moving the file
                if fileOperation == "copy" or fileOperation == "move":
                    if not os.path.exists(target):
                        try:
                            os.makedirs(target)
                        except OSError:
                            pass

                    # handle the file (either copy, move or remove it)
                    targetFile = os.path.join(target,filename)
                    self.log.debug("targetFile: " + str(targetFile))

                try:
                    if fileOperation == "copy":
                        shutil.copyfile(sourceFile, targetFile)
                        self.log.info( "Copying file '" + str(filename) + "' from '" + str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
                    elif fileOperation == "move":
                        shutil.move(sourceFile, targetFile)
                        self.log.info( "Moving file '" + str(filename) + "' from '" + str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
                    elif fileOperation == "remove":
                        os.remove(sourceFile)
                        self.log.info("Removing file '" + str(sourceFile) + "' (attempt " + str(iterationCount) + ")...success.")

                    self.lastHandledFiles.append(filename)
                    fileWasHandled = True
                except Exception, e:
                    self.log.debug ("Checking if file was already handled: " + str(filename))
                    self.log.debug ("Error was: " + str(e))
                    if filename in self.lastHandledFiles:
                       self.log.info("File was found in history.")
                       fileWasHandled = True
                    else:
                       self.log.info("File was not found in history.")

            except IOError:
                self.log.debug ("IOError: " + str(filename))
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to " + str(fileOperation) + " file {FILE}.".format(FILE=str(source) + str(filename))
                self.log.debug(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.debug("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasHandled:
            if fileOperation == "copy":
                self.log.info( "Copying file '" + str(filename) + " from " + str(source) + " to " + str(target) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            elif fileOperation == "move":
                self.log.info( "Moving file '" + str(filename) + " from " + str(source) + " to " + str(target) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            elif fileOperation == "remove":
                self.log.info("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...FAILED.")

            raise Exception("maxAttemptsToHandleFile reached " + str(iterationCount) + ". Unable to " + str(fileOperation) + " file '" + str(filename) + "'.")


    def stop(self):
        self.log.debug("Closing socket")
        self.newJobSocket.close(0)
        if self.lvComSocket:
            self.lvComSocket.close(0)
        if not self.externalContext:
            self.log.debug("Destroying context")
            self.contextForCleaner.destroy()
