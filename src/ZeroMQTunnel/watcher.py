__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import argparse
import zmq
import os
import logging
import sys
import json
import helperScript
from InotifyDetector import InotifyDetector as EventDetector


#
#  --------------------------  class: DirectoryWatcherHandler  --------------------------------------
#

class DirectoryWatcherHandler():
    patterns                   = ["*"]
    zmqContext                 = None
    externalContext            = None    # if the context was created outside this class or not
    messageSocket              = None    # strings only, control plane
    fileEventServerIp          = None
    fileEventServerPort        = None
    watchFolder                = None
    eventDetector              = None
    monitoredDefaultSubfolders = ["commissioning", "current", "local"]


    def __init__(self, fileEventServerIp, watchFolder, fileEventServerPort, zmqContext = None):
        logging.debug("DirectoryWatcherHandler: __init__()")
        logging.info("registering zmq context")
        if zmqContext:
            self.zmqContext      = zmqContext
            self.externalContext = True
        else:
            self.zmqContext      = zmq.Context()
            self.externalContext = False

        self.watchFolder         = os.path.normpath(watchFolder)
        self.fileEventServerIp   = fileEventServerIp
        self.fileEventServerPort = fileEventServerPort


        monitoredFolders        = [self.watchFolder + os.sep + folder for folder in self.monitoredDefaultSubfolders]
        self.eventDetector       = EventDetector(monitoredFolders)


        assert isinstance(self.zmqContext, zmq.sugar.context.Context)

        #create zmq sockets
        self.messageSocket = self.zmqContext.socket(zmq.PUSH)
        zmqSocketStr = "tcp://" + self.fileEventServerIp + ":" + str(self.fileEventServerPort)
        self.messageSocket.connect(zmqSocketStr)
        logging.debug("Connecting to ZMQ socket: " + str(zmqSocketStr))

        self.process()


    def getLogger(self):
        logger = logging.getLogger("DirectoryWatchHandler")
        return logger


    def passFileToZeromq(self, targetSocket, sourcePath, relativePath, filename):
        '''
        Taking the filename, creating a buffer and then
        sending the data as multipart message to the socket.

        For testing currently the multipart message consists of only one message.

        :param sourcePath:     Pointing to the data which is going to be send
        :param relativePath: Relative path leading from the origin source path (not the filepath) to the file
        :param filename:     The name of the file to be send
        :return:
        '''

        #build message dict
        try:
            logging.debug("Building message dict...")
            messageDict = { "filename"     : filename,
                            "sourcePath"   : sourcePath,
                            "relativePath" : relativePath
                            }

            messageDictJson = json.dumps(messageDict)  #sets correct escape characters
            logging.debug("Building message dict...done.")
        except Exception, e:
            errorMessage = "Unable to assemble message dict."
            logging.error(errorMessage)
            logging.debug("Error was: " + str(e))
            logging.debug("Building message dict...failed.")
            raise Exception(e)


        #send message
        try:
            logging.info("Sending message...")
            logging.debug(str(messageDictJson))
            targetSocket.send(messageDictJson)
            logging.info("Sending message...done.")
        except KeyboardInterrupt:
            logging.error("Sending message...failed because of KeyboardInterrupt.")
        except Exception, e:
            logging.error("Sending message...failed.")
            logging.debug("Error was: " + str(e))
            raise Exception(e)


    def process(self):
        try:
            try:
                while True:
                    try:
                        # the event for a file /tmp/test/source/local/file1.tif is of the form:
                        # {
                        #   "sourcePath" : "/tmp/test/source/"
                        #   "relativePath": "local"
                        #   "filename"   : "file1.tif"
                        # }
                        workloadList = self.eventDetector.getNewEvent()
                    except Exception, e:
                        logging.error("Invalid fileEvent message received.")
                        logging.debug("Error was: " + str(e))
                        #skip all further instructions and continue with next iteration
                        continue

                    #TODO validate workload dict
                    for workload in workloadList:
                        sourcePath   = workload["sourcePath"]
                        # the folders local, current, and commissioning are monitored by default
                        (sourcePath,relDir) = os.path.split(sourcePath)
                        relativePath = os.path.normpath(relDir + os.sep + workload["relativePath"])
                        filename     = workload["filename"]

                        # send the file to the fileMover
                        self.passFileToZeromq(self.messageSocket, sourcePath, relativePath, filename)
            except KeyboardInterrupt:
                logging.info("Keyboard interruption detected. Shuting down")
        finally:
            self.eventDetector.stop()

        self.shuttingDown()


    def shuttingDown(self):
        self.messageSocket.close(0)
        if not self.externalContext:
            self.zmqContext.destroy()


def getDefaultConfig():
    if helperScript.isWindows():
        defaultConfigDict = {
                            "logfilePath"    : "C:\\",
                            "logfileName"    : "watchFolder.log",
                            "pushServerPort" : "6060",
                            "pushServerIp"   : "127.0.0.1",
                        }

    elif helperScript.isLinux():
        defaultConfigDict = {
                            "logfilePath"    : "/tmp/log/",
                            "logfileName"    : "watchFolder.log",
                            "pushServerPort" : "6060",
                            "pushServerIp"   : "127.0.0.1",
                        }
    else:
        return ""

    return defaultConfigDict



def argumentParsing():

    defaultConfig = getDefaultConfig()

    parser = argparse.ArgumentParser()
    parser.add_argument("--watchFolder" , type=str, help="folder you want to monitor for changes")
    parser.add_argument("--staticNotification",
                        help="disables new file-events. just sends a list of currently available files within the defined 'watchFolder'.",
                        action="store_true")
    parser.add_argument("--logfilePath" , type=str, help="path where logfile will be created", default=defaultConfig["logfilePath"])
    parser.add_argument("--logfileName" , type=str, help="filename used for logging", default=defaultConfig["logfileName"])
    parser.add_argument("--pushServerIp", type=str, help="zqm endpoint (IP-address) to send file events to", default=defaultConfig["pushServerIp"])
    parser.add_argument("--pushServerPort", type=str, help="zqm endpoint (port) to send file events to", default=defaultConfig["pushServerPort"])
    parser.add_argument("--verbose"     ,           help="more verbose output", action="store_true")

    arguments = parser.parse_args()

    # TODO: check watchFolder-directory for existance

    watchFolder = str(arguments.watchFolder)
    assert isinstance(type(watchFolder), type(str))

    #exit with error if now watchFolder path was provided
    if (watchFolder == None) or (watchFolder == "") or (watchFolder == "None"):
        print """You need to set the following option:
--watchFolder {FOLDER}
"""
        sys.exit(1)


    #abort if watch-folder does not exist
    helperScript.checkFolderExistance(watchFolder)


    #error if logfile cannot be written
    try:
        fullPath = os.path.join(arguments.logfilePath, arguments.logfileName)
        logFile = open(fullPath, "a")
    except:
        print "Unable to create the logfile """ + str(fullPath)
        print """Please specify a new target by setting the following arguments:
--logfileName
--logfilePath
"""
        sys.exit(1)

    #check logfile-path for existance
    helperScript.checkFolderExistance(arguments.logfilePath)


    return arguments




if __name__ == '__main__':
    arguments   = argumentParsing()
    watchFolder = arguments.watchFolder
    verbose     = arguments.verbose
    logfileFilePath = os.path.join(arguments.logfilePath, arguments.logfileName)

    fileEventServerIp   = str(arguments.pushServerIp)
    fileEventServerPort = str(arguments.pushServerPort)
    communicationWithLcyncdIp   = "127.0.0.1"
    communicationWithLcyncdPort = "6080"

    #enable logging
    helperScript.initLogging(logfileFilePath, verbose)


    #run only once, skipping file events
    #just get a list of all files in watchDir and pass to zeromq
    directoryWatcher = DirectoryWatcherHandler(fileEventServerIp, watchFolder, fileEventServerPort)

