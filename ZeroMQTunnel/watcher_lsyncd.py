__author__ = 'Marco Strutz <marco.strutz@desy.de>', 'Manuela Kuhn <manuela.kuhn@desy.de>'


import time
import argparse
import zmq
import os
import logging
import sys
import json
import helperScript



class DirectoryWatcherHandler():
    patterns            = ["*"]
    zmqContext          = None
    messageSocket       = None    # strings only, control plane
    fileEventServerIp   = None
    fileEventServerPort = None
    watchFolder         = None


    def __init__(self, zmqContext, fileEventServerIp, watchFolder, fileEventServerPort):
        logging.debug("DirectoryWatcherHandler: __init__()")
        logging.info("registering zmq context")
        self.zmqContext          = zmqContext
        self.watchFolder         = os.path.normpath(watchFolder)
        self.fileEventServerIp   = fileEventServerIp
        self.fileEventServerPort = fileEventServerPort

        assert isinstance(self.zmqContext, zmq.sugar.context.Context)

        #create zmq sockets
        self.messageSocket = zmqContext.socket(zmq.PUSH)
        zmqSocketStr = "tcp://" + self.fileEventServerIp + ":" + str(self.fileEventServerPort)
        self.messageSocket.connect(zmqSocketStr)
        logging.debug("Connecting to ZMQ socket: " + str(zmqSocketStr))


    def passFileToZeromq(self, filepath, targetPath):
        """
        :param rootDirectorty: where to start traversing. including subdirs.
        :return:
        """

        try:
            self.sendFilesystemEventToMessagePipe(filepath, self.messageSocket, targetPath)
        except KeyboardInterrupt:
            logging.info("Keyboard interruption detected. Stop passing file to zmq.")
        except Exception, e:
            logging.error("Unable to process file '" + str(filepath) + "'")
            logging.warning("Skip file '" + str(filepath) + "'. Reason was: " + str(e))


    def sendFilesystemEventToMessagePipe(self, filepath, targetSocket, targetPath):
        '''
        Taking the filename, creating a buffer and then
        sending the data as multipart message to the targetSocket.

        For testing currently the multipart message consists of only one message.

        :param filepath:     Pointing to the data which is going to be send
        :param targetSocket: Where to send the data to
        :return:
        '''


        #extract relative pathname and filename for the file.
        try:
            # why?
            # The receiver might need to save the file at a different
            # path than it was created in at the senders side.
            #
            # example:
            # source_filepath    = /tmp/inotify/source/dir3/2.txt
            # source_watchFolder = /tmp/inotify/source
            # relative basepath  = dir3/2.txt
            # relative parent    = dir3
            # target_root        = /storage/raw/
            # target_filepath    = /storage/raw/dir3/2.txt
            logging.debug("Building relative path names...")
            filepathNormalised   = os.path.normpath(filepath)
            (parentDir,filename) = os.path.split(filepathNormalised)
            commonPrefix         = os.path.commonprefix([self.watchFolder,filepathNormalised])
            relativeBasepath     = os.path.relpath(filepathNormalised, commonPrefix)
            (relativeParent, blub) = os.path.split(relativeBasepath)
            logging.debug("Common prefix     : " + str(commonPrefix))
            logging.debug("Relative basepath : " + str(relativeBasepath))
            logging.debug("Relative parent   : " + str(relativeParent))
            logging.debug("Building relative path names...done.")
        except Exception, e:
            errorMessage = "Unable to generate relative path names."
            logging.error(errorMessage)
            logging.debug("Error was: " + str(e))
            logging.debug("Building relative path names...failed.")
            raise Exception(e)


        #build message dict
        try:
            logging.debug("Building message dict...")
            messageDict = { "filename"      : filename,
                            "sourcePath"    : parentDir,
                            "relativeParent": relativeParent,
                            "targetPath"    : targetPath}

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

    def shuttingDown(self):
        self.messageSocket.close()


def getDefaultConfig_logfilePath():
    defaultConfig = getDefaultConfig()

    if helperScript.isWindows():
        osName = "Windows"
    elif helperScript.isLinux():
        osName = "Linux"
    else:
        return ""

    returnValue = defaultConfig["logfilePath"][osName]
    return returnValue


def getDefaultConfig_logfileName():
    defaultConfig = getDefaultConfig()

    if helperScript.isWindows():
        osName = "Windows"
    elif helperScript.isLinux():
        osName = "Linux"
    else:
        return ""

    returnValue = defaultConfig["logfileName"][osName]
    return returnValue


def getDefaultConfig_pushServerPort():
    defaultConfig = getDefaultConfig()

    if helperScript.isWindows():
        osName = "Windows"
    elif helperScript.isLinux():
        osName = "Linux"
    else:
        return ""

    returnValue = defaultConfig["pushServerPort"][osName]
    return returnValue


def getDefaultConfig_pushServerIp():
    defaultConfig = getDefaultConfig()

    if helperScript.isWindows():
        osName = "Windows"
    elif helperScript.isLinux():
        osName = "Linux"
    else:
        return ""

    returnValue = defaultConfig["pushServerIp"][osName]
    return returnValue


def getDefaultConfig():
    defaultConfigDict = {
                            "logfilePath"    : { "Windows" : "C:\\",
                                                 "Linux"   : "/tmp/log/"
                                               },
                            "logfileName"    : { "Windows" : "watchFolder.log",
                                                 "Linux"   : "watchFolder.log"
                                               },
                            "pushServerPort" : { "Windows": "6060",
                                                 "Linux" : "6060"
                                               },
                            "pushServerIp"   : { "Windows": "127.0.0.1",
                                                 "Linux" : "127.0.0.1"
                                               },
                        }
    return defaultConfigDict



def argumentParsing():


    parser = argparse.ArgumentParser()
    parser.add_argument("--watchFolder" , type=str, help="folder you want to monitor for changes")
    parser.add_argument("--staticNotification",
                        help="disables new file-events. just sends a list of currently available files within the defined 'watchFolder'.",
                        action="store_true")
    parser.add_argument("--logfilePath" , type=str, help="path where logfile will be created", default=getDefaultConfig_logfilePath())
    parser.add_argument("--logfileName" , type=str, help="filename used for logging", default=getDefaultConfig_logfileName())
    parser.add_argument("--pushServerIp", type=str, help="zqm endpoint (IP-address) to send file events to", default=getDefaultConfig_pushServerIp())
    parser.add_argument("--pushServerPort", type=str, help="zqm endpoint (port) to send file events to", default=getDefaultConfig_pushServerPort())
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

    #abort if watch-folder does not exist
    helperScript.checkFolderExistance(watchFolder)


    #enable logging
    helperScript.initLogging(logfileFilePath, verbose)


    #create zmq context
    zmqContext = zmq.Context()


    #run only once, skipping file events
    #just get a list of all files in watchDir and pass to zeromq
    directoryWatcher = DirectoryWatcherHandler(zmqContext, fileEventServerIp, watchFolder, fileEventServerPort)


    workers = zmqContext.socket(zmq.PULL)
    zmqSocketStr = 'tcp://' + communicationWithLcyncdIp + ':' + communicationWithLcyncdPort
    workers.bind(zmqSocketStr)
    logging.debug("Bind to lcyncd ZMQ socket: " + str(zmqSocketStr))

    try:
        while True:
            #waiting for new jobs
            workload = workers.recv()

            #transform to dictionary
            try:
                workloadDict = json.loads(str(workload))
            except:
                errorMessage = "invalid job received. skipping job"
                logging.error(errorMessage)
                logging.debug("workload=" + str(workload))
                continue

            #extract fileEvent metadata
            try:
                #TODO validate fileEventMessageDict dict
                filepath   = workloadDict["filepath"]
                targetPath = workloadDict["targetPath"]
                logging.info("Received message: filepath: " + str(filepath) + ", targetPath: " + str(targetPath))
            except Exception, e:
                errorMessage   = "Invalid fileEvent message received."
                logging.error(errorMessage)
                logging.debug("Error was: " + str(e))
                logging.debug("workloadDict=" + str(workloadDict))
                #skip all further instructions and continue with next iteration
                continue

            # send the file to the fileMover
            directoryWatcher.passFileToZeromq(filepath, targetPath)
    except KeyboardInterrupt:
        logging.info("Keyboard interruption detected. Shuting down")

    workers.close()
    directoryWatcher.shuttingDown()
    zmqContext.destroy()

