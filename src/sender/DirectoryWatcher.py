__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import argparse
import zmq
import os
import logging
import sys
import json
from InotifyDetector import InotifyDetector as EventDetector


#
#  --------------------------  class: DirectoryWatcherHandler  --------------------------------------
#

class DirectoryWatcher():
    patterns                = ["*"]
    zmqContext              = None
    externalContext         = None    # if the context was created outside this class or not
    messageSocket           = None    # strings only, control plane
    fileEventIp             = None
    fileEventPort           = None
    watchDir                = None
    eventDetector           = None
    monitoredEventType      = "IN_CLOSE_WRITE"
    monitoredDefaultSubdirs = ["commissioning", "current", "local"]
    monitoredSuffixes       = [".tif", ".cbf"]
    log                     = None


    def __init__(self, fileEventIp, watchDir, fileEventPort, monitoredEventType = None, monitoredDefaultSubdirs = None, monitoredSuffixes = None, zmqContext = None):

        self.log = self.getLogger()

        self.log.debug("DirectoryWatcherHandler: __init__()")
        self.log.debug("registering zmq context")

        if zmqContext:
            self.zmqContext      = zmqContext
            self.externalContext = True
        else:
            self.zmqContext      = zmq.Context()
            self.externalContext = False

        self.watchDir            = os.path.normpath(watchDir)
        self.fileEventIp         = fileEventIp
        self.fileEventPort       = fileEventPort

        self.monitoredEventType  = monitoredEventType or None

        self.log.info ("Monitored event type is: " + str( monitoredEventType ))

        self.monitoredDefaultSubdirs = monitoredDefaultSubdirs or None
        self.monitoredSuffixes   = monitoredSuffixes or None

        self.log.info ("Monitored suffixes are: " + str( monitoredSuffixes ))

        monitoredDirs            = [self.watchDir]
        self.eventDetector       = EventDetector(monitoredDirs, self.monitoredEventType, self.monitoredDefaultSubdirs, self.monitoredSuffixes)

#        assert isinstance(self.zmqContext, zmq.sugar.context.Context)

        self.createSockets()

        try:
            self.process()
        except KeyboardInterrupt:
            self.log.debug("Keyboard interruption detected. Shuting down")
#        finally:
#            self.eventDetector.stop()
#            self.stop()


    def getLogger(self):
        logger = logging.getLogger("DirectoryWatchHandler")
        return logger


    def createSockets(self):
        #create zmq socket
        self.messageSocket = self.zmqContext.socket(zmq.PUSH)
        connectionStr = "tcp://" + self.fileEventIp + ":" + str(self.fileEventPort)
        try:
            self.messageSocket.connect(connectionStr)
            self.log.debug("Connecting to ZMQ socket: " + str(connectionStr))
        except Exception as e:
            self.log.error("Failed to start ZMQ Socket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))


    def process(self):
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
                self.log.error("Invalid fileEvent message received.")
                self.log.debug("Error was: " + str(e))
                #skip all further instructions and continue with next iteration
                continue

            #TODO validate workload dict
            for workload in workloadList:
                # build message dict
                try:
                    self.log.debug("Building message dict...")
                    messageDict = { "filename"     : workload["filename"],
                                    "sourcePath"   : workload["sourcePath"],
                                    "relativePath" : workload["relativePath"]
                                    }

                    messageDictJson = json.dumps(messageDict)  #sets correct escape characters
                    self.log.debug("Building message dict...done.")
                except Exception, e:
                    self.log.error("Unable to assemble message dict.")
                    self.log.debug("Error was: " + stri(e))
                    continue

                # send the file to the fileMover
                try:
                    self.log.debug("Sending message...")
                    self.log.debug(str(messageDictJson))
                    self.messageSocket.send(messageDictJson)
                    self.log.debug("Sending message...done.")
                except Exception, e:
                    self.log.error("Sending message...failed.")
                    self.log.debug("Error was: " + str(e))



    def stop(self):
        self.messageSocket.close(0)
        if not self.externalContext:
            self.zmqContext.destroy()


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()



def argumentParsing():

    parser = argparse.ArgumentParser()
    parser.add_argument("--watchDir"  , type=str, help="directory you want to monitor for changes")
    parser.add_argument("--staticNotification",
                        help="disables new file-events. just sends a list of currently available files within the defined 'watchDir'.",
                        action="store_true")
    parser.add_argument("--logfilePath"  , type=str, help="path where logfile will be created"              , default="/tmp/log/")
    parser.add_argument("--logfileName"  , type=str, help="filename used for logging"                       , default="watchDir.log")
    parser.add_argument("--fileEventIp"  , type=str, help="zqm endpoint (IP-address) to send file events to", default="127.0.0.1")
    parser.add_argument("--fileEventPort", type=str, help="zqm endpoint (port) to send file events to"      , default="6060")
    parser.add_argument("--verbose"      ,           help="more verbose output", action="store_true")

    arguments = parser.parse_args()

    # TODO: check watchDir-directory for existance

    watchDir = str(arguments.watchDir)
    assert isinstance(type(watchDir), type(str))

    #exit with error if no watchDir path was provided
    if (watchDir == None) or (watchDir == "") or (watchDir == "None"):
        print """You need to set the following option:
--watchDir {DIRECTORY}
"""
        sys.exit(1)


    #abort if watchDir does not exist
    helperScript.checkDirExistance(watchDir)


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
    helperScript.checkDirExistance(arguments.logfilePath)


    return arguments




if __name__ == '__main__':
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
    SRC_PATH  = BASE_PATH + os.sep + "src"

    sys.path.append ( SRC_PATH )

    import shared.helperScript as helperScript

    arguments       = argumentParsing()
    watchDir        = arguments.watchDir
    verbose         = arguments.verbose
    logfileFilePath = os.path.join(arguments.logfilePath, arguments.logfileName)

    fileEventIp     = str(arguments.fileEventIp)
    fileEventPort   = str(arguments.fileEventPort)


    #enable logging
    helperScript.initLogging(logfileFilePath, verbose)


    #run only once, skipping file events
    #just get a list of all files in watchDir and pass to zeromq
    directoryWatcher = DirectoryWatcher(fileEventIp, watchDir, fileEventPort)

