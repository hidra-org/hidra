__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import argparse
import zmq
import os
import logging
import sys
import json
import trace


#
#  --------------------------  class: TaskHandler  --------------------------------------
#

class TaskHandler():
    def __init__ (self, eventDetectorConfig, requestFwPort, distrPort, context = None):
        #eventDetectorConfig = {
        #        configType: ... ,
        #        watchDir : ... ,
        #        monEventType : ... ,
        #        monDefaultSubdirs : ... ,
        #        monSuffixes : ... ,
        #}

        self.log               = self.getLogger()
        self.log.debug("TaskHandler: __init__()")

        self.eventDetector     = None
        self.watchDir          = None
        self.monEventType      = "IN_CLOSE_WRITE"
        self.monDefaultSubdirs = ["commissioning", "current", "local"]
        self.monSuffixes       = [".tif", ".cbf"]

        self.config = eventDetectorConfig

        self.localhost         = "127.0.0.1"
        self.extIp             = "0.0.0.0"
        self.requestFwPort     = requestFwPort
        self.distrPort         = distrPort
        self.requestFwSocket   = None
        self.distrSocket       = None

        self.log.debug("Registering ZMQ context")
        # remember if the context was created outside this class or not
        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False


        if self.config["configType"] == "inotify":

            from InotifyDetector import InotifyDetector as EventDetector

            self.watchDir          = os.path.normpath(watchDir)

            self.monEventType      = self.config["monEventType"] or None
            self.log.info ("Monitored event type is: " + str(self.monEventType))

            self.monDefaultSubdirs = self.config["monDefaultSubdirs"] or None
            self.monSuffixes       = self.config["monSuffixes"] or None
            self.log.info ("Monitored suffixes are: " + str(self.monSuffixes))

            monDirs                = [self.config["self.watchDir"]]
            #TODO forward self.config instead of seperate variables
            self.eventDetector     = EventDetector(monDirs, self.monEventType, self.monDefaultSubdirs, self.monSuffixes)
        else:
            self.log.error("Type of event detector is not supported: " + str( self.config["configType"] ))
            return -1

        self.createSockets()

        try:
            self.run()
        except KeyboardInterrupt:
            self.log.debug("Keyboard interruption detected. Shuting down")
        except:
            trace = traceback.format_exc()
            self.log.info("Stopping TaskHandler due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))


    def getLogger (self):
        logger = logging.getLogger("TaskHandler")
        return logger


    def createSockets (self):
        # socket to get requests
        self.requestFwSocket = self.context.socket(zmq.REQ)
        connectionStr  = "tcp://{ip}:{port}".format( ip=self.localhost, port=self.requestFwPort )
        try:
            self.requestFwSocket.connect(connectionStr)
            self.log.debug("Connecting to requestFwSocket (connect): " + str(connectionStr))
        except Exception as e:
            self.log.error("Failed to start requestFwSocket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # socket to disribute the events to the worker
        self.distrSocket = self.context.socket(zmq.PUSH)
        connectionStr  = "tcp://{ip}:{port}".format( ip=self.localhost, port=self.requestFwPort )
        try:
            self.distrSocket.bind(connectionStr)
            self.log.debug("Connecting to distributing socket (bind): " + str(connectionStr))
        except Exception as e:
            self.log.error("Failed to start distributing Socket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))


    def run (self):
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
                # get requests for this event
                try:
                    self.log.debug("Get requests...")
                    self.requestFwSocket.send("")
                    requests = self.requestFwSocket.recv_multipart()
                    self.log.debug("Get requests... done.")
                    self.log.debug("Requests: " + str(requests))
                except:
                    self.log.error("Get Requests... failed.")
                    trace = traceback.format_exc()
                    self.log.debug("Error was: " + str(trace))


                # build message dict
                try:
                    self.log.debug("Building message dict...")
                    messageDict = json.dumps(workload)  #sets correct escape characters
                    self.log.debug("Building message dict...done.")
                except Exception, e:
                    self.log.error("Unable to assemble message dict.")
                    self.log.debug("Error was: " + stri(e))
                    continue

                # send the file to the fileMover
                try:
                    self.log.debug("Sending message...")
                    self.log.debug(str(messageDict))
                    self.distrSocket.send_multipart([messageDict, requests])
                    self.log.debug("Sending message...done.")
                except Exception, e:
                    self.log.error("Sending message...failed.")
                    self.log.debug("Error was: " + str(e))



    def stop(self):
        if self.distrSocket:
            self.distrSocket.close(0)
            self.distrSocket = None
        if self.requestFwSocket:
            self.requestFwSocket.close(0)
            self.requestFwSocket = None
        if not self.extContext and if self.context:
            self.context.destroy()
            self.context = None


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()



if __name__ == '__main__':

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
        if watchDir in [ None, "", "None" ]:
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

