__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import argparse
import zmq
import os
import logging
import sys
import json
from multiprocessing import Process, freeze_support
import ConfigParser

#BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))
BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) ))
CONFIG_PATH = BASE_PATH + os.sep + "conf"

import shared.helpers as helpers
from sender.SignalHandler import SignalHandler
from sender.TaskProvider import TaskProvider
from sender.DataDispatcher import DataDispatcher


from version import __version__


def argumentParsing():
    configFile = CONFIG_PATH + os.sep + "dataManager.conf"

    config = ConfigParser.RawConfigParser()
    config.readfp(helpers.FakeSecHead(open(configFile)))

    logfilePath         = config.get('asection', 'logfilePath')
    logfileName         = config.get('asection', 'logfileName')

    comPort             = config.get('asection', 'comPort')
    whitelist           = json.loads(config.get('asection', 'whitelist'))

    requestPort         = config.get('asection', 'requestPort')
    requestFwPort       = config.get('asection', 'requestFwPort')

    eventDetectorType   = config.get('asection', 'eventDetectorType')
    monitoredDir        = config.get('asection', 'monitoredDir')
    monitoredEventType  = config.get('asection', 'monitoredEventType')
    monitoredSubdirs    = json.loads(config.get('asection', 'monitoredSubdirs'))
    monitoredFormats    = json.loads(config.get('asection', 'monitoredFormats'))
    timeTillClosed      = int(config.get('asection', 'timeTillClosed'))

    useDataStream       = config.getboolean('asection', 'useDataStream')
    fixedStreamHost     = config.get('asection', 'fixedStreamHost')
    fixedStreamPort     = config.get('asection', 'fixedStreamPort')

    parallelDataStreams = config.get('asection', 'parallelDataStreams')
    chunkSize           = int(config.get('asection', 'chunkSize'))

    routerPort          = config.get('asection', 'routerPort')

    localTarget         = config.get('asection', 'localTarget')
    cleanerPort         = config.get('asection', 'cleanerPort')


    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"        , type    = str,
                                                 help    = "Path where the logfile will be created (default=" + str(logfilePath) + ")",
                                                 default = logfilePath )
    parser.add_argument("--logfileName"        , type    = str,
                                                 help    = "Filename used for logging (default=" + str(logfileName) + ")",
                                                 default = logfileName )
    parser.add_argument("--verbose"            , help    = "More verbose output",
                                                 action  = "store_true")
    parser.add_argument("--onScreen"           , type    = str,
                                                 help    = "Display logging on screen (options are CRITICAL, ERROR, WARNING, INFO, DEBUG)",
                                                 default = False )

    parser.add_argument("--comPort"            , type    = str,
                                                 help    = "Port number to receive signals (default=" + str(comPort) + ")",
                                                 default = comPort )
    parser.add_argument("--whitelist"          , type    = str,
                                                 help    = "List of hosts allowed to connect (default=" + str(whitelist) + ")",
                                                 default = whitelist )

    parser.add_argument("--requestPort"        , type    = str,
                                                 help    = "ZMQ port to get new requests (default=" + str(requestPort) + ")",
                                                 default = requestPort )
    parser.add_argument("--requestFwPort"      , type    = str,
                                                 help    = "ZMQ port to forward requests (default=" + str(requestFwPort) + ")",
                                                 default = requestFwPort )

    parser.add_argument("--eventDetectorType"  , type    = str,
                                                 help    = "Type of event detector to use (default=" + str(eventDetectorType) + ")",
                                                 default = eventDetectorType )
    parser.add_argument("--monitoredDir"       , type    = str,
                                                 help    = "Directory to be monitor for changes; inside this directory only the specified \
                                                            subdirectories are monitred (default=" + str(monitoredDir) + ")",
                                                 default = monitoredDir )
    parser.add_argument("--monitoredEventType" , type    = str,
                                                 help    = "Event type of files to be monitored (default=" + str(monitoredEventType) + ")",
                                                 default = monitoredEventType )
    parser.add_argument("--monitoredSubdirs"   , type    = str,
                                                 help    = "Subdirectories of 'monitoredDirs' to be monitored (default=" + str(monitoredSubdirs) + ")",
                                                 default = monitoredSubdirs )
    parser.add_argument("--monitoredFormats"   , type    = str,
                                                 help    = "The formats to be monitored, files in an other format will be be neglected \
                                                            (default=" + str(monitoredFormats) + ")",
                                                 default = monitoredFormats )
    parser.add_argument("--timeTillClosed"     , type    = str,
                                                 help    = "Time (in seconds) since last modification after which a file will be seen as closed \
                                                            (default=" + str(timeTillClosed) + ")",
                                                 default = timeTillClosed )

    parser.add_argument("--useDataStream"      , type    = str,
                                                 help    = "Enable ZMQ pipe into storage system (if set to false: the file is moved \
                                                            into the localTarget) (default=" + str(useDataStream) + ")",
                                                 default = useDataStream )
    parser.add_argument("--fixedStreamHost"    , type    = str,
                                                 help    = "Fixed host to send the data to with highest priority \
                                                         (only active is useDataStream is set; default=" + str(fixedStreamHost) + ")",
                                                 default = fixedStreamHost )
    parser.add_argument("--fixedStreamPort"    , type    = str,
                                                 help    = "Fixed port to send the data to with highest priority \
                                                         (only active is useDataStream is set; default=" + str(fixedStreamPort) + ")",
                                                 default = fixedStreamPort )
    parser.add_argument("--parallelDataStreams", type    = int,
                                                 help    = "Number of parallel data streams (default=" + str(parallelDataStreams) + ")",
                                                 default = parallelDataStreams )
    parser.add_argument("--chunkSize"          , type    = int,
                                                 help    = "Chunk size of file-parts getting send via ZMQ (default=" + str(chunkSize) + ")",
                                                 default = chunkSize )

    parser.add_argument("--routerPort"         , type    = str,
                                                 help    = "ZMQ-router port which coordinates the load-balancing \
                                                            to the worker-processes (default=" + str(routerPort) + ")",
                                                 default = routerPort )

    parser.add_argument("--localTarget"        , type    = str,
                                                 help    = "Target to move the files into (default=" + str(localTarget) + ")",
                                                 default = localTarget )
    parser.add_argument("--cleanerPort"        , type    = str,
                                                 help    = "ZMQ-pull-socket port which deletes/moves given file \
                                                            (default=" + str(cleanerPort) + ")",
                                                 default = cleanerPort )

    arguments           = parser.parse_args()

    logfilePath         = str(arguments.logfilePath)
    logfileName         = str(arguments.logfileName)
    logfileFullPath     = os.path.join(logfilePath, logfileName)
    verbose             = arguments.verbose
    onScreen            = arguments.onScreen

    eventDetectorType   = arguments.eventDetectorType.lower()
    supportedEDTypes    = ["inotifyx", "watchdog"]
    monitoredDir        = str(arguments.monitoredDir)
    monitoredSubdirs    = arguments.monitoredSubdirs
    localTarget         = str(arguments.localTarget)

    parallelDataStreams = arguments.parallelDataStreams

    # check if logfile is writable
    helpers.checkLogFileWritable(logfilePath, logfileName)

    #enable logging
    helpers.initLogging(logfileFullPath, verbose, onScreen)

    # check if the eventDetectorType is supported
    helpers.checkEventDetectorType(eventDetectorType, supportedEDTypes)

    # check if directories exists
    helpers.checkDirExistance(logfilePath)
    helpers.checkDirExistance(monitoredDir)
    helpers.checkSubDirExistance(monitoredDir, monitoredSubdirs)
    helpers.checkDirExistance(localTarget)

    return arguments


class Sender():
    def __init__(self):
        arguments = argumentParsing()

        self.comPort             = arguments.comPort
        self.whitelist           = arguments.whitelist

        self.requestPort         = arguments.requestPort
        self.requestFwPort       = arguments.requestFwPort

        if arguments.eventDetectorType == "inotifyx":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "monDir"            : arguments.monitoredDir,
                    "monEventType"      : arguments.monitoredEventType,
                    "monSubdirs"        : arguments.monitoredSubdirs,
                    "monSuffixes"       : arguments.monitoredFormats
                    }
        elif arguments.eventDetectorType == "watchdog":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "monDir"            : arguments.monitoredDir,
                    "monEventType"      : arguments.monitoredEventType,
                    "monSubdirs"        : arguments.monitoredSubdirs,
                    "monSuffixes"       : arguments.monitoredFormats,
                    "timeTillClosed"    : arguments.timeTillClosed
                    }

        if arguments.useDataStream:
            self.fixedStreamId    = "{host}:{port}".format( host=arguments.fixedStreamHost, port=arguments.fixedStreamPort )
        else:
            self.fixedStreamId    = None

        self.parallelDataStreams = arguments.parallelDataStreams
        self.chunkSize           = arguments.chunkSize

        self.routerPort          = arguments.routerPort

        self.localTarget         = arguments.localTarget
        self.cleanerPort         = arguments.cleanerPort

        self.signalHandlerPr  = None
        self.taskProviderPr   = None
        self.dataDispatcherPr = None

        logging.info("Version: " + str(__version__))

        #create zmq context
        # there should be only one context in one process
        self.zmqContext = zmq.Context.instance()
        logging.debug("Registering global ZMQ context")

        self.run()


    def run(self):
        logging.info("Start SignalHandler...")
        self.signalHandlerPr = Process ( target = SignalHandler, args = (self.whitelist, self.comPort, self.requestFwPort, self.requestPort) )
        self.signalHandlerPr.start()
        logging.debug("Start SignalHandler...done")

        # needed, because otherwise the requests for the first files are not forwarded properly
        time.sleep(0.5)

        logging.info("Start TaskProvider...")
        self.taskProviderPr = Process ( target = TaskProvider, args = (self.eventDetectorConfig, self.requestFwPort, self.routerPort) )
        self.taskProviderPr.start()
        logging.info("Start TaskProvider...done")

        logging.info("Start DataDispatcher...")
        self.dataDispatcherPr = Process ( target = DataDispatcher, args = ( 1, self.routerPort, self.chunkSize, self.fixedStreamId, self.localTarget) )
        self.dataDispatcherPr.start()
        logging.info("Start DataDispatcher...done")

    def stop(self):
        self.signalHandlerPr.terminate()
        self.taskProviderPr.terminate()
        self.dataDispatcherPr.terminate()


    def __exit__(self):
        self.stop()

#    def __del__(self):
#        self.stop()



if __name__ == '__main__':
    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows

    test = True

    if test:
        import time
        import cPickle
        from shutil import copyfile
        from subprocess import call


        logfile = BASE_PATH + os.sep + "logs" + os.sep + "dataManager_test.log"
        #enable logging
        helpers.initLogging(logfile, verbose=True, onScreenLogLevel="debug")

        class Test_Receiver_Stream():
            def __init__(self, comPort, fixedRecvPort, receivingPort, receivingPort2):
                context       = zmq.Context.instance()

                self.comSocket       = context.socket(zmq.REQ)
                connectionStr   = "tcp://zitpcx19282:" + comPort
                self.comSocket.connect(connectionStr)
                logging.info("=== comSocket connected to " + connectionStr)

                self.fixedRecvSocket = context.socket(zmq.PULL)
                connectionStr   = "tcp://0.0.0.0:" + fixedRecvPort
                self.fixedRecvSocket.bind(connectionStr)
                logging.info("=== fixedRecvSocket connected to " + connectionStr)

                self.receivingSocket = context.socket(zmq.PULL)
                connectionStr   = "tcp://0.0.0.0:" + receivingPort
                self.receivingSocket.bind(connectionStr)
                logging.info("=== receivingSocket connected to " + connectionStr)

                self.receivingSocket2 = context.socket(zmq.PULL)
                connectionStr   = "tcp://0.0.0.0:" + receivingPort2
                self.receivingSocket2.bind(connectionStr)
                logging.info("=== receivingSocket2 connected to " + connectionStr)

                self.sendSignal("START_STREAM", receivingPort, 1)
                self.sendSignal("START_STREAM", receivingPort2, 0)

                self.run()

            def sendSignal(self, signal, ports, prio = None):
                logging.info("=== sendSignal : " + signal + ", " + str(ports))
                sendMessage = ["0.0.1",  signal]
                targets = []
                if type(ports) == list:
                    for port in ports:
                        targets.append(["zitpcx19282:" + port, prio])
                else:
                    targets.append(["zitpcx19282:" + ports, prio])
                targets = cPickle.dumps(targets)
                sendMessage.append(targets)
                self.comSocket.send_multipart(sendMessage)
                receivedMessage = self.comSocket.recv()
                logging.info("=== Responce : " + receivedMessage )

            def run(self):
                try:
                    while True:
                        recv_message = self.fixedRecvSocket.recv_multipart()
                        logging.info("=== received fixed: " + str(cPickle.loads(recv_message[0])))
                        recv_message = self.receivingSocket.recv_multipart()
                        logging.info("=== received: " + str(cPickle.loads(recv_message[0])))
                        recv_message = self.receivingSocket2.recv_multipart()
                        logging.info("=== received 2: " + str(cPickle.loads(recv_message[0])))
                except KeyboardInterrupt:
                    pass

            def __exit__(self):
                self.receivingSocket.close(0)
                self.receivingSocket2.close(0)
                context.destroy()


        comPort        = "50000"
        fixedRecvPort  = "50100"
        receivingPort  = "50101"
        receivingPort2 = "50102"

        testPr = Process ( target = Test_Receiver_Stream, args = (comPort, fixedRecvPort, receivingPort, receivingPort2))
        testPr.start()

        sourceFile = BASE_PATH + os.sep + "test_file.cbf"
        targetFileBase = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep


    try:
        sender = Sender()
    except:
        sender = None

    i = 100
    try:
        if sender:
            while i <= 110:
                if test:
                    time.sleep(0.5)
                    targetFile = targetFileBase + str(i) + ".cbf"
                    logging.debug("copy to " + targetFile)
#                    call(["cp", sourceFile, targetFile])
                    copyfile(sourceFile, targetFile)
                    i += 1

                    time.sleep(1)
                else:
                    pass
    except Exception as e:
        logging.error("Exception detected: " + str(e))
    finally:
        if test:
            testPr.terminate()

            for number in range(100, i):
                targetFile = targetFileBase + str(number) + ".cbf"
                logging.debug("remove " + targetFile)
                try:
                    os.remove(targetFile)
                except:
                    pass
        if sender:
            sender.stop()



