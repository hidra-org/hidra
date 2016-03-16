__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import argparse
import zmq
import os
import logging
import sys
import json
import time
import cPickle
from multiprocessing import Process, freeze_support, Queue
import ConfigParser
from logutils.queue import QueueHandler

from SignalHandler import SignalHandler
from TaskProvider import TaskProvider
from DataDispatcher import DataDispatcher

try:
    BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"
CONFIG_PATH = BASE_PATH + os.sep + "conf"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers
from version import __version__


def argumentParsing():
    configFile = CONFIG_PATH + os.sep + "dataManager.conf"

    config = ConfigParser.RawConfigParser()
    config.readfp(helpers.FakeSecHead(open(configFile)))

    logfilePath        = config.get('asection', 'logfilePath')
    logfileName        = config.get('asection', 'logfileName')
    logfileSize        = config.get('asection', 'logfileSize') #100*1048576

    comPort            = config.get('asection', 'comPort')
    whitelist          = json.loads(config.get('asection', 'whitelist'))

    requestPort        = config.get('asection', 'requestPort')
    requestFwPort      = config.get('asection', 'requestFwPort')

    eventDetectorType  = config.get('asection', 'eventDetectorType')
    monitoredDir       = config.get('asection', 'monitoredDir')
    monitoredEventType = config.get('asection', 'monitoredEventType')
    monitoredSubdirs   = json.loads(config.get('asection', 'monitoredSubdirs'))
    monitoredFormats   = json.loads(config.get('asection', 'monitoredFormats'))
    timeTillClosed     = int(config.get('asection', 'timeTillClosed'))

    useDataStream      = config.getboolean('asection', 'useDataStream')
    fixedStreamHost    = config.get('asection', 'fixedStreamHost')
    fixedStreamPort    = config.get('asection', 'fixedStreamPort')

    numberOfStreams    = config.get('asection', 'numberOfStreams')
    chunkSize          = int(config.get('asection', 'chunkSize'))

    routerPort         = config.get('asection', 'routerPort')

    localTarget        = config.get('asection', 'localTarget')
    cleanerPort        = config.get('asection', 'cleanerPort')


    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"       , type    = str,
                                                help    = "Path where the logfile will be created (default=" + str(logfilePath) + ")",
                                                default = logfilePath )
    parser.add_argument("--logfileName"       , type    = str,
                                                help    = "Filename used for logging (default=" + str(logfileName) + ")",
                                                default = logfileName )
    parser.add_argument("--logfileSize"       , type    = int,
                                                help    = "File size before rollover in B (linux only; (default=" + str(logfileSize) + ")",
                                                default = logfileSize )
    parser.add_argument("--verbose"           , help    = "More verbose output",
                                                action  = "store_true")
    parser.add_argument("--onScreen"          , type    = str,
                                                help    = "Display logging on screen (options are CRITICAL, ERROR, WARNING, INFO, DEBUG)",
                                                default = False )

    parser.add_argument("--comPort"           , type    = str,
                                                help    = "Port number to receive signals (default=" + str(comPort) + ")",
                                                default = comPort )
    parser.add_argument("--whitelist"         , type    = str,
                                                help    = "List of hosts allowed to connect (default=" + str(whitelist) + ")",
                                                default = whitelist )

    parser.add_argument("--requestPort"       , type    = str,
                                                help    = "ZMQ port to get new requests (default=" + str(requestPort) + ")",
                                                default = requestPort )
    parser.add_argument("--requestFwPort"     , type    = str,
                                                help    = "ZMQ port to forward requests (default=" + str(requestFwPort) + ")",
                                                default = requestFwPort )

    parser.add_argument("--eventDetectorType" , type    = str,
                                                help    = "Type of event detector to use (default=" + str(eventDetectorType) + ")",
                                                default = eventDetectorType )
    parser.add_argument("--monitoredDir"      , type    = str,
                                                help    = "Directory to be monitor for changes; inside this directory only the specified \
                                                            subdirectories are monitred (default=" + str(monitoredDir) + ")",
                                                default = monitoredDir )
    parser.add_argument("--monitoredEventType", type    = str,
                                                help    = "Event type of files to be monitored (default=" + str(monitoredEventType) + ")",
                                                default = monitoredEventType )
    parser.add_argument("--monitoredSubdirs"  , type    = str,
                                                help    = "Subdirectories of 'monitoredDirs' to be monitored (default=" + str(monitoredSubdirs) + ")",
                                                default = monitoredSubdirs )
    parser.add_argument("--monitoredFormats"  , type    = str,
                                                help    = "The formats to be monitored, files in an other format will be be neglected \
                                                           (default=" + str(monitoredFormats) + ")",
                                                default = monitoredFormats )
    parser.add_argument("--timeTillClosed"    , type    = str,
                                                help    = "Time (in seconds) since last modification after which a file will be seen as closed \
                                                           (default=" + str(timeTillClosed) + ")",
                                                default = timeTillClosed )

    parser.add_argument("--useDataStream"     , type    = str,
                                                help    = "Enable ZMQ pipe into storage system (if set to false: the file is moved \
                                                           into the localTarget) (default=" + str(useDataStream) + ")",
                                                default = useDataStream )
    parser.add_argument("--fixedStreamHost"   , type    = str,
                                                help    = "Fixed host to send the data to with highest priority \
                                                        (only active is useDataStream is set; default=" + str(fixedStreamHost) + ")",
                                                default = fixedStreamHost )
    parser.add_argument("--fixedStreamPort"   , type    = str,
                                                help    = "Fixed port to send the data to with highest priority \
                                                        (only active is useDataStream is set; default=" + str(fixedStreamPort) + ")",
                                                default = fixedStreamPort )
    parser.add_argument("--numberOfStreams"   , type    = int,
                                                help    = "Number of parallel data streams (default=" + str(numberOfStreams) + ")",
                                                default = numberOfStreams )
    parser.add_argument("--chunkSize"         , type    = int,
                                                help    = "Chunk size of file-parts getting send via ZMQ (default=" + str(chunkSize) + ")",
                                                default = chunkSize )

    parser.add_argument("--routerPort"        , type    = str,
                                                help    = "ZMQ-router port which coordinates the load-balancing \
                                                           to the worker-processes (default=" + str(routerPort) + ")",
                                                default = routerPort )

    parser.add_argument("--localTarget"       , type    = str,
                                                help    = "Target to move the files into (default=" + str(localTarget) + ")",
                                                default = localTarget )
    parser.add_argument("--cleanerPort"       , type    = str,
                                                help    = "ZMQ-pull-socket port which deletes/moves given file \
                                                           (default=" + str(cleanerPort) + ")",
                                                default = cleanerPort )

    arguments         = parser.parse_args()

    logfilePath       = str(arguments.logfilePath)
    logfileName       = str(arguments.logfileName)
    logfileFullPath   = os.path.join(logfilePath, logfileName)
    verbose           = arguments.verbose
    onScreen          = arguments.onScreen

    eventDetectorType = arguments.eventDetectorType.lower()
    supportedEDTypes  = ["inotifyx", "watchdog"]
    monitoredDir      = str(arguments.monitoredDir)
    monitoredSubdirs  = arguments.monitoredSubdirs
    localTarget       = str(arguments.localTarget)

    useDataStream     = arguments.useDataStream
    numberOfStreams   = arguments.numberOfStreams

    # check if logfile is writable
    helpers.checkLogFileWritable(logfilePath, logfileName)

    # check if the eventDetectorType is supported
    helpers.checkEventDetectorType(eventDetectorType, supportedEDTypes)

    # check if directories exists
    helpers.checkDirExistance(logfilePath)
    helpers.checkDirExistance(monitoredDir)
    helpers.checkSubDirExistance(monitoredDir, monitoredSubdirs)
    if useDataStream:
        helpers.checkDirExistance(localTarget)

    return arguments


class DataManager():
    def __init__(self, logQueue = None):
        arguments = argumentParsing()

        logfilePath           = arguments.logfilePath
        logfileName           = arguments.logfileName
        logfile               = os.path.join(logfilePath, logfileName)
        logsize               = arguments.logfileSize
        verbose               = arguments.verbose
        onScreen              = arguments.onScreen

        self.extLogQueue      = False

        if logQueue:
            self.logQueue    = logQueue
            self.extLogQueue = True
        else:
            # Get queue
            self.logQueue    = Queue(-1)

            # Get the log Configuration for the lisener
            if onScreen:
                h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose, onScreen)

                # Start queue listener using the stream handler above
                self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1, h2)
            else:
                h1 = helpers.getLogHandlers(logfile, logsize, verbose, onScreen)

                # Start queue listener using the stream handler above
                self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1)

            self.logQueueListener.start()

        # Create log and set handler to queue handle
        self.log = self.getLogger(self.logQueue)

        self.comPort          = arguments.comPort
        self.whitelist        = arguments.whitelist

        self.requestPort      = arguments.requestPort
        self.requestFwPort    = arguments.requestFwPort

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
        elif arguments.eventDetectorType == "lambda":
            self.eventDetectorConfig = {
                    "eventPort"         : "6001", #arguments.eventPort,
                    "numberOfStreams"   : arguments.numberOfStreams,
                    "context"           : None,
                    }


        if arguments.useDataStream:
            self.fixedStreamId = "{host}:{port}".format( host=arguments.fixedStreamHost, port=arguments.fixedStreamPort )
        else:
            self.fixedStreamId = None

        self.numberOfStreams  = arguments.numberOfStreams
        self.chunkSize        = arguments.chunkSize

        self.routerPort       = arguments.routerPort

        self.localTarget      = arguments.localTarget
        self.cleanerPort      = arguments.cleanerPort

        self.signalHandlerPr  = None
        self.taskProviderPr   = None
        self.dataDispatcherPr = []

        self.log.info("Version: " + str(__version__))

        #create zmq context
        # there should be only one context in one process
        self.zmqContext = zmq.Context.instance()
        self.log.debug("Registering global ZMQ context")

        self.run()

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("DataManager")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger



    def run(self):
        self.signalHandlerPr = Process ( target = SignalHandler, args = (self.whitelist, self.comPort, self.requestFwPort, self.requestPort, self.logQueue) )
        self.signalHandlerPr.start()

        # needed, because otherwise the requests for the first files are not forwarded properly
        time.sleep(0.5)

        self.taskProviderPr = Process ( target = TaskProvider, args = (self.eventDetectorConfig, self.requestFwPort, self.routerPort, self.logQueue) )
        self.taskProviderPr.start()

        for id in range(self.numberOfStreams):
            pr = Process ( target = DataDispatcher, args = ( id, self.routerPort, self.chunkSize, self.fixedStreamId, self.logQueue, self.localTarget) )
            pr.start()
            self.dataDispatcherPr.append(pr)


    def stop(self):
        if self.signalHandlerPr:
            self.log.info("terminate SignalHandler...")
            self.signalHandlerPr.terminate()
            self.signalHandlerPr = None
            self.log.info("terminate SignalHandler...done")

        if self.taskProviderPr:
            self.log.info("terminate TaskProvider...")
            self.taskProviderPr.terminate()
            self.taskProviderPr = None
            self.log.info("terminate TaskProvider...done")

        for pr in self.dataDispatcherPr:
            id = self.dataDispatcherPr.index(pr)
            self.log.info("terminate DataDispatcher-" + str(id) + "...")
            pr.terminate()
            pr = None
            self.log.info("terminate DataDispatcher-" + str(id) + "...done")

        if self.dataDispatcherPr == [ None for i in self.dataDispatcherPr ]:
            self.dataDispatcher = []

        if not self.extLogQueue and self.logQueueListener:
            self.logQueue.put_nowait(None)
            self.logQueueListener.stop()
            self.logQueueListener = None


    def __exit__(self):
        self.stop()


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class Test_Receiver_Stream():
    def __init__(self, comPort, fixedRecvPort, receivingPort, receivingPort2, logQueue):

        self.log = self.getLogger(logQueue)

        context = zmq.Context.instance()

        self.comSocket       = context.socket(zmq.REQ)
        connectionStr   = "tcp://localhost:" + comPort
        self.comSocket.connect(connectionStr)
        self.log.info("=== comSocket connected to " + connectionStr)

        self.fixedRecvSocket = context.socket(zmq.PULL)
        connectionStr   = "tcp://0.0.0.0:" + fixedRecvPort
        self.fixedRecvSocket.bind(connectionStr)
        self.log.info("=== fixedRecvSocket connected to " + connectionStr)

        self.receivingSocket = context.socket(zmq.PULL)
        connectionStr   = "tcp://0.0.0.0:" + receivingPort
        self.receivingSocket.bind(connectionStr)
        self.log.info("=== receivingSocket connected to " + connectionStr)

        self.receivingSocket2 = context.socket(zmq.PULL)
        connectionStr   = "tcp://0.0.0.0:" + receivingPort2
        self.receivingSocket2.bind(connectionStr)
        self.log.info("=== receivingSocket2 connected to " + connectionStr)

        self.sendSignal("START_STREAM", receivingPort, 1)
        self.sendSignal("START_STREAM", receivingPort2, 0)

        self.run()


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("Test_Receiver_Stream")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def sendSignal(self, signal, ports, prio = None):
        self.log.info("=== sendSignal : " + signal + ", " + str(ports))
        sendMessage = ["0.0.1",  signal]
        targets = []
        if type(ports) == list:
            for port in ports:
                targets.append(["localhost:" + port, prio])
        else:
            targets.append(["localhost:" + ports, prio])

        targets = cPickle.dumps(targets)
        sendMessage.append(targets)
        self.comSocket.send_multipart(sendMessage)
        receivedMessage = self.comSocket.recv()
        self.log.info("=== Responce : " + receivedMessage )

    def run(self):
        try:
            while True:
                recv_message = self.fixedRecvSocket.recv_multipart()
                self.log.info("=== received fixed: " + str(cPickle.loads(recv_message[0])))
                recv_message = self.receivingSocket.recv_multipart()
                self.log.info("=== received: " + str(cPickle.loads(recv_message[0])))
                recv_message = self.receivingSocket2.recv_multipart()
                self.log.info("=== received 2: " + str(cPickle.loads(recv_message[0])))
        except KeyboardInterrupt:
            pass

    def __exit__(self):
        self.receivingSocket.close(0)
        self.receivingSocket2.close(0)
        context.destroy()


if __name__ == '__main__':
    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows

    test = False

    if test:
        import time
        from shutil import copyfile
        from subprocess import call


        logfile = BASE_PATH + os.sep + "logs" + os.sep + "dataManager_test.log"
        logsize = 10485760

        logQueue = Queue(-1)

        # Get the log Configuration for the lisener
        h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose=True, onScreenLogLevel="debug")

        # Start queue listener using the stream handler above
        logQueueListener = helpers.CustomQueueListener(logQueue, h1, h2)
        logQueueListener.start()

        # Create log and set handler to queue handle
        root = logging.getLogger()
        root.setLevel(logging.DEBUG) # Log level = DEBUG
        qh = QueueHandler(logQueue)
        root.addHandler(qh)


        comPort        = "50000"
        fixedRecvPort  = "50100"
        receivingPort  = "50101"
        receivingPort2 = "50102"

        testPr = Process ( target = Test_Receiver_Stream, args = (comPort, fixedRecvPort, receivingPort, receivingPort2, logQueue))
        testPr.start()
        logging.debug("test receiver started")

        sourceFile = BASE_PATH + os.sep + "test_file.cbf"
        targetFileBase = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep

        try:
            sender = DataManager(logQueue)
        except:
            sender = None

        if sender:
            time.sleep(0.5)
            i = 100
            try:
                while i <= 105:
                    targetFile = targetFileBase + str(i) + ".cbf"
                    logging.debug("copy to " + targetFile)
                    copyfile(sourceFile, targetFile)
                    i += 1

                    time.sleep(1)
            except Exception as e:
                logging.error("Exception detected: " + str(e), exc_info=True)
            finally:
                time.sleep(3)
                testPr.terminate()

                for number in range(100, i):
                    targetFile = targetFileBase + str(number) + ".cbf"
                    try:
                        os.remove(targetFile)
                        logging.debug("remove " + targetFile)
                    except:
                        pass

                sender.stop()
                logQueue.put_nowait(None)
                logQueueListener.stop()

    else:
        sender = DataManager()

        try:
            while True:
                pass
        except:
            pass
        finally:
            sender.stop()

