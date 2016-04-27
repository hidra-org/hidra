__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import argparse
import zmq
import zmq.devices
import os
import logging
import sys
import json
import time
import cPickle
from multiprocessing import Process, freeze_support, Queue
import ConfigParser
import threading
import signal

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

from logutils.queue import QueueHandler
import helpers
from version import __version__


def argumentParsing():
    configFile = CONFIG_PATH + os.sep + "dataManager.conf"

    config = ConfigParser.RawConfigParser()
    config.readfp(helpers.FakeSecHead(open(configFile)))

    parser = argparse.ArgumentParser()

    # Logging

    logfilePath        = config.get('asection', 'logfilePath')
    logfileName        = config.get('asection', 'logfileName')
    logfileSize        = config.get('asection', 'logfileSize')

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

    # SignalHandler config

    controlPubPort     = config.get('asection', 'controlPubPort')
    controlSubPort     = config.get('asection', 'controlSubPort')
    comPort            = config.get('asection', 'comPort')
    whitelist          = json.loads(config.get('asection', 'whitelist'))

    requestPort        = config.get('asection', 'requestPort')
    requestFwPort      = config.get('asection', 'requestFwPort')

    parser.add_argument("--controlPubPort"    , type    = str,
                                                help    = "Port number to publish control signals (default=" + str(controlPubPort) + ")",
                                                default = controlPubPort )
    parser.add_argument("--controlSubPort"    , type    = str,
                                                help    = "Port number to receive control signals (default=" + str(controlSubPort) + ")",
                                                default = controlSubPort )
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

    # EventDetector config

    eventDetectorType  = config.get('asection', 'eventDetectorType')
    # for InotifyxDetector and WatchdogDetector and getFromFile:
    fixSubdirs         = json.loads(config.get('asection', 'fixSubdirs'))
    # for InotifyxDetector and WatchdogDetector:
    monitoredDir       = config.get('asection', 'monitoredDir')
    monitoredEventType = config.get('asection', 'monitoredEventType')
    monitoredFormats   = json.loads(config.get('asection', 'monitoredFormats'))
    # for InotifyxDetector:
    historySize        = config.getint('asection', 'historySize')
    # for WatchdogDetector:
    timeTillClosed     = config.getfloat('asection', 'timeTillClosed')
    # for ZmqDetector:
    eventPort          = config.get('asection', 'eventPort')
    # for HttpGetDetector:
    detectorDevice     = config.get('asection', 'detectorDevice')
    filewriterDevice   = config.get('asection', 'filewriterDevice')

    parser.add_argument("--eventDetectorType" , type    = str,
                                                help    = "Type of event detector to use (default=" + str(eventDetectorType) + ")",
                                                default = eventDetectorType )
    parser.add_argument("--fixSubdirs"        , type    = str,
                                                help    = "Subdirectories to be monitored and to store data to \
                                                           (only needed if eventDetector is InotifyxDetector or WatchdogDetector \
                                                           and dataFetcher is getFromFile; default=" + str(fixSubdirs) + ")",
                                                default = fixSubdirs )

    parser.add_argument("--monitoredDir"      , type    = str,
                                                help    = "Directory to be monitor for changes; inside this directory only the specified \
                                                           subdirectories are monitred (only needed if eventDetector is InotifyxDetector \
                                                           or WatchdogDetector; default=" + str(monitoredDir) + ")",
                                                default = monitoredDir )
    parser.add_argument("--monitoredEventType", type    = str,
                                                help    = "Event type of files to be monitored (only needed if eventDetector is InotifyxDetector \
                                                           or WatchdogDetector; default=" + str(monitoredEventType) + ")",
                                                default = monitoredEventType )
    parser.add_argument("--monitoredFormats"  , type    = str,
                                                help    = "The formats to be monitored, files in an other format will be be neglected \
                                                           (only needed if eventDetector is InotifyxDetector or WatchdogDetector; \
                                                           default=" + str(monitoredFormats) + ")",
                                                default = monitoredFormats )

    parser.add_argument("--historySize"       , type    = int,
                                                help    = "Number of events stored to look for doubles \
                                                           (needed if eventDetector is InotifyxDetector; default=" + str(historySize) + ")",
                                                default = historySize)

    parser.add_argument("--timeTillClosed"    , type    = float,
                                                help    = "Time (in seconds) since last modification after which a file will be seen as closed \
                                                           (only needed if eventDetectorType is WatchdogDetector; default=" + str(timeTillClosed) + ")",
                                                default = timeTillClosed )

    parser.add_argument("--eventPort"         , type    = str,
                                                help    = "ZMQ port to get events from \
                                                           (only needed if eventDetectorType is ZmqDetector; default=" + str(eventPort) + ")",
                                                default = eventPort )

    parser.add_argument("--detectorDevice"    , type    = str,
                                                help    = "Tango device proxy for the detector \
                                                           (only needed if eventDetectorType is HttpDetector; default=" + str(detectorDevice) + ")",
                                                default = detectorDevice )
    parser.add_argument("--filewriterDevice"  , type    = str,
                                                help    = "Tango device proxy for the filewriter \
                                                           (only needed if eventDetectorType is HttpDetector; default=" + str(filewriterDevice) + ")",
                                                default = filewriterDevice )

    # DataFetcher config

    dataFetcherType    = config.get('asection', 'dataFetcherType')

    # for getFromZMQ:
    dataFetcherPort    = config.get('asection', 'dataFetcherPort')

    useDataStream      = config.getboolean('asection', 'useDataStream')
    fixedStreamHost    = config.get('asection', 'fixedStreamHost')
    fixedStreamPort    = config.get('asection', 'fixedStreamPort')

    numberOfStreams    = config.getint('asection', 'numberOfStreams')
    chunkSize          = config.getint('asection', 'chunkSize')

    eventPort          = config.get('asection', 'eventPort')
    routerPort         = config.get('asection', 'routerPort')

    localTarget        = config.get('asection', 'localTarget')

    storeData          = config.getboolean('asection', 'storeData')
    removeData         = config.getboolean('asection', 'removeData')


    parser.add_argument("--dataFetcherType"   , type    = str,
                                                help    = "Module with methods specifying how to get the data (default=" + str(dataFetcherType) + ")",
                                                default = dataFetcherType )
    parser.add_argument("--dataFetcherPort"   , type    = str,
                                                help    = "If 'getFromZmq is specified as dataFetcherType it needs a port to listen to \
                                                           (default=" + str(dataFetcherType) + ")",
                                                default = dataFetcherPort )

    parser.add_argument("--useDataStream"     , type    = str,
                                                help    = "Enable ZMQ pipe into storage system (if set to false: the file is moved \
                                                           into the localTarget) (default=" + str(useDataStream) + ")",
                                                default = useDataStream )
    parser.add_argument("--fixedStreamHost"   , type    = str,
                                                help    = "Fixed host to send the data to with highest priority \
                                                           (only active if useDataStream is set; default=" + str(fixedStreamHost) + ")",
                                                default = fixedStreamHost )
    parser.add_argument("--fixedStreamPort"   , type    = str,
                                                help    = "Fixed port to send the data to with highest priority \
                                                           (only active if useDataStream is set; default=" + str(fixedStreamPort) + ")",
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

    parser.add_argument("--storeData"         , type    = bool,
                                                help    = "Flag describing if the data should be stored in localTarget \
                                                           (needed if dataFetcherType is getFromFile or getFromHttp; default=" + str(storeData) + ")",
                                                default = storeData )
    parser.add_argument("--removeData"        , type    = bool,
                                                help    = "Flag describing if the files should be removed from the source \
                                                           (needed if dataFetcherType is getFromHttp; default=" + str(removeData) + ")",
                                                default = removeData )

    arguments         = parser.parse_args()

    # Check given arguments

    logfilePath       = arguments.logfilePath
    logfileName       = arguments.logfileName
    verbose           = arguments.verbose
    onScreen          = arguments.onScreen

    eventDetectorType = arguments.eventDetectorType.lower()
    supportedEDTypes  = ["inotifyxdetector", "watchdogdetector", "zmqdetector", "httpdetector"]
    supportedDFTypes  = ["getfromfile", "getfromzmq", "getfromhttp"]
    fixSubdirs        = arguments.fixSubdirs
    monitoredDir      = arguments.monitoredDir
    localTarget       = arguments.localTarget

    useDataStream     = arguments.useDataStream
    numberOfStreams   = arguments.numberOfStreams

    storeData         = arguments.storeData


    # check if logfile is writable
    helpers.checkLogFileWritable(logfilePath, logfileName)

    # check if the eventDetectorType is supported
    helpers.checkEventDetectorType(eventDetectorType, supportedEDTypes)

    # check if the dataFetcherType is supported
#    helpers.checkDataFetcherType(dataFetcherType, supportedDFTypes)

    # check if directories exists
    helpers.checkDirExistance(logfilePath)
    helpers.checkDirExistance(monitoredDir)
    helpers.checkAnySubDirExists(monitoredDir, fixSubdirs)
    if storeData:
        helpers.checkDirExistance(localTarget)
        helpers.checkAllSubDirExist(localTarget, fixSubdirs)

    return arguments


class DataManager():
    def __init__ (self, logQueue = None):
        arguments = argumentParsing()

        logfilePath           = arguments.logfilePath
        logfileName           = arguments.logfileName
        logfile               = os.path.join(logfilePath, logfileName)
        logsize               = arguments.logfileSize
        verbose               = arguments.verbose
        onScreen              = arguments.onScreen

        self.currentPID       = os.getpid()

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

                # Start queue listener using the stream handler above.
                self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1, h2)
            else:
                h1 = helpers.getLogHandlers(logfile, logsize, verbose, onScreen)

                # Start queue listener using the stream handler above
                self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1)

            self.logQueueListener.start()


        # Create log and set handler to queue handle
        self.log = self.getLogger(self.logQueue)

        self.log.info("DataManager started (PID " + str(self.currentPID) + ").")

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.localhost        = "127.0.0.1"
        self.extIp            = "0.0.0.0"
        self.ipcPath          = "/tmp/zeromq-data-transfer"

        if not os.path.exists(self.ipcPath):
            os.makedirs(self.ipcPath)

        self.controlPubPort   = arguments.controlPubPort
        self.controlSubPort   = arguments.controlSubPort
        self.comPort          = arguments.comPort
        self.requestPort      = arguments.requestPort
        self.requestFwPort    = arguments.requestFwPort
        self.routerPort       = arguments.routerPort

        self.comConId         = "tcp://{ip}:{port}".format(ip=self.extIp,     port=arguments.comPort)
        self.requestConId     = "tcp://{ip}:{port}".format(ip=self.extIp,     port=arguments.requestPort)

        if helpers.isWindows():
            self.log.info("Using tcp for internal communication.")
            self.controlPubConId  = "tcp://{ip}:{port}".format(ip=self.localhost, port=arguments.controlPubPort)
            self.controlSubConId  = "tcp://{ip}:{port}".format(ip=self.localhost, port=arguments.controlSubPort)
            self.requestFwConId   = "tcp://{ip}:{port}".format(ip=self.localhost, port=arguments.requestFwPort)
            self.routerConId      = "tcp://{ip}:{port}".format(ip=self.localhost, port=arguments.routerPort)
        else:
            self.log.info("Using ipc for internal communication.")
            self.controlPubConId  = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="controlPub")
            self.controlSubConId  = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="controlSub")
            self.requestFwConId   = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="requestFw")
            self.routerConId      = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="router")


        self.device           = None
        self.controlSocket   = None

        self.whitelist        = arguments.whitelist

        if arguments.useDataStream:
            self.fixedStreamId = "{host}:{port}".format( host=arguments.fixedStreamHost, port=arguments.fixedStreamPort )
        else:
            self.fixedStreamId = None

        self.numberOfStreams  = arguments.numberOfStreams
        self.chunkSize        = arguments.chunkSize

        self.localTarget      = arguments.localTarget

        # Assemble configuration for eventDetector.
        self.log.info("Configured type of eventDetector: " + arguments.eventDetectorType)
        if arguments.eventDetectorType == "InotifyxDetector":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "monDir"            : arguments.monitoredDir,
                    "monEventType"      : arguments.monitoredEventType,
                    "monSubdirs"        : arguments.fixSubdirs,
                    "monSuffixes"       : arguments.monitoredFormats,
                    "timeout"           : 0.1,
                    "historySize"       : arguments.historySize
                    }
        elif arguments.eventDetectorType == "WatchdogDetector":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "monDir"            : arguments.monitoredDir,
                    "monEventType"      : arguments.monitoredEventType,
                    "monSubdirs"        : arguments.fixSubdirs,
                    "monSuffixes"       : arguments.monitoredFormats,
                    "timeTillClosed"    : arguments.timeTillClosed
                    }
        elif arguments.eventDetectorType == "ZmqDetector":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "eventPort"         : arguments.eventPort,
                    "numberOfStreams"   : self.numberOfStreams,
                    "context"           : None
                    }
        elif arguments.eventDetectorType == "HttpDetector":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "detectorDevice"    : arguments.detectorDevice,
                    "filewriterDevice"  : arguments.filewriterDevice,
                    "historySize"       : arguments.historySize
                    }


        # Assemble configuration for dataFetcher
        self.log.info("Configured Type of dataFetcher: " + arguments.dataFetcherType)
        if arguments.dataFetcherType == "getFromFile":
            self.dataFetcherProp = {
                    "type"        : arguments.dataFetcherType,
                    "fixSubdirs"  : arguments.fixSubdirs,
                    "storeData"   : arguments.storeData,
                    }
        elif arguments.dataFetcherType == "getFromZmq":
            self.dataFetcherProp = {
                    "type"        : arguments.dataFetcherType,
                    "context"     : None,
                    "extIp"       : "0.0.0.0",
                    "port"        : arguments.dataFetcherPort,
                    }
        elif arguments.dataFetcherType == "getFromHttp":
            self.dataFetcherProp = {
                    "type"        : arguments.dataFetcherType,
                    "session"     : None,
                    "fixSubdirs"  : arguments.fixSubdirs,
                    "storeData"   : arguments.storeData,
                    "removeData"  : arguments.removeData
                    }


        self.signalHandlerPr  = None
        self.taskProviderPr   = None
        self.dataDispatcherPr = []

        self.log.info("Version: " + str(__version__))

        # IP and DNS name should be both in the whitelist
        helpers.extendWhitelist(self.whitelist, self.log)

        # Create zmq context
        # there should be only one context in one process
#        self.context = zmq.Context.instance()
        self.context = zmq.Context()
        self.log.debug("Registering global ZMQ context")

        try:
            self.createSockets()

            self.run()
        except:
            pass
        finally:
            self.stop()


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


    def createSockets(self):

        # initiate forwarder for control signals (multiple pub, multiple sub)
        try:
            self.device = zmq.devices.ThreadDevice(zmq.FORWARDER, zmq.SUB, zmq.PUB)
            self.device.bind_in(self.controlPubConId)
            self.device.bind_out(self.controlSubConId)
            self.device.setsockopt_in(zmq.SUBSCRIBE, b"")
            self.device.start()
            self.log.info("Start thead device forwarding messages from '" + str(self.controlPubConId) + "' to '" + str(self.controlSubConId) + "'")
        except:
            self.log.error("Failed to start thead device forwarding messages from '" + str(self.controlPubConId) + "' to '" + str(self.controlSubConId) + "'", exc_info=True)
            raise


        # socket for control signals
        try:
            self.controlSocket = self.context.socket(zmq.PUB)
            self.controlSocket.connect(self.controlPubConId)
            self.log.info("Start controlSocket (connect): '" + str(self.controlPubConId) + "'")
        except:
            self.log.error("Failed to start controlSocket (connect): '" + self.controlPubConId + "'", exc_info=True)
            raise


    def run (self):
        self.signalHandlerPr = threading.Thread ( target = SignalHandler,
                                                  args   = (
                                                      self.controlPubConId,
                                                      self.controlSubConId,
                                                      self.whitelist,
                                                      self.comConId,
                                                      self.requestFwConId,
                                                      self.requestConId,
                                                      self.logQueue,
                                                      self.context
                                                      )
                                                  )
        self.signalHandlerPr.start()

        # needed, because otherwise the requests for the first files are not forwarded properly
        time.sleep(0.5)

        if not self.signalHandlerPr.is_alive():
            return

        self.taskProviderPr = Process ( target = TaskProvider,
                                        args   = (
                                            self.eventDetectorConfig,
                                            self.controlSubConId,
                                            self.requestFwConId,
                                            self.routerConId,
                                            self.logQueue
                                            )
                                        )
        self.taskProviderPr.start()

        for i in range(self.numberOfStreams):
            id = str(i) + "/" + str(self.numberOfStreams)
            pr = Process ( target = DataDispatcher,
                           args   = (
                               id,
                               self.controlSubConId,
                               self.routerConId,
                               self.chunkSize,
                               self.fixedStreamId,
                               self.dataFetcherProp,
                               self.logQueue,
                               self.localTarget
                               )
                           )
            pr.start()
            self.dataDispatcherPr.append(pr)

        while self.signalHandlerPr.is_alive() and self.taskProviderPr.is_alive() and all(dataDispatcher.is_alive() for dataDispatcher in self.dataDispatcherPr):
            pass

        # notify which subprocess terminated
        if not self.signalHandlerPr.is_alive():
            self.log.info("SignalHandler terminated.")
        if not self.taskProviderPr.is_alive():
            self.log.info("TaskProvider terminated.")
        if not any(dataDispatcher.is_alive() for dataDispatcher in self.dataDispatcherPr):
            self.log.info("One DataDispatcher terminated.")


    def stop (self):

        if self.controlSocket:
            self.log.info("Sending 'Exit' signal")
            self.controlSocket.send_multipart(["control", "EXIT"])

        # waiting till the other processes are finished
        time.sleep(0.5)

        if self.controlSocket:
            self.log.info("Closing controlSocket")
            self.controlSocket.close(0)
            self.controlSocket = None

        if self.context:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

        if not self.extLogQueue and self.logQueueListener:
            self.log.info("Stopping logQueue")
            self.logQueue.put_nowait(None)
            self.logQueueListener.stop()
            self.logQueueListener = None


    def signal_term_handler(self, signal, frame):
        self.log.debug('got SIGTERM')
        self.stop()


    def __exit__ (self):
        self.stop()


    def __def__ (self):
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


    def sendSignal (self, signal, ports, prio = None):
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

    def run (self):
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

    def __exit__ (self):
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
        sender = None
        try:
            sender = DataManager()
        finally:
            if sender:
                sender.stop()

