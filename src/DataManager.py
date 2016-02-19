from __builtin__ import open, type

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import time
import argparse
import zmq
import os
import logging
import sys
import json
from multiprocessing import Process, freeze_support
import ConfigParser

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))
CONFIG_PATH = BASE_PATH + os.sep + "conf"

import shared.helperScript as helperScript
from shared.LiveViewCommunicator import LiveViewCommunicator
from sender.SignalHandler import SignalHandler
from sender.TaskProvider import TaskProvider
from sender.DataDispatcher import DataDispatcher


from version import __version__


def argumentParsing():
    configFile = CONFIG_PATH + os.sep + "sender.conf"

    config = ConfigParser.RawConfigParser()
    config.readfp(helperScript.FakeSecHead(open(configFile)))

    logfilePath         = config.get('asection', 'logfilePath')
    logfileName         = config.get('asection', 'logfileName')

    watchDir            = config.get('asection', 'watchDir')
    monitoredEventType  = config.get('asection', 'monitoredEventType')
    monitoredSubdirs    = json.loads(config.get('asection', 'monitoredSubdirs'))
    monitoredFormats    = json.loads(config.get('asection', 'monitoredFormats'))
    fileEventIp         = config.get('asection', 'fileEventIp')
    fileEventPort       = config.get('asection', 'fileEventPort')

    useDataStream       = config.getboolean('asection', 'useDataStream')
    dataStreamIp        = config.get('asection', 'dataStreamIp')
    dataStreamPort      = config.get('asection', 'dataStreamPort')
    cleanerTargetPath   = config.get('asection', 'cleanerTargetPath')
    cleanerIp           = config.get('asection', 'cleanerIp')
    cleanerPort         = config.get('asection', 'cleanerPort')
    routerPort          = config.get('asection', 'routerPort')

    receiverComIp       = config.get('asection', 'receiverComIp')
    receiverComPort     = config.get('asection', 'receiverComPort')
    ondaIps             = json.loads(config.get('asection', 'ondaIps'))
    ondaPorts           = json.loads(config.get('asection', 'ondaPorts'))
    receiverWhiteList   = json.loads(config.get('asection', 'receiverWhiteList'))

    parallelDataStreams = config.get('asection', 'parallelDataStreams')
    chunkSize           = int(config.get('asection', 'chunkSize'))

    useRingbuffer       = config.getboolean('asection', 'useRingbuffer')
    cleanerExchangePort = config.get('asection', 'cleanerExchangePort')

    liveViewerComPort   = config.get('asection', 'liveViewerComPort')
    liveViewerComIp     = config.get('asection', 'liveViewerComIp')
    liveViewerWhiteList   = json.loads(config.get('asection', 'liveViewerWhiteList'))
    maxRingBufferSize   = config.get('asection', 'maxRingBufferSize')
    maxQueueSize        = config.get('asection', 'maxQueueSize')


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

    parser.add_argument("--watchDir"           , type    = str,
                                                 help    = "Dir you want to monitor for changes; inside this directory only the specified \
                                                            subdirectories are monitred (default=" + str(watchDir) + ")",
                                                 default = watchDir )
    parser.add_argument("--monitoredEventType" , type    = str,
                                                 help    = "Event type of files to be monitored (default=" + str(monitoredEventType) + ")",
                                                 default = monitoredEventType )
    parser.add_argument("--monitoredSubdirs"   , type    = str,
                                                 help    = "Subdirectories of watchDirs to be monitored (default=" + str(monitoredSubdirs) + ")",
                                                 default = monitoredSubdirs )
    parser.add_argument("--monitoredFormats"   , type    = str,
                                                 help    = "The formats to be monitored, files in an other format will be be neglected \
                                                            (default=" + str(monitoredFormats) + ")",
                                                 default = monitoredFormats )
    parser.add_argument("--fileEventIp"        , type    = str,
                                                 help    = "ZMQ endpoint (IP-address) to send file events to for the live viewer \
                                                            (default=" + str(fileEventIp) + ")",
                                                 default = fileEventIp )
    parser.add_argument("--fileEventPort"      , type    = str,
                                                 help    = "ZMQ endpoint (port) to send file events to for the live viewer \
                                                            (default=" + str(fileEventPort) + ")",
                                                 default = fileEventPort )

    parser.add_argument("--useDataStream"      , type    = str,
                                                 help    = "Enable ZMQ pipe into storage system (if set to false: the file is moved \
                                                            into the cleanerTargetPath) (default=" + str(useDataStream) + ")",
                                                 default = useDataStream )
    parser.add_argument("--dataStreamIp"       , type    = str,
                                                 help    = "IP of dataStream-socket to push new files to \
                                                            (default=" + str(dataStreamIp) + ")",
                                                 default = dataStreamIp )
    parser.add_argument("--dataStreamPort"     , type    = str,
                                                 help    = "Port number of dataStream-socket to push new \
                                                            files to (default=" + str(dataStreamPort) + ")",
                                                 default = dataStreamPort )
    parser.add_argument("--cleanerTargetPath"  , type    = str,
                                                 help    = "Target to move the files into (default=" + str(cleanerTargetPath) + ")",
                                                 default = cleanerTargetPath )
    parser.add_argument("--cleanerIp"          , type    = str,
                                                 help    = "ZMQ-pull-socket IP which deletes/moves given files \
                                                            (default=" + str(cleanerIp) + ")",
                                                 default = cleanerIp )
    parser.add_argument("--cleanerPort"        , type    = str,
                                                 help    = "ZMQ-pull-socket port which deletes/moves given file \
                                                            (default=" + str(cleanerPort) + ")",
                                                 default = cleanerPort )
    parser.add_argument("--routerPort"         , type    = str,
                                                 help    = "ZMQ-router port which coordinates the load-balancing \
                                                            to the worker-processes (default=" + str(routerPort) + ")",
                                                 default = routerPort )

    parser.add_argument("--receiverComIp"      , type    = str,
                                                 help    = "IP receive signals from the receiver (default=" + str(receiverComIp) + ")",
                                                 default = receiverComIp )
    parser.add_argument("--receiverComPort"    , type    = str,
                                                 help    = "Port number to receive signals from the receiver \
                                                            (default=" + str(receiverComPort) + ")",
                                                 default = receiverComPort )
    parser.add_argument("--ondaIps"            , type    = str,
                                                 help    = "IPs to communicate with onda/realtime analysis; there needs to be one entry \
                                                            for each streams (default=" + str(ondaIps) + ")",
                                                 default = ondaIps )
    parser.add_argument("--ondaPorts"          , type    = str,
                                                 help    = "Ports to communicate with onda/realtime analysis; there needs to be one entry \
                                                            for each streams (default=" + str(ondaPorts) + ")",
                                                 default = ondaPorts )
    parser.add_argument("--receiverWhiteList"  , type    = str,
                                                 help    = "List of hosts allowed to connect to the sender \
                                                            (default=" + str(receiverWhiteList) + ")",
                                                 default = receiverWhiteList )

    parser.add_argument("--parallelDataStreams", type    = int,
                                                 help    = "Number of parallel data streams (default=" + str(parallelDataStreams) + ")",
                                                 default = parallelDataStreams )
    parser.add_argument("--chunkSize"          , type    = int,
                                                 help    = "Chunk size of file-parts getting send via ZMQ (default=" + str(chunkSize) + ")",
                                                 default = chunkSize )

    parser.add_argument("--useRingbuffer"      , type    = str,
                                                 help    = "Put the data into a ringbuffer followed by a queue to delay the \
                                                            removal of the files(default=" + str(useRingbuffer) + ")",
                                                 default = useRingbuffer )
    parser.add_argument("--cleanerExchangePort", type    = str,
                                                 help    = "Port number to exchange data and signals between Cleaner and \
                                                            LiveViewCommunicator (default=" + str(cleanerExchangePort) + ")",
                                                 default = cleanerExchangePort )
    parser.add_argument("--liveViewerComIp"    , type    = str,
                                                 help    = "IP to bind communication to LiveViewer to (default=" + str(liveViewerComIp) + ")",
                                                 default = liveViewerComIp )
    parser.add_argument("--liveViewerComPort"  , type    = str,
                                                 help    = "Port number to communicate with live viewer (default=" + str(liveViewerComPort) + ")",
                                                 default = liveViewerComPort )
    parser.add_argument("--liveViewerWhiteList", type    = str,
                                                 help    = "List of hosts allowed to connect to the receiver \
                                                            (default=" + str(liveViewerWhiteList) + ")",
                                                 default = liveViewerWhiteList )
    parser.add_argument("--maxRingBufferSize"  , type    = int,
                                                 help    = "Size of the ring buffer for the live viewer (default=" + str(maxRingBufferSize) + ")",
                                                 default = maxRingBufferSize )
    parser.add_argument("--maxQueueSize"       , type    = int,
                                                 help    = "Size of the queue for the live viewer (default=" + str(maxQueueSize) + ")",
                                                 default = maxQueueSize )

    arguments           = parser.parse_args()

    logfilePath         = str(arguments.logfilePath)
    logfileName         = str(arguments.logfileName)
    logfileFullPath     = os.path.join(logfilePath, logfileName)
    verbose             = arguments.verbose
    onScreen            = arguments.onScreen

    watchDir            = str(arguments.watchDir)

    monitoredSubdirs    = arguments.monitoredSubdirs
    cleanerTargetPath   = str(arguments.cleanerTargetPath)

    ondaIps             = arguments.ondaIps
    ondaPorts           = arguments.ondaPorts
    parallelDataStreams = arguments.parallelDataStreams

    #enable logging
    helperScript.initLogging(logfileFullPath, verbose, onScreen)

    # check if directories exists
    helperScript.checkDirExistance(logfilePath)
    helperScript.checkDirExistance(watchDir)
    helperScript.checkSubDirExistance(watchDir, monitoredSubdirs)
    helperScript.checkDirExistance(cleanerTargetPath)

    # check if logfile is writable
    helperScript.checkLogFileWritable(logfilePath, logfileName)

    # check if there are enough ports specified (OnDA), corresponding to the number of streams
    helperScript.checkStreamConfig(ondaIps, ondaPorts, parallelDataStreams)

    return arguments


class Sender():
    def __init__(self):
        arguments = argumentParsing()

        self.watchDir            = arguments.watchDir
        self.monitoredEventType  = arguments.monitoredEventType
        self.monitoredSubdirs    = arguments.monitoredSubdirs
        self.monitoredFormats    = arguments.monitoredFormats
        self.fileEventIp         = arguments.fileEventIp
        self.fileEventPort       = arguments.fileEventPort

        self.useDataStream       = arguments.useDataStream

        self.dataStreamIp        = arguments.dataStreamIp
        self.dataStreamPort      = arguments.dataStreamPort
        self.cleanerTargetPath   = arguments.cleanerTargetPath
        self.cleanerIp           = arguments.cleanerIp
        self.cleanerPort         = arguments.cleanerPort
        self.routerPort          = arguments.routerPort
        self.receiverComIp       = arguments.receiverComIp
        self.receiverComPort     = arguments.receiverComPort
        self.ondaIps             = arguments.ondaIps
        self.ondaPorts           = arguments.ondaPorts
        self.receiverWhiteList   = arguments.receiverWhiteList

        self.parallelDataStreams = arguments.parallelDataStreams
        self.chunkSize           = arguments.chunkSize

        self.useRingbuffer       = arguments.useRingbuffer
        self.cleanerExchangePort = arguments.cleanerExchangePort
        self.liveViewerComPort   = arguments.liveViewerComPort
        self.liveViewerComIp     = arguments.liveViewerComIp
        self.liveViewerWhiteList = arguments.liveViewerWhiteList
        self.maxRingBufferSize   = arguments.maxRingBufferSize
        self.maxQueueSize        = arguments.maxQueueSize

        logging.info("Version: " + str(__version__))

        #create zmq context
        # there should be only one context in one process
        self.zmqContext = zmq.Context.instance()
        logging.debug("Registering global ZMQ context")

        self.run()


    def run(self):

        whiteList     = ["localhost", "zitpcx19282"]
        comPort       = "6000"
        requestFwPort = "6001"
        requestPort   = "6002"
        routerPort    = "7000"
        chunkSize     = 10485760 ; # = 1024*1024*10 = 10 MiB
        useDataStream = False
        eventDetectorConfig = {
                "configType"   : "inotifyx",
                "monDir"       : BASE_PATH + "/data/source",
                "monEventType" : "IN_CLOSE_WRITE",
                "monSubdirs"   : ["commissioning", "current", "local"],
                "monSuffixes"  : [".tif", ".cbf"]
                }

        self.signalHandlerPr  = None
        self.taskProviderPr   = None
        self.dataDispatcherPr = None


        logging.info("Start SignalHandler...")
        self.signalHandlerPr = Process ( target = SignalHandler, args = (whiteList, comPort, requestFwPort, requestPort) )
        self.signalHandlerPr.start()
        logging.debug("Start SignalHandler...done")

        logging.info("Start TaskProvider...")
        self.taskProviderPr = Process ( target = TaskProvider, args = (eventDetectorConfig, requestFwPort, routerPort) )
        self.taskProviderPr.start()
        logging.info("Start TaskProvider...done")

        logging.info("Start DataDispatcher...")
        self.dataDispatcherPr = Process ( target = DataDispatcher, args = ( 1, routerPort, chunkSize, useDataStream) )
        self.dataDispatcherPr.start()
        logging.info("Start DataDispatcher...done")

    def stop(self):
        self.signalHandlerPr.terminate()
        self.taskProviderPr.terminate()
        self.dataDispatcherPr.terminate()


    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()



if __name__ == '__main__':
    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows

    import cPickle
    BASE_PATH = "/space/projects/live-viewer"

    #enable logging
    helperScript.initLogging(BASE_PATH + "/logs/dataManager.log", verbose=True, onScreenLogLevel="debug")

    class Test_Receiver_Stream():
        def __init__(self, comPort, receivingPort, receivingPort2):
            context       = zmq.Context.instance()

            self.comSocket       = context.socket(zmq.REQ)
            connectionStr   = "tcp://zitpcx19282:" + comPort
            self.comSocket.connect(connectionStr)
            logging.info("=== comSocket connected to " + connectionStr)

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


    comPort        = "6000"
    receivingPort  = "6005"
    receivingPort2 = "6006"

    testPr = Process ( target = Test_Receiver_Stream, args = (comPort, receivingPort, receivingPort2))
    testPr.start()

    sender = Sender()

    try:
        while True:
            pass
    finally:
        testPr.terminate()
        sender.stop()



