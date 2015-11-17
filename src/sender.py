from __builtin__ import open, type

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


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

sys.path.append ( CONFIG_PATH )

import shared.helperScript as helperScript
from shared.LiveViewCommunicator import LiveViewCommunicator
from sender.DirectoryWatcher import DirectoryWatcher
from sender.FileMover import FileMover
from sender.Cleaner import Cleaner


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
    liveViewerPort      = config.get('asection', 'liveViewerPort')
    liveViewerIp        = config.get('asection', 'liveViewerIp')
    maxRingBufferSize   = config.get('asection', 'maxRingBufferSize')
    maxQueueSize        = config.get('asection', 'maxQueueSize')

    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"        , type=str, default=logfilePath,
                                                 help="Path where the logfile will be created (default=" + str(logfilePath) + ")")
    parser.add_argument("--logfileName"        , type=str, default=logfileName,
                                                 help="Filename used for logging (default=" + str(logfileName) + ")")
    parser.add_argument("--verbose"            , action="store_true",
                                                 help="More verbose output")

    parser.add_argument("--watchDir"           , type=str, default=watchDir,
                                                 help="Dir you want to monitor for changes; inside this directory only the specified subdirectories are monitred (default=" + str(watchDir) + ")")
    parser.add_argument("--monitoredEventType" , type=str, default=monitoredEventType,
                                                 help="Event type of files to be monitored (default=" + str(monitoredEventType) + ")")
    parser.add_argument("--monitoredSubdirs"   , type=str, default=monitoredSubdirs,
                                                 help="Subdirectories of watchDirs to be monitored (default=" + str(monitoredSubdirs) + ")")
    parser.add_argument("--monitoredFormats"   , type=str, default=monitoredFormats,
                                                 help="The formats to be monitored, files in an other format will be be neglected (default=" + str(monitoredFormats) + ")")
    parser.add_argument("--fileEventIp"        , type=str, default=fileEventIp,
                                                 help="ZMQ endpoint (IP-address) to send file events to for the live viewer (default=" + str(fileEventIp) + ")")
    parser.add_argument("--fileEventPort"      , type=str, default=fileEventPort,
                                                 help="ZMQ endpoint (port) to send file events to for the live viewer (default=" + str(fileEventPort) + ")")

    parser.add_argument("--useDataStream"      , type=str, default=useDataStream,
                                                 help="Enable ZMQ pipe into storage system (if set to false: the file is moved into the cleanerTargetPath) (default=" + str(useDataStream) + ")")
    parser.add_argument("--dataStreamIp"       , type=str, default=dataStreamIp,
                                                 help="IP of dataStream-socket to push new files to (default=" + str(dataStreamIp) + ")")
    parser.add_argument("--dataStreamPort"     , type=str, default=dataStreamPort,
                                                 help="Port number of dataStream-socket to push new files to (default=" + str(dataStreamPort) + ")")
    parser.add_argument("--cleanerTargetPath"  , type=str, default=cleanerTargetPath,
                                                 help="Target to move the files into (default=" + str(cleanerTargetPath) + ")")
    parser.add_argument("--cleanerIp"          , type=str, default=cleanerIp,
                                                 help="ZMQ-pull-socket IP which deletes/moves given files (default=" + str(cleanerIp) + ")")
    parser.add_argument("--cleanerPort"        , type=str, default=cleanerPort,
                                                 help="ZMQ-pull-socket port which deletes/moves given file (default=" + str(cleanerPort) + ")")
    parser.add_argument("--routerPort"         , type=str, default=routerPort,
                                                 help="ZMQ-router port which coordinates the load-balancing to the worker-processes (default=" + str(routerPort) + ")")

    parser.add_argument("--receiverComIp"      , type=str, default=receiverComIp,
                                                 help="IP receive signals from the receiver (default=" + str(receiverComIp) + ")")
    parser.add_argument("--receiverComPort"    , type=str, default=receiverComPort,
                                                 help="Port number to receive signals from the receiver (default=" + str(receiverComPort) + ")")
    parser.add_argument("--ondaIps"            , type=str, default=ondaIps,
                                                 help="IPs to communicate with onda/realtime analysis; there needs to be one entry for each streams (default=" + str(ondaIps) + ")")
    parser.add_argument("--ondaPorts"          , type=str, default=ondaPorts,
                                                 help="Ports to communicate with onda/realtime analysis; there needs to be one entry for each streams (default=" + str(ondaPorts) + ")")
    parser.add_argument("--receiverWhiteList"  , type=str, default=receiverWhiteList,
                                                 help="List of hosts allowed to connect to the sender (default=" + str(receiverWhiteList) + ")")

    parser.add_argument("--parallelDataStreams", type=int, default=parallelDataStreams,
                                                 help="Number of parallel data streams (default=" + str(parallelDataStreams) + ")")
    parser.add_argument("--chunkSize"          , type=int, default=chunkSize,
                                                 help="Chunk size of file-parts getting send via ZMQ (default=" + str(chunkSize) + ")")

    parser.add_argument("--useRingbuffer"      , type=str, default=useRingbuffer,
                                                 help="Put the data into a ringbuffer followed by a queue to delay the removal of the files(default=" + str(useRingbuffer) + ")")
    parser.add_argument("--cleanerExchangePort", type=str, default=cleanerExchangePort,
                                                 help="Port number to exchange data and signals between Cleaner and LiveViewCommunicator (default=" + str(cleanerExchangePort) + ")")
    parser.add_argument("--liveViewerIp"       , type=str, default=liveViewerIp,
                                                 help="IP to bind communication to LiveViewer to (default=" + str(liveViewerIp) + ")")
    parser.add_argument("--liveViewerPort"     , type=str, default=liveViewerPort,
                                                 help="Port number to communicate with live viewer (default=" + str(liveViewerPort) + ")")
    parser.add_argument("--maxRingBufferSize"  , type=int, default=maxRingBufferSize,
                                                 help="Size of the ring buffer for the live viewer (default=" + str(maxRingBufferSize) + ")")
    parser.add_argument("--maxQueueSize"       , type=int, default=maxQueueSize,
                                                 help="Size of the queue for the live viewer (default=" + str(maxQueueSize) + ")")

    arguments         = parser.parse_args()

    logfilePath       = str(arguments.logfilePath)
    logfileName       = str(arguments.logfileName)
    logfileFullPath     = os.path.join(logfilePath, logfileName)
    verbose             = arguments.verbose

    watchDir          = str(arguments.watchDir)

    monitoredSubdirs  = arguments.monitoredSubdirs
    cleanerTargetPath = str(arguments.cleanerTargetPath)

    #enable logging
    helperScript.initLogging(logfileFullPath, verbose)

    # check if directories exists
    helperScript.checkDirExistance(logfilePath)
    helperScript.checkDirExistance(watchDir)
    helperScript.checkSubDirExistance(watchDir, monitoredSubdirs)
    helperScript.checkDirExistance(cleanerTargetPath)

    # check if logfile is writable
    helperScript.checkLogFileWritable(logfilePath, logfileName)

    return arguments


class Sender():
    watchDir            = None
    monitoredEventType  = None
    monitoredSubdirs    = None
    monitoredFormats    = None
    fileEventIp         = None
    fileEventPort       = None

    useDataStream       = False

    dataStreamIp        = None
    dataStreamPort      = None
    cleanerTargetPath   = None
    cleanerIp           = None
    cleanerPort         = None
    routerPort          = None
    receiverComIp       = None
    receiverComPort     = None
    ondaIps             = None
    ondaPorts           = None
    receiverWhiteList   = None

    parallelDataStreams = None
    chunkSize           = None

    useRingbuffer       = False
    cleanerExchangePort = None
    liveViewerPort      = None
    liveViewerIp        = None
    maxRingBufferSize   = None
    maxQueueSize        = None

    zmqContext          = None

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
        self.liveViewerPort      = arguments.liveViewerPort
        self.liveViewerIp        = arguments.liveViewerIp
        self.maxRingBufferSize   = arguments.maxRingBufferSize
        self.maxQueueSize        = arguments.maxQueueSize

        #create zmq context
        # there should be only one context in one process
        self.zmqContext = zmq.Context.instance()
        logging.debug("registering zmq global context")

        self.run()


    def run(self):
        logging.info("start watcher process...")
        watcherProcess = Process(target=DirectoryWatcher, args=(self.fileEventIp, self.watchDir, self.fileEventPort, self.monitoredEventType, self.monitoredSubdirs, self.monitoredFormats, self.zmqContext))
        watcherProcess.start()
        logging.debug("start watcher process...done")

        if self.useRingbuffer:
            logging.info("start liveViewercommunicator process...")
            liveViewercommunicatorProcess = Process(target=LiveViewCommunicator, args=(self.cleanerExchangePort, self.liveViewerPort, self.liveViewerIp, self.maxRingBufferSize, self.maxQueueSize, self.zmqContext))
            liveViewercommunicatorProcess.start()
            logging.debug("start liveViewercommunicator process...done")

            logging.info("start cleaner process...")
            cleanerProcess = Process(target=Cleaner, args=(self.cleanerTargetPath, self.cleanerIp, self.cleanerPort, self.useDataStream, self.cleanerExchangePort, self.zmqContext))
        else:
            logging.info("start cleaner process...")
            cleanerProcess = Process(target=Cleaner, args=(self.cleanerTargetPath, self.cleanerIp, self.cleanerPort, self.useDataStream, None, self.zmqContext))

        cleanerProcess.start()
        logging.debug("start cleaner process...done")

        #start new fileMover
        fileMover = FileMover(self.fileEventIp, self.fileEventPort, self.dataStreamIp, self.dataStreamPort,
                              self.receiverComIp, self.receiverComPort, self.receiverWhiteList,
                              self.parallelDataStreams, self.chunkSize,
                              self.cleanerIp, self.cleanerPort, self.routerPort,
                              self.ondaIps, self.ondaPorts,
                              self.useDataStream,
                              self.zmqContext)
        try:
            fileMover.process()
        except KeyboardInterrupt:
            logging.debug("Keyboard interruption detected. Shutting down")
        # except Exception, e:
        #     print "unknown exception detected."
        finally:
            logging.debug("shutting down zeromq...")
            try:
                fileMover.stop()
                logging.debug("shutting down zeromq...done.")
            except:
                logging.error(sys.exc_info())
                logging.error("shutting down zeromq...failed.")

            # give the other processes time to close the sockets
            time.sleep(0.1)
            try:
                logging.debug("closing zmqContext...")
                self.zmqContext.destroy()
                logging.debug("closing zmqContext...done.")
            except:
                logging.debug("closing zmqContext...failed.")
                logging.error(sys.exc_info())





if __name__ == '__main__':
    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows
    sender = Sender()
