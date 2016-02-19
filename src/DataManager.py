__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


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
    configFile = CONFIG_PATH + os.sep + "dataManager.conf"

    config = ConfigParser.RawConfigParser()
    config.readfp(helperScript.FakeSecHead(open(configFile)))

    logfilePath         = config.get('asection', 'logfilePath')
    logfileName         = config.get('asection', 'logfileName')

    comPort             = config.get('asection', 'comPort')
    whitelist           = json.loads(config.get('asection', 'whitelist'))

    requestPort         = config.get('asection', 'requestPort')
    requestFwPort       = config.get('asection', 'requestFwPort')

    monitoredDir        = config.get('asection', 'monitoredDir')
    monitoredEventType  = config.get('asection', 'monitoredEventType')
    monitoredSubdirs    = json.loads(config.get('asection', 'monitoredSubdirs'))
    monitoredFormats    = json.loads(config.get('asection', 'monitoredFormats'))

    useDataStream       = config.getboolean('asection', 'useDataStream')
    fixedStreamHost      = config.get('asection', 'fixedStreamHost')
    fixedStreamPort      = config.get('asection', 'fixedStreamPort')

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

    parser.add_argument("--monitoredDir"       , type    = str,
                                                 help    = "Dirextory you want to monitor for changes; inside this directory only the specified \
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

    monitoredDir        = str(arguments.monitoredDir)
    monitoredSubdirs    = arguments.monitoredSubdirs
    localTarget   = str(arguments.localTarget)

    parallelDataStreams = arguments.parallelDataStreams

    #enable logging
    helperScript.initLogging(logfileFullPath, verbose, onScreen)

    # check if directories exists
    helperScript.checkDirExistance(logfilePath)
    helperScript.checkDirExistance(monitoredDir)
    helperScript.checkSubDirExistance(monitoredDir, monitoredSubdirs)
    helperScript.checkDirExistance(localTarget)

    # check if logfile is writable
    helperScript.checkLogFileWritable(logfilePath, logfileName)

    return arguments


class Sender():
    def __init__(self):
        arguments = argumentParsing()

        self.comPort             = arguments.comPort
        self.whitelist           = arguments.whitelist

        self.requestPort         = arguments.requestPort
        self.requestFwPort       = arguments.requestFwPort

        self.eventDetectorConfig = {
                "configType"   : "inotifyx",
                "monDir"       : arguments.monitoredDir,
                "monEventType" : arguments.monitoredEventType,
                "monSubdirs"   : arguments.monitoredSubdirs,
                "monSuffixes"  : arguments.monitoredFormats
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

        whiteList     = ["localhost", "zitpcx19282"]
        comPort       = "6000"
        requestFwPort = "6001"
        requestPort   = "6002"
        routerPort    = "7000"
        chunkSize     = 10485760 ; # = 1024*1024*10 = 10 MiB
        eventDetectorConfig = {
                "configType"   : "inotifyx",
                "monDir"       : BASE_PATH + "/data/source",
                "monEventType" : "IN_CLOSE_WRITE",
                "monSubdirs"   : ["commissioning", "current", "local"],
                "monSuffixes"  : [".tif", ".cbf"]
                }
        localTarget   = BASE_PATH + "/data/target"


        logging.info("Start SignalHandler...")
        self.signalHandlerPr = Process ( target = SignalHandler, args = (whiteList, comPort, requestFwPort, requestPort) )
        self.signalHandlerPr.start()
        logging.debug("Start SignalHandler...done")

        # needed, because otherwise the requests for the first files are not forwarded properly
        time.sleep(0.5)

        logging.info("Start TaskProvider...")
        self.taskProviderPr = Process ( target = TaskProvider, args = (eventDetectorConfig, requestFwPort, routerPort) )
        self.taskProviderPr.start()
        logging.info("Start TaskProvider...done")

        logging.info("Start DataDispatcher...")
        self.dataDispatcherPr = Process ( target = DataDispatcher, args = ( 1, routerPort, chunkSize, self.fixedStreamId, localTarget) )
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

    test = True

    if test:
        import time
        import cPickle
        from shutil import copyfile

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
            if test:
                copyfile(BASE_PATH + "/test_file.cbf", BASE_PATH + "/data/source/local/raw/100.cbf")
                time.sleep(1)
                break
            else:
                pass
    finally:
        if test:
            testPr.terminate()
        sender.stop()



