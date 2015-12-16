__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import sys
import argparse
import logging
import os
import json
import ConfigParser
import zmq
import time
from multiprocessing import Process, freeze_support

import shared.helperScript as helperScript
from shared.LiveViewCommunicator import LiveViewCommunicator
from receiverLiveViewer.FileReceiver import FileReceiver

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) )
CONFIG_PATH = BASE_PATH + os.sep + "conf"


def argumentParsing():
    configFile = CONFIG_PATH + os.sep + "receiverLiveViewer.conf"

    config = ConfigParser.RawConfigParser()
    config.readfp(helperScript.FakeSecHead(open(configFile)))


    logfilePath           = config.get('asection', 'logfilePath')
    logfileName           = config.get('asection', 'logfileName')
    targetDir             = config.get('asection', 'targetDir')
    dataStreamPort        = config.get('asection', 'dataStreamPort')
    liveViewerComIp       = config.get('asection', 'liveViewerComIp')
    liveViewerComPort     = config.get('asection', 'liveViewerComPort')
    liveViewerWhiteList   = json.loads(config.get('asection', 'liveViewerWhiteList'))
    lvCommunicatorPort    = config.get('asection', 'lvCommunicatorPort')
    signalIp              = config.get('asection', 'signalIp')
    maxRingBufferSize     = config.get('asection', 'maxRingBufferSize')
    maxQueueSize          = config.get('asection', 'maxQueueSize')
    senderResponseTimeout = config.get('asection', 'senderResponseTimeout')



    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"           , type=str, default=logfilePath,
                                                    help="Path where logfile will be created (default=" + str(logfilePath) + ")")
    parser.add_argument("--logfileName"           , type=str, default=logfileName,
                                                    help="Filename used for logging (default=" + str(logfileName) + ")")
    parser.add_argument("--targetDir"             , type=str, default=targetDir,
                                                    help="Where incoming data will be stored to (default=" + str(targetDir) + ")")

    parser.add_argument("--dataStreamPort"        , type=str, default=dataStreamPort,
                                                    help="Port number of dataStream-socket to pull new files from; there needs to be one entry for each streams (default=" + str(dataStreamPort) + ")")

    parser.add_argument("--liveViewerComIp"       , type=str, default=liveViewerComIp,
                                                    help="IP to bind LiveViewer to (default=" + str(liveViewerComIp) + ")")
    parser.add_argument("--liveViewerComPort"     , type=str, default=liveViewerComPort,
                                                    help="TCP port of live viewer (default=" + str(liveViewerComPort) + ")")
    parser.add_argument("--liveViewerWhiteList"   , type=str, default=liveViewerWhiteList,
                                                     help="List of hosts allowed to connect to the receiver (default=" + str(liveViewerWhiteList) + ")")

    parser.add_argument("--lvCommunicatorPort"    , type=str, default=lvCommunicatorPort,
                                                    help="Port to exchange data and signals between receiver and lvcommunicator (default=" + str(lvCommunicatorPort) + ")")

    parser.add_argument("--signalIp"              , type=str, default=signalIp,
                                                    help="Port number of dataStream-socket to send signals back to the sender (default=" + str(signalIp) + ")")

    parser.add_argument("--maxRingBufferSize"     , type=int, default=maxRingBufferSize,
                                                    help="Size of the ring buffer for the live viewer (default=" + str(maxRingBufferSize) + ")")
    parser.add_argument("--maxQueueSize"          , type=int, default=maxQueueSize,
                                                    help="Size of the queue for the live viewer (default=" + str(maxQueueSize) + ")")
    parser.add_argument("--senderResponseTimeout" , type=int, default=senderResponseTimeout,
                                                    help=argparse.SUPPRESS)

    parser.add_argument("--verbose"               , action="store_true",
                                                    help="More verbose output")
    parser.add_argument("--onScreen"              , type=str, default=False,
                                                    help="Display logging on screen (options are CRITICAL, ERROR, WARNING, INFO, DEBUG)")

    arguments   = parser.parse_args()

    targetDir       = str(arguments.targetDir)
    logfilePath     = str(arguments.logfilePath)
    logfileName     = str(arguments.logfileName)
    logfileFullPath = os.path.join(logfilePath, logfileName)
    verbose         = arguments.verbose
    onScreen        = arguments.onScreen


    #enable logging
    helperScript.initLogging(logfileFullPath, verbose, onScreen)

    # check target directory for existance
    helperScript.checkDirExistance(targetDir)
    helperScript.checkDirEmpty(targetDir)

    # check if logfile is writable
    helperScript.checkLogFileWritable(logfilePath, logfileName)

    return arguments


class ReceiverLiveViewer():
    targetDir             = None
    dataStreamPort        = None

    liveViewerComIp       = None
    liveViewerComPort     = None
    liveViewerWhiteList   = None
    lvCommunicatorPort    = None
    signalIp              = None
    maxRingBufferSize     = None
    maxQueueSize          = None
    senderResponseTimeout = None

    def __init__(self):
        arguments = argumentParsing()

        self.targetDir             = arguments.targetDir
        self.dataStreamPort        = arguments.dataStreamPort

        self.liveViewerComIp       = arguments.liveViewerComIp
        self.liveViewerComPort     = arguments.liveViewerComPort
        self.liveViewerWhiteList   = arguments.liveViewerWhiteList
        self.lvCommunicatorPort    = arguments.lvCommunicatorPort
        self.signalIp              = arguments.signalIp
        self.maxRingBufferSize     = arguments.maxRingBufferSize
        self.maxQueueSize          = arguments.maxQueueSize
        self.senderResponseTimeout = arguments.senderResponseTimeout

#        self.context = zmq.Context.instance()
#        logging.debug("registering zmq global context")

        self.run()


    def run(self):
        # start file receiver
#        lvCommunicatorProcess = threading.Thread(target=LiveViewCommunicator, args=(self.lvCommunicatorPort, self.liveViewerComPort, self.liveViewerComIp, self.maxRingBuffersize, self.maxQueueSize))
        logging.info("start lvCommunicator process...")
        lvCommunicatorProcess = Process(target=LiveViewCommunicator, args=(self.lvCommunicatorPort,
                                                               self.liveViewerComPort, self.liveViewerComIp, self.liveViewerWhiteList,
                                                               self.maxRingBufferSize, self.maxQueueSize))
        lvCommunicatorProcess.start()

        #start file receiver
        fileReceiver = FileReceiver(self.targetDir,
                self.signalIp, self.dataStreamPort,
                self.lvCommunicatorPort, self.senderResponseTimeout)

        try:
            fileReceiver.process()
        except KeyboardInterrupt:
            logging.debug("Keyboard interruption detected. Shutting down")
        # except Exception, e:
        #     print "unknown exception detected."
#        finally:
#            try:
#                logging.debug("Destroying ZMQ context...")
#                self.context.destroy()
#                logging.debug("Destroying ZMQ context...done.")
#            except:
#                logging.debug("Destroying ZMQ context...failed.")
#                logging.error(sys.exc_info())


if __name__ == "__main__":
    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows
    receiver = ReceiverLiveViewer()
