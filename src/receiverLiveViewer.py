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
    dataStreamIp          = config.get('asection', 'dataStreamIp')
    dataStreamPort        = config.get('asection', 'dataStreamPort')
    liveViewerIp          = config.get('asection', 'liveViewerIp')
    liveViewerPort        = config.get('asection', 'liveViewerPort')
    lvCommunicatorPort    = config.get('asection', 'lvCommunicatorPort')
    senderComIp           = config.get('asection', 'senderComIp')
    senderComPort         = config.get('asection', 'senderComPort')
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
    parser.add_argument("--dataStreamIp"          , type=str, default=dataStreamIp,
                                                    help="IP of dataStream-socket to pull new files from (default=" + str(dataStreamIp) + ")")
    parser.add_argument("--dataStreamPort"        , type=str, default=dataStreamPort,
                                                    help="Port number of dataStream-socket to pull new files from; there needs to be one entry for each streams (default=" + str(dataStreamPort) + ")")
    parser.add_argument("--liveViewerIp"          , type=str, default=liveViewerIp,
                                                    help="IP to bind LiveViewer to (default=" + str(liveViewerIp) + ")")
    parser.add_argument("--liveViewerPort"        , type=str, default=liveViewerPort,
                                                    help="TCP port of live viewer (default=" + str(liveViewerPort) + ")")
    parser.add_argument("--lvCommunicatorPort"    , type=str, default=lvCommunicatorPort,
                                                    help="Port to exchange data and signals between receiver and lvcommunicator (default=" + str(lvCommunicatorPort) + ")")
    parser.add_argument("--senderComIp"           , type=str, default=senderComIp,
                                                    help="Port number of dataStream-socket to send signals back to the sender (default=" + str(senderComIp) + ")")
    parser.add_argument("--senderComPort"         , type=str, default=senderComPort,
                                                    help="Port number of dataStream-socket to send signals back to the sender (default=" + str(senderComPort) + ")")
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
    dataStreamIp          = None
    dataStreamPort        = None

    liveViewerIp          = None
    liveViewerPort        = None
    lvCommunicatorPort    = None
    senderComIp           = None
    senderComPort         = None
    maxRingBufferSize     = None
    maxQueueSize          = None
    senderResponseTimeout = None

    def __init__(self):
        arguments = argumentParsing()

        self.targetDir             = arguments.targetDir
        self.dataStreamIp          = arguments.dataStreamIp
        self.dataStreamPort        = arguments.dataStreamPort

        self.liveViewerIp          = arguments.liveViewerIp
        self.liveViewerPort        = arguments.liveViewerPort
        self.lvCommunicatorPort    = arguments.lvCommunicatorPort
        self.senderComIp           = arguments.senderComIp
        self.senderComPort         = arguments.senderComPort
        self.maxRingBufferSize     = arguments.maxRingBufferSize
        self.maxQueueSize          = arguments.maxQueueSize
        self.senderResponseTimeout = arguments.senderResponseTimeout

        self.context = zmq.Context.instance()
        logging.debug("registering zmq global context")

        self.run()


    def run(self):
        # start file receiver
#        lvCommunicatorProcess = threading.Thread(target=LiveViewCommunicator, args=(self.lvCommunicatorPort, self.liveViewerPort, self.liveViewerIp, self.maxRingBuffersize, self.maxQueueSize))
        logging.info("start lvCommunicator process...")
        lvCommunicatorProcess = Process(target=LiveViewCommunicator, args=(self.lvCommunicatorPort,
                                                               self.liveViewerPort, self.liveViewerIp,
                                                               self.maxRingBufferSize, self.maxQueueSize,
                                                               self.context))
        lvCommunicatorProcess.start()

        #start file receiver
        fileReceiver = FileReceiver(self.targetDir,
                self.senderComIp, self.senderComPort,
                self.dataStreamIp, self.dataStreamPort,
                self.lvCommunicatorPort, self.senderResponseTimeout)

        try:
            fileReceiver.process()
        except KeyboardInterrupt:
            logging.debug("Keyboard interruption detected. Shutting down")
        # except Exception, e:
        #     print "unknown exception detected."
        finally:
            try:
                logging.debug("closing ZMQ context...")
                self.context.destroy()
                logging.debug("closing ZMQ context...done.")
            except:
                logging.debug("closing ZMQ context...failed.")
                logging.error(sys.exc_info())


if __name__ == "__main__":
    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows
    receiver = ReceiverLiveViewer()
