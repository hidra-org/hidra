__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import sys
import argparse
import logging
import os
import json
import ConfigParser

import shared.helperScript as helperScript
from receiverLiveViewer.FileReceiver import FileReceiver

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) )
CONFIG_PATH = BASE_PATH + os.sep + "conf"


def argumentParsing():
    configFile = CONFIG_PATH + os.sep + "receiverLiveViewer.conf"

    config = ConfigParser.RawConfigParser()
    config.readfp(helperScript.FakeSecHead(open(configFile)))


    logfilePath             = config.get('asection', 'logfilePath')
    logfileName             = config.get('asection', 'logfileName')
    targetDir               = config.get('asection', 'targetDir')
    dataStreamIp            = config.get('asection', 'dataStreamIp')
    dataStreamPort          = config.get('asection', 'dataStreamPort')
    liveViewerIp            = config.get('asection', 'liveViewerIp')
    liveViewerPort          = config.get('asection', 'liveViewerPort')
    coordinatorExchangePort = config.get('asection', 'coordinatorExchangePort')
    senderComIp             = config.get('asection', 'senderComIp')
    senderComPort           = config.get('asection', 'senderComPort')
    maxRingBufferSize       = config.get('asection', 'maxRingBufferSize')
    senderResponseTimeout   = config.get('asection', 'senderResponseTimeout')



    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"            , type=str, default=logfilePath,
                                                     help="path where logfile will be created (default=" + str(logfilePath) + ")")
    parser.add_argument("--logfileName"            , type=str, default=logfileName,
                                                     help="filename used for logging (default=" + str(logfileName) + ")")
    parser.add_argument("--targetDir"              , type=str, default=targetDir,
                                                     help="where incoming data will be stored to (default=" + str(targetDir) + ")")
    parser.add_argument("--dataStreamIp"           , type=str, default=dataStreamIp,
                                                     help="ip of dataStream-socket to pull new files from (default=" + str(dataStreamIp) + ")")
    parser.add_argument("--dataStreamPort"         , type=str, default=dataStreamPort,
                                                     help="port number of dataStream-socket to pull new files from; there needs to be one entry for each streams (default=" + str(dataStreamPort) + ")")
    parser.add_argument("--liveViewerIp"           , type=str, default=liveViewerIp,
                                                     help="ip to bind LiveViewer to (default=" + str(liveViewerIp) + ")")
    parser.add_argument("--liveViewerPort"         , type=str, default=liveViewerPort,
                                                     help="tcp port of live viewer (default=" + str(liveViewerPort) + ")")
    parser.add_argument("--coordinatorExchangePort", type=str, default=coordinatorExchangePort,
                                                     help="port to exchange data and signals between receiver and coordinato (default=" + str(coordinatorExchangePort) + ")")
    parser.add_argument("--senderComIp"            , type=str, default=senderComIp,
                                                     help="port number of dataStream-socket to send signals back to the sender (default=" + str(senderComIp) + ")")
    parser.add_argument("--senderComPort"          , type=str, default=senderComPort,
                                                     help="port number of dataStream-socket to send signals back to the sender (default=" + str(senderComPort) + ")")
    parser.add_argument("--maxRingBufferSize"      , type=int, default=maxRingBufferSize,
                                                     help="size of the ring buffer for the live viewer (default=" + str(maxRingBufferSize) + ")")
    parser.add_argument("--senderResponseTimeout"  , type=int, default=senderResponseTimeout,
                                                     help=argparse.SUPPRESS)
    parser.add_argument("--verbose"                , action="store_true",
                                                     help="more verbose output")

    arguments   = parser.parse_args()

    targetDir   = str(arguments.targetDir)
    logfilePath = str(arguments.logfilePath)
    logfileName = str(arguments.logfileName)

    # check target directory for existance
    helperScript.checkDirExistance(targetDir)

    # check if logfile is writable
    helperScript.checkLogFileWritable(logfilePath, logfileName)

    return arguments


class ReceiverLiveViewer():
    logfilePath             = None
    logfileName             = None
    logfileFullPath         = None
    verbose                 = None

    targetDir               = None
    dataStreamIp            = None
    dataStreamPort          = None

    liveViewerIp            = None
    liveViewerPort          = None
    coordinatorExchangePort = None
    senderComIp             = None
    senderComPort           = None
    maxRingBufferSize       = None
    senderResponseTimeout   = None

    def __init__(self):
        arguments = argumentParsing()

        self.logfilePath             = arguments.logfilePath
        self.logfileName             = arguments.logfileName
        self.logfileFullPath         = os.path.join(self.logfilePath, self.logfileName)
        self.verbose                 = arguments.verbose

        self.targetDir               = arguments.targetDir
        self.dataStreamIp            = arguments.dataStreamIp
        self.dataStreamPort          = arguments.dataStreamPort

        self.liveViewerIp            = arguments.liveViewerIp
        self.liveViewerPort          = arguments.liveViewerPort
        self.coordinatorExchangePort = arguments.coordinatorExchangePort
        self.senderComIp             = arguments.senderComIp
        self.senderComPort           = arguments.senderComPort
        self.maxRingBufferSize       = arguments.maxRingBufferSize
        self.senderResponseTimeout   = arguments.senderResponseTimeout


        #enable logging
        helperScript.initLogging(self.logfileFullPath, self.verbose)


        #start file receiver
        myWorker = FileReceiver(self.targetDir,
                self.senderComIp, self.senderComPort,
                self.dataStreamIp, self.dataStreamPort,
                self.liveViewerPort, self.liveViewerIp,
                self.coordinatorExchangePort, self.maxRingBufferSize, self.senderResponseTimeout)

if __name__ == "__main__":
    receiver = ReceiverLiveViewer()
