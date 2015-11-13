__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import sys
import argparse
import logging
import os
import ConfigParser

import shared.helperScript as helperScript
from receiver.FileReceiver import FileReceiver

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) )
CONFIG_PATH = BASE_PATH + os.sep + "conf"


def argumentParsing():
    configFile = CONFIG_PATH + os.sep + "receiver.conf"

    config = ConfigParser.RawConfigParser()
    config.readfp(helperScript.FakeSecHead(open(configFile)))

    logfilePath    = config.get('asection', 'logfilePath')
    logfileName    = config.get('asection', 'logfileName')
    targetDir      = config.get('asection', 'targetDir')
    dataStreamIp   = config.get('asection', 'dataStreamIp')
    dataStreamPort = config.get('asection', 'dataStreamPort')

    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"          , type=str, default=logfilePath,
                                                   help="path where logfile will be created (default=" + str(logfilePath) + ")")
    parser.add_argument("--logfileName"          , type=str, default=logfileName,
                                                   help="filename used for logging (default=" + str(logfileName) + ")")
    parser.add_argument("--targetDir"            , type=str, default=targetDir,
                                                   help="where incoming data will be stored to (default=" + str(targetDir) + ")")
    parser.add_argument("--dataStreamIp"         , type=str, default=dataStreamIp,
                                                   help="ip of dataStream-socket to pull new files from (default=" + str(dataStreamIp) + ")")
    parser.add_argument("--dataStreamPort"       , type=str, default=dataStreamPort,
                                                   help="port number of dataStream-socket to pull new files from (default=" + str(dataStreamPort) + ")")
    parser.add_argument("--verbose"              , action="store_true",
                                                   help="more verbose output")

    arguments = parser.parse_args()

    logfilePath     = str(arguments.logfilePath)
    logfileName     = str(arguments.logfileName)
    logfileFullPath = os.path.join(logfilePath, logfileName)
    verbose         = arguments.verbose

    targetDir   = str(arguments.targetDir)

    #enable logging
    helperScript.initLogging(logfileFullPath, verbose)

    # check target directory for existance
    helperScript.checkDirExistance(targetDir)

    # check if logfile is writable
    helperScript.checkLogFileWritable(logfilePath, logfileName)

    return arguments


class ReceiverLiveViewer():
    targetDir             = None
    zmqDataStreamIp       = None
    zmqDataStreamPort     = None

    zmqLiveViewerIp       = None
    zmqLiveViewerPort     = None
    senderComPort         = None
    maxRingBufferSize     = None
    senderResponseTimeout = None

    def __init__(self):
        arguments = argumentParsing()

        self.targetDir             = arguments.targetDir
        self.zmqDataStreamIp       = arguments.dataStreamIp
        self.zmqDataStreamPort     = arguments.dataStreamPort

        #start file receiver
        myWorker = FileReceiver(self.targetDir, self.zmqDataStreamIp, self.zmqDataStreamPort)


if __name__ == "__main__":
    receiver = ReceiverLiveViewer()
