__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import sys
import argparse
import logging
import os

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) )
CONFIG_PATH = BASE_PATH + os.sep + "conf"

sys.path.append ( CONFIG_PATH )

import shared.helperScript as helperScript
from receiver.FileReceiver import FileReceiver

from receiverConf import defaultConfig


def argumentParsing():
    defConf = defaultConfig()

    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"          , type=str, default=defConf.logfilePath          , help="path where logfile will be created (default=" + str(defConf.logfilePath) + ")")
    parser.add_argument("--logfileName"          , type=str, default=defConf.logfileName          , help="filename used for logging (default=" + str(defConf.logfileName) + ")")
    parser.add_argument("--targetDir"            , type=str, default=defConf.targetDir            , help="where incoming data will be stored to (default=" + str(defConf.targetDir) + ")")
    parser.add_argument("--dataStreamIp"         , type=str, default=defConf.dataStreamIp         , help="ip of dataStream-socket to pull new files from (default=" + str(defConf.dataStreamIp) + ")")
    parser.add_argument("--dataStreamPort"       , type=str, default=defConf.dataStreamPort       , help="port number of dataStream-socket to pull new files from (default=" + str(defConf.dataStreamPort) + ")")
    parser.add_argument("--verbose"              ,           action="store_true"                  , help="more verbose output")

    arguments = parser.parse_args()

    targetDir   = str(arguments.targetDir)
    logfilePath = str(arguments.logfilePath)
    logfileName = str(arguments.logfileName)

    # check target directory for existance
    helperScript.checkFolderExistance(targetDir)

    # check if logfile is writable
    helperScript.checkLogFileWritable(logfilePath, logfileName)

    return arguments


class ReceiverLiveViewer():
    logfilePath           = None
    logfileName           = None
    logfileFullPath       = None
    verbose               = None

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

        self.logfilePath           = arguments.logfilePath
        self.logfileName           = arguments.logfileName
        self.logfileFullPath       = os.path.join(self.logfilePath, self.logfileName)
        self.verbose               = arguments.verbose

        self.targetDir             = arguments.targetDir
        self.zmqDataStreamIp       = arguments.dataStreamIp
        self.zmqDataStreamPort     = arguments.dataStreamPort

        #enable logging
        helperScript.initLogging(self.logfileFullPath, self.verbose)


        #start file receiver
        myWorker = FileReceiver(self.targetDir, self.zmqDataStreamIp, self.zmqDataStreamPort)


if __name__ == "__main__":
    receiver = ReceiverLiveViewer()
