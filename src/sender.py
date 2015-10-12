from __builtin__ import open, type

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import time
import argparse
import zmq
import os
import logging
import sys
from multiprocessing import Process, freeze_support

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))
CONFIG_PATH = BASE_PATH + os.sep + "conf"

sys.path.append ( CONFIG_PATH )

import shared.helperScript as helperScript
from sender.DirectoryWatcher import DirectoryWatcher
from sender.FileMover import FileMover
from sender.Cleaner import Cleaner

from senderConf import defaultConfig


def argumentParsing():
    defConf = defaultConfig()

    parser = argparse.ArgumentParser()
    parser.add_argument("--logfilePath"          , type=str, default=defConf.logfilePath          , help="path where logfile will be created (default=" + str(defConf.logfilePath) + ")")
    parser.add_argument("--logfileName"          , type=str, default=defConf.logfileName          , help="filename used for logging (default=" + str(defConf.logfileName) + ")")
    parser.add_argument("--verbose"              ,           action="store_true"                  , help="more verbose output")

    parser.add_argument("--watchFolder"          , type=str, default=defConf.watchFolder          , help="(default=" + str(defConf.watchFolder) + ")")
    parser.add_argument("--monitoredSubfolders"  , type=str, default=defConf.monitoredSubfolders  , help="(default=" + str(defConf.monitoredSubfolders) + ")")
    parser.add_argument("--monitoredFormats"     , type=str, default=defConf.monitoredFormats     , help="(default=" + str(defConf.monitoredFormats) + ")")
    parser.add_argument("--fileEventIp"          , type=str, default=defConf.fileEventIp          , help="(default=" + str(defConf.fileEventIp) + ")")
    parser.add_argument("--fileEventPort"        , type=str, default=defConf.fileEventPort        , help="(default=" + str(defConf.fileEventPort) + ")")

    parser.add_argument("--useDataStream"        , type=str, default=defConf.useDataStream        , help="(default=" + str(defConf.useDataStream) + ")")
    parser.add_argument("--dataStreamIp"         , type=str, default=defConf.dataStreamIp         , help="(default=" + str(defConf.dataStreamIp) + ")")
    parser.add_argument("--dataStreamPort"       , type=str, default=defConf.dataStreamPort       , help="(default=" + str(defConf.dataStreamPort) + ")")
    parser.add_argument("--cleanerTargetPath"    , type=str, default=defConf.cleanerTargetPath    , help="(default=" + str(defConf.cleanerTargetPath) + ")")
    parser.add_argument("--cleanerIp"            , type=str, default=defConf.cleanerIp            , help="(default=" + str(defConf.cleanerIp) + ")")
    parser.add_argument("--cleanerPort"          , type=str, default=defConf.cleanerPort          , help="(default=" + str(defConf.cleanerPort) + ")")
    parser.add_argument("--receiverComIp"        , type=str, default=defConf.receiverComIp        , help="(default=" + str(defConf.receiverComIp) + ")")
    parser.add_argument("--receiverComPort"      , type=str, default=defConf.receiverComPort      , help="(default=" + str(defConf.receiverComPort) + ")")
    parser.add_argument("--liveViewerIp"         , type=str, default=defConf.liveViewerIp         , help="(default=" + str(defConf.liveViewerIp) + ")")
    parser.add_argument("--liveViewerPort"       , type=str, default=defConf.liveViewerPort       , help="(default=" + str(defConf.liveViewerPort) + ")")
    parser.add_argument("--ondaIps"              , type=str, default=defConf.ondaIps              , help="(default=" + str(defConf.ondaIps) + ")")
    parser.add_argument("--ondaPorts"            , type=str, default=defConf.ondaPorts            , help="(default=" + str(defConf.ondaPorts) + ")")
    parser.add_argument("--receiverWhiteList"    , type=str, default=defConf.receiverWhiteList    , help="(default=" + str(defConf.receiverWhiteList) + ")")

    parser.add_argument("--parallelDataStreams"  , type=str, default=defConf.parallelDataStreams  , help="(default=" + str(defConf.parallelDataStreams) + ")")
    parser.add_argument("--chunkSize"            , type=str, default=defConf.chunkSize            , help="(default=" + str(defConf.chunkSize) + ")")

    arguments         = parser.parse_args()

    logfilePath       = str(arguments.logfilePath)
    logfileName       = str(arguments.logfileName)
    watchFolder       = str(arguments.watchFolder)
    cleanerTargetPath = str(arguments.cleanerTargetPath)

    # check if folders exists
    helperScript.checkFolderExistance(logfilePath)
    helperScript.checkFolderExistance(watchFolder)
    helperScript.checkFolderExistance(cleanerTargetPath)

    # check if logfile is writable
    helperScript.checkLogFileWritable(logfilePath, logfileName)

    return arguments


class Sender():
    logfilePath         = None
    logfileName         = None
    logfileFullPath     = None
    verbose             = None

    watchFolder         = None
    monitoredSubfolders = None
    monitoredSuffixes   = None
    fileEventIp         = None
    fileEventPort       = None

    dataStreamIp        = None
    dataStreamPort      = None
    cleanerTargetPath   = None
    zmqCleanerIp        = None
    zmqCleanerPort      = None
    cleanerComPort      = None
    receiverComPort     = None
    liveViewerIp        = None
    liveViewerPort      = None
    ondaIps             = None
    ondaPorts           = None
    receiverWhiteList   = None

    parallelDataStreams = None
    chunkSize           = None

    zmqContext          = None

    def __init__(self):
#        defConf                  = defaultConfig()
        arguments = argumentParsing()

        self.logfilePath         = arguments.logfilePath
        self.logfileName         = arguments.logfileName
        self.logfileFullPath     = os.path.join(self.logfilePath, self.logfileName)
        self.verbose             = arguments.verbose

        self.watchFolder         = arguments.watchFolder
        self.monitoredSubfolders = arguments.monitoredSubfolders
        self.monitoredFormats    = arguments.monitoredFormats
        self.fileEventIp         = arguments.fileEventIp
        self.fileEventPort       = arguments.fileEventPort

        self.useDataStream       = arguments.useDataStream
        self.dataStreamIp        = arguments.dataStreamIp
        self.dataStreamPort      = arguments.dataStreamPort
        self.cleanerTargetPath   = arguments.cleanerTargetPath
        self.cleanerIp           = arguments.cleanerIp
        self.cleanerPort         = arguments.cleanerPort
        self.receiverComIp       = arguments.receiverComIp
        self.receiverComPort     = arguments.receiverComPort
        self.liveViewerIp        = arguments.liveViewerIp
        self.liveViewerPort      = arguments.liveViewerPort
        self.ondaIps             = arguments.ondaIps
        self.ondaPorts           = arguments.ondaPorts
        self.receiverWhiteList   = arguments.receiverWhiteList

        self.parallelDataStreams = arguments.parallelDataStreams
        self.chunkSize           = arguments.chunkSize

        #enable logging
        helperScript.initLogging(self.logfileFullPath, self.verbose)



        #create zmq context
        # there should be only one context in one process
        self.zmqContext = zmq.Context.instance()
        logging.debug("registering zmq global context")

        self.run()


    def run(self):
        logging.debug("start watcher process...")
        watcherProcess = Process(target=DirectoryWatcher, args=(self.fileEventIp, self.watchFolder, self.fileEventPort, self.monitoredSubfolders, self.monitoredFormats, self.zmqContext))
        logging.debug("watcher process registered")
        watcherProcess.start()
        logging.debug("start watcher process...done")

        logging.debug("start cleaner process...")
        cleanerProcess = Process(target=Cleaner, args=(self.cleanerTargetPath, self.cleanerIp, self.cleanerPort, self.useDataStream, self.zmqContext))
        logging.debug("cleaner process registered")
        cleanerProcess.start()
        logging.debug("start cleaner process...done")


        #start new fileMover
        fileMover = FileMover(self.fileEventIp, self.fileEventPort, self.dataStreamIp, self.dataStreamPort,
                              self.receiverComIp, self.receiverComPort, self.receiverWhiteList,
                              self.parallelDataStreams, self.chunkSize,
                              self.cleanerIp, self.cleanerPort,
                              self.liveViewerIp, self.liveViewerPort,
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
