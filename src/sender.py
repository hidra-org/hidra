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

from config import defaultConfigSender


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
    receiverWhiteList   = None

    parallelDataStreams = None
    chunkSize           = None

    zmqContext          = None

    def __init__(self, verbose = True):
        defConf                  = defaultConfigSender()

        self.logfilePath         = defConf.logfilePath
        self.logfileName         = defConf.logfileName
        self.logfileFullPath     = os.path.join(self.logfilePath, self.logfileName)
        self.verbose             = verbose

        self.watchFolder         = defConf.watchFolder
        self.monitoredSubfolders = defConf.monitoredSubfolders
        self.monitoredFormats    = defConf.monitoredFormats
        self.fileEventIp         = defConf.fileEventIp
        self.fileEventPort       = defConf.fileEventPort

        self.dataStreamIp        = defConf.dataStreamIp
        self.dataStreamPort      = defConf.dataStreamPort
        self.cleanerTargetPath   = defConf.cleanerTargetPath
        self.cleanerIp           = defConf.cleanerIp
        self.cleanerPort         = defConf.cleanerPort
        self.receiverComPort     = defConf.receiverComPort
        self.ondaIps             = defConf.ondaIps
        self.ondaPorts           = defConf.ondaPorts
        self.receiverWhiteList   = defConf.receiverWhiteList

        self.parallelDataStreams = defConf.parallelDataStreams
        self.chunkSize           = defConf.chunkSize

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
        cleanerProcess = Process(target=Cleaner, args=(self.cleanerTargetPath, self.cleanerIp, self.cleanerPort, self.zmqContext))
        logging.debug("cleaner process registered")
        cleanerProcess.start()
        logging.debug("start cleaner process...done")


        #start new fileMover
        fileMover = FileMover(self.fileEventIp, self.fileEventPort, self.dataStreamIp, self.dataStreamPort,
                              self.receiverComPort, self.receiverWhiteList,
                              self.parallelDataStreams, self.chunkSize,
                              self.cleanerIp, self.cleanerPort,
                              self.ondaIps, self.ondaPorts,
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

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true", help="more verbose output")
    arguments = parser.parse_args()

    sender = Sender(arguments.verbose)
