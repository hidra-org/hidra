import os
import sys
import time
import multiprocessing
import logging


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH

from dataTransferAPI import dataTransfer

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

#enable logging
logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
logfile     = os.path.join(logfilePath, "test_force_stop.log")
helpers.initLogging(logfile, True, "DEBUG")

if __name__ == "__main__":

    signalHost = "zitpcx19282.desy.de"
#    signalHost = "lsdma-lab04.desy.de"
#    signalHost = "asap3-bl-prx07.desy.de"

#    targets = [["asap3-bl-prx07.desy.de", "50101", 1, [".cbf"]], ["asap3-bl-prx07.desy.de", "50102", 1, [".cbf"]], ["asap3-bl-prx07.desy.de", "50103", 1, [".cbf"]]]
#    targets = [["zitpcx19282.desy.de", "50101", 1, [".cbf"]]]
    targets = [["zitpcx19282.desy.de", "50100", 1, [".cbf"]], ["zitpcx19282.desy.de", "50101", 1, [".cbf"]], ["zitpcx19282.desy.de", "50102", 1, [".cbf"]]]
#    targets = [["zitpcx19282.desy.de", "50101", 1], ["zitpcx19282.desy.de", "50102", 1], ["zitpcx19282.desy.de", "50103", 1]]
#    targets = [["zitpcx19282.desy.de", "50101", 1, [".cbf"]], ["zitpcx19282.desy.de", "50102", 1, [".cbf"]], ["zitpcx19282.desy.de", "50103", 1, [".cbf"]], ["lsdma-lab04.desy.de", "50104", 1, [".cbf"]]]

    transferType = "queryNext"
#    transferType = "stream"
#    transferType = "streamMetadata"
#    transferType = "queryMetadata"

    basePath = BASE_PATH + os.sep + "data" + os.sep + "target"
#    basePath = "/asap3/petra3/gpfs/p00/2016/commissioning/c20160205_000_smbtest/"

    query = dataTransfer(transferType, signalHost, useLog = True)
    query.forceStop(targets)

