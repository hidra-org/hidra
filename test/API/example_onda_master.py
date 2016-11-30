import os
import sys
import time
import multiprocessing
import logging


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = os.path.join(BASE_PATH, "src", "APIs")
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

try:
    # search in global python modules first
    from hidra import dataTransfer
except:
    # then search in local modules
    if not API_PATH in sys.path:
        sys.path.append ( API_PATH )
    del API_PATH

    from hidra import dataTransfer


#enable logging
logfilePath = os.path.join(BASE_PATH, "logs")
logfile     = os.path.join(logfilePath, "test_onda.log")
helpers.initLogging(logfile, True, "DEBUG")

del BASE_PATH


if __name__ == "__main__":

    signalHost = "zitpcx19282.desy.de"

    # a list of targets of the form [<host>, <port, <priority>]
    targets = [["zitpcx19282.desy.de", "50101", 1], ["zitpcx19282.desy.de", "50102", 1], ["zitpcx19282.desy.de", "50103", 1], ["lsdma-lab04.desy.de", "50104", 1]]


    query = dataTransfer("queryNext", signalHost, useLog = True)
    query.initiate(targets)

    try:
        while True:
            pass
    finally:
        query.stop()

