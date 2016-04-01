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
logfile     = os.path.join(logfilePath, "test_onda.log")
helpers.initLogging(logfile, True, "DEBUG")

del BASE_PATH


class worker(multiprocessing.Process):
    def __init__(self, id, transferType, signalHost, port):

        self.id    = id
        self.port  = port

        self.log   = logging.getLogger("worker-"+str(self.id))

        self.query = dataTransfer(transferType, signalHost, useLog = True)

        self.log.debug("start dataTransfer on port " +str(port))
        self.query.start(port)

        self.run()


    def run(self):
        while True:
            try:
                self.log.debug("worker-" + str(self.id) + ": waiting")
                [metadata, data] = self.query.get()
                time.sleep(0.1)
            except:
                break

            self.log.debug("worker-" + str(self.id) + ": metadata " + str(metadata["filename"]))
        #    print "data", str(data)[:10]


    def stop(self):
        self.query.stop()


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()



if __name__ == "__main__":

    signalHost = "zitpcx19282.desy.de"

    targets = [["zitpcx19282.desy.de", "50101", 1], ["zitpcx19282.desy.de", "50102", 1], ["zitpcx19282.desy.de", "50103", 1]]
#    targets = [["zitpcx19282.desy.de", "50101", 1], ["zitpcx19282.desy.de", "50102", 1], ["zitpcx19282.desy.de", "50103", 1], ["lsdma-lab04.desy.de", "50104", 1]]

    transferType = "stream"

    w1 = multiprocessing.Process(target=worker, args=(0, transferType, signalHost, "50101"))
    w2 = multiprocessing.Process(target=worker, args=(1, transferType, signalHost, "50102"))
    w3 = multiprocessing.Process(target=worker, args=(2, transferType, signalHost, "50103"))

    query = dataTransfer(transferType, signalHost, useLog = True)
    query.initiate(targets)

    w1.start()
    w2.start()
    w3.start()

    try:
        while True:
            pass
    except:
        pass
    finally:
        w1.terminate()
        w2.terminate()
        w3.terminate()

        query.stop()

