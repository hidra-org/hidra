import os
import sys
import time
import multiprocessing
import logging
import setproctitle
import socket
import argparse

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


class worker(multiprocessing.Process):
    def __init__(self, id, transferType, basePath, signalHost, port):

        self.id    = id
        self.port  = port

        self.log   = logging.getLogger("worker-"+str(self.id))

        self.query = dataTransfer(transferType, signalHost, useLog = True)

        self.basePath = basePath

        self.log.debug("start dataTransfer on port " +str(port))
        # targets are locally
        self.query.start([signalHost, port])
#        self.query.start(port)

        self.run()


    def run(self):
        while True:
            try:
                self.log.debug("worker-" + str(self.id) + ": waiting")
                [metadata, data] = self.query.get()
                time.sleep(0.1)
            except:
                break

            if transferType in ["queryMetadata", "streamMetadata"]:
                self.log.debug("worker-" + str(self.id) + ": metadata " + str(metadata["filename"]))
                filepath = self.query.generateTargetFilepath(self.basePath, metadata)
                self.log.debug("worker-" + str(self.id) + ": filepath " + filepath)

                with open(filepath, "r") as fileDescriptor:
                    content = fileDescriptor.read()
                    self.log.debug("worker-" + str(self.id) + ": file " + filepath + " read")
            else:
                print "metadata", str(metadata)

            print "data", str(data)[:100]



    def stop(self):
        self.query.stop()


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()



if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--signalHost"        , type    = str,
                                                default = socket.gethostname(),
                                                help    = "Host where HiDRA is runnning")
    parser.add_argument("--procname"          , type    = str,
                                                default = "example_onda",
                                                help    = "Name with which the service should be running")

    arguments                    = parser.parse_args()


    setproctitle.setproctitle(arguments.procname)


    signalHost = arguments.signalHost
#    signalHost = "zitpcx22614.fritz.box"
#    signalHost = "zitpcx22614w.desy.de"
#    signalHost = "zitpcx19282.desy.de"
#    signalHost = "lsdma-lab04.desy.de"
#    signalHost = "asap3-bl-prx07.desy.de"

    transferType = "queryNext"
#    transferType = "stream"
#    transferType = "streamMetadata"
#    transferType = "queryMetadata"

    basePath = BASE_PATH + os.sep + "data" + os.sep + "target"
#    basePath = "/asap3/petra3/gpfs/p00/2016/commissioning/c20160205_000_smbtest/"

    numberOfWorker = 3
    workers = []

    targets = []

    for n in range(numberOfWorker):
        p = str(50100 + n)

        targets.append([signalHost, p, 1, [".cbf"]])

        w = multiprocessing.Process(target=worker, args=(n, transferType, basePath, signalHost, p))
        workers.append(w)

#    targets = [[signalHost, "50101", 1, [".cbf"]]]
#    targets = [[signalHost, "50101", 1], [signalHost, "50102", 1], [signalHost, "50103", 1]]
#    targets = [["zitpcx19282.desy.de", "50101", 1, [".cbf"]], ["zitpcx19282.desy.de", "50102", 1, [".cbf"]], ["zitpcx19282.desy.de", "50103", 1, [".cbf"]], ["lsdma-lab04.desy.de", "50104", 1, [".cbf"]]]

    query = dataTransfer(transferType, signalHost, useLog = True)
    query.initiate(targets)

    for w in workers:
        w.start()

    try:
        while True:
            pass
    except:
        pass
    finally:
        for w in workers:
            w.terminate()

        query.stop()

