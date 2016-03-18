import os
import sys
import time
import zmq
import logging
import threading
import cPickle

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH

from dataIngestAPI import dataIngest
from nexusTransferAPI import nexusTransfer

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

#enable logging
logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
logfile     = os.path.join(logfilePath, "testDataIngestWithNexusTransfer.log")
helpers.initLogging(logfile, True, "DEBUG")


print
print "==== TEST: data ingest together with nexus transfer ===="
print


class ZmqDataManager(threading.Thread):
    def __init__(self):
        self.extHost      = "0.0.0.0"
        self.localhost    = "localhost"
        self.eventPort    = "50003"
        self.dataInPort   = "50010"
        self.dataOutPort  = "50100"

        self.log          = logging.getLogger("ZmqDataManager")

        self.context      = zmq.Context()

        self.eventSocket  = self.context.socket(zmq.PULL)
        connectionStr     = "tcp://" + str(self.extHost) + ":" + str(self.eventPort)
        self.eventSocket.bind(connectionStr)
        self.log.info("eventSocket started (bind) for '" + connectionStr + "'")

        self.dataInSocket  = self.context.socket(zmq.PULL)
        connectionStr      = "tcp://" + str(self.extHost) + ":" + str(self.dataInPort)
        self.dataInSocket.bind(connectionStr)
        self.log.info("dataInSocket started (bind) for '" + connectionStr + "'")

        self.dataOutSocket = self.context.socket(zmq.PUSH)
        connectionStr      = "tcp://" + str(self.localhost) + ":" + str(self.dataOutPort)
        self.dataOutSocket.connect(connectionStr)
        self.log.info("dataOutSocket started (connect) for '" + connectionStr + "'")

        threading.Thread.__init__(self)


    def run(self):
        for i in range(6):
            try:
                metadata = self.eventSocket.recv()

                if metadata == b"CLOSE_FILE":
                    self.log.debug("eventSocket recv: " + metadata)
                    dataMessage = [metadata, "0/1"]
                else:
                    self.log.debug("eventSocket recv: " + str(cPickle.loads(metadata)))

                    data = self.dataInSocket.recv()
                    self.log.debug("dataSocket recv: " + data)

                    dataMessage = [metadata, data]

                self.dataOutSocket.send_multipart(dataMessage)
                self.log.debug("Send")
            except:
                self.log.error("Error in run", exc_info=True)


    def stop(self):
        try:
            if self.eventSocket:
                self.log.info("closing eventSocket...")
                self.eventSocket.close(linger=0)
                self.eventSocket = None
            if self.dataInSocket:
                self.log.info("closing dataInSocket...")
                self.dataInSocket.close(linger=0)
                self.dataInSocket = None
            if self.dataOutSocket:
                self.log.info("closing dataOutSocket...")
                self.dataOutSocket.close(linger=0)
                self.dataOutSocket = None
            if self.context:
                self.log.info("destroying context...")
                self.context.destroy()
                self.context = None
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)


def runDataIngest(numbToSend):
    dI = dataIngest(useLog = True)

    dI.createFile("1.h5")

    for i in range(numbToSend):
        try:
            data = "THISISTESTDATA-" + str(i)
            dI.write(data)
            logging.info("write")

        except:
            logging.error("break", exc_info=True)
            break

    dI.closeFile()

    dI.stop()




def runNexusTransfer(numbToRecv):
    nT = nexusTransfer(useLog = True)

    # number to receive + open signal + close signal
    for i in range(numbToRecv + 2):
        try:
            data = nT.read()
            logging.info("Retrieved: " + str(data))
        except:
            logging.error("break", exc_info=True)
            break

    nT.stop()


zmqDataManagerThread = ZmqDataManager()
zmqDataManagerThread.start()

number = 5

runDataIngestThread = threading.Thread(target=runDataIngest, args = (number, ))
runNexusTransferThread = threading.Thread(target=runNexusTransfer, args = (number, ))

runDataIngestThread.start()
runNexusTransferThread.start()


runDataIngestThread.join()
runNexusTransferThread.join()
zmqDataManagerThread.stop()

print
print "==== TEST END: data ingest together with nexus transfer ===="
print



