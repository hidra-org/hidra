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
    def __init__(self, context = None):
        self.extHost      = "0.0.0.0"
        self.localhost    = "localhost"
        self.dataOutPort  = "50100"

        self.log          = logging.getLogger("ZmqDataManager")

        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False

        self.eventSocket  = self.context.socket(zmq.PULL)
        connectionStr     = "ipc:///tmp/zeromq-data-transfer/eventDetConId"
#        connectionStr     = "tcp://" + str(self.extHost) + ":" + str(self.eventPort)
        self.eventSocket.bind(connectionStr)
        self.log.info("eventSocket started (bind) for '" + connectionStr + "'")

        self.dataInSocket  = self.context.socket(zmq.PULL)
        connectionStr      = "ipc:///tmp/zeromq-data-transfer/dataFetchConId"
#        connectionStr      = "tcp://" + str(self.extHost) + ":" + str(self.dataInPort)
        self.dataInSocket.bind(connectionStr)
        self.log.info("dataInSocket started (bind) for '" + connectionStr + "'")

        self.dataOutSocket = self.context.socket(zmq.PUSH)
        connectionStr      = "tcp://" + str(self.localhost) + ":" + str(self.dataOutPort)
        self.dataOutSocket.connect(connectionStr)
        self.log.info("dataOutSocket started (connect) for '" + connectionStr + "'")

        self.poller = zmq.Poller()
        self.poller.register(self.eventSocket, zmq.POLLIN)
        self.poller.register(self.dataInSocket, zmq.POLLIN)

        threading.Thread.__init__(self)


    def run(self):
        while True:
            try:
                socks = dict(self.poller.poll())
                dataMessage = None
                metadata    = None

                if socks and self.eventSocket in socks and socks[self.eventSocket] == zmq.POLLIN:

                    metadata = self.eventSocket.recv()
                    self.log.debug("eventSocket recv: " + metadata)

                    if metadata == b"CLOSE_FILE":
                        self.dataOutSocket.send_multipart([metadata, "0/1"])

                if socks and self.dataInSocket in socks and socks[self.dataInSocket] == zmq.POLLIN:

                    data = self.dataInSocket.recv()
                    self.log.debug("dataSocket recv: " + str(data))

                    dataMessage = [cPickle.dumps(metadata), data]

                    self.dataOutSocket.send_multipart(dataMessage)
                    self.log.debug("Send")

            except zmq.ZMQError, e:
                if not str(e) == "Socket operation on non-socket":
                    self.log.error("Error in run", exc_info=True)
                break
            except:
                self.log.error("Error in run", exc_info=True)
                break


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
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)


        if not self.extContext and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)



def runDataIngest(numbToSend):
    dI = dataIngest(useLog = True)

    dI.createFile("1.h5")

    for i in range(numbToSend):
        try:
            data = "THISISTESTDATA-" + str(i)
            dI.write(data)
            logging.info("write")
        except:
            logging.error("runDataIngest break", exc_info=True)
            break


    try:
        dI.closeFile()
    except:
        logging.error("Could not close file", exc_info=True)

    dI.stop()




def runNexusTransfer(numbToRecv):
    nT = nexusTransfer(useLog = True)

    # number to receive + open signal + close signal
    for i in range(numbToRecv + 2):
        try:
            data = nT.read()
            logging.info("Retrieved: " + str(data))

            if data == "CLOSE_FILE":
                break
        except:
            logging.error("runNexusTransfer break", exc_info=True)
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



