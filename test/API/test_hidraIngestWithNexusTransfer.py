import os
import sys
import time
import zmq
import logging
import threading
import json
import tempfile

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = os.path.join(BASE_PATH, "src", "APIs")
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

try:
    # search in global python modules first
    from hidra.transfer import dataTransfer
except:
    # then search in local modules
    if not API_PATH in sys.path:
        sys.path.append ( API_PATH )

    from hidra.transfer import dataTransfer

try:
    # search in global python modules first
    from hidra.ingest import dataIngest
except:
    # then search in local modules
    if not API_PATH in sys.path:
        sys.path.append ( API_PATH )
    del API_PATH

    from hidra.ingest import dataIngest


#enable logging
logfilePath = os.path.join(BASE_PATH, "logs")
logfile     = os.path.join(logfilePath, "testHidraIngestWithDataTransfer.log")
helpers.initLogging(logfile, True, "DEBUG")

del BASE_PATH

print
print "==== TEST: hidraIngest together with nexus transfer ===="
print


class ZmqDataManager(threading.Thread):
    def __init__(self, context = None):
        self.extHost      = "0.0.0.0"
        self.localhost    = "zitpcx19282"
#        self.localhost    = "localhost"
        self.dataOutPort  = "50100"

        self.log          = logging.getLogger("ZmqDataManager")

        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False

        self.eventSocket  = self.context.socket(zmq.PULL)
        connectionStr     = "ipc://{0}".format(os.path.join(tempfile.gettempdir(), "hidra", "eventDet"))
#        connectionStr     = "tcp://{h}:{p}".format(h=self.extHost, p=self.eventPort)
        self.eventSocket.bind(connectionStr)
        self.log.info("eventSocket started (bind) for '" + connectionStr + "'")

        self.dataInSocket  = self.context.socket(zmq.PULL)
        connectionStr      = "ipc://{0}".format(os.path.join(tempfile.gettempdir(), "hidra", "dataFetch"))
#        connectionStr      = "tcp://{h}:{p}".format(h=self.extHost, p=self.dataInPort)
        self.dataInSocket.bind(connectionStr)
        self.log.info("dataInSocket started (bind) for '" + connectionStr + "'")

        self.dataOutSocket = self.context.socket(zmq.PUSH)
        connectionStr      = "tcp://{h}:{p}".format(h=self.localhost, p=self.dataOutPort)
        self.dataOutSocket.connect(connectionStr)
        self.log.info("dataOutSocket started (connect) for '" + connectionStr + "'")

        self.poller = zmq.Poller()
        self.poller.register(self.eventSocket, zmq.POLLIN)
        self.poller.register(self.dataInSocket, zmq.POLLIN)

        threading.Thread.__init__(self)


    def run(self):
        filename = "1.h5"

        while True:
            try:
                socks = dict(self.poller.poll())
                dataMessage = None
                metadata    = None

                if socks and self.eventSocket in socks and socks[self.eventSocket] == zmq.POLLIN:

                    metadata = self.eventSocket.recv()
                    self.log.debug("eventSocket recv: " + metadata)

                    if metadata == b"CLOSE_FILE":
                        self.dataOutSocket.send_multipart([metadata, filename, "0/1"])

                if socks and self.dataInSocket in socks and socks[self.dataInSocket] == zmq.POLLIN:

                    data = self.dataInSocket.recv()
                    self.log.debug("dataSocket recv: " + str(data))

                    dataMessage = [json.dumps(metadata), data]

                    self.dataOutSocket.send_multipart(dataMessage)
                    self.log.debug("Send")

            except zmq.ZMQError as e:
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



def runHidraIngest(numbToSend):
    dI = dataIngest(useLog = True)

    dI.createFile("1.h5")

    for i in range(numbToSend):
        try:
            data = "THISISTESTDATA-" + str(i)
            dI.write(data)
            logging.info("write")
        except:
            logging.error("runHidraIngest break", exc_info=True)
            break

    try:
        dI.closeFile()
    except:
        logging.error("Could not close file", exc_info=True)

    dI.stop()


def openCallback (params, retrievedParams):
    print "openCallback", params, retrievedParams

def closeCallback (params, retrievedParams):
    params["runLoop"] = False
    print "closeCallback",  params, retrievedParams

def readCallback (params, retrievedParams):
    print "readCallback", params, retrievedParams



def runNexusTransfer(numbToRecv):
    dT = dataTransfer("nexus", useLog = True)
    dT.start(["zitpcx19282", "50100"])
#    dT.start(["localhost", "50100"])

    callbackParams = {
            "runLoop" : True
            }

    # number to receive + open signal + close signal
    try:
        while callbackParams["runLoop"]:
            try:
                dT.read(callbackParams, openCallback, readCallback, closeCallback)
            except KeyboardInterrupt:
                break
            except:
                logging.error("runNexusTransfer break", exc_info=True)
                break
    finally:
        dT.stop()

useTest = True
#useTest = False

if useTest:
    zmqDataManagerThread = ZmqDataManager()
    zmqDataManagerThread.start()

number = 5

runHidraIngestThread = threading.Thread(target=runHidraIngest, args = (number, ))
runNexusTransferThread = threading.Thread(target=runNexusTransfer, args = (number, ))

runHidraIngestThread.start()
runNexusTransferThread.start()

runHidraIngestThread.join()
runNexusTransferThread.join()

if useTest:
    zmqDataManagerThread.stop()

print
print "==== TEST END: hidraIngest together with nexus transfer ===="
print



