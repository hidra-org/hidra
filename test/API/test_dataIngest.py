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

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

#enable logging
logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
logfile     = os.path.join(logfilePath, "testIngestAPI.log")
helpers.initLogging(logfile, True, "DEBUG")

del BASE_PATH


print
print "==== TEST: data ingest ===="
print

class Receiver(threading.Thread):
    def __init__(self):
        self.extHost    = "0.0.0.0"
        self.signalPort = "6000"
        self.eventPort  = "6001"
        self.dataPort   = "6002"

        self.context       = zmq.Context()

        self.signalSocket  = self.context.socket(zmq.REP)
        connectionStr = "tcp://" + str(self.extHost) + ":" + str(self.signalPort)
        self.signalSocket.bind(connectionStr)
        logging.info("signalSocket started (bind) for '" + connectionStr + "'")

        self.eventSocket   = self.context.socket(zmq.PULL)
        connectionStr = "tcp://" + str(self.extHost) + ":" + str(self.eventPort)
        self.eventSocket.bind(connectionStr)
        logging.info("eventSocket started (bind) for '" + connectionStr + "'")

        self.dataSocket    = self.context.socket(zmq.PULL)
        connectionStr = "tcp://" + str(self.extHost) + ":" + str(self.dataPort)
        self.dataSocket.bind(connectionStr)
        logging.info("dataSocket started (bind) for '" + connectionStr + "'")

        threading.Thread.__init__(self)

    def run(self):
        message = self.signalSocket.recv()
        logging.debug("signalSocket recv: " + message)

        self.signalSocket.send(message)
        logging.debug("signalSocket send: " + message)


        for i in range(5):
            logging.debug("eventSocket recv: " + str(cPickle.loads(self.eventSocket.recv())))
            logging.debug("dataSocket recv: " + self.dataSocket.recv())


        message = self.signalSocket.recv()
        logging.debug("signalSocket recv: " + message)
        self.signalSocket.send(message)
        logging.debug("signalSocket send: " + message)

        logging.debug("eventSocket recv: " + self.eventSocket.recv())


    def stop(self):
        try:
            if self.signalSocket:
                logging.info("closing eventSocket...")
                self.signalSocket.close(linger=0)
                self.signalSocket = None
            if self.eventSocket:
                logging.info("closing eventSocket...")
                self.eventSocket.close(linger=0)
                self.eventSocket = None
            if self.dataSocket:
                logging.info("closing dataSocket...")
                self.dataSocket.close(linger=0)
                self.dataSocket = None
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)



receiverThread = Receiver()
receiverThread.start()



obj = dataIngest(useLog = True)

obj.createFile("1.h5")

for i in range(5):
    try:
        data = "asdfasdasdfasd"
        obj.write(data)
        print "write"

    except:
        logging.error("break", exc_info=True)
        break

obj.closeFile()


receiverThread.stop()
obj.stop()

print
print "==== TEST END: data Ingest ===="
print



