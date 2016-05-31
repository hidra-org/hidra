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
logfile     = os.path.join(logfilePath, "testDataIngestAPI.log")
helpers.initLogging(logfile, True, "DEBUG")

del BASE_PATH


print
print "==== TEST: data ingest ===="
print

class Receiver (threading.Thread):
    def __init__ (self, context = None):
        self.extHost    = "0.0.0.0"
        self.signalPort = "50050"
        self.eventPort  = "50003"
        self.dataPort   = "50010"

        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False

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

        self.poller = zmq.Poller()
        self.poller.register(self.signalSocket, zmq.POLLIN)
        self.poller.register(self.eventSocket, zmq.POLLIN)
        self.poller.register(self.dataSocket, zmq.POLLIN)

        threading.Thread.__init__(self)


    def run (self):
        while True:
            try:
                socks = dict(self.poller.poll())

                if socks and self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:

                    message = self.signalSocket.recv()
                    logging.debug("signalSocket recv: " + message)

                    self.signalSocket.send(message)
                    logging.debug("signalSocket send: " + message)

                    if message == "CLOSE_FILE":
                        break

                if socks and self.eventSocket in socks and socks[self.eventSocket] == zmq.POLLIN:

#                    logging.debug("eventSocket recv: " + str(cPickle.loads(self.eventSocket.recv())))
                    logging.debug("eventSocket recv: " + self.eventSocket.recv())

                if socks and self.dataSocket in socks and socks[self.dataSocket] == zmq.POLLIN:

                    logging.debug("dataSocket recv: " + self.dataSocket.recv())
            except:
                break


    def stop (self):
        try:
            if self.signalSocket:
                logging.info("closing signalSocket...")
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
            logging.error("closing ZMQ Sockets...failed.", exc_info=True)

        if not self.extContext and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)


    def __del__ (self):
        self.stop()


    def __exit__ (self):
        self.stop()


context    = zmq.Context()

receiverThread = Receiver(context)
receiverThread.start()



obj = dataIngest(useLog = True, context = context)

obj.createFile("1.h5")

for i in range(5):
    try:
        data = "asdfasdasdfasd"
        obj.write(data)
        print "write"

    except:
        logging.error("break", exc_info=True)
        break

try:
    obj.closeFile()
except:
    logging.error("Failed to close file", exc_info=True)

logging.info("Stopping")

receiverThread.stop()
obj.stop()

print
print "==== TEST END: data Ingest ===="
print



