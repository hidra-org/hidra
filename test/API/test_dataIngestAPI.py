import os
import sys
import time
import zmq
import logging
import threading
import json

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


class Receiver ():
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
        connectionStr = "ipc:///tmp/HiDRA/eventDet"
        self.eventSocket.bind(connectionStr)
        logging.info("eventSocket started (bind) for '" + connectionStr + "'")

        self.dataSocket    = self.context.socket(zmq.PULL)
        connectionStr = "ipc:///tmp/HiDRA/dataFetch"
        self.dataSocket.bind(connectionStr)
        logging.info("dataSocket started (bind) for '" + connectionStr + "'")

        self.poller = zmq.Poller()
        self.poller.register(self.signalSocket, zmq.POLLIN)
        self.poller.register(self.eventSocket, zmq.POLLIN)
        self.poller.register(self.dataSocket, zmq.POLLIN)


    def run (self):
        fileDescriptor = None
        filename = os.path.join(BASE_PATH, "data", "target", "local", "test.cbf")

        mark_as_close = False
        all_closed = False
        while True:
            try:
                socks = dict(self.poller.poll())

                if socks and self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:

                    message = self.signalSocket.recv()
                    logging.debug("signalSocket recv: " + message)

                    if message == "OPEN_FILE":
                        targetFile = filename
                        # Open file
                        fileDescriptor = open(targetFile, "wb")
                        logging.debug("Opened file")

                        self.signalSocket.send(message)
                        logging.debug("signalSocket send: " + message)

                    elif message == "CLOSE_FILE":
                        if all_closed:
                            self.signalSocket.send(message)
                            logging.debug("signalSocket send: " + message)
                            all_close = None

                            # Close file
                            fileDescriptor.close()
                            logging.debug("Closed file")

                            break
                        else:
                            mark_as_close = message
                    else:
                        self.signalSocket.send(message)
                        logging.debug("signalSocket send: " + message)

                if socks and self.eventSocket in socks and socks[self.eventSocket] == zmq.POLLIN:

                    eventMessage = self.eventSocket.recv()
#                    logging.debug("eventSocket recv: " + str(json.loads(eventMessage)))
                    logging.debug("eventSocket recv: " + eventMessage)

                    if eventMessage == "CLOSE_FILE":
                        if mark_as_close:
                            self.signalSocket.send(mark_as_close)
                            logging.debug("signalSocket send: " + mark_as_close)
                            mark_as_close = None

                            # Close file
                            fileDescriptor.close()
                            logging.debug("Closed file")

                            break
                        else:
                            all_closed = True

                if socks and self.dataSocket in socks and socks[self.dataSocket] == zmq.POLLIN:

                    data = self.dataSocket.recv()

                    logging.debug("dataSocket recv (len={s}): {d}".format(d=data[:100], s=len(data)))

                    fileDescriptor.write(data)
                    logging.debug("Write file content")

            except:
                logging.error("Exception in run", exc_info=True)
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
                logging.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                logging.info("Closing ZMQ context...done.")
            except:
                logging.error("Closing ZMQ context...failed.", exc_info=True)


    def __del__ (self):
        self.stop()


    def __exit__ (self):
        self.stop()


if __name__ == '__main__':
    r = Receiver()
    r.run()
    r.stop()


