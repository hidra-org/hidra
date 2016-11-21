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

from dataTransferAPI import dataTransfer

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

#enable logging
logfilePath = os.path.join(BASE_PATH + os.sep + "logs")
logfile     = os.path.join(logfilePath, "testNexusTransferAPI.log")
helpers.initLogging(logfile, True, "DEBUG")


print
print "==== TEST: nexus transfer ===="
print

class Sender_as_thread (threading.Thread):
    def __init__ (self):
        self.extHost    = "0.0.0.0"
        self.localhost  = "localhost"
        self.signalPort = "50050"
        self.dataPort   = "50100"

        self.context       = zmq.Context()

        self.fileOpSocket  = self.context.socket(zmq.REQ)
        connectionStr = "tcp://" + str(self.localhost) + ":" + str(self.signalPort)
        self.fileOpSocket.connect(connectionStr)
        logging.info("fileOpSocket started (connect) for '" + connectionStr + "'")

        self.dataSocket    = self.context.socket(zmq.PUSH)
        connectionStr = "tcp://" + str(self.localhost) + ":" + str(self.dataPort)
        self.dataSocket.connect(connectionStr)
        logging.info("dataSocket started (connect) for '" + connectionStr + "'")

        threading.Thread.__init__(self)


    def run (self):
        self.fileOpSocket.send("OPEN_FILE")

        recvMessage = self.fileOpSocket.recv()

        for i in range(5):
            metadata = {
                "sourcePath"  : BASE_PATH + os.sep +"data" + os.sep + "source",
                "relativePath": "local",
                "filename"    : str(i) + ".cbf"
                }
            metadata = json.dumps(metadata).encode("utf-8")

            data     = "THISISTESTDATA-" + str(i)

            dataMessage = [metadata, data]
            self.dataSocket.send_multipart(dataMessage)
            logging.debug("Send")

        message = "CLOSE_FILE"
        logging.debug("Send " + message)
        self.fileOpSocket.send(message)

        self.dataSocket.send_multipart([message, "0/1"])

        recvMessage = self.fileOpSocket.recv()
        logging.debug("Recv confirmation: " + recvMessage)


    def stop (self):
        try:
            if self.fileOpSocket:
                logging.info("Closing fileOpSocket...")
                self.fileOpSocket.close(linger=0)
                self.fileOpSocket = None
            if self.dataSocket:
                logging.info("Closing dataSocket...")
                self.dataSocket.close(linger=0)
                self.dataSocket = None
            if self.context:
                logging.info("Destroying context...")
                self.context.destroy()
                self.context = None
                logging.info("Destroying context...done")
        except:
            logging.error("Closing ZMQ Sockets...failed.", exc_info=True)


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


def openCallback (params, retrievedParams):
    print params, retrievedParams

def closeCallback (params, retrievedParams):
    print params, retrievedParams

def readCallback (params, retrievedParams):
    print params, retrievedParams


senderThread = Sender_as_thread()
senderThread.start()

obj = dataTransfer("nexus", useLog = True)
obj.start(["zitpcx19282", "50100"])

callbackParams = None

try:
    while True:
        try:
            data = obj.read(callbackParams, openCallback, readCallback, closeCallback)
            logging.debug("Retrieved: " + str(data))

            if data == "CLOSE_FILE":
                break
        except KeyboardInterrupt:
            break
        except:
            logging.error("break", exc_info=True)
            break
finally:
    senderThread.stop()
    obj.stop()

print
print "==== TEST END: nexus transfer ===="
print



