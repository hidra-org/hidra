import os
import sys
import time
import zmq
import logging
import threading
import json
#import cPickle

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


class Sender ():
    def __init__ (self):
        self.extHost    = "0.0.0.0"
        self.localhost  = "zitpcx19282"
#        self.localhost  = "localhost"
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


    def run (self):

        sourceFile = BASE_PATH + os.sep + "test_file.cbf"
        chunkSize = 10485760 # 1024*1024*10 = 10MB
        filepart = 0
        timeout = 2

        message = "OPEN_FILE"
        logging.debug("Send " + message)
        self.fileOpSocket.send(message)

        recvMessage = self.fileOpSocket.recv()
        logging.debug("Recv confirmation: " + recvMessage)

        metadata = {
            "sourcePath"  : BASE_PATH + os.sep +"data" + os.sep + "source",
            "relativePath": "local",
            "filename"    : "test.cbf",
            "filePart"    : filepart,
            "chunkSize"   : chunkSize
        }

        # Checking if receiver is alive
#        self.dataSocket.send_multipart([b"ALIVE_TEST"])
        tracker = self.dataSocket.send_multipart([b"ALIVE_TEST"], copy=False, track=True)
        if not tracker.done:
            tracker.wait(timeout)
        logging.debug("tracker.done = {t}".format(t=tracker.done))
        if not tracker.done:
            logging.error("Failed to send ALIVE_TEST", exc_info=True)
        else:
            logging.info("Sending ALIVE_TEST...success")


        # Open file
        source_fp = open(sourceFile, "rb")
        logging.debug("Opened file: {s}".format(s=sourceFile))

        while True:
            # Read file content
            content = source_fp.read(chunkSize)
            logging.debug("Read file content")

            if not content:
                logging.debug("break")
                break

            # Build message
            metadata["filePart"] = filepart

            payload = []
            payload.append(json.dumps(metadata))
            payload.append(content)

            # Send message over ZMQ
            #self.dataSocket.send_multipart(payload)

            tracker = self.dataSocket.send_multipart(payload, copy=False, track=True)
            if not tracker.done:
                    logging.debug("Message part from file " + str(sourceFile) +
                             " has not been sent yet, waiting...")
                    tracker.wait(timeout)
                    logging.debug("Message part from file " + str(sourceFile) +
                             " has not been sent yet, waiting...done")


            logging.debug("Send")

            filepart += 1


        message = "CLOSE_FILE"
        logging.debug("Send " + message)
        self.fileOpSocket.send(message)

        self.dataSocket.send_multipart([message, "0/1"])

        recvMessage = self.fileOpSocket.recv()
        logging.debug("Recv confirmation: " + recvMessage)

        # Close file
        source_fp.close()
        logging.debug("Closed file: {f}".format(f=sourceFile))


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

if __name__ == '__main__':
    s = Sender()
    s.run()
    s.stop()

