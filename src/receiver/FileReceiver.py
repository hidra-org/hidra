__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import zmq
import sys
import json
import logging
import errno
import os
import traceback

BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH
del BASE_PATH

from dataTransferAPI import dataTransfer

#
#  --------------------------  class: FileReceiver  --------------------------------------
#
class FileReceiver:
    context          = None
    outputDir        = None
    dataStreamIp     = None
    dataStreamPort   = None
    log              = None

    def __init__(self, outputDir, dataStreamIp, dataStreamPort, context = None):

        self.outputDir          = os.path.normpath(outputDir)
        self.dataStreamIp       = dataStreamIp
        self.dataStreamPort     = dataStreamPort

        self.context = context or zmq.Context()

        self.log = self.getLogger()
        self.log.debug("Init")

        self.dataTransferObject = dataTransfer("", dataStreamPort, dataIp = dataStreamIp, useLog = True, context = self.context)

        try:
            self.log.info("Start receiving new files")
            self.startReceiving()
            self.log.info("Stopped receiving.")
        except Exception, e:
            trace = traceback.format_exc()
            self.log.info("Unkown error state. Shutting down...")
            self.log.debug("Error was: " + str(e))
            self.log.debug("Trace was: " + str(trace))
        finally:
            self.log.info("Quitting.")
            self.stop()



    def getLogger(self):
        logger = logging.getLogger("fileReceiver")
        return logger


    def startReceiving(self):

        try:
            self.dataTransferObject.start("priorityStream")

            continueReceiving = True #receiving will stop if value gets False
            self.log.debug("Waiting for new messages...")
        except Exception as e:
            self.log.error("could not initiate stream")
            self.log.debug("Error was: " + str(e))
            continueReceiving = False


        #run loop, and wait for incoming messages
        while continueReceiving:
            try:
                self.dataTransferObject.storeFile(self.outputDir)
            except KeyboardInterrupt:
                self.log.debug("Keyboard interrupt detected. Stop receiving.")
                continueReceiving = False
                break
            except:
                self.log.error("receive message...failed.")
                self.log.error(sys.exc_info())
                continueReceiving = False
                break

        self.log.info("shutting down receiver...")
        self.stop()


    def stop(self):
        self.dataTransferObject.stop()

