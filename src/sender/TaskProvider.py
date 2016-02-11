__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import argparse
import zmq
import os
import logging
import sys
import json
import trace


#
#  --------------------------  class: TaskProvider  --------------------------------------
#

class TaskProvider():
    def __init__ (self, eventDetectorConfig, requestFwPort, distrPort, context = None):
        #eventDetectorConfig = {
        #        configType   : ... ,
        #        monDir       : ... ,
        #        monEventType : ... ,
        #        monSubdirs   : ... ,
        #        monSuffixes  : ... ,
        #}

        self.log               = self.getLogger()
        self.log.debug("TaskProvider: __init__()")

        self.eventDetector     = None

        self.config = eventDetectorConfig

        self.localhost         = "127.0.0.1"
        self.extIp             = "0.0.0.0"
        self.requestFwPort     = requestFwPort
        self.distrPort         = distrPort
        self.requestFwSocket   = None
        self.distrSocket       = None

        self.log.debug("Registering ZMQ context")
        # remember if the context was created outside this class or not
        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False


        if self.config.has_key("configType") and self.config["configType"] == "inotifyx":

            from InotifyxDetector import InotifyxDetector as EventDetector

            # check format of config
            if ( not self.config.has_key("monDir") or
                    not self.config.has_key("monEventType") or
                    not self.config.has_key("monSubdirs") or
                    not self.config.has_key("monSuffixes") ):
                self.log.error ("Configuration of wrong format")

            #TODO forward self.config instead of seperate variables
            self.eventDetector     = EventDetector(self.config)
        else:
            self.log.error("Type of event detector is not supported: " + str( self.config["configType"] ))
            return -1

        self.createSockets()

        try:
            self.run()
        except KeyboardInterrupt:
            self.log.debug("Keyboard interruption detected. Shuting down")
        except:
            trace = traceback.format_exc()
            self.log.info("Stopping TaskProvider due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))


    def getLogger (self):
        logger = logging.getLogger("TaskProvider")
        return logger


    def createSockets (self):
        # socket to get requests
        self.requestFwSocket = self.context.socket(zmq.REQ)
        connectionStr  = "tcp://{ip}:{port}".format( ip=self.localhost, port=self.requestFwPort )
        try:
            self.requestFwSocket.connect(connectionStr)
            self.log.debug("Connecting to requestFwSocket (connect): " + str(connectionStr))
        except Exception as e:
            self.log.error("Failed to start requestFwSocket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # socket to disribute the events to the worker
        self.distrSocket = self.context.socket(zmq.PUSH)
        connectionStr  = "tcp://{ip}:{port}".format( ip=self.localhost, port=self.requestFwPort )
        try:
            self.distrSocket.bind(connectionStr)
            self.log.debug("Connecting to distributing socket (bind): " + str(connectionStr))
        except Exception as e:
            self.log.error("Failed to start distributing Socket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))


    def run (self):
        while True:
            try:
                # the event for a file /tmp/test/source/local/file1.tif is of the form:
                # {
                #   "sourcePath" : "/tmp/test/source/"
                #   "relativePath": "local"
                #   "filename"   : "file1.tif"
                # }
                workloadList = self.eventDetector.getNewEvent()
            except Exception, e:
                self.log.error("Invalid fileEvent message received.")
                self.log.debug("Error was: " + str(e))
                #skip all further instructions and continue with next iteration
                continue

            #TODO validate workload dict
            for workload in workloadList:
                # get requests for this event
                try:
                    self.log.debug("Get requests...")
                    self.requestFwSocket.send("")
                    requests = self.requestFwSocket.recv_multipart()
                    self.log.debug("Get requests... done.")
                    self.log.debug("Requests: " + str(requests))
                except:
                    self.log.error("Get Requests... failed.")
                    trace = traceback.format_exc()
                    self.log.debug("Error was: " + str(trace))


                # build message dict
                try:
                    self.log.debug("Building message dict...")
                    messageDict = json.dumps(workload)  #sets correct escape characters
                    self.log.debug("Building message dict...done.")
                except Exception, e:
                    self.log.error("Unable to assemble message dict.")
                    self.log.debug("Error was: " + stri(e))
                    continue

                # send the file to the fileMover
                try:
                    self.log.debug("Sending message...")
                    self.log.debug(str(messageDict))
                    self.distrSocket.send_multipart([messageDict, requests])
                    self.log.debug("Sending message...done.")
                except Exception, e:
                    self.log.error("Sending message...failed.")
                    self.log.debug("Error was: " + str(e))



    def stop(self):
        if self.distrSocket:
            self.distrSocket.close(0)
            self.distrSocket = None
        if self.requestFwSocket:
            self.requestFwSocket.close(0)
            self.requestFwSocket = None
        if not self.extContext and self.context:
            self.context.destroy()
            self.context = None


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()



if __name__ == '__main__':
    from multiprocessing import Process

    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
    SRC_PATH  = BASE_PATH + os.sep + "src"

    sys.path.append ( SRC_PATH )

    import shared.helperScript as helperScript

    #enable logging
    helperScript.initLogging("/space/projects/live-viewer/logs/signalHandler.log", verbose=True, onScreenLogLevel="debug")

    eventDetectorConfig = {
            "configType"   : "inotifyx",
            "monDir"       : "/space/projects/live-viewer/data/src",
            "monEventType" : "IN_CLOSE_WRITE",
            "monSubdirs"   : ["commissioning", "current", "local"],
            "monSuffixes"  : [".tif", ".cbf"]
            }

    requestFwPort = "6001"
    distrPort     = "7000"

    taskProviderPr = Process ( target = TaskProvider, args = (eventDetectorConfig, requestFwPort, distrPort) )
    taskProviderPr.start()

    taskProviderPr.join()
