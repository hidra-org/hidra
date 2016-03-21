__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import socket
import zmq
import os
import logging
import sys
import trace
import cPickle
from logutils.queue import QueueHandler

try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
SHARED_PATH  = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

EVENTDETECTOR_PATH = BASE_PATH + os.sep + "src" + os.sep + "sender" + os.sep + "eventDetectors"
if not EVENTDETECTOR_PATH in sys.path:
    sys.path.append ( EVENTDETECTOR_PATH )
del EVENTDETECTOR_PATH

import helpers


#
#  --------------------------  class: TaskProvider  --------------------------------------
#

class TaskProvider():
    def __init__ (self, eventDetectorConfig, requestFwPort, routerPort, logQueue, context = None):
        global BASE_PATH

        #eventDetectorConfig = {
        #        eventDetectorType   : ... ,
        #        monDir       : ... ,
        #        monEventType : ... ,
        #        monSubdirs   : ... ,
        #        monSuffixes  : ... ,
        #}

        self.log                = self.getLogger(logQueue)
        self.log.debug("TaskProvider started (PID " + str(os.getpid()) + ").")

        self.dataDetectorModule = None
        self.eventDetector      = None

        self.config             = eventDetectorConfig
        self.log.debug("Configuration for event detector: " + str(self.config))

        eventDetectorModule     = self.config["eventDetectorType"]

        self.localhost          = "127.0.0.1"
        self.extIp              = "0.0.0.0"
        self.requestFwPort      = requestFwPort
        self.routerPort         = routerPort
        self.requestFwSocket    = None
        self.routerSocket       = None

        self.log.debug("Registering ZMQ context")
        # remember if the context was created outside this class or not
        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False


        self.log.info("Loading eventDetector: " + eventDetectorModule)
        self.eventDetectorModule = __import__(eventDetectorModule)

        self.eventDetector = self.eventDetectorModule.EventDetector(self.config, logQueue)

        self.createSockets()

        try:
            self.run()
        except KeyboardInterrupt:
            self.log.debug("Keyboard interruption detected. Shuting down")
            self.stop()
        except:
            self.log.info("Stopping TaskProvider due to unknown error condition.", exc_info=True)
            self.stop()


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("TaskProvider")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def createSockets (self):
        # socket to get requests
        self.requestFwSocket = self.context.socket(zmq.REQ)
        connectionStr  = "tcp://{ip}:{port}".format( ip=self.localhost, port=self.requestFwPort )
        try:
            self.requestFwSocket.connect(connectionStr)
            self.log.info("Start requestFwSocket (connect): '" + str(connectionStr) + "'")
        except:
            self.log.error("Failed to start requestFwSocket (connect): '" + connectionStr + "'", exc_info=True)

        # socket to disribute the events to the worker
        self.routerSocket = self.context.socket(zmq.PUSH)
        connectionStr  = "tcp://{ip}:{port}".format( ip=self.localhost, port=self.routerPort )
        try:
            self.routerSocket.bind(connectionStr)
            self.log.info("Start to router socket (bind): '" + str(connectionStr) + "'")
        except:
            self.log.error("Failed to start router Socket (bind): '" + connectionStr + "'", exc_info=True)


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
            except KeyboardInterrupt:
                break
            except:
                self.log.error("Invalid fileEvent message received.", exc_info=True)
                #skip all further instructions and continue with next iteration
                continue

            #TODO validate workload dict
            for workload in workloadList:
                # get requests for this event
                try:
                    self.log.debug("Get requests...")
                    self.requestFwSocket.send("")
                    requests = cPickle.loads(self.requestFwSocket.recv())
                    self.log.debug("Get requests... done.")
                    self.log.debug("Requests: " + str(requests))
                except:
                    self.log.error("Get Requests... failed.", exc_info=True)


                # build message dict
                try:
                    self.log.debug("Building message dict...")
                    messageDict = cPickle.dumps(workload)  #sets correct escape characters
                    self.log.debug("Building message dict...done.")
                except:
                    self.log.error("Unable to assemble message dict.", exc_info=True)
                    continue

                # send the file to the fileMover
                try:
                    self.log.debug("Sending message...")
                    message = [messageDict]
                    if requests != ["None"]:
                        message.append(cPickle.dumps(requests))
                    self.log.debug(str(message))
                    self.routerSocket.send_multipart(message)
                    self.log.debug("Sending message...done.")
                except:
                    self.log.error("Sending message...failed.", exc_info=True)



    def stop (self):
        if self.routerSocket:
            self.routerSocket.close(0)
            self.routerSocket = None
        if self.requestFwSocket:
            self.requestFwSocket.close(0)
            self.requestFwSocket = None
        if not self.extContext and self.context:
            self.context.destroy(0)
            self.context = None


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class requestResponder():
    def __init__ (self, requestFwPort, logQueue, context = None):
        # Send all logs to the main process
        self.log = self.getLogger(logQueue)

        self.context         = context or zmq.Context.instance()
        self.requestFwSocket = self.context.socket(zmq.REP)
        connectionStr   = "tcp://127.0.0.1:" + requestFwPort
        self.requestFwSocket.bind(connectionStr)
        self.log.info("[requestResponder] requestFwSocket started (bind) for '" + connectionStr + "'")

        self.run()

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("requestResponder")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def run (self):
        hostname = socket.gethostname()
        self.log.info("[requestResponder] Start run")
        openRequests = [[hostname + ':6003', 1], [hostname + ':6004', 0]]
        while True:
            request = self.requestFwSocket.recv()
            self.log.debug("[requestResponder] Received request: " + str(request) )

            self.requestFwSocket.send(cPickle.dumps(openRequests))
            self.log.debug("[requestResponder] Answer: " + str(openRequests) )


    def __exit__(self):
        self.requestFwSocket.close(0)
        self.context.destroy()



if __name__ == '__main__':
    from multiprocessing import Process, freeze_support, Queue
    import time
    from shutil import copyfile
    from subprocess import call

    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows

    logfile = BASE_PATH + os.sep + "logs" + os.sep + "taskProvider.log"
    logsize = 10485760

#    eventDetectorConfig = {
#            "eventDetectorType"   : "inotifyx",
#            "monDir"              : BASE_PATH + os.sep + "data" + os.sep + "source",
#            "monEventType"        : "IN_CLOSE_WRITE",
#            "monSubdirs"          : ["commissioning", "current", "local"],
#            "monSuffixes"         : [".tif", ".cbf"]
#            }

    eventDetectorConfig = {
            "eventDetectorType" : "watchdog",
            "monDir"            : BASE_PATH + os.sep + "data" + os.sep + "source",
            "monEventType"      : "IN_CLOSE_WRITE",
            "monSubdirs"        : ["commissioning", "current", "local"],
            "monSuffixes"       : [".tif", ".cbf"],
            "timeTillClosed"    : 1 #s
            }

    requestFwPort = "6001"
    routerPort    = "7000"

    logQueue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose=True, onScreenLogLevel="debug")

    # Start queue listener using the stream handler above
    logQueueListener    = helpers.CustomQueueListener(logQueue, h1, h2)
    logQueueListener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG) # Log level = DEBUG
    qh = QueueHandler(logQueue)
    root.addHandler(qh)


    taskProviderPr = Process ( target = TaskProvider, args = (eventDetectorConfig, requestFwPort, routerPort, logQueue) )
    taskProviderPr.start()

    requestResponderPr = Process ( target = requestResponder, args = ( requestFwPort, logQueue) )
    requestResponderPr.start()

    context       = zmq.Context.instance()

    routerSocket  = context.socket(zmq.PULL)
    connectionStr = "tcp://localhost:" + routerPort
    routerSocket.connect(connectionStr)
    logging.info("=== routerSocket connected to " + connectionStr)

    sourceFile = BASE_PATH + os.sep + "test_file.cbf"
    targetFileBase = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep

    i = 100
    try:
        while i <= 105:
            time.sleep(0.5)
            targetFile = targetFileBase + str(i) + ".cbf"
            logging.debug("copy to " + targetFile)
            copyfile(sourceFile, targetFile)
#            call(["cp", sourceFile, targetFile])
            i += 1

            workload = routerSocket.recv_multipart()
            logging.info("=== next workload " + str(workload))
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:

        requestResponderPr.terminate()
        taskProviderPr.terminate()

        routerSocket.close(0)
        context.destroy()

        for number in range(100, i):
            targetFile = targetFileBase + str(number) + ".cbf"
            logging.debug("remove " + targetFile)
            os.remove(targetFile)

        logQueue.put_nowait(None)
        logQueueListener.stop()

