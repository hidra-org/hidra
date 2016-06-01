__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import os
import zmq
import logging
import cPickle
import json
import sys


try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( sys.argv[0] ) ))))
SHARED_PATH  = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH


from logutils.queue import QueueHandler


#class ZmqDetector():
class EventDetector():

    def __init__ (self, config, logQueue):

        self.log = self.getLogger(logQueue)

        # check format of config
        if ( not config.has_key("context") or
                not config.has_key("eventDetConStr") or
                not config.has_key("numberOfStreams") ):
            self.log.error ("Configuration of wrong format")
            self.log.debug ("config="+ str(config))
            checkPassed = False
        else:
            checkPassed = True


        if checkPassed:
            self.eventDetConStr  = config["eventDetConStr"]
            self.eventSocket     = None

            self.numberOfStreams = config["numberOfStreams"]

            # remember if the context was created outside this class or not
            if config["context"]:
                self.context    = config["context"]
                self.extContext = True
            else:
                self.log.info("Registering ZMQ context")
                self.context    = zmq.Context()
                self.extContext = False

            self.createSockets()


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("ZmqDetector")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def createSockets (self):

        # Create zmq socket to get events
        try:
            self.eventSocket = self.context.socket(zmq.PULL)
            self.eventSocket.bind(self.eventDetConStr)
            self.log.info("Start eventSocket (bind): '" + self.eventDetConStr + "'")
        except:
            self.log.error("Failed to start eventSocket (bind): '" + self.eventDetConStr + "'", exc_info=True)
            raise


    def getNewEvent (self):

        eventMessage = self.eventSocket.recv()

        if eventMessage == b"CLOSE_FILE":
            eventMessageList = [ eventMessage for i in range(self.numberOfStreams) ]
        else:

            eventMessage = json.loads(eventMessage)
#            eventMessage = cPickle.loads(eventMessage)

            # Convert back to ascii
            eventMessage["filename"] = eventMessage["filename"].encode('ascii')

            eventMessageList = [ eventMessage ]

        self.log.debug("eventMessage: " + str(eventMessageList))

        return eventMessageList



    def stop (self):
        #close ZMQ
        if self.eventSocket:
            self.eventSocket.close(0)
            self.eventSocket = None

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.extContext and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


if __name__ == '__main__':
    import sys
    import time
    from subprocess import call
    from multiprocessing import Queue

    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))))
    SHARED_PATH  = BASE_PATH + os.sep + "src" + os.sep + "shared"
    print "SHARED", SHARED_PATH

    if not SHARED_PATH in sys.path:
        sys.path.append ( SHARED_PATH )
    del SHARED_PATH

    import helpers

    logfile  = BASE_PATH + os.sep + "logs" + os.sep + "zmqDetector.log"
    logsize  = 10485760

    logQueue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose=True, onScreenLogLevel="debug")

    # Start queue listener using the stream handler above
    logQueueListener = helpers.CustomQueueListener(logQueue, h1, h2)
    logQueueListener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG) # Log level = DEBUG
    qh = QueueHandler(logQueue)
    root.addHandler(qh)


    eventDetConStr  = "ipc://{ip}/{port}".format(ip="/tmp/zeromq-data-transfer", port="eventDetConId")
    print "eventDetConStr", eventDetConStr
    numberOfStreams = 1
    config = {
            "eventDetectorType" : "ZmqDetector",
            "context"           : None,
            "eventDetConStr"    : eventDetConStr,
            "numberOfStreams"   : numberOfStreams,
            }


#    eventDetector = ZmqDetector(config, logQueue)
    eventDetector = EventDetector(config, logQueue)

    sourceFile = BASE_PATH + os.sep + "test_file.cbf"
    targetFileBase = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep


    context        = zmq.Context.instance()

    # create zmq socket to send events
    eventSocket    = context.socket(zmq.PUSH)
    eventSocket.connect(eventDetConStr)
    logging.info("Start eventSocket (connect): '" + eventDetConStr + "'")


    i = 100
    while i <= 101:
        try:
            logging.debug("generate event")
            targetFile = targetFileBase + str(i) + ".cbf"
#            message = {
#                    "filename" : targetFile,
#                    "filepart" : 0
#                    }
            message = '{ "filePart": 0, "filename": "' + targetFile + '" }'
#            eventSocket.send(cPickle.dumps(message))
            eventSocket.send(message)
            i += 1

            eventList = eventDetector.getNewEvent()
            if eventList:
                logging.debug("eventList: " + str(eventList))

            time.sleep(1)
        except KeyboardInterrupt:
            break

    eventSocket.send(b"CLOSE_FILE")

    eventList = eventDetector.getNewEvent()
    logging.debug("eventList: " + str(eventList))

    logQueue.put_nowait(None)
    logQueueListener.stop()
