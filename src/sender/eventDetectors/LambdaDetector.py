__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import os
import zmq
import logging
import cPickle
from logutils.queue import QueueHandler


class LambdaDetector():

    def __init__(self, config, logQueue):

        self.log = self.getLogger(logQueue)

        # check format of config
        checkPassed = True
        if ( not config.has_key("context") or
                not config.has_key("eventPort") or
                not config.has_key("numberOfStreams") ):
            self.log.error ("Configuration of wrong format")
            self.log.debug ("config="+ str(config))
            checkPassed = False


        if checkPassed:
            self.eventPort       = config["eventPort"]
            self.extIp           = "0.0.0.0"
            self.eventSocket     = None

            self.numberOfStreams = config["numberOfStreams"]

            self.log.debug("Registering ZMQ context")
            # remember if the context was created outside this class or not
            if config["context"]:
                self.context    = config["context"]
                self.extContext = True
            else:
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
        logger = logging.getLogger("lambdaDetector")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def createSockets(self):
        # create zmq socket to get events
        self.eventSocket = self.context.socket(zmq.PULL)
        connectionStr  = "tcp://{ip}:{port}".format(ip=self.extIp, port=self.eventPort)
        try:
            self.eventSocket.bind(connectionStr)
            self.log.info("Start eventSocket (bind): '" + connectionStr + "'")
        except:
            self.log.error("Failed to start eventSocket (bind): '" + connectionStr + "'", exc_info=True)


    def getNewEvent(self):

        eventMessage = self.eventSocket.recv()

        if eventMessage == b"CLOSE_FILE":
            eventMessageList = [ eventMessage for i in range(self.numberOfStreams) ]
        else:
            eventMessageList = [ cPickle.loads(eventMessage) ]

        self.log.debug("eventMessage: " + str(eventMessageList))

        return eventMessageList



    def stop(self):
        #close ZMQ
        if self.eventSocket:
            self.eventSocket.close(0)
            self.eventSocket = None

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.externalContext and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy()
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)


    def __exit__(self):
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

    logfile  = BASE_PATH + os.sep + "logs" + os.sep + "lambdaDetector.log"
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


    eventPort       = "6001"
    numberOfStreams = 4
    config = {
            "eventDetectorType" : "lambda",
            "eventPort"         : eventPort,
            "numberOfStreams"   : numberOfStreams,
            "context"           : None,
            }


    eventDetector = LambdaDetector(config, logQueue)

    sourceFile = BASE_PATH + os.sep + "test_file.cbf"
    targetFileBase = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep


    context        = zmq.Context.instance()

    # create zmq socket to send events
    eventSocket    = context.socket(zmq.PUSH)
    connectionStr  = "tcp://localhost:{port}".format(port=eventPort)
    eventSocket.connect(connectionStr)
    logging.info("Start eventSocket (connect): '" + connectionStr + "'")


    i = 100
    while i <= 101:
        try:
            logging.debug("generate event")
            targetFile = targetFileBase + str(i) + ".cbf"
            message = {
                    "filename" : targetFile,
                    "filePart" : 0
                    }
            eventSocket.send(cPickle.dumps(message))
            i += 1

            eventList = eventDetector.getNewEvent()
            if eventList:
                print "eventList:", eventList

            time.sleep(1)
        except KeyboardInterrupt:
            break

    eventSocket.send(b"CLOSE_FILE")

    eventList = eventDetector.getNewEvent()
    print "eventList:", eventList

    logQueue.put_nowait(None)
    logQueueListener.stop()
