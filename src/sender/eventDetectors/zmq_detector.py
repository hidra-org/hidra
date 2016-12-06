from __future__ import print_function
from __future__ import unicode_literals

import os
import zmq
import logging
import json
import sys
import tempfile

try:
    # try to use the system module
    from logutils.queue import QueueHandler
except:
    # there is no module logutils installed, fallback on the one in shared

    try:
        BASE_PATH = os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.realpath(__file__)))))
    except:
        BASE_PATH = os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.realpath(sys.argv[0])))))
    SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

    if not SHARED_PATH in sys.path:
        sys.path.append(SHARED_PATH)
    del SHARED_PATH

    from logutils.queue import QueueHandler

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetector():

    def __init__(self, config, logQueue):

        self.log = self.get_logger(logQueue)

        # check format of config
        if ("context" not in config
                or "eventDetConStr" not in config
                or "numberOfStreams" not in config):
            self.log.error("Configuration of wrong format")
            self.log.debug("config={0}".format(config))
            checkPassed = False
        else:
            checkPassed = True

        if checkPassed:
            self.eventDetConStr = config["eventDetConStr"]
            self.eventSocket = None

            self.numberOfStreams = config["numberOfStreams"]

            # remember if the context was created outside this class or not
            if config["context"]:
                self.context = config["context"]
                self.extContext = True
            else:
                self.log.info("Registering ZMQ context")
                self.context = zmq.Context()
                self.extContext = False

            self.create_sockets()

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("zmq_detector")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def create_sockets(self):

        # Create zmq socket to get events
        try:
            self.eventSocket = self.context.socket(zmq.PULL)
            self.eventSocket.bind(self.eventDetConStr)
            self.log.info("Start eventSocket (bind): '{0}'"
                          .format(self.eventDetConStr))
        except:
            self.log.error("Failed to start eventSocket (bind): '{0}'"
                           .format(self.eventDetConStr), exc_info=True)
            raise

    def get_new_event(self):

        eventMessage = self.eventSocket.recv_multipart()

        if eventMessage[0] == b"CLOSE_FILE":
            eventMessageList = [eventMessage
                                for i in range(self.numberOfStreams)]
        else:
            eventMessageList = [json.loads(eventMessage[0].decode("utf-8"))]

        self.log.debug("eventMessage: {0}".format(eventMessageList))

        return eventMessageList

    def stop(self):
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

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    import sys
    import time
    from multiprocessing import Queue

    try:
        BASE_PATH = os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.realpath(__file__)))))
    except:
        BASE_PATH = os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.realpath(sys.argv[0])))))
    SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")
    print ("SHARED", SHARED_PATH)

    if not SHARED_PATH in sys.path:
        sys.path.append(SHARED_PATH)
    del SHARED_PATH

    import helpers

    logfile = os.path.join(BASE_PATH, "logs", "zmqDetector.log")
    logsize = 10485760

    logQueue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True,
                                      onScreenLogLevel="debug")

    # Start queue listener using the stream handler above
    logQueueListener = helpers.CustomQueueListener(logQueue, h1, h2)
    logQueueListener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(logQueue)
    root.addHandler(qh)

    eventDetConStr = "ipc://{ip}/{port}".format(
        ip=os.path.join(tempfile.gettempdir(), "hidra"),
        port="eventDet")
    print ("eventDetConStr", eventDetConStr)
    numberOfStreams = 1
    config = {
        "eventDetectorType": "ZmqDetector",
        "context": None,
        "eventDetConStr": eventDetConStr,
        "numberOfStreams": numberOfStreams,
        }

    eventDetector = EventDetector(config, logQueue)

    sourceFile = os.path.join(BASE_PATH, "test_file.cbf")
    targetFileBase = os.path.join(
        BASE_PATH, "data", "source", "local", "raw") + os.sep

    context = zmq.Context.instance()

    # create zmq socket to send events
    eventSocket = context.socket(zmq.PUSH)
    eventSocket.connect(eventDetConStr)
    logging.info("Start eventSocket (connect): '{0}'".format(eventDetConStr))

    i = 100
    while i <= 101:
        try:
            logging.debug("generate event")
            targetFile = "{0}{1}.cbf".format(targetFileBase, i)
            message = {
                "filename": targetFile,
                "filePart": 0,
                "chunkSize": 10
                }

            eventSocket.send_multipart([json.dumps(message).encode("utf-8")])
            i += 1

            eventList = eventDetector.get_new_event()
            if eventList:
                logging.debug("eventList: {0}".format(eventList))

            time.sleep(1)
        except KeyboardInterrupt:
            break

    eventSocket.send_multipart([b"CLOSE_FILE", "test_file.cbf"])

    eventList = eventDetector.get_new_event()
    logging.debug("eventList: {0}".format(eventList))

    logQueue.put_nowait(None)
    logQueueListener.stop()
