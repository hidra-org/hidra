__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import zmq
import os
import sys
import logging
import traceback
import cPickle
import shutil
from logutils.queue import QueueHandler

try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

DATAFETCHER_PATH = BASE_PATH + os.sep + "src" + os.sep + "sender" + os.sep + "dataFetchers"
if not DATAFETCHER_PATH in sys.path:
    sys.path.append ( DATAFETCHER_PATH )
del DATAFETCHER_PATH

import helpers

#
#  --------------------------  class: DataDispatcher  --------------------------------------
#
class DataDispatcher():

    def __init__(self, id, routerPort, chunkSize, fixedStreamId, logQueue, localTarget = None, context = None):

        supportedDataFetchers = ["getFromFile"]
        dataFetcherProp = {
                "type"       : "getFromFile",
                "removeFlag" : False
                }

#        dataFetcherProp = {
#                "type"       : "getFromQueue",
#                "context"    : context,
#                "extIp"      : "0.0.0.0",
#                "port"       : "6050"
#                }


        self.id              = id
        self.log             = self.getLogger(logQueue)

        self.log.debug("DataDispatcher-" + str(self.id) + " started (PID " + str(os.getpid()) + ").")

        self.localhost       = "127.0.0.1"
        self.extIp           = "0.0.0.0"
        self.routerPort      = routerPort
        self.chunkSize       = chunkSize

        self.routerSocket    = None

        self.fixedStreamId   = fixedStreamId
        self.localTarget     = localTarget

        self.dataFetcherProp = dataFetcherProp
        dataFetcher          = self.dataFetcherProp["type"]

        # dict with informations of all open sockets to which a data stream is opened (host, port,...)
        self.openConnections = dict()

        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False
            if self.dataFetcherProp.has_key("context") and not self.dataFetcherProp["context"]:
                self.dataFetcherProp["context"]    = self.context


        self.createSockets()


        if dataFetcher in supportedDataFetchers:
            self.log.info("Loading data Fetcher: " + dataFetcher)
            self.dataFetcher = __import__(dataFetcher)
            self.dataFetcher.setup(dataFetcherProp)
        else:
            raise Exception("DataFetcher type " + dataFetcher + " not supported")

        try:
            self.process()
        except KeyboardInterrupt:
            self.log.debug("KeyboardInterrupt detected. Shutting down DataDispatcher-" + str(self.id) + ".")
            self.stop()
        except:
            self.log.error("Stopping DataDispatcher-" + str(self.id) + " due to unknown error condition.", exc_info=True)
            self.stop()


    def createSockets(self):
        self.routerSocket = self.context.socket(zmq.PULL)
        connectionStr  = "tcp://{ip}:{port}".format( ip=self.localhost, port=self.routerPort )
        self.routerSocket.connect(connectionStr)
        self.log.info("Start routerSocket (connect): '" + str(connectionStr) + "'")


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("DataDispatcher-" + str(self.id))
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def process(self):

        while True:
            # Get workload from router, until finished
            self.log.debug("DataDispatcher-" + str(self.id) + ": waiting for new job")
            message = self.routerSocket.recv_multipart()
            self.log.debug("DataDispatcher-" + str(self.id) + ": new job received")
            self.log.debug("message = " + str(message))


            if len(message) >= 2:
                workload = cPickle.loads(message[0])
                targets  = cPickle.loads(message[1])
                if self.fixedStreamId:
                    targets.insert(0,[self.fixedStreamId, 0])
                # sort the target list by the priority
                targets = sorted(targets, key=lambda target: target[1])

            elif message[0] == b"CLOSE_FILE":
                self.log.debug("Router requested to send signal that file was closed.")
                payload = [ metadata, self.id ]

                # socket already known
                if self.fixedStreamId in openConnections:
                    tracker = openConnections[self.fixedStreamId].send_multipart(payload, copy=False, track=True)
                    log.info("Sending close file signal to '" + self.fixedStreamId + "' with priority 0")
                else:
                    # open socket
                    socket        = context.socket(zmq.PUSH)
                    connectionStr = "tcp://" + str(self.fixedStreamId)

                    socket.connect(connectionStr)
                    log.info("Start socket (connect): '" + str(connectionStr) + "'")

                    # register socket
                    openConnections[self.fixedStreamId] = socket

                    # send data
                    tracker = openConnections[self.fixedStreamId].send_multipart(payload, copy=False, track=True)
                    log.info("Sending close file signal to '" + self.fixedStreamId + "' with priority 0" )

                # socket not known
                if not tracker.done:
                    log.info("Close file signal has not been sent yet, waiting...")
                    tracker.wait()
                    log.info("Close file signal has not been sent yet, waiting...done")

                time.sleep(2)
                self.log.debug("Continue after sleeping.")
                continue

            elif message[0] == b"EXIT":
                self.log.debug("Router requested to shutdown DataDispatcher-"+ str(self.id) + ".")
                break

            else:
                workload = cPickle.loads(message[0])
                if self.fixedStreamId:
                    targets = [[self.fixedStreamId, 0]]
                else:
                    targets = None

            # get metadata of the file
            try:
                self.log.debug("Getting file metadata")
                sourceFile, targetFile, metadata = self.dataFetcher.getMetadata(self.log, workload, self.chunkSize, self.localTarget)

            except:
                self.log.error("Building of metadata dictionary failed for workload: " + str(workload) + ".", exc_info=True)
                #skip all further instructions and continue with next iteration
                continue

            # send data
            try:
                self.dataFetcher.sendData(self.log, targets, sourceFile, metadata, self.openConnections, self.context, self.dataFetcherProp)
            except:
                self.log.error("DataDispatcher-"+str(self.id) + ": Passing new file to data stream...failed.", exc_info=True)

            # finish data handling
            self.dataFetcher.finishDataHandling(self.log, sourceFile, targetFile, self.dataFetcherProp)


    def stop(self):
        self.log.debug("Closing sockets for DataDispatcher-" + str(self.id))
        for connection in self.openConnections:
            if self.openConnections[connection]:
                self.openConnections[connection].close(0)
                self.openConnections[connection] = None
        if self.routerSocket:
            self.routerSocket.close(0)
            self.routerSocket = None
        self.dataFetcher.clean(self.dataFetcherProp)
        if not self.extContext and self.context:
            self.log.debug("Destroying context")
            self.context.destroy()
            self.context = None


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


if __name__ == '__main__':
    from multiprocessing import Process, freeze_support, Queue
    import time
    from shutil import copyfile

    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows

    logfile  = BASE_PATH + os.sep + "logs" + os.sep + "dataDispatcher.log"
    logsize  = 10485760

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


    sourceFile = BASE_PATH + os.sep + "test_file.cbf"
    targetFile = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep + "100.cbf"

    copyfile(sourceFile, targetFile)
    time.sleep(0.5)


    routerPort    = "7000"
    receivingPort = "6005"
    receivingPort2 = "6006"
    chunkSize     = 10485760 ; # = 1024*1024*10 = 10 MiB

    localTarget   = BASE_PATH + os.sep + "data" + os.sep + "target"
    fixedStreamId = False
    fixedStreamId = "localhost:6006"

    logConfig = "test"

    dataDispatcherPr = Process ( target = DataDispatcher, args = ( 1, routerPort, chunkSize, fixedStreamId, logQueue, localTarget) )
    dataDispatcherPr.start()

    context       = zmq.Context.instance()

    routerSocket  = context.socket(zmq.PUSH)
    connectionStr = "tcp://127.0.0.1:" + routerPort
    routerSocket.bind(connectionStr)
    logging.info("=== routerSocket connected to " + connectionStr)

    receivingSocket = context.socket(zmq.PULL)
    connectionStr   = "tcp://0.0.0.0:" + receivingPort
    receivingSocket.bind(connectionStr)
    logging.info("=== receivingSocket connected to " + connectionStr)

    receivingSocket2 = context.socket(zmq.PULL)
    connectionStr   = "tcp://0.0.0.0:" + receivingPort2
    receivingSocket2.bind(connectionStr)
    logging.info("=== receivingSocket2 connected to " + connectionStr)


    metadata = {
            "sourcePath"  : BASE_PATH + os.sep +"data" + os.sep + "source",
            "relativePath": os.sep + "local" + os.sep + "raw",
            "filename"    : "100.cbf"
            }
    targets = [['localhost:6005', 1], ['localhost:6006', 0]]

    message = [ cPickle.dumps(metadata), cPickle.dumps(targets) ]
#    message = [ cPickle.dumps(metadata)]

    time.sleep(1)

    workload = routerSocket.send_multipart(message)
    logging.info("=== send message")

    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: " + str(cPickle.loads(recv_message[0])))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: " + str(cPickle.loads(recv_message[0])))
    except KeyboardInterrupt:
        pass
    finally:
        dataDispatcherPr.terminate()

        routerSocket.close(0)
        receivingSocket.close(0)
        receivingSocket2.close(0)
        context.destroy()

        logQueue.put_nowait(None)
        logQueueListener.stop()

