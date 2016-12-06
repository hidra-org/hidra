from __future__ import unicode_literals

import zmq
import os
import sys
import time
import logging
import json
import signal
from multiprocessing import Process

try:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.realpath(__file__))))
except:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.abspath(sys.argv[0]))))
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)
del SHARED_PATH

DATAFETCHER_PATH = os.path.join(BASE_PATH, "src", "sender", "dataFetchers")
if DATAFETCHER_PATH not in sys.path:
    sys.path.append(DATAFETCHER_PATH)
del DATAFETCHER_PATH

from logutils.queue import QueueHandler
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


#
# --------------------------  class: DataDispatcher  --------------------------
#
class DataDispatcher():

    def __init__(self, id, controlConId, routerConId, chunkSize,
                 fixedStreamId, dataFetcherProp, logQueue,
                 localTarget=None, context=None):

        self.id = id
        self.log = self.__get_logger(logQueue)

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.currentPID = os.getpid()
        self.log.debug("DataDispatcher-{0} started (PID {1}).".format(
            self.id, self.currentPID))

        self.controlConId = controlConId
        self.routerConId = routerConId

        self.controlSocket = None
        self.routerSocket = None

        self.poller = None

        self.chunkSize = chunkSize

        self.fixedStreamId = fixedStreamId
        self.localTarget = localTarget

        self.dataFetcherProp = dataFetcherProp
        self.log.info("Configuration for dataFetcher: {0}".format(
            self.dataFetcherProp))

        dataFetcher = self.dataFetcherProp["type"]

        # dict with information of all open sockets to which a data stream is
        # opened (host, port,...)
        self.openConnections = dict()

        if context:
            self.context = context
            self.extContext = True
        else:
            self.context = zmq.Context()
            self.extContext = False
            if ("context" in self.dataFetcherProp
                    and not self.dataFetcherProp["context"]):
                self.dataFetcherProp["context"] = self.context

        self.log.info("Loading dataFetcher: {0}".format(dataFetcher))
        self.dataFetcher = __import__(dataFetcher)

        self.continueRun = True

        if (self.dataFetcher.setup(self.log, dataFetcherProp)):
            try:
                self.__create_sockets()

                self.run()
            except zmq.ZMQError:
                pass
            except KeyboardInterrupt:
                pass
            except:
                self.log.error("Stopping DataDispatcher-{0} due to unknown \
                               error condition.".format(self.id),
                               exc_info=True)
            finally:
                self.stop()

    def __create_sockets(self):

        # socket for control signals
        try:
            self.controlSocket = self.context.socket(zmq.SUB)
            self.controlSocket.connect(self.controlConId)
            self.log.info("Start controlSocket (connect): '{0}'".format(
                self.controlConId))
        except:
            self.log.error("Failed to start controlSocket (connect): \
                '{0}'".format(self.controlConId), exc_info=True)
            raise

        self.controlSocket.setsockopt_string(zmq.SUBSCRIBE, "control")
        self.controlSocket.setsockopt_string(zmq.SUBSCRIBE, "signal")

        # socket to get new workloads from
        try:
            self.routerSocket = self.context.socket(zmq.PULL)
            self.routerSocket.connect(self.routerConId)
            self.log.info("Start routerSocket (connect): '{0}'".format(
                self.routerConId))
        except:
            self.log.error("Failed to start routerSocket (connect): \
                '{0}'".format(self.routerConId), exc_info=True)
            raise

        self.poller = zmq.Poller()
        self.poller.register(self.controlSocket, zmq.POLLIN)
        self.poller.register(self.routerSocket, zmq.POLLIN)

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def __get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("DataDispatcher-{0}".format(self.id))
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def run(self):

        fixedStreamId = [self.fixedStreamId, 0, [""], "data"]

        while self.continueRun:
            self.log.debug("DataDispatcher-{0}: waiting for new job".format(
                self.id))
            socks = dict(self.poller.poll())

            ######################################
            #     messages from TaskProvider     #
            ######################################
            if (self.routerSocket in socks
                    and socks[self.routerSocket] == zmq.POLLIN):

                try:
                    message = self.routerSocket.recv_multipart()
                    self.log.debug("DataDispatcher-{0}: new job received\
                            ".format(self.id))
                    self.log.debug("message = {0}".format(message))
                except:
                    self.log.error("DataDispatcher-{0}: waiting for new job\
                            ...failed".format(self.id), exc_info=True)
                    continue

                if len(message) >= 2:

                    workload = json.loads(message[0].decode("utf-8"))
                    targets = json.loads(message[1].decode("utf-8"))

                    if self.fixedStreamId:
                        targets.insert(0, fixedStreamId)
                        self.log.debug("Added fixedStreamId {0} to targets \
                            {1}".format(fixedStreamId, targets))

                    # sort the target list by the priority
                    targets = sorted(targets, key=lambda target: target[1])

                else:
                    workload = json.loads(message[0].decode("utf-8"))

                    if type(workload) == list and workload[0] == b"CLOSE_FILE":

                        # woraround for error
                        # "TypeError: Frame 0 (u'CLOSE_FILE') does not support
                        # the buffer interface."
                        workload[0] = b"CLOSE_FILE"
                        for i in range(1, len(workload)):
                            workload[i] = json.dumps(workload[i]).encode(
                                "utf-8")

                        if self.fixedStreamId:
                            self.log.debug("Router requested to send signal \
                                           that file was closed.")
                            workload.append(self.id)

                            # socket already known
                            if self.fixedStreamId in self.openConnections:
                                tracker = (
                                    self.openConnections[self.fixedStreamId]
                                    .send_multipart(workload,
                                                    copy=False,
                                                    track=True)
                                    )
                                self.log.info("Sending close file signal to \
                                              '{0}' with priority 0". format(
                                              self.fixedStreamId))
                            else:
                                # open socket
                                socket = self.context.socket(zmq.PUSH)
                                connectionStr = "tcp://{0}".format(
                                    self.fixedStreamId)

                                socket.connect(connectionStr)
                                self.log.info("Start socket (connect): \
                                    '{0}'".format(connectionStr))

                                # register socket
                                self.openConnections[self.fixedStreamId] = (
                                    socket
                                    )

                                # send data
                                tracker = (
                                    self.openConnections[self.fixedStreamId]
                                    .send_multipart(workload,
                                                    copy=False,
                                                    track=True)
                                    )
                                self.log.info("Sending close file signal to \
                                              '{0}' with priority 0".format(
                                              fixedStreamId))

                            # socket not known
                            if not tracker.done:
                                self.log.info("Close file signal has not been \
                                    sent yet, waiting...")
                                tracker.wait()
                                self.log.info("Close file signal has not been \
                                    sent yet, waiting...done")

                            time.sleep(2)
                            self.log.debug("Continue after sleeping")
                            continue
                        else:
                            self.log.warning("Router requested to send signal \
                                that file was closed, but no target specified")
                            continue

                    elif self.fixedStreamId:
                        targets = [fixedStreamId]
                        self.log.debug("Added fixedStreamId to targets \
                            {0}.".format(targets))

                    else:
                        targets = []

                # get metadata and paths of the file
                try:
                    self.log.debug("Getting file paths and metadata")
                    sourceFile, targetFile, metadata = (
                        self.dataFetcher.get_metadata(
                            self.log, self.dataFetcherProp, targets, workload,
                            self.chunkSize, self.localTarget)
                        )

                except:
                    self.log.error("Building of metadata dictionary failed \
                        for workload: {0}".format(workload), exc_info=True)
                    # skip all further instructions and
                    # continue with next iteration
                    continue

                # send data
                try:
                    self.dataFetcher.send_data(self.log, targets, sourceFile,
                                               targetFile, metadata,
                                               self.openConnections,
                                               self.context,
                                               self.dataFetcherProp)
                except:
                    self.log.error("DataDispatcher-{0}: Passing new file to \
                                   data stream...failed".format(self.id),
                                   exc_info=True)

                # finish data handling
                self.dataFetcher.finish_data_handling(self.log, targets,
                                                      sourceFile, targetFile,
                                                      metadata,
                                                      self.openConnections,
                                                      self.context,
                                                      self.dataFetcherProp)

            ######################################
            #         control commands           #
            ######################################
            if (self.controlSocket in socks
                    and socks[self.controlSocket] == zmq.POLLIN):

                try:
                    message = self.controlSocket.recv_multipart()
                    self.log.debug("DataDispatcher-{0}: control signal \
                        received".format(self.id))
                    self.log.debug("message = {0}".format(message))
                except:
                    self.log.error("DataDispatcher-{0}: waiting for control \
                        signal...failed".format(self.id), exc_info=True)
                    continue

                # remove subsription topic
                del message[0]

                if message[0] == b"EXIT":
                    self.log.debug("Router requested to shutdown \
                        DataDispatcher-{0}.".format(self.id))
                    break

                elif message[0] == b"CLOSE_SOCKETS":

                    targets = json.loads(message[1].decode("utf-8"))

                    for socketId, prio, suffix in targets:
                        if socketId in self.openConnections:
                            self.log.info("Closing socket {0}".format(
                                socketId))
                            if self.openConnections[socketId]:
                                self.openConnections[socketId].close(0)
                            del self.openConnections[socketId]
                    continue
                else:
                    self.log.error("Unhandled control signal received: \
                        {0}".format(message))

    def stop(self):
        self.continueRun = False
        self.log.debug("Closing sockets for DataDispatcher-{0}".format(
            self.id))

        for connection in self.openConnections:
            if self.openConnections[connection]:
                self.log.info("Closing socket {0}".format(connection))
                self.openConnections[connection].close(0)
                self.openConnections[connection] = None

        if self.controlSocket:
            self.log.info("Closing controlSocket")
            self.controlSocket.close(0)
            self.controlSocket = None

        if self.routerSocket:
            self.log.info("Closing routerSocket")
            self.routerSocket.close(0)
            self.routerSocket = None

        self.dataFetcher.clean(self.dataFetcherProp)

        if not self.extContext and self.context:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

    def signal_term_handler(self, signal, frame):
        self.log.debug('got SIGTERM')
        self.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    from multiprocessing import freeze_support, Queue
    from shutil import copyfile

    # see https://docs.python.org/2/library/multiprocessing.html#windows
    freeze_support()

    logfile = os.path.join(BASE_PATH, "logs", "dataDispatcher.log")
    logsize = 10485760

    logQueue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize,
                                      verbose=True,
                                      onScreenLogLevel="debug")

    # Start queue listener using the stream handler above
    logQueueListener = helpers.CustomQueueListener(logQueue, h1, h2)
    logQueueListener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(logQueue)
    root.addHandler(qh)

    sourceFile = os.path.join(BASE_PATH, "test_file.cbf")
    targetFile = os.path.join(BASE_PATH, "data", "source", "local", "100.cbf")

    copyfile(sourceFile, targetFile)
    time.sleep(0.5)

    localhost = "127.0.0.1"
    controlPort = "50005"
    routerPort = "7000"

    controlConId = "tcp://{ip}:{port}".format(ip=localhost, port=controlPort)
    routerConId = "tcp://{ip}:{port}".format(ip=localhost, port=routerPort)

    receivingPort = "6005"
    receivingPort2 = "6006"

    chunkSize = 10485760  # = 1024*1024*10 = 10 MiB

    localTarget = os.path.join(BASE_PATH, "data", "target")
    fixedStreamId = False
    fixedStreamId = "localhost:6006"

    logConfig = "test"

    dataFetcherProp = {
        "type": "file_fetcher",
        "fixSubdirs": ["commissioning", "current", "local"],
        "storeData": False,
        "removeData": False
        }

    context = zmq.Context.instance()

    dataDispatcherPr = Process(target=DataDispatcher,
                               args=(1, controlConId, routerConId, chunkSize,
                                     fixedStreamId, dataFetcherProp, logQueue,
                                     localTarget, context))
    dataDispatcherPr.start()

    routerSocket = context.socket(zmq.PUSH)
    connectionStr = "tcp://127.0.0.1:{0}".format(routerPort)
    routerSocket.bind(connectionStr)
    logging.info("=== routerSocket connected to {0}".format(connectionStr))

    receivingSocket = context.socket(zmq.PULL)
    connectionStr = "tcp://0.0.0.0:{0}".format(receivingPort)
    receivingSocket.bind(connectionStr)
    logging.info("=== receivingSocket connected to {0}".format(connectionStr))

    receivingSocket2 = context.socket(zmq.PULL)
    connectionStr = "tcp://0.0.0.0:{0}".format(receivingPort2)
    receivingSocket2.bind(connectionStr)
    logging.info("=== receivingSocket2 connected to {0}".format(connectionStr))

    metadata = {
        "sourcePath": os.path.join(BASE_PATH, "data", "source"),
        "relativePath": "local",
        "filename": "100.cbf"
        }
    targets = [['localhost:6005', 1, [".cbf"], "data"],
               ['localhost:6006', 0, [".cbf"], "data"]]

    message = [json.dumps(metadata).encode("utf-8"),
               json.dumps(targets).encode("utf-8")]
#    message = [json.dumps(metadata).encode("utf-8")]

    time.sleep(1)

    routerSocket.send_multipart(message)
    logging.info("=== send message")

    try:
        recv_message = receivingSocket.recv_multipart()
        logging.info("=== received: {0}".format(
            json.loads(recv_message[0].decode("utf-8"))))
        recv_message = receivingSocket2.recv_multipart()
        logging.info("=== received 2: {0}".format(
            json.loads(recv_message[0].decode("utf-8"))))
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
