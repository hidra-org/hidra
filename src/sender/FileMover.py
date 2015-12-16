__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'

import time
import zmq
import logging
import os
import sys
import traceback
from multiprocessing import Process
from WorkerProcess import WorkerProcess

#path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SHARED_PATH = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helperScript

#
#  --------------------------  class: FileMover  --------------------------------------
#
class FileMover():
    zmqContext          = None
    fileEventIp         = None      # serverIp for incoming messages
    fileEventPort       = None
    dataStreamIp        = None      # ip of dataStream-socket to push new files to
    dataStreamPort      = None      # port number of dataStream-socket to push new files to
    cleanerIp           = None      # zmq pull endpoint, responsable to delete/move files
    cleanerPort         = None      # zmq pull endpoint, responsable to delete/move files
    routerIp            = None      # ip  of router for load-balancing worker-processes
    routerPort          = None      # port of router for load-balancing worker-processes
    receiverComIp       = None      # ip for socket to communicate with receiver
    receiverComPort     = None      # port for socket to communicate receiver
    liveViewer          = None
    ondaIps             = []
    ondaPorts           = []
    receiverWhiteList   = None
    parallelDataStreams = None
    chunkSize           = None

    # sockets
    fileEventSocket     = None      # to receive fileMove-jobs as json-encoded dictionary
    receiverComSocket   = None      # to exchange messages with the receiver
    routerSocket        = None

    useDataStream       = False     # boolian to inform if the data should be send to the data stream pipe (to the storage system)
    openConnections     = dict()    # list of all open hosts and ports to which a data stream is opened


    # to get the logging only handling this class
    log                 = None


    def __init__(self, fileEventIp, fileEventPort, dataStreamIp, dataStreamPort,
                 receiverComIp, receiverComPort, receiverWhiteList,
                 parallelDataStreams, chunkSize,
                 cleanerIp, cleanerPort, routerPort,
                 ondaIps, ondaPorts,
                 useDataStream,
                 context = None):

#        assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext          = context or zmq.Context()
        self.fileEventIp         = fileEventIp
        self.fileEventPort       = fileEventPort
        self.dataStreamIp        = dataStreamIp
        self.dataStreamPort      = dataStreamPort
        self.cleanerIp           = cleanerIp
        self.cleanerPort         = cleanerPort
        self.routerIp            = "127.0.0.1"
        self.routerPort          = routerPort
        self.receiverComIp       = receiverComIp            # ip for socket to communicate with receiver;
        self.receiverComPort     = receiverComPort

        self.ondaIps             = ondaIps
        self.ondaPorts           = ondaPorts

        self.useDataStream       = useDataStream
        self.openConnections     = { "streams" : [], "queryNext" : [] }

        #remove .desy.de from hostnames
        self.receiverWhiteList = []
        for host in receiverWhiteList:
            if host.endswith(".desy.de"):
                self.receiverWhiteList.append(host[:-8])
            else:
                self.receiverWhiteList.append(host)

        self.parallelDataStreams = parallelDataStreams
        self.chunkSize           = chunkSize

        self.freeWorker          = []                       # list of workers which sent a ready signal (again) during signal distributing process


        self.log = self.getLogger()
        self.log.debug("Init")

        self.createSockets()


    def getLogger(self):
        logger = logging.getLogger("fileMover")
        return logger

    def createSockets(self):
        # create zmq socket for incoming file events
        self.fileEventSocket = self.zmqContext.socket(zmq.PULL)
        connectionStr        = "tcp://{ip}:{port}".format(ip=self.fileEventIp, port=self.fileEventPort)
        try:
            self.fileEventSocket.bind(connectionStr)
            self.log.debug("fileEventSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start fileEventSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))


        # create zmq socket for communitation with receiver
        self.receiverComSocket = self.zmqContext.socket(zmq.REP)
        connectionStr          = "tcp://{ip}:{port}".format(ip=self.receiverComIp, port=self.receiverComPort)
        try:
            self.receiverComSocket.bind(connectionStr)
            self.log.info("receiverComSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start receiverComSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # Poller to get either messages from the watcher or communication messages to stop sending data to the live viewer
        self.poller = zmq.Poller()
        self.poller.register(self.fileEventSocket, zmq.POLLIN)
        self.poller.register(self.receiverComSocket, zmq.POLLIN)

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        self.routerSocket = self.zmqContext.socket(zmq.ROUTER)
        connectionStr     = "tcp://{ip}:{port}".format(ip=self.routerIp, port=self.routerPort)
        try:
            self.routerSocket.bind(connectionStr)
            self.log.debug("routerSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start routerSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))


    def process(self):
        try:
            self.startReceiving()
        except zmq.error.ZMQError as e:
            self.log.error("ZMQError: "+ str(e))
            self.log.debug("Shutting down workerProcess.")
        except:
            trace = traceback.format_exc()
            self.log.info("Stopping fileMover due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))


    def startReceiving(self):

        self.log.debug("new message-socket crated for: new file events.")

        incomingMessageCounter = 0

        #start worker-processes. each will have its own PushSocket.
        numberOfWorkerProcesses = int(self.parallelDataStreams)
        for processNumber in range(numberOfWorkerProcesses):
            self.log.debug("instantiate new workerProcess (nr " + str(processNumber) + " )")
            newWorkerProcess = Process(target=WorkerProcess, args=(processNumber,
                                                                  self.dataStreamIp,
                                                                  self.dataStreamPort,
                                                                  self.chunkSize,
                                                                  self.cleanerIp,
                                                                  self.cleanerPort,
                                                                  self.ondaIps[processNumber],
                                                                  self.ondaPorts[processNumber],
                                                                  self.useDataStream
                                                                  ))

            self.log.debug("start worker process nr " + str(processNumber))
            newWorkerProcess.start()

        #run loop, and wait for incoming messages
        continueReceiving = True
        self.log.debug("waiting for new fileEvent-messages")
        try:
            while continueReceiving:
                socks = dict(self.poller.poll())

                if self.fileEventSocket in socks and socks[self.fileEventSocket] == zmq.POLLIN:

                    try:
                        incomingMessage = self.fileEventSocket.recv()
                        self.log.debug("new fileEvent-message received.")
                        self.log.debug("message content: " + str(incomingMessage))
                        incomingMessageCounter += 1

                        self.log.debug("processFileEvent..." + str(incomingMessageCounter))
                        self.processFileEvent(incomingMessage)  #TODO refactor as separate process to emphasize unblocking
                        self.log.debug("processFileEvent...done")
                    except Exception, e:
                        self.log.error("Failed to receive new fileEvent-message.")
                        self.log.error(sys.exc_info())

                        #TODO might using a error-count and threshold when to stop receiving, e.g. after 100 misses?
                        # continueReceiving = False
                    continue

                if self.receiverComSocket in socks and socks[self.receiverComSocket] == zmq.POLLIN:

                    incomingMessage = self.receiverComSocket.recv_multipart()
                    self.log.debug("Recieved signal: %s" % incomingMessage )

                    checkStatus, signal, host, port = self.checkSignal(incomingMessage)
                    if not checkStatus:
                        continue

                    self.reactToSignal(signal, host, port)

        except KeyboardInterrupt:
            self.log.debug("Keyboard interuption detected. Stop receiving")


    def processFileEvent(self, fileEventMessage):
        self.log.debug("waiting for available workerProcess.")

        if self.freeWorker:
            address = freeWorker.pop()
            self.log.debug("Available waiting workerProcess detected.")
        else:

            # address == "worker-0"
            # empty   == b''                   # as delimiter
            # ready   == b'READY'
            address, empty, ready = self.routerSocket.recv_multipart()
            self.log.debug("Available workerProcess detected.")

        self.log.debug("Passing job to workerProcess (" + address + ")...")
        self.routerSocket.send_multipart([
                                     address,
                                     b'',
                                     fileEventMessage,
                                    ])

        self.log.debug("Passing job to workerProcess...done.")


    def checkSignal(self, incomingMessage):

        if len(incomingMessage) != 4:

            log.info("Received signal is of the wrong format")
            log.debug("Received signal is too short or too long: " + str(incomingMessage))
            return False, None, None, None

        else:

            version, signal, host, port = incomingMessage

            if version:
                if helperScript.checkVersion(version, self.log):
                    self.log.debug("Versions are compatible: " + str(version))
                else:
                    self.log.debug("Version are not compatible")
                    self.sendResponse("VERSION_CONFLICT")
                    return False, None, None, None

            if signal and host and port :

                # Checking signal sending host
                self.log.debug("Check if signal sending host is in WhiteList...")
                if helperScript.checkHost(host, self.receiverWhiteList):
                    self.log.debug("Host " + str(host) + " is allowed to connect.")
                else:
                    self.log.debug("Host " + str(host) + " is not allowed to connect.")
                    self.sendResponse("NO_VALID_HOST")
                    return False, None, None, None

        return True, signal, host, port


    def sendResponse(self, signal):
            self.log.debug("send confirmation back to receiver: " + str(signal) )
            self.receiverComSocket.send(signal, zmq.NOBLOCK)


    def reactToSignal(self, signal, host, port):
        message = signal + "," + host + "," + port

        # React to signal
        if signal == "START_STREAM":
            self.log.info("Received signal to start stream to host " + str(host) + " on port " + str(port))
            if [host, port] not in self.openConnections["streams"]:
                self.openConnections["streams"].append([host, port])
                # send signal to workerProcesses and back to receiver
                self.sendSignalToWorker(message)
                self.sendResponse(signal)
            else:
                self.log.info("Stream to host " + str(host) + " on port " + str(port) + " is already started")
                self.sendResponse("CONNECTION_ALREADY_OPEN")
            return

        elif signal == "STOP_STREAM":
            self.log.info("Received signal to stop stream to host " + str(host) + " on port " + str(port))
            if [host, port] in self.openConnections["streams"]:
                self.openConnections["streams"].remove([host, port])
                # send signal to workerProcesses and back to receiver
                self.sendSignalToWorker(message)
                self.sendResponse(signal)
            else:
                self.log.info("No stream to close was found for host " + str(host) + " on port " + str(port))
                self.sendResponse("NO_OPEN_CONNECTION_FOUND")
            return

        elif signal == "START_QUERY_NEXT" or signal == "START_REALTIME_ANALYSIS":
            self.log.info("Received signal from host " + str(host) + " to enable querying for data")
            if [host, port] not in self.openConnections["queryNext"]:
                self.openConnections["queryNext"].append([host, port])
                # send signal to workerProcesses and back to receiver
                self.sendSignalToWorker(message)
                self.sendResponse(signal)
            else:
                self.log.info("Query connection to host " + str(host) + " on port " + str(port) + " is already started")
                self.sendResponse("CONNECTION_ALREADY_OPEN")
            return

        elif signal == "STOP_QUERY_NEXT" or signal == "STOP_REALTIME_ANALYSIS":
            self.log.info("Received signal from host " + str(host) + " to disable querying for data")
            if [host, port] in self.openConnections["queryNext"]:
                self.openConnections["queryNext"].remove([host, port])
                # send signal to workerProcesses and back to receiver
                self.sendSignalToWorker(message)
                self.log.debug("Send signal to worker: " + str(signal))
                self.sendResponse(signal)
                self.log.debug("Setime.sleep(0.1)nd response back: " + str(signal))
            else:
                self.log.info("No query connection to close was found for host " + str(host) + " on port " + str(port))
                self.sendResponse("NO_OPEN_CONNECTION_FOUND")
            return

        else:
            self.log.info("Received signal from host " + str(host) + " unkown: " + str(signal))
            self.sendResponse("NO_VALID_SIGNAL")


    def sendSignalToWorker(self, signal, individual = False):
        numberOfWorkerProcesses = int(self.parallelDataStreams)
        alreadySignalled = []

#        for processNumber in range(numberOfWorkerProcesses):
        while len(alreadySignalled) != numberOfWorkerProcesses:

            address, empty, ready = self.routerSocket.recv_multipart()
            self.log.debug("Available workerProcess detected.")

            if address not in alreadySignalled:
                self.log.debug("Send signal " + str(signal) + " to " + str(address))
                # address == "worker-0"
                # empty   == b''                   # as delimiter
                # signal  == b'START_STREAM'
                self.routerSocket.send_multipart([
                                             address,
                                             b'',
                                             signal,
                                            ])

                alreadySignalled.append(address)
            else:
                self.log.debug("Signal " + str(signal) + " already sent to " + str(address) + ". Mark this worker as ready/.")
                self.freeWorker.append(address)


    def stop(self):
        self.log.debug("Closing sockets")
        self.fileEventSocket.close(0)
        self.receiverComSocket.close(0)
        self.sendSignalToWorker("EXIT")
        self.routerSocket.close(0)


