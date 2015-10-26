__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import time
import zmq
import logging
import sys
import traceback
from multiprocessing import Process
from WorkerProcess import WorkerProcess

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
    receiverComIp       = None      # ip for socket to communicate with receiver
    receiverComPort     = None      # port for socket to communicate receiver
    liveViewer          = None
    liveViewerPorts     = []
    ondaIps             = []
    ondaPorts           = []
    receiverWhiteList   = None
    parallelDataStreams = None
    chunkSize           = None

    # sockets
    fileEventSocket     = None      # to receive fileMove-jobs as json-encoded dictionary
    receiverComSocket   = None      # to exchange messages with the receiver
    routerSocket        = None

    useDataStream       = True      # boolian to inform if the data should be send to the data stream pipe (to the storage system)
    useLiveViewer       = False     # boolian to inform if the receiver for the live viewer is running

    # to get the logging only handling this class
    log                 = None


    def __init__(self, fileEventIp, fileEventPort, dataStreamIp, dataStreamPort,
                 receiverComIp, receiverComPort, receiverWhiteList,
                 parallelDataStreams, chunkSize,
                 cleanerIp, cleanerPort,
                 liveViewerIp, liveViewerPorts,
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
        self.receiverComIp       = receiverComIp            # ip for socket to communicate with receiver;
        self.receiverComPort     = receiverComPort
        self.liveViewerIp        = liveViewerIp
        self.liveViewerPorts     = liveViewerPorts          # needs a list of ports because every WorkerProcess
                                                            # binds to one port (this is not possible for only one port
        self.ondaIps             = ondaIps
        self.ondaPorts           = ondaPorts

        self.useDataStream       = useDataStream

        #remove .desy.de from hostnames
        self.receiverWhiteList = []
        for host in receiverWhiteList:
            if host.endswith(".desy.de"):
                self.receiverWhiteList.append(host[:-8])
            else:
                self.receiverWhiteList.append(host)

        self.parallelDataStreams = parallelDataStreams
        self.chunkSize           = chunkSize


        self.log = self.getLogger()
        self.log.debug("Init")

        # create zmq socket for incoming file events
        self.fileEventSocket = self.zmqContext.socket(zmq.PULL)
        connectionStr        = "tcp://{ip}:{port}".format(ip=self.fileEventIp, port=self.fileEventPort)
        self.fileEventSocket.bind(connectionStr)
        self.log.debug("fileEventSocket started (bind) for '" + connectionStr + "'")


        # create zmq socket for communitation with receiver
        self.receiverComSocket = self.zmqContext.socket(zmq.REP)
        connectionStr          = "tcp://{ip}:{port}".format(ip=self.receiverComIp, port=self.receiverComPort)
        self.receiverComSocket.bind(connectionStr)
        self.log.info("receiverComSocket started (bind) for '" + connectionStr + "'")

        # Poller to get either messages from the watcher or communication messages to stop sending data to the live viewer
        self.poller = zmq.Poller()
        self.poller.register(self.fileEventSocket, zmq.POLLIN)
        self.poller.register(self.receiverComSocket, zmq.POLLIN)

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        routerIp   = "127.0.0.1"
        routerPort = "50000"

        self.routerSocket = self.zmqContext.socket(zmq.ROUTER)
        connectionStr     = "tcp://{ip}:{port}".format(ip=routerIp, port=routerPort)
        self.routerSocket.bind(connectionStr)
        self.log.debug("routerSocket started (bind) for '" + connectionStr + "'")


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


    def getLogger(self):
        logger = logging.getLogger("fileMover")
        return logger


    def startReceiving(self):

        self.log.debug("new message-socket crated for: new file events.")

        incomingMessageCounter = 0

        #start worker-processes. each will have its own PushSocket.
        workerProcessList      = list()
        numberOfWorkerProcesses = int(self.parallelDataStreams)
        for processNumber in range(numberOfWorkerProcesses):
            self.log.debug("instantiate new workerProcess (nr " + str(processNumber) + " )")
            newWorkerProcess = Process(target=WorkerProcess, args=(processNumber,
                                                                  self.dataStreamIp,
                                                                  self.dataStreamPort,
                                                                  self.chunkSize,
                                                                  self.cleanerIp,
                                                                  self.cleanerPort,
                                                                  self.liveViewerIp,
                                                                  self.liveViewerPorts[processNumber],
                                                                  self.ondaIps[processNumber],
                                                                  self.ondaPorts[processNumber],
                                                                  self.useDataStream
                                                                  ))
            workerProcessList.append(newWorkerProcess)

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
                    # signals are of the form [signal, hostname]
                    incomingMessage = self.receiverComSocket.recv()
                    self.log.debug("Recieved control command: %s" % incomingMessage )

                    signal         = None
                    signalHostname = None

                    try:
                        incomingMessage = incomingMessage.split(',')
                        signal          = incomingMessage[0]
                        signalHostname  = incomingMessage[1]
                    except Exception as e:
                        self.log.info("Received live viewer signal from host " + str(signalHostname) + " is of the wrong format")
                        self.receiverComSocket.send("NO_VALID_SIGNAL", zmq.NOBLOCK)
                        continue

                    if signalHostname.endswith(".desy.de"):
                        signalHostnameModified = signalHostname[:-8]
                    else:
                        signalHostnameModified = signalHostname

                    self.log.debug("Check if signal sending host is in WhiteList...")
                    if signalHostname in self.receiverWhiteList or signalHostnameModified in self.receiverWhiteList:
                        self.log.debug("Check if signal sending host is in WhiteList...Host " + str(signalHostname) + " is allowed to connect.")
                    else:
                        self.log.debug("Check if signal sending host is in WhiteList...Host " + str(signalHostname) + " is not allowed to connect.")
                        self.log.info("Signal from host " + str(signalHostname) + " is discarded.")
                        self.receiverComSocket.send("NO_VALID_HOST", zmq.NOBLOCK)
                        continue

                    if signal == "STOP_LIVE_VIEWER":
                        self.log.info("Received live viewer stop signal from host " + str(signalHostname) + "...stopping live viewer")
                        self.useLiveViewer = False
                        self.sendSignalToReceiver(signal)
                        continue
                    elif signal == "START_LIVE_VIEWER":
                        self.log.info("Received live viewer start signal from host " + str(signalHostname) + "...starting live viewer")
                        self.useLiveViewer = True
                        self.sendSignalToReceiver(signal)
                        continue
                    elif signal == "STOP_REALTIME_ANALYSIS":
                        self.log.info("Received realtime analysis stop signal from host " + str(signalHostname) + "...stopping realtime analysis")
                        # send signal to workerProcesses and back to receiver
                        self.sendSignalToReceiver(signal)
                        continue
                    elif signal == "START_REALTIME_ANALYSIS":
                        self.log.info("Received realtime analysis start signal from host " + str(signalHostname) + "...starting realtime analysis")
                        # send signal to workerProcesses and back to receiver
                        self.sendSignalToReceiver(signal)
                        continue
                    else:
                        self.log.info("Received live viewer signal from host " + str(signalHostname) + " unkown: " + str(signal))
                        self.receiverComSocket.send("NO_VALID_SIGNAL", zmq.NOBLOCK)

        except KeyboardInterrupt:
            self.log.debug("Keyboard interuption detected. Stop receiving")



    def processFileEvent(self, fileEventMessage):
        self.log.debug("waiting for available workerProcess.")

        # address == "worker-0"
        # empty   == b''                   # as delimiter
        # ready   == b'READY'
        address, empty, ready = self.routerSocket.recv_multipart()
        self.log.debug("available workerProcess detected.")

        self.log.debug("passing job to workerProcess...")
        self.routerSocket.send_multipart([
                                     address,
                                     b'',
                                     fileEventMessage,
                                    ])

        self.log.debug("passing job to workerProcess...done.")


    def sendSignalToReceiver(self, signal):
        numberOfWorkerProcesses = int(self.parallelDataStreams)
        for processNumber in range(numberOfWorkerProcesses):
            self.log.debug("send signal " + str(signal) + " to workerProcess (nr " + str(processNumber) + " )")

            address, empty, ready = self.routerSocket.recv_multipart()
            self.log.debug("available workerProcess detected.")

            # address == "worker-0"
            # empty   == b''                   # as delimiter
            # signal  == b'START_LIVE_VIEWER'
            self.routerSocket.send_multipart([
                                         address,
                                         b'',
                                         signal,
                                        ])
            self.log.debug("send confirmation back to receiver: " + str(signal) )
            self.receiverComSocket.send(signal, zmq.NOBLOCK)


    def stop(self):
        self.log.debug("Closing sockets")
        self.fileEventSocket.close(0)
        self.receiverComSocket.close(0)
        self.routerSocket.close(0)


