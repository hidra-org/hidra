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
    cleanerComIp        = None      # ip to communicate with cleaner about new requests from the receiver (the same as cleanerIp)
    cleanerComPort      = None      # port to communicate with cleaner about new requests from the receiver
    receiverComIp       = None      # ip for socket to communicate with receiver
    receiverComPort     = None      # port for socket to communicate receiver
    receiverWhiteList   = None
    parallelDataStreams = None
    chunkSize           = None

    # sockets
    fileEventSocket     = None      # to receive fileMove-jobs as json-encoded dictionary
    receiverComSocket   = None      # to exchange messages with the receiver
    routerSocket        = None
    cleanerSocket       = None      # to echange if a realtime analysis receiver is online
    cleanerComSocket    = None      # to echange data to send to the realtime analysis receiver

    useLiveViewer       = False     # boolian to inform if the receiver for the live viewer is running

    # to get the logging only handling this class
    log                 = None


    def __init__(self, fileEventIp, fileEventPort, dataStreamIp, dataStreamPort,
                 receiverComPort, receiverWhiteList,
                 parallelDataStreams, chunkSize,
                 cleanerIp, cleanerPort, cleanerComPort,
                 context = None):

        assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext          = context or zmq.Context()
        self.fileEventIp         = fileEventIp
        self.fileEventPort       = fileEventPort
        self.dataStreamIp        = dataStreamIp
        self.dataStreamPort      = dataStreamPort
        self.cleanerIp           = cleanerIp
        self.cleanerPort         = cleanerPort
        self.cleanerComIp        = self.cleanerIp       # always the same ip as as cleanerIp
        self.cleanerComPort      = cleanerComPort
        self.receiverComIp       = dataStreamIp         # ip for socket to communicate with receiver; is the same ip as the data stream ip
        self.receiverComPort     = receiverComPort
        self.receiverWhiteList   = receiverWhiteList
        self.parallelDataStreams = parallelDataStreams
        self.chunkSize           = chunkSize


        self.log = self.getLogger()
        self.log.debug("Init")

        # create zmq socket for incoming file events
        self.fileEventSocket         = self.zmqContext.socket(zmq.PULL)
        connectionStrFileEventSocket = "tcp://{ip}:{port}".format(ip=self.fileEventIp, port=self.fileEventPort)
        self.fileEventSocket.bind(connectionStrFileEventSocket)
        self.log.debug("fileEventSocket started (bind) for '" + connectionStrFileEventSocket + "'")


        # create zmq socket for communitation with receiver
        self.receiverComSocket         = self.zmqContext.socket(zmq.REP)
        connectionStrReceiverComSocket = "tcp://{ip}:{port}".format(ip=self.receiverComIp, port=self.receiverComPort)
        print "connectionStrReceiverComSocket", connectionStrReceiverComSocket
        self.receiverComSocket.bind(connectionStrReceiverComSocket)
        self.log.debug("receiverComSocket started (bind) for '" + connectionStrReceiverComSocket + "'")

        #init Cleaner message-pipe
        self.cleanerSocket            = self.zmqContext.socket(zmq.PUSH)
        connectionStrCleanerSocket    = "tcp://{ip}:{port}".format(ip=self.cleanerIp, port=self.cleanerPort)
        self.cleanerSocket.connect(connectionStrCleanerSocket)
        self.log.debug("cleanerSocket started (connect) for '" + connectionStrCleanerSocket + "'")

        #init Cleaner message-pipe
        self.cleanerComSocket         = self.zmqContext.socket(zmq.REQ)
        connectionStrCleanerSocket    = "tcp://{ip}:{port}".format(ip=self.cleanerComIp, port=self.cleanerComPort)
        self.cleanerComSocket.connect(connectionStrCleanerSocket)
        self.log.debug("cleanerComSocket started (connect) for '" + connectionStrCleanerSocket + "'")

        # Poller to get either messages from the watcher or communication messages to stop sending data to the live viewer
        self.poller = zmq.Poller()
        self.poller.register(self.fileEventSocket, zmq.POLLIN)
        self.poller.register(self.receiverComSocket, zmq.POLLIN)

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        routerIp   = "127.0.0.1"
        routerPort = "50000"

        self.routerSocket         = self.zmqContext.socket(zmq.ROUTER)
        connectionStrRouterSocket = "tcp://{ip}:{port}".format(ip=routerIp, port=routerPort)
        self.routerSocket.bind(connectionStrRouterSocket)
        self.log.debug("routerSocket started (bind) for '" + connectionStrRouterSocket + "'")


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
                                                                  self.cleanerPort))
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

                    if signal != "NEXT_FILE":
                        self.log.debug("Check if signal sending host is in WhiteList...")
                        if signalHostname in self.receiverWhiteList:
                            self.log.info("Check if signal sending host is in WhiteList...Host " + str(signalHostname) + " is allowed to connect.")
                        else:
                            self.log.info("Check if signal sending host is in WhiteList...Host " + str(signalHostname) + " is not allowed to connect.")
                            self.log.debug("Signal from host " + str(signalHostname) + " is discarded.")
                            print "Signal from host " + str(signalHostname) + " is discarded."
                            self.receiverComSocket.send("NO_VALID_HOST", zmq.NOBLOCK)
                            continue


                    if signal == "STOP_LIVE_VIEWER":
                        self.log.info("Received live viewer stop signal from host " + str(signalHostname) + "...stopping live viewer")
                        print "Received live viewer stop signal from host " + signalHostname + "...stopping live viewer"
                        self.useLiveViewer = False
                        self.sendSignalToReceiver(signal)
                        continue
                    elif signal == "START_LIVE_VIEWER":
                        self.log.info("Received live viewer start signal from host " + str(signalHostname) + "...starting live viewer")
                        print "Received live viewer start signal from host " + str(signalHostname) + "...starting live viewer"
                        self.useLiveViewer = True
                        self.sendSignalToReceiver(signal)
                        continue
                    elif signal == "STOP_REALTIME_ANALYSIS":
                        self.log.info("Received realtime analysis stop signal from host " + str(signalHostname) + "...stopping realtime analysis")
                        print "Received realtime analysis stop signal from host " + signalHostname + "...stopping realtime analysis"
                        # send signal to cleaner
                        self.sendSignalToCleaner(signal)
                        # send signal to workerProcesses and back to receiver
                        self.sendSignalToReceiver(signal)
                        continue
                    elif signal == "START_REALTIME_ANALYSIS":
                        self.log.info("Received realtime analysis start signal from host " + str(signalHostname) + "...starting realtime analysis")
                        print "Received realtime analysis start signal from host " + str(signalHostname) + "...starting realtime analysis"
                        # send signal to cleaner
                        self.sendSignalToCleaner(signal)
                        # send signal to workerProcesses and back to receiver
                        self.sendSignalToReceiver(signal)
                        continue
                    elif signal == "NEXT_FILE":
                        self.log.info("Received request for next file")
                        print "Received request for next file"
                        self.sendRequestToCleaner(signal)
                    else:
                        self.log.info("Received live viewer signal from host " + str(signalHostname) + " unkown: " + str(signal))
                        self.receiverComSocket.send("NO_VALID_SIGNAL", zmq.NOBLOCK)

        except KeyboardInterrupt:
            self.log.info("Keyboard interuption detected. Stop receiving")



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


    def sendRequestToCleaner(self, message):
        self.log.debug("send request to cleaner: " + str(message) )
        self.cleanerComSocket.send(message)
        self.log.debug("waiting for answer of cleaner")
        answer = self.cleanerComSocket.recv()
        self.log.debug("send confirmation back to receiver: " + str(answer) )
        try:
            self.receiverComSocket.send(answer, zmq.NOBLOCK)
            print "send answer", answer[:45]
        except zmq.error.Again:
            self.log.error("Unable to send answer for file " + str(sourceFilePathFull))
            self.log.error("Receiver has disconnected")
            self.log.info("Stopping realtime analysis")
            self.sendSignalToCleaner("STOP_REALTIME_ANALYSIS")
        except Exception, e:
            self.log.error("Unable to send answer for file " + str(sourceFilePathFull))
            self.log.debug("Error was: " + str(e))


    def sendSignalToCleaner(self, signal):
        self.log.debug("send signal to cleaner: " + str(signal) )
        self.cleanerSocket.send(signal)
#        self.cleanerComSocket.send(signal)
#        self.log.debug("send confirmation back to receiver: " + str(signal) )
#        self.receiverComSocket.send(signal, zmq.NOBLOCK)


    def sendSignalToReceiver(self, signal):
        numberOfWorkerProcesses = int(self.parallelDataStreams)
        for processNumber in range(numberOfWorkerProcesses):
            self.log.debug("send signal to receiver " + str(signal) + " to workerProcess (nr " + str(processNumber) + " )")

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
        self.cleanerSocket.close(0)
        self.cleanerComSocket.close(0)
        self.routerSocket.close(0)


