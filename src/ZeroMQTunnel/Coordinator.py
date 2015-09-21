__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import zmq
import logging
import traceback
from RingBuffer import RingBuffer

#
#  --------------------------  class: Coordinator  --------------------------------------
#
class Coordinator:
    zmqContext               = None
    liveViewerZmqContext     = None
    outputDir                = None
    zmqDataStreamIp          = None
    zmqDataStreamPort        = None
    zmqLiveViewerIp          = None
    zmqLiveViewerPort        = None
    receiverExchangeIp       = "127.0.0.1"
    receiverExchangePort     = "6072"

    ringBuffer               = []
    maxRingBufferSize        = None

    log                      = None

    receiverThread           = None
    liveViewerThread         = None

    # sockets
    receiverExchangeSocket   = None         # socket to communicate with FileReceiver class
    zmqliveViewerSocket      = None         # socket to communicate with live viewer


    def __init__(self, outputDir, zmqDataStreamPort, zmqDataStreamIp, zmqLiveViewerPort, zmqLiveViewerIp, maxRingBufferSize, context = None):
        self.outputDir          = outputDir
        self.zmqDataStreamIp    = zmqDataStreamIp
        self.zmqDataStreamPort  = zmqDataStreamPort
        self.zmqLiveViewerIp    = zmqLiveViewerIp
        self.zmqLiveViewerPort  = zmqLiveViewerPort

        self.maxRingBufferSize  = maxRingBufferSize
#        # TODO remove outputDir from ringBuffer?
#        self.ringBuffer         = RingBuffer(self.maxRingBufferSize, self.outputDir)
        self.ringBuffer         = RingBuffer(self.maxRingBufferSize)

        self.log = self.getLogger()
        self.log.debug("Init")

#        if context:
#            assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext = context or zmq.Context()

        # create sockets
        self.receiverExchangeSocket         = self.zmqContext.socket(zmq.PAIR)
        connectionStrReceiverExchangeSocket = "tcp://" + self.receiverExchangeIp + ":%s" % self.receiverExchangePort
        self.receiverExchangeSocket.bind(connectionStrReceiverExchangeSocket)
        self.log.debug("receiverExchangeSocket started (bind) for '" + connectionStrReceiverExchangeSocket + "'")

        # create socket for live viewer
        self.zmqliveViewerSocket         = self.zmqContext.socket(zmq.REP)
        connectionStrLiveViewerSocket    = "tcp://" + self.zmqLiveViewerIp + ":%s" % self.zmqLiveViewerPort
        self.zmqliveViewerSocket.bind(connectionStrLiveViewerSocket)
        self.log.debug("zmqLiveViewerSocket started (bind) for '" + connectionStrLiveViewerSocket + "'")

        self.poller = zmq.Poller()
        self.poller.register(self.receiverExchangeSocket, zmq.POLLIN)
        self.poller.register(self.zmqliveViewerSocket, zmq.POLLIN)

        try:
            self.log.info("Start communication")
            self.communicate()
            self.log.info("Stopped communication.")
        except Exception, e:
            trace = traceback.format_exc()
            self.log.info("Unkown error state. Shutting down...")
            self.log.debug("Error was: " + str(e))


        self.log.info("Quitting.")


    def getLogger(self):
        logger = logging.getLogger("coordinator")
        return logger


    def communicate(self):
        should_continue = True

        while should_continue:
            socks = dict(self.poller.poll())

            if self.receiverExchangeSocket in socks and socks[self.receiverExchangeSocket] == zmq.POLLIN:
                message = self.receiverExchangeSocket.recv()
                self.log.debug("Recieved control command: %s" % message )
                if message == "Exit":
                    self.log.debug("Received exit command, coordinator thread will stop recieving messages")
                    should_continue = False
                    # TODO why sending signal to live viewer?
#                    self.zmqliveViewerSocket.send("Exit", zmq.NOBLOCK)
                    break
                elif message.startswith("AddFile"):
                    self.log.debug("Received AddFile command")
                    # add file to ring buffer
                    splittedMessage = message[7:].split(", ")
                    filename        = splittedMessage[0]
                    fileModTime     = splittedMessage[1]
                    self.log.debug("Add new file to ring buffer: " + str(filename) + ", " + str(fileModTime))
                    self.ringBuffer.add(filename, fileModTime)

            if self.zmqliveViewerSocket in socks and socks[self.zmqliveViewerSocket] == zmq.POLLIN:
                message = self.zmqliveViewerSocket.recv()
                self.log.debug("Call for next file... " + message)
                # send newest element in ring buffer to live viewer
                answer = self.ringBuffer.getNewestFile()
                print answer
                try:
                    self.zmqliveViewerSocket.send(answer)
                except zmq.error.ContextTerminated:
                    break

        self.log.debug("Closing socket")
        self.receiverExchangeSocket.close(0)
        self.zmqliveViewerSocket.close(0)

