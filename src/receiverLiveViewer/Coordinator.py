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
    liveViewerIp             = None
    liveViewerPort           = None
    receiverExchangeIp       = None
    receiverExchangePort     = None

    ringBuffer               = []
    maxRingBufferSize        = None
    maxQueueSize             = None

    log                      = None

    receiverThread           = None
    liveViewerThread         = None

    # sockets
    receiverExchangeSocket   = None         # socket to communicate with FileReceiver class
    liveViewerSocket         = None         # socket to communicate with live viewer


    def __init__(self, receiverExchangePort,
            liveViewerPort, liveViewerIp,
            maxRingBufferSize, maxQueueSize,
            context = None):

        self.receiverExchangeIp   = "127.0.0.1"
        self.receiverExchangePort = receiverExchangePort
        self.liveViewerIp         = liveViewerIp
        self.liveViewerPort       = liveViewerPort

        self.maxRingBufferSize  = maxRingBufferSize
        self.maxQueueSize        = maxQueueSize

        self.ringBuffer         = RingBuffer(self.maxRingBufferSize, self.maxQueueSize)

        self.log = self.getLogger()
        self.log.debug("Init")

#        if context:
#            assert isinstance(context, zmq.sugar.context.Context)

        self.zmqContext = context or zmq.Context()

        # create socket to exchange informations with FileReceiver
        self.receiverExchangeSocket = self.zmqContext.socket(zmq.PAIR)
        connectionStr               = "tcp://" + self.receiverExchangeIp + ":%s" % self.receiverExchangePort
        try:
            self.receiverExchangeSocket.bind(connectionStr)
            self.log.debug("receiverExchangeSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start receiverExchangeSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # create socket for live viewer
        self.liveViewerSocket = self.zmqContext.socket(zmq.REP)
        connectionStr         = "tcp://" + self.liveViewerIp + ":%s" % self.liveViewerPort
        try:
            self.liveViewerSocket.bind(connectionStr)
            self.log.debug("liveViewerSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start liveViewerSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        self.poller = zmq.Poller()
        self.poller.register(self.receiverExchangeSocket, zmq.POLLIN)
        self.poller.register(self.liveViewerSocket, zmq.POLLIN)

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
                    self.log.debug("Received exit command, coordinator thread will stop receiving messages")
                    should_continue = False
                    # TODO why sending signal to live viewer?
#                    self.liveViewerSocket.send("Exit", zmq.NOBLOCK)
                    break
                elif message.startswith("AddFile"):
                    self.log.debug("Received AddFile command")
                    # add file to ring buffer
                    splittedMessage = message[7:].split(", ")
                    filename        = splittedMessage[0]
                    fileModTime     = splittedMessage[1]
                    self.log.debug("Add new file to ring buffer: " + str(filename) + ", " + str(fileModTime))
                    self.ringBuffer.add(filename, fileModTime)

            if self.liveViewerSocket in socks and socks[self.liveViewerSocket] == zmq.POLLIN:
                message = self.liveViewerSocket.recv()
                self.log.debug("Call for next file... " + message)
                # send newest element in ring buffer to live viewer
                answer = self.ringBuffer.getNewestFile()
                try:
                    self.liveViewerSocket.send(answer)
                except zmq.error.ContextTerminated:
                    break

        self.log.debug("Closing socket")
        self.receiverExchangeSocket.close(0)
        self.liveViewerSocket.close(0)

        self.log.debug("Clearing Ringbuffer")
        self.ringBuffer.removeAll()

