__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import os
import sys
import zmq
import logging
import traceback

from RingBuffer import RingBuffer
import helperScript
#
#  --------------------------  class: LiveViewCommunicator  --------------------------------------
#
class LiveViewCommunicator:
    context                  = None
    liveViewerComIp          = None
    liveViewerComPort        = None
    liveViewerWhiteList      = None

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
    liveViewerComSocket      = None         # socket to communicate with live viewer
    liveViewerDataSocket     = None         # socket to send data to live viewer


    def __init__(self, receiverExchangePort,
            liveViewerComPort, liveViewerComIp, liveViewerWhiteList,
            maxRingBufferSize, maxQueueSize,
            context = None):

        self.receiverExchangeIp   = "127.0.0.1"
        self.receiverExchangePort = receiverExchangePort
        self.liveViewerComIp      = liveViewerComIp
        self.liveViewerComPort    = liveViewerComPort
        self.liveViewerWhiteList  = liveViewerWhiteList

        self.maxRingBufferSize    = maxRingBufferSize
        self.maxQueueSize         = maxQueueSize

        self.ringBuffer           = RingBuffer(self.maxRingBufferSize, self.maxQueueSize)

        self.log = self.getLogger()
        self.log.debug("Init")

        #self.context = context or zmq.Context()
        if context:
            self.context      = context
            self.externalContext = True
        else:
            self.context      = zmq.Context()
            self.externalContext = False

        #create sockets
        self.createSockets()

        try:
            self.log.info("Start communication")
            self.communicate()
        except Exception as e:
            trace = traceback.format_exc()
            self.log.error("Unkown error state. Shutting down...")
            self.log.debug("Error was: " + str(e))
            self.log.debug("Trace was: " + str(trace))
        finally:
            self.stop()




    def getLogger(self):
        logger = logging.getLogger("coordinator")
        return logger


    def createSockets(self):
        # create socket to exchange informations with FileReceiver
        self.receiverExchangeSocket = self.context.socket(zmq.PAIR)
        connectionStr               = "tcp://" + self.receiverExchangeIp + ":%s" % self.receiverExchangePort
        try:
            self.receiverExchangeSocket.bind(connectionStr)
            self.log.info("receiverExchangeSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start receiverExchangeSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # create communication socket for live viewer
        self.liveViewerComSocket = self.context.socket(zmq.REP)
        connectionStr         = "tcp://" + self.liveViewerComIp + ":%s" % self.liveViewerComPort
        try:
            self.liveViewerComSocket.bind(connectionStr)
            self.log.info("liveViewerComSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start liveViewerComSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        self.poller = zmq.Poller()
        self.poller.register(self.receiverExchangeSocket, zmq.POLLIN)
        self.poller.register(self.liveViewerComSocket, zmq.POLLIN)



    def communicate(self):
        should_continue = True

        while should_continue:
            try:
                socks = dict(self.poller.poll())
            except KeyboardInterrupt:
                self.log.info("Message could not be received due to KeyboardInterrupt during polling.")
                should_continue = False
                break
            except Exception as e:
                self.log.error("Message could not be received due to unknown error during polling.")
                self.log.debug("Error was: " + str(e))
                break


            if self.receiverExchangeSocket in socks and socks[self.receiverExchangeSocket] == zmq.POLLIN:
                message = self.receiverExchangeSocket.recv()
                self.log.debug("Received control command: %s" % message )
                if message == "Exit":
                    self.log.debug("Received exit command, coordinator thread will stop receiving messages")
                    should_continue = False
                    break
                elif message.startswith("AddFile"):
                    self.log.debug("Received AddFile command")
                    # add file to ring buffer
                    splittedMessage = message[7:].split(", ")
                    filename        = splittedMessage[0]
                    fileModTime     = splittedMessage[1]
                    self.log.debug("Add new file to ring buffer: " + str(filename) + ", " + str(fileModTime))
                    self.ringBuffer.add(filename, fileModTime)

            if self.liveViewerComSocket in socks and socks[self.liveViewerComSocket] == zmq.POLLIN:
                message = self.liveViewerComSocket.recv_multipart()
                self.log.debug("Recieved signal: %s" % message )

                checkStatus, signal, host, port = self.checkSignal(message)
                if not checkStatus:
                    continue

                if signal == "START_DISPLAYER":
                    if self.liveViewerDataSocket:
                        self.liveViewerDataSocket.close(0)
                        self.liveViewerDataSocket = None
                        self.log.debug("liveViewerDataSocket refreshed")

                    # create data socket for live viewer
                    self.liveViewerDataIp   = signalHostname
                    self.liveViewerDataPort = port
                    self.liveViewerDataSocket = self.context.socket(zmq.REP)
                    connectionStr         = "tcp://" + self.liveViewerDataIp + ":%s" % self.liveViewerDataPort
                    try:
                        self.liveViewerDataSocket.connect(connectionStr)
                        self.log.info("liveViewerDataSocket started (connect) for '" + connectionStr + "'")

                        self.poller.register(self.liveViewerDataSocket, zmq.POLLIN)

                    except Exception as e:
                        self.log.error("Failed to start liveViewerDataSocket (connect): '" + connectionStr + "'")
                        self.log.debug("Error was:" + str(e))

                    try:
                        self.liveViewerComSocket.send(signal)
                        self.log.debug("configmation send back: "+ str(signal) )
                    except Exception as e:
                        self.log.error("Could not send verification to LiveViewer. Signal was: " +  str(signal))
                        self.log.debug("Error was: " + str(e))
                    continue

                elif signal == "STOP_DISPLAYER":
                    # create data socket for live viewer
                    if self.liveViewerDataSocket:
                        self.log.info("Closing liveViewerDataSocket")
                        self.liveViewerDataSocket.close(0)
                        self.liveViewerDataSocket = None

                    try:
                        self.liveViewerComSocket.send(signal)
                    except Exception as e:
                        self.log.error("Could not send verification to LiveViewer. Signal was: " +  str(signal))
                    continue

                else:
                    self.log.debug("liveViewer signal not supported: " + str(signal) )
                    self.liveViewerComSocket.send("NO_VALID_SIGNAL", zmq.NOBLOCK)


            if self.liveViewerDataSocket in socks and socks[self.liveViewerDataSocket] == zmq.POLLIN:
                signal = self.liveViewerDataSocket.recv()

                if signal == "NEXT_FILE":
                    self.log.debug("Call for next file... " + signal)
                    # send newest element in ring buffer to live viewer
                    answer = self.ringBuffer.getNewestFile()
                    try:
                        self.liveViewerDataSocket.send(answer)
                    except zmq.error.ContextTerminated:
                        self.log.error("zmq.error.ContextTerminated")
                        break
                else:
                    self.log.debug("liveViewer signal not supported: " + str(signal) )

        self.stop()


    def checkSignal(self, message):

        if len(message) != 4:

            log.info("Received signal is of the wrong format")
            log.debug("Received signal is too short or too long: " + str(message))
            return False, None, None, None

        else:

            version, signal, host, port = message
            host = host.split(',')
            port = port.split(',')

            if version:
                if helperScript.checkVersion(version, self.log):
                    self.log.debug("Versions are compatible: " + str(version))
                else:
                    self.log.debug("Version are not compatible")
                    self.sendResponse("VERSION_CONFLICT")
                    return False, None, None, None

            if signal and host and port :

                # Checking signal sending host
                self.log.debug("Check if hosts is in WhiteList...")
                if helperScript.checkHost(host, self.receiverWhiteList, self.log):
                    self.log.debug("One of the hosts is allowed to connect.")
                    self.log.debug("hosts: " + str(host))
                else:
                    self.log.debug("One of the hosts is not allowed to connect.")
                    self.log.debug("hosts: " + str(host))
                    self.sendResponse("NO_VALID_HOST")
                    return False, None, None, None

        return True, signal, host, port



    def stop(self):
        try:
            if self.receiverExchangeSocket:
                self.receiverExchangeSocket.close(0)
                self.receiverExchangeSocket = None

            if self.liveViewerComSocket:
                self.liveViewerComSocket.close(0)
                self.liveViewerComSocket = None

            if self.liveViewerDataSocket:
                self.liveViewerDataSocket.close(0)
                self.liveViewerDataSocket = None
        except Exception as e:
            self.log.debug("Closing sockets...failed")
            self.log.info("Error was: " + str(e))

        if not self.externalContext:
            try:
                if self.context:
                    self.log.info("Destroying ZMQ context...")
                    self.context.destroy()
                    self.context = None
                    self.log.debug("Destroying ZMQ context...done.")
            except Exception as e:
                self.log.error("Destroying ZMQ context...failed.")
                self.log.debug("Error was: " + str(e))
                self.log.debug(sys.exc_info())

        if self.ringBuffer:
            self.log.debug("Clearing Ringbuffer")
            self.ringBuffer.removeAll()
            self.ringBuffer = None

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
