__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import os
import sys
import zmq
import logging
import traceback
from RingBuffer import RingBuffer

SHARED_PATH = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helperScript
#
#  --------------------------  class: Coordinator  --------------------------------------
#
class LiveViewCommunicator:
    zmqContext               = None
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
    liveViewerDataSocket     = None         # socket to communicate with live viewer


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

#        if context:
#            assert isinstance(context, zmq.sugar.context.Context)

        #self.zmqContext = context or zmq.Context()
        if context:
            self.zmqContext      = context
            self.externalContext = True
        else:
            self.zmqContext      = zmq.Context()
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
        finally:
            self.stop()


        self.log.info("Quitting Coordinator.")


    def getLogger(self):
        logger = logging.getLogger("coordinator")
        return logger


    def createSockets(self):
        # create socket to exchange informations with FileReceiver
        self.receiverExchangeSocket = self.zmqContext.socket(zmq.PAIR)
        connectionStr               = "tcp://" + self.receiverExchangeIp + ":%s" % self.receiverExchangePort
        try:
            self.receiverExchangeSocket.bind(connectionStr)
            self.log.info("receiverExchangeSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start receiverExchangeSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # create communication socket for live viewer
        self.liveViewerComSocket = self.zmqContext.socket(zmq.REP)
        connectionStr         = "tcp://" + self.liveViewerComIp + ":%s" % self.liveViewerComPort
        try:
            self.liveViewerComSocket.bind(connectionStr)
            self.log.info("liveViewerComSocket started (connect) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start liveViewerComSocket (connect): '" + connectionStr + "'")
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
                message = self.liveViewerComSocket.recv()

                signal, signalHostname, port = helperScript.checkSignal(message, self.liveViewerWhiteList, self.liveViewerComSocket, self.log)

                if signal == "START_DISPLAYER":
                    if self.liveViewerDataSocket:
                        self.liveViewerDataSocket.close(0)
                        self.liveViewerDataSocket = None
                        self.log.debug("liveViewerDataSocket refreshed")

                    # create data socket for live viewer
                    self.liveViewerDataIp   = signalHostname
                    self.liveViewerDataPort = port
                    self.liveViewerDataSocket = self.zmqContext.socket(zmq.REP)
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

    def stop(self):
        self.log.debug("Closing socket")
        self.receiverExchangeSocket.close(0)
        self.liveViewerComSocket.close(0)

        if self.liveViewerDataSocket:
            self.liveViewerDataSocket.close(0)

        if not self.externalContext:
            self.log.debug("Destroying context")
            try:
                self.zmqContext.destroy()
                self.log.debug("closing ZMQ context...done.")
            except:
                self.log.error("closing ZMQ context...failed.")
                self.log.error(sys.exc_info())

        self.log.debug("Clearing Ringbuffer")
        self.ringBuffer.removeAll()
