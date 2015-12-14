# API to communicate with a data transfer unit

__version__ = '0.0.1'

import zmq
import socket
import logging
import json

class dataTransfer():

    context         = None
    externalContext = True

    signalIp        = None
    signalPort      = None
    dataIp          = None
    hostname        = None
    dataPort        = None

    signalSocket    = None
    dataSocket      = None

    log             = None

    supportedConnections = ["priorityStream", "stream", "queryNext", "OnDA", "queryMetadata"]

    signalPort_MetadataOnly = "50021"
    signalPort_data         = "50000"

    prioStreamStarted    = False
    streamStarted        = False
    queryNextStarted     = False
    ondaStarted          = False
    queryMetadataStarted = False

    socketResponseTimeout = None

    def __init__(self, signalIp, dataPort, dataIp = "0.0.0.0", useLog = False, context = None):

        if useLog:
            self.log = logging.getLogger("dataTransferAPI")
        else:
            class loggingFunction:
                def out(self, x):
                    print x
                def __init__(self):
                    self.debug    = lambda x: self.out(x)
                    self.info     = lambda x: self.out(x)
                    self.warning  = lambda x: self.out(x)
                    self.error    = lambda x: self.out(x)
                    self.critical = lambda x: self.out(x)

            self.log = loggingFunction()

        # ZMQ applications always start by creating a context,
        # and then using that for creating sockets
        # (source: ZeroMQ, Messaging for Many Applications by Pieter Hintjens)
        if context:
            self.context         = context
            self.externalContext = True
        else:
            self.context         = zmq.Context()
            self.externalContext = False


        self.signalIp   = signalIp
        self.dataIp     = dataIp
        self.hostname   = socket.gethostname()
        self.dataPort   = dataPort

        self.socketResponseTimeout = 1000



    ##
    #
    # Initailizes the signal and data transfer sockets and
    # send a signal which the kind of connection to be established
    #
    # Returns 0 if the connection could be initializes without errors
    # Error Codes are:
    # 10    if the connection type is not supported
    # 11    if there is already one connection running
    # 12    Could not send signal
    # 13    Could not poll new for new message
    # 14    Could not receive answer to signal
    # 15    if the response was not correct
    #
    ##
    def start(self, connectionType):

        if connectionType not in self.supportedConnections:
            raise Exception("Chosen type of connection is not supported.")

        alreadyConnected = self.streamStarted or self.queryNextStarted or self.ondaStarted or self.queryMetadataStarted or self.prioStreamStarted

        signal = None
        if connectionType == "priorityStream" and not alreadyConnected:

            self.dataSocket = self.context.socket(zmq.PULL)
            # An additional socket is needed to establish the data retriving mechanism
            connectionStr = "tcp://" + str(self.dataIp) + ":" + str(self.dataPort)
            try:
                self.dataSocket.bind(connectionStr)
                self.log.info("dataSocket started (bind) for '" + connectionStr + "'")
            except Exception as e:
                self.log.error("Failed to start dataStreamSocket (bind): '" + connectionStr + "'")
                self.log.debug("Error was:" + str(e))

            self.prioStreamStarted = True

        else:

            if connectionType == "stream" and not alreadyConnected:
                signalPort = self.signalPort_data
                signal     = "START_LIVE_VIEWER"
            elif connectionType == "queryNext" and not alreadyConnected:
                signalPort = self.signalPort_data
                signal     = "START_QUERY_NEWEST"
            elif connectionType == "OnDA" and not alreadyConnected:
                signalPort = self.signalPort_data
                signal     = "START_REALTIME_ANALYSIS"
            elif connectionType == "queryMetadata" and not alreadyConnected:
                signalPort = self.signalPort_MetadataOnly
                signal     = "START_DISPLAYER"
            else:
                raise Exception("Other connection type already running.\
                        More than one connection type is currently not supported.")

            self.__creatSignalSocket(signalPort)

            message = self.__sendSignal(signal)

            if message and message == "NO_VALID_SIGNAL":
                raise Exception("Connection type is not supported for this kind of sender.")

            # if the response was correct
            elif message and message.startswith(signal):

                self.log.info("Received confirmation ...start receiving files")

                if connectionType == "stream":

                    self.dataSocket = self.context.socket(zmq.PULL)
                    # An additional socket is needed to establish the data retriving mechanism
                    connectionStr = "tcp://" + str(self.dataIp) + ":" + str(self.dataPort)
                    try:
                        self.dataSocket.bind(connectionStr)
                        self.log.info("dataSocket started (bind) for '" + connectionStr + "'")
                    except Exception as e:
                        self.log.error("Failed to start dataStreamSocket (bind): '" + connectionStr + "'")
                        self.log.debug("Error was:" + str(e))

                    self.streamStarted = True

                elif connectionType == "queryNext":

                    self.dataSocket = self.context.socket(zmq.REQ)
                    # An additional socket is needed to establish the data retriving mechanism
                    connectionStr = "tcp://" + str(self.dataIp) + ":" + str(self.dataPort)
                    try:
                        self.dataSocket.bind(connectionStr)
                        self.log.info("dataSocket started (bind) for '" + connectionStr + "'")
                    except Exception as e:
                        self.log.error("Failed to start dataStreamSocket (bind): '" + connectionStr + "'")
                        self.log.debug("Error was:" + str(e))

                    self.queryNextStarted = True

                elif connectionType == "OnDA":

                    self.dataSocket = self.context.socket(zmq.REQ)
                    # An additional socket is needed to establish the data retriving mechanism
                    connectionStr = "tcp://" + str(self.dataIp) + ":" + str(self.dataPort)
                    try:
                        self.dataSocket.connect(connectionStr)
                        self.log.info("dataSocket started (bind) for '" + connectionStr + "'")
                    except Exception as e:
                        self.log.error("Failed to start dataStreamSocket (bind): '" + connectionStr + "'")
                        self.log.debug("Error was:" + str(e))

                    self.ondaStarted = True

                elif connectionType == "queryMetadata":

                    self.dataSocket = self.context.socket(zmq.REQ)
                    # An additional socket is needed to establish the data retriving mechanism
                    connectionStr = "tcp://" + str(self.dataIp) + ":" + str(self.dataPort)
                    try:
                        self.dataSocket.bind(connectionStr)
                        self.log.info("dataSocket started (bind) for '" + connectionStr + "'")
                    except Exception as e:
                        self.log.error("Failed to start dataStreamSocket (bind): '" + connectionStr + "'")
                        self.log.debug("Error was:" + str(e))

                    self.queryMetadataStarted = True

            # if there was no response or the response was of the wrong format, the receiver should be shut down
            else:
                raise Exception("Sending start signal ...failed.")


        return 0


    def __creatSignalSocket(self, signalPort):

        # To send a notification that a Displayer is up and running, a communication socket is needed
        # create socket to exchange signals with Sender
        self.signalSocket = self.context.socket(zmq.REQ)

        # time to wait for the sender to give a confirmation of the signal
#        self.signalSocket.RCVTIMEO = self.socketResponseTimeout
        connectionStr = "tcp://" + str(self.signalIp) + ":" + str(signalPort)
        try:
            self.signalSocket.connect(connectionStr)
            self.log.info("signalSocket started (connect) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start signalSocket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))
            raise

        # using a Poller to implement the signalSocket timeout (in older ZMQ version there is no option RCVTIMEO)
        self.poller = zmq.Poller()
        self.poller.register(self.signalSocket, zmq.POLLIN)


    def __sendSignal(self, signal):

        # Send the signal that the communication infrastructure should be established
        self.log.info("Sending Signal")
        sendMessage = str(signal) + "," + str(self.hostname) + "," + str(self.dataPort) + "," + str(__version__)
        self.log.debug("Signal: " + sendMessage)
        try:
            self.signalSocket.send(sendMessage)
        except Exception as e:
            self.log.error("Could not send signal")
            self.log.info("Error was: " + str(e))
            raise

        message = None
        try:
            socks = dict(self.poller.poll(self.socketResponseTimeout))
        except Exception as e:
            self.log.error("Could not poll for new message")
            self.log.info("Error was: " + str(e))
            raise


        # if there was a response
        if self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:
            try:
                #  Get the reply.
                message = self.signalSocket.recv()
                self.log.info("Received answer to signal: " + str(message) )
            except KeyboardInterrupt:
                self.log.error("KeyboardInterrupt: No message received")
                self.stop()
                raise
            except Exception as e:
                self.log.error("Could not receive answer to signal")
                self.log.debug("Error was: " + str(e))
                self.stop()
                raise

        return message


    ##
    #
    # Receives or queries for new files depending on the connection initialized
    #
    # returns either
    #   the next file
    #       (if connection type "stream" was choosen)
    #   the newest file
    #       (if connection type "queryNewest" was choosen)
    #   the path of the newest file
    #       (if connection type "queryMetadata" was choosen)
    #
    ##
    def getData(self):

        if self.prioStreamStarted or self.streamStarted:

            #run loop, and wait for incoming messages
            self.log.debug("Waiting for new messages...")
            try:
                return self.__getMultipartMessage()
            except KeyboardInterrupt:
                self.log.debug("Keyboard interrupt detected. Stopping to receive.")
                return None
            except Exception, e:
                self.log.error("Unknown error while receiving files. Need to abort.")
                self.log.debug("Error was: " + str(e))
                return None

        elif self.queryNextStarted or self.ondaStarted or self.queryMetadataStarted:

            sendMessage = "NEXT_FILE"
            self.log.info("Asking for next file with message " + str(sendMessage))
            try:
                self.dataSocket.send(sendMessage)
            except Exception as e:
                self.log.info("Could not send request to dataSocket")
                self.log.info("Error was: " + str(e))
                return None

            try:
                #  Get the reply.
                if self.queryNextStarted or self.ondaStarted:
                    message = self.dataSocket.recv_multipart()
                else:
                    message = self.dataSocket.recv()
            except Exception as e:
                message = ""
                self.log.info("Could not receive answer to request")
                self.log.info("Error was: " + str(e))
                return None

            return message

        else:
            self.log.info("Could not communicate, no connection was initialized.")
            return None


    def __getMultipartMessage(self):

        receivingMessages = True
        #save all chunks to file
        multipartMessage = self.dataSocket.recv_multipart()

        #extract multipart message
        try:
            #TODO is string conversion needed here?
            metadata = str(multipartMessage[0])
        except:
            self.log.error("An empty config was transferred for the multipart-message.")

        #TODO validate multipartMessage (like correct dict-values for metadata)

        #extraction metadata from multipart-message
        try:
            metadataDict = json.loads(metadata)
        except Exception as e:
            self.log.error("Could not extract metadata from the multipart-message.")
            self.log.debug("Error was:" + str(e))

        try:
            payload = multipartMessage[1:]
        except Exception as e:
            self.log.warning("An empty file was received within the multipart-message")
            self.log.debug("Error was:" + str(e))
            payload = None

        return [metadata, payload]


    ##
    #
    # Send signal that the displayer is quitting, close ZMQ connections, destoying context
    #
    ##
    def stop(self):
        if self.dataSocket and not self.prioStreamStarted:
            if self.streamStarted:
                signal = "STOP_LIVE_VIEWER"
            elif self.queryNextStarted:
                signal = "STOP_QUERY_NEWEST"
            elif self.ondaStarted:
                signal = "STOP_REALTIME_ANALYSIS"
            elif self.queryMetadataStarted:
                signal = "STOP_DISPLAYER"

            message = self.__sendSignal(signal)
            #TODO need to check correctness of signal?

        try:
            if self.signalSocket:
                self.log.info("closing signalSocket...")
                self.signalSocket.close(linger=0)
                self.signalSocket = None
            if self.dataSocket:
                self.log.info("closing dataSocket...")
                self.dataSocket.close(linger=0)
                self.dataSocket = None
        except Exception as e:
            self.log.info("closing ZMQ Sockets...failed.")
            self.log.info("Error was: " + str(e))

        if not self.externalContext:
            try:
                if self.context:
                    self.log.info("closing zmqContext...")
                    self.context.destroy()
                    self.context = None
                    self.log.info("closing zmqContext...done.")
            except Exception as e:
                self.log.info("closing zmqContext...failed.")
                self.log.info("Error was: " + str(e))


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


