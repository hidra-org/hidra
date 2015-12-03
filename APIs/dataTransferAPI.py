# API to communicate with a data transfer unit

from __future__ import print_function
import zmq
import socket
import logging
import sys
import json

class dataTransferQuery():

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

    streamStarted        = False
    queryStarted         = False
    queryMetadataStarted = False

    socketResponseTimeout = None

    def __init__(self, signalPort, signalIp, dataPort, dataIp = "0.0.0.0", useLog = False, context = None):

        if useLog:
            self.log = logging.getLogger("dataTransferAPI")
        else:
            class loggingFunction:
                def __init__(self):
                    self.debug    = lambda x: print(x)
                    self.info     = lambda x: print(x)
                    self.warning  = lambda x: print(x)
                    self.error    = lambda x: print(x)
                    self.critical = lambda x: print(x)

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
        self.signalPort = signalPort
        self.dataIp     = dataIp
        self.hostname   = socket.gethostname()
        self.dataPort   = dataPort

        self.socketResponseTimeout = 1000


        # To send a notification that a Displayer is up and running, a communication socket is needed
        # create socket to exchange signals with Sender
        self.signalSocket = self.context.socket(zmq.REQ)
        # time to wait for the sender to give a confirmation of the signal
#        self.signalSocket.RCVTIMEO = self.socketResponseTimeout
        connectionStr = "tcp://" + str(self.signalIp) + ":" + str(self.signalPort)
        try:
            self.signalSocket.connect(connectionStr)
            self.log.info("signalSocket started (connect) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start signalSocket (connect): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # using a Poller to implement the signalSocket timeout (in older ZMQ version there is no option RCVTIMEO)
        self.poller = zmq.Poller()
        self.poller.register(self.signalSocket, zmq.POLLIN)


    ##
    #
    # Initailizes the data transfer socket and
    # signals the sender which kind of connection should be used
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
    def initConnection(self, connectionType):

        supportedConnections = ["stream", "query", "queryMetadata"]

        if connectionType not in supportedConnections:
            self.log.info("Chosen type of connection is not supported.")
            return 10

        alreadyConnected = self.streamStarted or self.queryStarted or self.queryMetadataStarted

        signal = None
        if connectionType == "stream" and not alreadyConnected:
            signal = "START_LIVE_VIEWER"
        elif connectionType == "query" and not alreadyConnected:
            signal = "START_REALTIME_ANALYSIS"
        elif connectionType == "queryMetadata" and not alreadyConnected:
            signal = "START_DISPLAYER"
        else:
            self.log.info("Other connection type already runnging.")
            self.log.info("More than one connection type is currently not supported.")
            return 11


        # Send the signal that the communication infrastructure should be established
        self.log.info("Sending Start Signal")
        sendMessage = str(signal) + "," + str(self.hostname) + "," + str(self.dataPort)
        try:
            self.signalSocket.send(sendMessage)
        except Exception as e:
            self.log.info("Could not send signal")
            self.log.info("Error was: " + str(e))
            return 12

        message = None
        try:
            socks = dict(self.poller.poll(self.socketResponseTimeout))
        except Exception as e:
            self.log.info("Could not poll for new message")
            self.log.info("Error was: " + str(e))
            return 13

        # if there was a response
        if self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:
            try:
                #  Get the reply.
                message = self.signalSocket.recv()
                self.log.info("Received answer to signal: " + str(message) )
            except KeyboardInterrupt:
                self.log.error("KeyboardInterrupt: No message received")
                self.stop()
                return 14
            except Exception as e:
                self.log.info("Could not receive answer to signal")
                self.log.debug("Error was: " + str(e))
                self.stop()
                return 14


        # if the response was correct
        if message and message.startswith(signal):

            self.log.info("Received confirmation ...start receiving files")

            if connectionType == "stream":
                self.dataSocket = self.context.socket(zmq.PULL)
                self.streamStarted = True
            elif connectionType == "query":
                self.dataSocket = self.context.socket(zmq.REQ)
                self.queryStarted = True
            elif connectionType == "queryMetadata":
                self.dataSocket = self.context.socket(zmq.REQ)
                self.queryMetadataStarted = True

        # if there was no response or the response was of the wrong format, the receiver should be shut down
        else:
            self.log.error("Sending start signal ...failed.")
            sys.exit(1)
            return 15

        # An additional socket is needed to establish the data retriving mechanism
        connectionStr = "tcp://" + str(self.dataIp) + ":" + str(self.dataPort)
        try:
            self.dataSocket.bind(connectionStr)
            self.log.info("dataSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start dataStreamSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        return 0



    ##
    #
    # Receives or queries for new files depending on the connection initialized
    #
    # returns either
    #   the next file
    #       (if connection type "stream" was choosen)
    #   the newest file
    #       (if connection type "query" was choosen)
    #   the path of the newest file
    #       (if connection type "queryMetadata" was choosen)
    #
    ##
    def getData(self):
        if self.streamStarted:
            #run loop, and wait for incoming messages
            self.log.debug("Waiting for new messages...")
            try:
                return self.__getMultipartMessage()
            except KeyboardInterrupt:
                self.log.debug("Keyboard interrupt detected. Stop receiving.")
                return None
            except Exception, e:
                self.log.error("Unknown error while receiving files. Need to abort.")
                self.log.debug("Error was: " + str(e))
                return None

        elif self.queryStarted:
            pass

        elif self.queryMetadataStarted:

            sendMessage = "NEXT_FILE"
            self.log.info("Asking for next file with message " + str(sendMessage))
            try:
                self.dataSocket.send (sendMessage)
            except Exception as e:
                self.log.info("Could not send request to dataSocket")
                self.log.info("Error was: " + str(e))
                return None

            try:
                #  Get the reply.
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
#        self.log.debug("multipartMessage.metadata = " + str(metadata))

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

#        self.log.debug("multipartMessage.data = " + str(payload)[:20])

        return metadata, payload


    ##
    #
    # Send signal that the displayer is quitting, close ZMQ connections, destoying context
    #
    ##
    def stop(self):
        if self.dataSocket:
            if self.streamStarted:
                signal = "STOP_LIVE_VIEWER"
            elif self.queryStarted:
                signal = "STOP_REALTIME_ANALYSIS"
            elif self.queryMetadataStarted:
                signal = "STOP_DISPLAYER"

            self.log.info("Sending Stop Signal")
            sendMessage = str(signal) + "," + str(self.hostname) + "," + str(self.dataPort)
            try:
                self.signalSocket.send (sendMessage)
                #  Get the reply.
                message = self.signalSocket.recv()
                self.log.info("Recieved signal: " + message)
            except Exception as e:
                self.log.info("Could not communicate")
                self.log.info("Error was: " + str(e))

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


