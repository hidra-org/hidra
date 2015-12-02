# API to communicate with a data transfer unit

from __future__ import print_function
import zmq
import socket
import logging

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


        # To send a notification that a Displayer is up and running, a communication socket is needed
        self.signalSocket = self.context.socket(zmq.REQ)
        connectionStr = "tcp://" + str(self.signalIp) + ":" + str(self.signalPort)
        self.signalSocket.connect(connectionStr)
        self.log.info( "signalSocket is connected to " + str(connectionStr))


        # An additional socket is needed to establish the data retriving mechanism
        self.dataSocket = self.context.socket(zmq.REQ)
        connectionStr = "tcp://" + str(self.dataIp) + ":" + str(self.dataPort)
        self.dataSocket.bind(connectionStr)
        self.log.info( "dataSocket is bound to " + str(connectionStr))


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
    # 13    Could not receive answer to signal
    ##
    def initConnection(self, connectionType):

        supportedConnections = ["stream", "query", "queryMetadata"]

        if connectionType not in supportedConnections:
            self.log.info("Chosen type of connection is not supported.")
            return 10

        alreadyConnected = self.streamStarted or self.queryStarted or self.queryMetadataStarted

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
            self.signalSocket.send (sendMessage)
        except Exception as e:
            self.log.info("Could not send signal")
            self.log.info("Error was: " + str(e))
            return 12

        try:
            #  Get the reply.
            message = self.signalSocket.recv()
        except Exception as e:
            self.log.info("Could not receive answer to signal")
            self.log.info("Error was: " + str(e))
            return 13

        #TODO check if received signal is correct

        if connectionType == "stream":
            self.streamStarted = True
        elif connectionType == "query":
            self.queryStarted = True
        elif connectionType == "queryMetadata":
            self.queryMetadataStarted = True


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
    def communicate(self):
        if self.streamStarted:
            pass
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


    ##
    #
    # Send signal that the displayer is quitting, close ZMQ connections, destoying context
    #
    ##
    def stop(self):
        if self.streamStarted:
            signal = "START_LIVE_VIEWER"
        elif self.queryStarted:
            signal = "START_REALTIME_ANALYSIS"
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
            self.log.info("closing ZMQ sockets...")
            if self.signalSocket:
                self.signalSocket.close(linger=0)
            if self.dataSocket:
                self.dataSocket.close(linger=0)
            self.log.info("closing ZMQ Sockets...done.")
        except Exception as e:
            self.log.info("closing ZMQ Sockets...failed.")
            self.log.info("Error was: " + str(e))

        if not self.externalContext:
            try:
                self.log.info("closing zmqContext...")
                if self.context:
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


