# API to communicate with receiverLiveViewer

import zmq
import socket


class ReceiverQuery():

    context      = None

    signalIp     = None
    signalPort   = None
    dataIp       = None
    hostname     = None
    dataPort     = None

    signalSocket = None
    dataSocket   = None

    context = None


    def __init__(self, signalPort, signalIp, dataPort, dataIp = "0.0.0.0"):

        # ZMQ applications always start by creating a context,
        # and then using that for creating sockets
        # (source: ZeroMQ, Messaging for Many Applications by Pieter Hintjens)
        self.context = zmq.Context()

        self.signalIp   = signalIp
        self.signalPort = signalPort
        self.dataIp     = dataIp
        self.hostname   = socket.gethostname()
        self.dataPort   = dataPort


        # To send a notification that a Displayer is up and running, a communication socket is needed
        self.signalSocket = self.context.socket(zmq.REQ)
        connectionStr = "tcp://" + str(self.signalIp) + ":" + str(self.signalPort)
        print connectionStr
        self.signalSocket.connect(connectionStr)
        print  "signalSocket is connected to ", connectionStr


        # An additional socket is needed to establish the data retriving mechanism
        self.dataSocket = self.context.socket(zmq.REQ)
        connectionStr = "tcp://" + str(self.dataIp) + ":" + str(self.dataPort)
        self.dataSocket.bind(connectionStr)
        print  "dataSocket is bound to ", connectionStr


        # Send the signal that the communication infrastructure should be established
        print "Sending Start Signal to receiver"
        sendMessage = "START_DISPLAYER," + str(self.hostname) + "," + str(self.dataPort)
        try:
            self.signalSocket.send (sendMessage)
            #  Get the reply.
            message = self.signalSocket.recv()
            print "Recieved signal: ", message
        except Exception as e:
            print "Could not communicate with receiver"
            print "Error was: ", e


    ##
    #
    # Queries the receiver for new files
    #
    # returns the path of the newest file
    #
    ##
    def communicateWithReceiver(self):

        sendMessage = "NEXT_FILE"
        print "Asking for next file with message", sendMessage
        try:
            self.dataSocket.send (sendMessage)
        except Exception as e:
            print "Could not communicate with receiver"
            print "Error was: ", e
            return None

        try:
            #  Get the reply.
            message = self.dataSocket.recv()
        except Exception as e:
            message = ""
            print "Could not communicate with receiver"
            print "Error was: ", e
            return None

        return message


    ##
    #
    # Send signal that the displayer is quitting, close ZMQ connections, destoying context
    #
    ##
    def stopZmq(self):

        print "Sending Start Signal to receiver"
        sendMessage = "STOP_DISPLAYER," + str(self.hostname) + "," + str(self.dataPort)
        try:
            self.signalSocket.send (sendMessage)
            #  Get the reply.
            message = self.signalSocket.recv()
            print "Recieved signal: ", message
        except Exception as e:
            print "Could not communicate with receiver"
            print "Error was: ", e

        try:
            print "closing ZMQ sockets..."
            if self.signalSocket:
                self.signalSocket.close(linger=0)
            if self.dataSocket:
                self.dataSocket.close(linger=0)
            print "closing ZMQ Sockets...done."
        except Exception as e:
            print "closing ZMQ Sockets...failed."
            print e

        try:
            print"closing zmqContext..."
            if self.context:
                self.context.destroy()
                self.context = None
            "closing zmqContext...done."
        except Exception as e:
            print "closing zmqContext...failed."
            print e


    def __exit__(self):
        self.stopZmq()


    def __del__(self):
        self.stopZmq()


