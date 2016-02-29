# API to communicate with a data transfer unit

__version__ = '0.0.1'

import zmq
import socket
import logging
import json
import errno
import os
import cPickle


class dataTransfer():

    context         = None
    externalContext = True

    signalHost      = None
    signalPort      = None
    dataIp          = None
    dataHost        = None
    dataPort        = None

    signalSocket    = None
    dataSocket      = None
    requestSocket   = None

    targets         = None

    log             = None

    connectionType       = None
    supportedConnections = ["stream", "queryNext"]

    signalPort_data      = "50000"

    streamStarted = None
    queryStarted  = None

    socketResponseTimeout = None


    def __init__(self, connectionType, signalHost = None, useLog = False, context = None):

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

        if connectionType in self.supportedConnections:
            self.connectionType = connectionType
        else:
            raise Exception("Chosen type of connection is not supported.")

        self.signalHost            = signalHost
        self.socketResponseTimeout = 1000



    # targets: [host, port, prio] or [[host, port, prio], ...]
    def initiate(self, targets):

        if type(targets) != list:
            self.stop()
            raise Excepition("Argument 'targets' must be list.")


        signal = None
        # Signal exchange
        if self.connectionType == "stream":
            signalPort = self.signalPort_data
            signal     = "START_STREAM"
        elif self.connectionType == "queryNext":
            signalPort = self.signalPort_data
            signal     = "START_QUERY_NEXT"

        self.log.debug("Create socket for signal exchange...")


        if self.signalHost:
            self.__createSignalSocket(signalPort)
        else:
            self.stop()
            raise Exception("No host to send signal to specified." )

        trg = []
        # [host, port, prio]
        if len(targets) == 3 and type(targets[0]) != str and type(targets[1]) != str and type(targets[2]) != str:
            trg = targets
        # [[host, port, prio], ...]
        else:
            for socket in sockets:
                if type(socket) == list:
                    host, port, prio = socket
                    trg.append([host + ":" + port, prio])
                else:
                    self.stop()
                    raise Exception("Argument 'targets' is of wrong format.")

#        if type(dataPort) == list:
#            self.dataHost = str([socket.gethostname() for i in dataPort])
#        else:
#            self.dataHost = socket.gethostname()

        message = self.__sendSignal(signal)

        if message and message == "VERSION_CONFLICT":
            self.stop()
            raise Exception("Versions are conflicting.")

        elif message and message == "NO_VALID_HOST":
            self.stop()
            raise Exception("Host is not allowed to connect.")

        elif message and message == "CONNECTION_ALREADY_OPEN":
            self.stop()
            raise Exception("Connection is already open.")

        elif message and message == "NO_VALID_SIGNAL":
            self.stop()
            raise Exception("Connection type is not supported for this kind of sender.")

        # if there was no response or the response was of the wrong format, the receiver should be shut down
        elif message and message.startswith(signal):
            self.log.info("Received confirmation ...")
            self.signalExchanged = True

        else:
            raise Exception("Sending start signal ...failed.")


    def __createSignalSocket(self, signalPort):

        # To send a notification that a Displayer is up and running, a communication socket is needed
        # create socket to exchange signals with Sender
        self.signalSocket = self.context.socket(zmq.REQ)

        # time to wait for the sender to give a confirmation of the signal
#        self.signalSocket.RCVTIMEO = self.socketResponseTimeout
        connectionStr = "tcp://" + str(self.signalHost) + ":" + str(signalPort)
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

        sendMessage = ["0.0.1",  signal]

        trg = cPickle.dumps(self.targets)
        sendMessage.append(trg)

#        sendMessage = [__version__, signal, self.dataHost, self.dataPort]

        self.log.debug("Signal: " + str(sendMessage))
        try:
            self.signalSocket.send_multipart(sendMessage)
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


    def start(self, dataPort = False):

#        if not self.connectionType:
#            raise Exception("No connection specified. Please initiate a connection first.")


        alreadyConnected = self.streamStarted or self.queryNextStarted


        if alreadyConnected:
            raise Exception("Connection already started.")

        ip   = "0.0.0.0"           #TODO use IP of hostname?

        if dataPort:
            if type(dataPort) == list:
                socketId = dataPort[0] + ":" + dataPort[1]
            else:
                socketId = ip + ":" + str(dataPort)
        elif len(self.targets) == 1:
            socketId = self.targets[0] + ":" + self.targets[1]
        else:
            raise Exception("Multipe possible ports. Please choose which one to use.")

        self.dataSocket = self.context.socket(zmq.PULL)
        # An additional socket is needed to establish the data retriving mechanism
        connectionStr = "tcp://" + socketId
        try:
            self.dataSocket.bind(connectionStr)
            self.log.info("Socket of type " + self.connectionType + " started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start Socket of type " + self.connectionType + " (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))


        if self.connectionType == "queryNext":

            self.requestSocket = self.context.socket(zmq.PUSH)
            # An additional socket is needed to establish the data retriving mechanism
            connectionStr = "tcp://" + socketId
            try:
                self.requestSocket.connect(connectionStr)
                self.log.info("Socket started (connect) for '" + connectionStr + "'")
            except Exception as e:
                self.log.error("Failed to start Socket of type " + self.connectionType + " (connect): '" + connectionStr + "'")
                self.log.debug("Error was:" + str(e))

            self.queryNextStarted = socketId
        else:
            self.streamStarted    = socketId




    ##
    #
    # Receives or queries for new files depending on the connection initialized
    #
    # returns either
    #   the next file
    #       (if connection type "stream" was choosen)
    #   the newest file
    #       (if connection type "queryNext" was choosen)
    #   the path of the newest file
    #       (if connection type "queryMetadata" was choosen)
    #
    ##
    def get(self):

        if not self.streamStarted and not self.queryNextStarted:
            self.log.info("Could not communicate, no connection was initialized.")
            return None


        if self.queryNextStarted :

            sendMessage = ["NEXT", self.queryNextStarted]
#            self.log.debug("Asking for next file with message " + str(sendMessage))
            try:
                self.requestSocket.send_multipart(sendMessage)
            except Exception as e:
                self.log.info("Could not send request to requestSocket")
                self.log.info("Error was: " + str(e))
                return None

        try:
            return self.__getMultipartMessage()
        except KeyboardInterrupt:
            self.log.debug("Keyboard interrupt detected. Stopping to receive.")
            raise
        except Exception as e:
            self.log.error("Unknown error while receiving files. Need to abort.")
            self.log.debug("Error was: " + str(e))
            return None


    def __getMultipartMessage(self):

        #save all chunks to file
        multipartMessage = self.dataSocket.recv_multipart()

        if len(multipartMessage) < 2:
            self.log.error("Received mutipart-message is too short. Either config or file content is missing.")
            self.log.debug("multipartMessage=" + str(mutipartMessage))

        #extract multipart message
        try:
            metadata = cPickle.loads(multipartMessage[0])
        except:
            self.log.error("Could not extract metadata from the multipart-message.")
            self.log.debug("Error was:" + str(e))

        #TODO validate multipartMessage (like correct dict-values for metadata)

        try:
            payload = multipartMessage[1:]
        except Exception as e:
            self.log.warning("An empty file was received within the multipart-message")
            self.log.debug("Error was:" + str(e))
            payload = None

        return [metadataDict, payload]


    def store(self, targetBasePath, dataObject):

        if type(dataObject) is not list and len(dataObject) != 2:
            raise Exception("Wrong input type for 'store'")

        payloadMetadata   = dataObject[0]
        payload           = dataObject[1]


        if type(payloadMetadata) is not dict or type(payload) is not list:
            raise Exception("payload: Wrong input format in 'store'")

        #save all chunks to file
        while True:

            if payloadMetadata and payload:
                #append to file
                try:
                    self.log.debug("append to file based on multipart-message...")
                    #TODO: save message to file using a thread (avoids blocking)
                    #TODO: instead of open/close file for each chunk recyle the file-descriptor for all chunks opened
                    self.__appendChunksToFile(targetBasePath, payloadMetadata, payload)
                    self.log.debug("append to file based on multipart-message...success.")
                except KeyboardInterrupt:
                    errorMessage = "KeyboardInterrupt detected. Unable to append multipart-content to file."
                    self.log.info(errorMessage)
                    break
                except Exception, e:
                    errorMessage = "Unable to append multipart-content to file."
                    self.log.error(errorMessage)
                    self.log.debug("Error was: " + str(e))
                    self.log.debug("append to file based on multipart-message...failed.")

                if len(payload) < payloadMetadata["chunkSize"] :
                    #indicated end of file. Leave loop
                    filename    = self.generateTargetFilepath(targetBasePath, payloadMetadata)
                    fileModTime = payloadMetadata["fileModificationTime"]

                    self.log.info("New file with modification time " + str(fileModTime) + " received and saved: " + str(filename))
                    break

            try:
                [payloadMetadata, payload] = self.get()
            except Exception as e:
                self.log.error("Getting data failed.")
                self.log.debug("Error was: " + str(e))
                break


    def __appendChunksToFile(self, targetBasePath, configDict, payload):

        chunkCount         = len(payload)

        #generate target filepath
        targetFilepath = self.generateTargetFilepath(targetBasePath, configDict)
        self.log.debug("new file is going to be created at: " + targetFilepath)


        #append payload to file
        try:
            newFile = open(targetFilepath, "a")
        except IOError, e:
            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:
                #TODO create subdirectory first, then try to open the file again
                try:
                    targetPath = self.__generateTargetPath(targetBasePath, configDict)
                    os.makedirs(targetPath)
                    newFile = open(targetFilepath, "w")
                    self.log.info("New target directory created: " + str(targetPath))
                except Exception, f:
                    errorMessage = "Unable to save payload to file: '" + targetFilepath + "'"
                    self.log.error(errorMessage)
                    self.log.debug("Error was: " + str(f))
                    self.log.debug("targetPath:" + str(targetPath))
                    raise Exception(errorMessage)
            else:
                self.log.error("Failed to append payload to file: '" + targetFilepath + "'")
                self.log.debug("Error was: " + str(e))
        except Exception, e:
            self.log.error("Failed to append payload to file: '" + targetFilepath + "'")
            self.log.debug("Error was: " + str(e))
            self.log.debug("ErrorTyp: " + str(type(e)))
            self.log.debug("e.errno = " + str(e.errno) + "        errno.EEXIST==" + str(errno.EEXIST))

        #only write data if a payload exist
        try:
            if payload != None:
                for chunk in payload:
                    newFile.write(chunk)
            newFile.close()
        except Exception, e:
            errorMessage = "unable to append data to file."
            self.log.error(errorMessage)
            self.log.debug("Error was: " + str(e))
            raise Exception(errorMessage)


    def generateTargetFilepath(self, basePath, configDict):
        """
        generates full path where target file will saved to.

        """
        filename     = configDict["filename"]
        relativePath = configDict["relativePath"]

        if relativePath is '' or relativePath is None:
            targetPath = basePath
        else:
            targetPath = os.path.normpath(basePath + os.sep + relativePath)

        filepath =  os.path.join(targetPath, filename)

        return filepath


    def __generateTargetPath(self, basePath, configDict):
        """
        generates path where target file will saved to.

        """
        relativePath = configDict["relativePath"]

        # if the relative path starts with a slash path.join will consider it as absolute path
        if relativePath.startswith("/"):
            relativePath = relativePath[1:]

        targetPath = os.path.join(basePath, relativePath)

        return targetPath


    ##
    #
    # Send signal that the displayer is quitting, close ZMQ connections, destoying context
    #
    ##
    def stop(self):
        if self.dataSocket and self.signalExchanged:
            if self.streamStarted:
                signal = "STOP_STREAM"
            elif self.queryNextStarted:
                signal = "STOP_QUERY_NEXT"

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
            self.log.error("closing ZMQ Sockets...failed.")
            self.log.info("Error was: " + str(e))

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.externalContext:
            try:
                if self.context:
                    self.log.info("closing ZMQ context...")
                    self.context.destroy()
                    self.context = None
                    self.log.info("closing ZMQ context...done.")
            except Exception as e:
                self.log.error("closing ZMQ context...failed.")
                self.log.debug("Error was: " + str(e))
                self.log.debug(sys.exc_info())


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


