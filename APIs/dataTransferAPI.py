# API to communicate with a data transfer unit

__version__ = '0.0.1'

import zmq
import socket
import logging
import json
import errno
import os

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

    log             = None

    connectionType       = None
    supportedConnections = ["priorityStream", "stream", "queryNext", "OnDA", "queryMetadata"]

    signalPort_MetadataOnly = "50021"
    signalPort_data         = "50000"

    prioStreamStarted    = False
    streamStarted        = False
    queryNextStarted     = False
    ondaStarted          = False
    queryMetadataStarted = False

    socketResponseTimeout = None


    def __init__(self, connectionType, signalHost, useLog = False, context = None):

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



    def initiate(self, dataPort, dataHost = False):

        if dataHost:
            self.dataHost = dataHost
        elif type(dataPort) == list:
            self.dataHost = str([socket.gethostname() for i in dataPort])
        else:
            self.dataHost = socket.gethostname()

        self.dataPort = str(dataPort)


        signal = None
        # Signal exchange
        if self.connectionType == "priorityStream":
            # for a priority stream not signal has to be exchanged,
            # this has to be configured at the sender
            return
        if self.connectionType == "stream":
            signalPort = self.signalPort_data
            signal     = "START_STREAM"
        elif self.connectionType == "queryNext":
            signalPort = self.signalPort_data
            signal     = "START_QUERY_NEXT"
        elif self.connectionType == "OnDA":
            signalPort = self.signalPort_data
            signal     = "START_REALTIME_ANALYSIS"
        elif self.connectionType == "queryMetadata":
            signalPort = self.signalPort_MetadataOnly
            signal     = "START_DISPLAYER"

        self.log.debug("create socket for signal exchange...")
        self.__createSignalSocket(signalPort)

        message = self.__sendSignal(signal)

        if message and message == "VERSION_CONFLICT":
            self.stop()
            raise Exception("Versions are conflicting.")

        elif message and message == "NO_VALID_HOST":
            self.stop()
            raise Exception("Host is not allowed to connect.")

        elif message and message == "INCORRECT_NUMBER_OF_HOSTS":
            self.stop()
            raise Exception("Specified number of hosts not working with the number of streams configured in the sender.")

        elif message and message == "INCORRECT_NUMBER_OF_PORTS":
            self.stop()
            raise Exception("Specified number of ports not working with the number of streams configured in the sender.")

        elif message and message == "NO_VALID_SIGNAL":
            self.stop()
            raise Exception("Connection type is not supported for this kind of sender.")

        # if there was no response or the response was of the wrong format, the receiver should be shut down
        elif message and message.startswith(signal):
            self.log.info("Received confirmation ...")

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
        sendMessage = [__version__, signal, self.dataHost, self.dataPort]
#        sendMessage = str(signal) + "," + str(__version__) + "," + str(self.dataHost) + "," + str(self.dataPort)
        self.log.debug("Signal: " + str(sendMessage))
        try:
            self.signalSocket.send_multipart(sendMessage)
#            self.signalSocket.send(sendMessage)
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


        alreadyConnected = self.streamStarted or self.queryNextStarted or self.ondaStarted or self.queryMetadataStarted or self.prioStreamStarted

        if alreadyConnected:
            raise Exception("Connection already started.")

        if dataPort:
            port = str(dataPort)
        elif type(self.dataPort) != list:
            port = self.dataPort
        else:
            raise Exception("Multipe possible ports. Please choose which one to use.")

        ip   = "0.0.0.0"           #TODO use IP of hostname?

        signal = None
        if self.connectionType in ["priorityStream", "stream"]:

            self.dataSocket = self.context.socket(zmq.PULL)
            # An additional socket is needed to establish the data retriving mechanism
            connectionStr = "tcp://" + ip + ":" + port
            try:
                self.dataSocket.bind(connectionStr)
                self.log.info("Socket started (bind) for '" + connectionStr + "'")
            except Exception as e:
                self.log.error("Failed to start Socket of type " + self.connectionType + " (bind): '" + connectionStr + "'")
                self.log.debug("Error was:" + str(e))

            if self.connectionType == "priorityStream":
                self.prioStreamStarted = True
            else:
                self.streamStarted = True

        elif self.connectionType in ["queryNext", "queryMetadata"]:

            self.dataSocket = self.context.socket(zmq.REQ)
            # An additional socket is needed to establish the data retriving mechanism
            connectionStr = "tcp://" + ip + ":" + port
            try:
                self.dataSocket.bind(connectionStr)
                self.log.info("Socket of type " + self.connectionType + " started (bind) for '" + connectionStr + "'")
            except Exception as e:
                self.log.error("Failed to start Socket of type " + self.connectionType + " (bind): '" + connectionStr + "'")
                self.log.debug("Error was:" + str(e))

            if self.connectionType == "queryNext":
                self.queryNextStarted = True
            else:
                self.queryMetadataStarted = True

        elif self.connectionType == "OnDA":

            self.dataSocket = self.context.socket(zmq.REQ)
            # An additional socket is needed to establish the data retriving mechanism
            connectionStr = "tcp://" + ip + ":" + port
            try:
                self.dataSocket.connect(connectionStr)
                self.log.info("Socket of type " + self.connectionType + " started (bind) for '" + connectionStr + "'")
            except Exception as e:
                self.log.error("Failed to start Socket of type " + self.connectionType + " (bind): '" + connectionStr + "'")
                self.log.debug("Error was:" + str(e))

            self.ondaStarted = True


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

        if self.prioStreamStarted or self.streamStarted:

            #run loop, and wait for incoming messages
            self.log.debug("Waiting for new messages...")
            try:
                return self.__getMultipartMessage()
            except KeyboardInterrupt:
                self.log.debug("Keyboard interrupt detected. Stopping to receive.")
                raise
            except Exception, e:
                self.log.error("Unknown error while receiving files. Need to abort.")
                self.log.debug("Error was: " + str(e))
                return None

        elif self.queryNextStarted or self.ondaStarted or self.queryMetadataStarted:

            sendMessage = "NEXT_FILE"
#            self.log.debug("Asking for next file with message " + str(sendMessage))
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
        if self.dataSocket and not self.prioStreamStarted:
            if self.streamStarted:
                signal = "STOP_STREAM"
            elif self.queryNextStarted:
                signal = "STOP_QUERY_NEXT"
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


