# API to communicate with a data transfer unit

__version__ = '0.0.1'

import zmq
import socket
import logging
import json
import errno
import os
import cPickle
import traceback


class loggingFunction:
    def out (self, x, exc_info = None):
        if exc_info:
            print x, traceback.format_exc()
        else:
            print x
    def __init__ (self):
        self.debug    = lambda x, exc_info=None: self.out(x, exc_info)
        self.info     = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning  = lambda x, exc_info=None: self.out(x, exc_info)
        self.error    = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


class noLoggingFunction:
    def out (self, x, exc_info = None):
        pass
    def __init__ (self):
        self.debug    = lambda x, exc_info=None: self.out(x, exc_info)
        self.info     = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning  = lambda x, exc_info=None: self.out(x, exc_info)
        self.error    = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


class NotSupported(Exception):
    pass

class FormatError(Exception):
    pass

class ConnectionFailed(Exception):
    pass

class VersionError(Exception):
    pass

class AuthenticationFailed(Exception):
    pass

class CommunicationFailed(Exception):
    pass

class DataSavingError(Exception):
    pass


class dataTransfer():
    def __init__ (self, connectionType, signalHost = None, useLog = False, context = None):

        if useLog:
            self.log = logging.getLogger("dataTransferAPI")
        elif useLog == None:
            self.log = noLoggingFunction()
        else:
            self.log = loggingFunction()

        # ZMQ applications always start by creating a context,
        # and then using that for creating sockets
        # (source: ZeroMQ, Messaging for Many Applications by Pieter Hintjens)
        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False


        self.signalHost            = signalHost
        self.signalPort            = "50000"
        self.requestPort           = "50001"
        self.dataHost              = None
        self.dataPort              = None

        self.signalSocket          = None
        self.dataSocket            = None
        self.requestSocket         = None
        self.poller                = zmq.Poller()

        self.targets               = None

        self.supportedConnections = ["stream", "streamMetadata", "queryNext", "queryMetadata"]

        self.signalExchanged       = None

        self.streamStarted         = None
        self.queryNextStarted      = None

        self.socketResponseTimeout = 1000

        if connectionType in self.supportedConnections:
            self.connectionType = connectionType
        else:
            raise NotSupported("Chosen type of connection is not supported.")


    # targets: [host, port, prio] or [[host, port, prio], ...]
    def initiate (self, targets):

        if type(targets) != list:
            self.stop()
            raise FormatError("Argument 'targets' must be list.")

        if not self.context:
            self.context    = zmq.Context()
            self.extContext = False

        signal = None
        # Signal exchange
        if self.connectionType == "stream":
            signalPort = self.signalPort
            signal     = "START_STREAM"
        elif self.connectionType == "streamMetadata":
            signalPort = self.signalPort
            signal     = "START_STREAM_METADATA"
        elif self.connectionType == "queryNext":
            signalPort = self.signalPort
            signal     = "START_QUERY_NEXT"
        elif self.connectionType == "queryMetadata":
            signalPort = self.signalPort
            signal     = "START_QUERY_METADATA"

        self.log.debug("Create socket for signal exchange...")


        if self.signalHost:
            self.__createSignalSocket(signalPort)
        else:
            self.stop()
            raise ConnectionFailed("No host to send signal to specified." )

        self.targets = []
        # [host, port, prio]
        if len(targets) == 3 and type(targets[0]) != list and type(targets[1]) != list and type(targets[2]) != list:
            host, port, prio = targets
            self.targets = [[host + ":" + port, prio]]
        # [[host, port, prio], ...]
        else:
            for t in targets:
                if type(t) == list and len(t) == 3:
                    host, port, prio = t
                    self.targets.append([host + ":" + port, prio])
                else:
                    self.stop()
                    self.log.debug("targets=" + str(targets))
                    raise FormatError("Argument 'targets' is of wrong format.")

#        if type(dataPort) == list:
#            self.dataHost = str([socket.gethostname() for i in dataPort])
#        else:
#            self.dataHost = socket.gethostname()

        message = self.__sendSignal(signal)

        if message and message == "VERSION_CONFLICT":
            self.stop()
            raise VersionError("Versions are conflicting.")

        elif message and message == "NO_VALID_HOST":
            self.stop()
            raise AuthenticationFailed("Host is not allowed to connect.")

        elif message and message == "CONNECTION_ALREADY_OPEN":
            self.stop()
            raise CommunicationFailed("Connection is already open.")

        elif message and message == "NO_VALID_SIGNAL":
            self.stop()
            raise CommunicationFailed("Connection type is not supported for this kind of sender.")

        # if there was no response or the response was of the wrong format, the receiver should be shut down
        elif message and message.startswith(signal):
            self.log.info("Received confirmation ...")
            self.signalExchanged = signal

        else:
            raise CommunicationFailed("Sending start signal ...failed.")


    def __createSignalSocket (self, signalPort):

        # To send a notification that a Displayer is up and running, a communication socket is needed
        # create socket to exchange signals with Sender
        self.signalSocket = self.context.socket(zmq.REQ)

        # time to wait for the sender to give a confirmation of the signal
#        self.signalSocket.RCVTIMEO = self.socketResponseTimeout
        connectionStr = "tcp://" + str(self.signalHost) + ":" + str(signalPort)
        try:
            self.signalSocket.connect(connectionStr)
            self.log.info("signalSocket started (connect) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start signalSocket (connect): '" + connectionStr + "'")
            raise

        # using a Poller to implement the signalSocket timeout (in older ZMQ version there is no option RCVTIMEO)
        self.poller.register(self.signalSocket, zmq.POLLIN)



    def __sendSignal (self, signal):

        if not signal:
            return

        # Send the signal that the communication infrastructure should be established
        self.log.info("Sending Signal")

        sendMessage = ["0.0.1",  signal]

        trg = cPickle.dumps(self.targets)
        sendMessage.append(trg)

#        sendMessage = [__version__, signal, self.dataHost, self.dataPort]

        self.log.debug("Signal: " + str(sendMessage))
        try:
            self.signalSocket.send_multipart(sendMessage)
        except:
            self.log.error("Could not send signal")
            raise

        message = None
        try:
            socks = dict(self.poller.poll(self.socketResponseTimeout))
        except:
            self.log.error("Could not poll for new message")
            raise


        # if there was a response
        if self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:
            try:
                #  Get the reply.
                message = self.signalSocket.recv()
                self.log.info("Received answer to signal: " + str(message) )

            except:
                self.log.error("Could not receive answer to signal")
                raise

        return message


    def start (self, dataSocket = False):

        alreadyConnected = self.streamStarted or self.queryNextStarted

        #TODO Do I need to raise an exception here?
        if alreadyConnected:
#            raise Exception("Connection already started.")
            self.log.info("Connection already started.")
            return

        ip   = "0.0.0.0"           #TODO use IP of hostname?

        host = ""
        port = ""
        if dataSocket:
            if type(dataSocket) == list:
                socketIdToConnect = dataSocket[0] + ":" + dataSocket[1]
                host = dataSocket[0]
                ip   = socket.gethostbyaddr(host)[2][0]
                port = dataSocket[1]
            else:
                port = str(dataSocket)

                host = socket.gethostname()
                socketId = host + ":" + port
                ipFromHost = socket.gethostbyaddr(host)[2]
                if len(ipFromHost) == 1:
                    ip = ipFromHost[0]

        elif len(self.targets) == 1:
            host, port = self.targets[0][0].split(":")
            ipFromHost = socket.gethostbyaddr(host)[2]
            if len(ipFromHost) == 1:
                ip = ipFromHost[0]

        else:
            raise FormatError("Multipe possible ports. Please choose which one to use.")

        socketId = host + ":" + port
        socketIdToConnect = ip + ":" + port

        self.dataSocket = self.context.socket(zmq.PULL)
        # An additional socket is needed to establish the data retriving mechanism
        connectionStr = "tcp://" + socketIdToConnect
        try:
            self.dataSocket.bind(connectionStr)
            self.log.info("Data socket of type " + self.connectionType + " started (bind) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start Socket of type " + self.connectionType + " (bind): '" + connectionStr + "'", exc_info=True)

        self.poller.register(self.dataSocket, zmq.POLLIN)

        if self.connectionType in ["queryNext", "queryMetadata"]:

            self.requestSocket = self.context.socket(zmq.PUSH)
            # An additional socket is needed to establish the data retriving mechanism
            connectionStr = "tcp://" + self.signalHost + ":" + self.requestPort
            try:
                self.requestSocket.connect(connectionStr)
                self.log.info("Request socket started (connect) for '" + connectionStr + "'")
            except:
                self.log.error("Failed to start Socket of type " + self.connectionType + " (connect): '" + connectionStr + "'", exc_info=True)

            self.queryNextStarted = socketId
        else:
            self.streamStarted    = socketId


    ##
    #
    # Receives or queries for new files depending on the connection initialized
    #
    # returns either
    #   the newest file
    #       (if connection type "queryNext" or "stream" was choosen)
    #   the path of the newest file
    #       (if connection type "queryMetadata" or "streamMetadata" was choosen)
    #
    ##
    def get (self, timeout=None):

        if not self.streamStarted and not self.queryNextStarted:
            self.log.info("Could not communicate, no connection was initialized.")
            return None, None

        if self.queryNextStarted :

            sendMessage = ["NEXT", self.queryNextStarted]
            try:
                self.requestSocket.send_multipart(sendMessage)
            except Exception as e:
                self.log.error("Could not send request to requestSocket", exc_info=True)
                return None, None

        # receive data
        if timeout:
            try:
                socks = dict(self.poller.poll(timeout))
            except:
                self.log.error("Could not poll for new message")
                raise
        else:
            try:
                socks = dict(self.poller.poll())
            except:
                self.log.error("Could not poll for new message")
                raise

        # if there was a response
        if self.dataSocket in socks and socks[self.dataSocket] == zmq.POLLIN:

            try:
                multipartMessage = self.dataSocket.recv_multipart()
            except:
                self.log.error("Receiving files..failed.")
                return [None, None]

            if len(multipartMessage) < 2:
                self.log.error("Received mutipart-message is too short. Either config or file content is missing.")
                self.log.debug("multipartMessage=" + str(mutipartMessage))
                return [None, None]

            # extract multipart message
            try:
                metadata = cPickle.loads(multipartMessage[0])
            except:
                self.log.error("Could not extract metadata from the multipart-message.", exc_info=True)
                metadata = None

            #TODO validate multipartMessage (like correct dict-values for metadata)

            try:
                payload = multipartMessage[1]
            except:
                self.log.warning("An empty file was received within the multipart-message", exc_info=True)
                payload = None

            return [metadata, payload]
        else:
            self.log.warning("Could not receive data in the given time.")

            if self.queryNextStarted :
                try:
                    self.requestSocket.send_multipart(["CANCEL", self.queryNextStarted])
                except Exception as e:
                    self.log.error("Could not cancel the next query", exc_info=True)

            return [None, None]


    def store (self, targetBasePath, dataObject):

        if type(dataObject) is not list and len(dataObject) != 2:
            raise FormatError("Wrong input type for 'store'")

        payloadMetadata   = dataObject[0]
        payload           = dataObject[1]


        if type(payloadMetadata) is not dict or type(payload) is not list:
            raise FormatError("payload: Wrong input format in 'store'")

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
                    self.log.info("KeyboardInterrupt detected. Unable to append multipart-content to file.")
                    break
                except Exception, e:
                    self.log.error("Unable to append multipart-content to file.", exc_info=True)
                    self.log.debug("Append to file based on multipart-message...failed.")

                if len(payload) < payloadMetadata["chunkSize"] :
                    #indicated end of file. Leave loop
                    filename    = self.generateTargetFilepath(targetBasePath, payloadMetadata)
                    fileModTime = payloadMetadata["fileModTime"]

                    self.log.info("New file with modification time " + str(fileModTime) + " received and saved: " + str(filename))
                    break

            try:
                [payloadMetadata, payload] = self.get()
            except:
                self.log.error("Getting data failed.", exc_info=True)
                break


    def __appendChunksToFile (self, targetBasePath, configDict, payload):

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
                try:
                    #TODO do not create commissioning, current, local
                    targetPath = self.__generateTargetPath(targetBasePath, configDict)
                    os.makedirs(targetPath)
                    newFile = open(targetFilepath, "w")
                    self.log.info("New target directory created: " + str(targetPath))
                except:
                    self.log.error("Unable to save payload to file: '" + targetFilepath + "'")
                    self.log.debug("targetPath:" + str(targetPath))
                    raise
            else:
                self.log.error("Failed to append payload to file: '" + targetFilepath + "'")
                raise
        except:
            self.log.error("Failed to append payload to file: '" + targetFilepath + "'")
#            self.log.debug("e.errno = " + str(e.errno) + "        errno.EEXIST==" + str(errno.EEXIST))
            raise

        #only write data if a payload exist
        try:
            if payload != None:
                for chunk in payload:
                    newFile.write(chunk)
            newFile.close()
        except:
            self.log.error("Unable to append data to file.")
            raise


    def generateTargetFilepath (self, basePath, configDict):
        """
        generates full path where target file will saved to.

        """
        if not configDict:
            return None

        filename     = configDict["filename"]
        #TODO This is due to Windows path names, check if there has do be done anything additionally to work
        # e.g. check sourcePath if it's a windows path
        relativePath = configDict["relativePath"].replace('\\', os.sep)

        if relativePath is '' or relativePath is None:
            targetPath = basePath
        else:
            targetPath = os.path.normpath(basePath + os.sep + relativePath)

        filepath =  os.path.join(targetPath, filename)

        return filepath


    def __generateTargetPath (self, basePath, configDict):
        """
        generates path where target file will saved to.

        """
        #TODO This is due to Windows path names, check if there has do be done anything additionally to work
        # e.g. check sourcePath if it's a windows path
        relativePath = configDict["relativePath"].replace('\\', os.sep)

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
    def stop (self):
        if self.signalSocket and self.signalExchanged:
            self.log.info("Sending close signal")
            signal = None
            if self.streamStarted or ( "STREAM" in self.signalExchanged):
                signal = "STOP_STREAM"
            elif self.queryNextStarted or ( "QUERY" in self.signalExchanged):
                signal = "STOP_QUERY_NEXT"


            message = self.__sendSignal(signal)
            #TODO need to check correctness of signal?

            self.streamStarted    = None
            self.queryNextStarted = None

        try:
            if self.signalSocket:
                self.log.info("closing signalSocket...")
                self.signalSocket.close(linger=0)
                self.signalSocket = None
            if self.dataSocket:
                self.log.info("closing dataSocket...")
                self.dataSocket.close(linger=0)
                self.dataSocket = None
            if self.requestSocket:
                self.log.info("closing requestSocket...")
                self.requestSocket.close(linger=0)
                self.requestSocket = None
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.extContext and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


