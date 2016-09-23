# API to communicate with a data transfer unit

__version__ = '2.4.0'

import zmq
import socket
import logging
import json
import errno
import os
import traceback
from zmq.auth.thread import ThreadAuthenticator


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

class UsageError(Exception):
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

        self.currentPID            = os.getpid()

        self.signalHost            = signalHost
        self.signalPort            = "50000"
        self.requestPort           = "50001"
        self.fileOpPort            = "50050"
        self.dataHost              = None
        self.dataPort              = None
        self.ipcPath               = "/tmp/HiDRA"

        self.signalSocket          = None
        self.requestSocket         = None
        self.fileOpSocket          = None
        self.dataSocket            = None
        self.controlSocket         = None

        self.poller                = zmq.Poller()

        self.auth                  = None

        self.targets               = None

        self.supportedConnections = ["stream", "streamMetadata", "queryNext", "queryMetadata", "nexus"]

        self.signalExchanged       = None

        self.streamStarted         = None
        self.queryNextStarted      = None
        self.nexusStarted          = None

        self.socketResponseTimeout = 1000

        self.numberOfStreams       = None
        self.recvdCloseFrom        = []
        self.replyToSignal         = False
        self.allCloseRecvd         = False

        self.fileDescriptors       = dict()

        self.fileOpened            = False
        self.callbackParams        = None
        self.openCallback          = None
        self.readCallback          = None
        self.closeCallback         = None

        if connectionType in self.supportedConnections:
            self.connectionType = connectionType
        else:
            raise NotSupported("Chosen type of connection is not supported.")


    # targets: [host, port, prio] or [[host, port, prio], ...]
    def initiate (self, targets):
        if self.connectionType == "nexus":
            self.log.info("There is no need for a signal exchange for connection type 'nexus'")
            return

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


        self.__setTargets (targets)

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
        connectionStr = "tcp://{h}:{p}".format(h=self.signalHost, p=signalPort)
        try:
            self.signalSocket.connect(connectionStr)
            self.log.info("signalSocket started (connect) for '{s}'".format(s=connectionStr))
        except:
            self.log.error("Failed to start signalSocket (connect): '{s}'".format(connectionStr))
            raise

        # using a Poller to implement the signalSocket timeout (in older ZMQ version there is no option RCVTIMEO)
        self.poller.register(self.signalSocket, zmq.POLLIN)


    def __setTargets (self, targets):
        self.targets = []

        # [host, port, prio]
        if len(targets) == 3 and type(targets[0]) != list and type(targets[1]) != list and type(targets[2]) != list:
            host, port, prio = targets
            self.targets = [["{h}:{p}".format(h=host, p=port), prio, [""]]]

        # [host, port, prio, suffixes]
        elif len(targets) == 4 and type(targets[0]) != list and type(targets[1]) != list and type(targets[2]) != list and type(targets[3]) == list:
            host, port, prio, suffixes = targets
            self.targets = [["{h}:{p}".format(h=host, p=port), prio, suffixes]]

        # [[host, port, prio], ...] or [[host, port, prio, suffixes], ...]
        else:
            for t in targets:
                if type(t) == list and len(t) == 3:
                    host, port, prio = t
                    self.targets.append(["{h}:{p}".format(h=host, p=port), prio, [""]])
                elif type(t) == list and len(t) == 4 and type(t[3]):
                    host, port, prio, suffixes = t
                    self.targets.append(["{h}:{p}".format(h=host, p=port), prio, suffixes])
                else:
                    self.stop()
                    self.log.debug("targets={t}".format(t=targets))
                    raise FormatError("Argument 'targets' is of wrong format.")


    def __sendSignal (self, signal):

        if not signal:
            return

        # Send the signal that the communication infrastructure should be established
        self.log.info("Sending Signal")

        sendMessage = [__version__,  signal]

        trg = json.dumps(self.targets)
        sendMessage.append(trg)

#        sendMessage = [__version__, signal, self.dataHost, self.dataPort]

        self.log.debug("Signal: {m}".format(m=sendMessage))
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
                self.log.info("Received answer to signal: {m}".format(m=message) )

            except:
                self.log.error("Could not receive answer to signal")
                raise

        return message


    def start (self, dataSocket = False, whitelist = None):

        # Receive data only from whitelisted nodes
        if whitelist:
            if type(whitelist) == list:
                self.auth = ThreadAuthenticator(self.context)
                self.auth.start()
                for host in whitelist:
                    try:
                        if host == "localhost":
                            ip = [socket.gethostbyname(host)]
                        else:
                            hostname, tmp, ip = socket.gethostbyaddr(host)

                        self.log.debug("Allowing host {h} ({ip})".format(h=host, ip=ip[0]))
                        self.auth.allow(ip[0])
                    except:
                        self.log.error("Error was: ", exc_info=True)
                        raise AuthenticationFailed("Could not get IP of host {h}".format(h=host))
            else:
                raise FormatError("Whitelist has to be a list of IPs")


        socketIdToBind = self.streamStarted or self.queryNextStarted or self.nexusStarted

        if socketIdToBind:
            self.log.info("Reopening already started connection.")
        else:

            ip   = "0.0.0.0"           #TODO use IP of hostname?

            host = ""
            port = ""

            if dataSocket:
                if type(dataSocket) == list:
                    socketIdToBind = "{h}:{p}".format(h=dataSocket[0], p=dataSocket[1])
                    host = dataSocket[0]
                    ip   = socket.gethostbyaddr(host)[2][0]
                    port = dataSocket[1]
                else:
                    port = str(dataSocket)

                    host = socket.gethostname()
                    socketId = "{h}:{p}".format(h=host, p=port)
                    ipFromHost = socket.gethostbyaddr(host)[2]
                    if len(ipFromHost) == 1:
                        ip = ipFromHost[0]

            elif self.targets:
                if len(self.targets) == 1:
                    host, port = self.targets[0][0].split(":")
                    ipFromHost = socket.gethostbyaddr(host)[2]
                    if len(ipFromHost) == 1:
                        ip = ipFromHost[0]

                else:
                    raise FormatError("Multipe possible ports. Please choose which one to use.")
            else:
                    raise FormatError("No target specified.")

            socketId = "{h}:{p}".format(h=host, p=port)
            socketIdToBind = "{h}:{p}".format(h=ip, p=port)

#            try:
#                socket.inet_aton(ip)
#                self.log.info("IPv4 address detected ({ip}).".format(ip=ip))
#                socketIdToBind = "{h}:{p}".format(h=ip, p=port)
#                isIPv6 = False
#            except socket.error:
#                self.log.info("Not a IPv4 address ({ip}), asume it's an IPv6 address.".format(ip=ip))
#                socketIdToBind = "[{h}]:{p}".format(h=ip, p=port)
#                isIPv6 = True

#            socketIdToBind = "192.168.178.25:{p}".format(p=port)

        self.dataSocket = self.context.socket(zmq.PULL)
        # An additional socket is needed to establish the data retriving mechanism
        connectionStr = "tcp://{id}".format(id=socketIdToBind)

        if whitelist:
            self.dataSocket.zap_domain = b'global'

#        if isIPv6:
#            self.dataSocket.ipv6 = True
#            self.log.debug("Enabling IPv6 socket")

        try:
            self.dataSocket.bind(connectionStr)
#            self.dataSocket.bind("tcp://[2003:ce:5bc0:a600:fa16:54ff:fef4:9fc0]:50102")
            self.log.info("Data socket of type {t} started (bind) for '{s}'".format(t=self.connectionType, s=connectionStr))
        except:
            self.log.error("Failed to start Socket of type {t} (bind): '{s}'".format(t=self.connectionType, s=connectionStr), exc_info=True)
            raise

        self.poller.register(self.dataSocket, zmq.POLLIN)

        if self.connectionType in ["queryNext", "queryMetadata"]:

            self.requestSocket = self.context.socket(zmq.PUSH)
            # An additional socket is needed to establish the data retriving mechanism
            connectionStr = "tcp://{h}:{p}".format(h=self.signalHost, p=self.requestPort)
            try:
                self.requestSocket.connect(connectionStr)
                self.log.info("Request socket started (connect) for '{s]'".format(s=connectionStr))
            except:
                self.log.error("Failed to start Socket of type {t} (connect): '{s}'".format(t=self.connectionType, s=connectionStr), exc_info=True)
                raise

            self.queryNextStarted = socketId

        elif self.connectionType in ["nexus"]:

            self.fileOpSocket = self.context.socket(zmq.REP)
            # An additional socket is needed to get signals to open and close nexus files
            connectionStr     = "tcp://{h}:{p}".format(h=ip, p=self.fileOpPort)
            try:
                self.fileOpSocket.bind(connectionStr)
                self.log.info("File operation socket started (bind) for '{s}'".format(s=connectionStr))
            except:
                self.log.error("Failed to start Socket of type {t} (bind): '{s}'".format(t=self.connectionType, s=connectionStr), exc_info=True)

            if not os.path.exists(self.ipcPath):
                os.makedirs(self.ipcPath)

            self.controlSocket   = self.context.socket(zmq.PULL)
            controlConStr        = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="control_API")
            try:
                self.controlSocket.bind(controlConStr)
                self.log.info("Internal controlling socket started (bind) for '{s}'".format(s=controlConStr))
            except:
                self.log.error("Failed to start internal controlling socket (bind): '{s}'".format(s=controlConStr), exc_info=True)

            self.poller.register(self.fileOpSocket, zmq.POLLIN)
            self.poller.register(self.controlSocket, zmq.POLLIN)

            self.nexusStarted = socketId
        else:
            self.streamStarted    = socketId


    def read(self, callbackParams, openCallback, readCallback, closeCallback):

        if not self.connectionType == "nexus" or not self.nexusStarted:
            raise UsageError("Wrong connection type (current: {conType}) or session not started.".format(conType=self.connectionType))

        self.callbackParams = callbackParams
        self.openCallback   = openCallback
        self.readCallback   = readCallback
        self.closeCallback  = closeCallback

        while True:
            self.log.debug("polling")
            try:
                socks = dict(self.poller.poll())
            except:
                self.log.error("Could not poll for new message")
                raise

            if self.fileOpSocket in socks and socks[self.fileOpSocket] == zmq.POLLIN:
                self.log.debug("fileOpSocket is polling")

                message = self.fileOpSocket.recv_multipart()
                self.log.debug("fileOpSocket recv: {m}".format(m=message))

                if message[0] == b"CLOSE_FILE":
                    if self.allCloseRecvd:
                        self.fileOpSocket.send_multipart(message)
                        logging.debug("fileOpSocket send: {m}".format(m=message))
                        self.allCloseRecvd = False

                        self.closeCallback(self.callbackParams, multipartMessage)
                        break
                    else:
                        self.replyToSignal = message
                elif message[0] == b"OPEN_FILE":
                    self.fileOpSocket.send_multipart(message)
                    self.log.debug("fileOpSocket send: {m}".format(m=message))

                    self.openCallback(self.callbackParams, message)
                    self.fileOpened = True
#                    return message
                else:
                    self.fileOpSocket.send_multipart(["ERROR"])
                    self.log.error("Not supported message received")

            if self.dataSocket in socks and socks[self.dataSocket] == zmq.POLLIN:
                self.log.debug("dataSocket is polling")

                try:
                    multipartMessage = self.dataSocket.recv_multipart()
#                    self.log.debug("multipartMessage={m}".format(m=str(multipartMessage)[:100]))
                except:
                    self.log.error("Could not receive data due to unknown error.", exc_info=True)

                if multipartMessage[0] == b"ALIVE_TEST":
                    continue

                if len(multipartMessage) < 2:
                    self.log.error("Received mutipart-message is too short. Either config or file content is missing.")
#                    self.log.debug("multipartMessage={m}".format(m=str(multipartMessage)[:100]))
                    #TODO return errorcode

                try:
                    self.__reactOnMessage(multipartMessage)
                except KeyboardInterrupt:
                    self.log.debug("Keyboard interrupt detected. Stopping to receive.")
                    raise
                except:
                    self.log.error("Unknown error while receiving files. Need to abort.", exc_info=True)
#                    raise Exception("Unknown error while receiving files. Need to abort.")

            if self.controlSocket in socks and socks[self.controlSocket] == zmq.POLLIN:
                self.log.debug("controlSocket is polling")
                self.controlSocket.recv()
#                self.log.debug("Control signal received. Stopping.")
                raise Exception("Control signal received. Stopping.")


    def __reactOnMessage(self, multipartMessage):

        if multipartMessage[0] == b"CLOSE_FILE":
            filename = multipartMessage[1]
            id = multipartMessage[2]
            self.recvdCloseFrom.append(id)
            self.log.debug("Received close-file-signal from DataDispatcher-{id}".format(id=id))

            # get number of signals to wait for
            if not self.numberOfStreams:
                self.numberOfStreams = int(id.split("/")[1])

            # have all signals arrived?
            self.log.debug("self.recvdCloseFrom={r}, self.numberOfStreams={n}".format(r=self.recvdCloseFrom, n=self.numberOfStreams))
            if len(self.recvdCloseFrom) == self.numberOfStreams:
                self.log.info("All close-file-signals arrived")
                if self.replyToSignal:
                    self.fileOpSocket.send_multipart(self.replyToSignal)
                    self.log.debug("fileOpSocket send: {m}".format(m=self.replyToSignal))
                    self.replyToSignal = False
                    self.recvdCloseFrom = []

                    self.closeCallback(self.callbackParams, multipartMessage)
                else:
                    self.allCloseRecvd = True

            else:
                self.log.info("self.recvdCloseFrom={r}, self.numberOfStreams={n}".format(r=self.recvdCloseFrom, n=self.numberOfStreams))

        else:
            #extract multipart message
            try:
                metadata = json.loads(multipartMessage[0])
            except:
                self.log.error("Could not extract metadata from the multipart-message.", exc_info=True)
                metadata = None

            #TODO validate multipartMessage (like correct dict-values for metadata)

            try:
                payload = multipartMessage[1]
            except:
                self.log.warning("An empty file was received within the multipart-message", exc_info=True)
                payload = None

            self.readCallback(self.callbackParams, [metadata, payload])




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

        while True:
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
                    self.log.error("Receiving data..failed.", exc_info=True)
                    return [None, None]


                if multipartMessage[0] == b"ALIVE_TEST":
                    continue
                elif len(multipartMessage) < 2:
                    self.log.error("Received mutipart-message is too short. Either config or file content is missing.")
                    self.log.debug("multipartMessage={m}".format(m=str(mutipartMessage)[:100]))
                    return [None, None]

                # extract multipart message
                try:
                    metadata = json.loads(multipartMessage[0])
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
#                self.log.warning("Could not receive data in the given time.")

                if self.queryNextStarted :
                    try:
                        self.requestSocket.send_multipart(["CANCEL", self.queryNextStarted])
                    except Exception as e:
                        self.log.error("Could not cancel the next query", exc_info=True)

                return [None, None]


    def store (self, targetBasePath):

        #save all chunks to file
        while True:

            try:
                [payloadMetadata, payload] = self.get()
            except KeyboardInterrupt:
                raise
            except:
                self.log.error("Getting data failed.", exc_info=True)
                raise

            if payloadMetadata and payload:

                #generate target filepath
                targetFilepath = self.generateTargetFilepath(targetBasePath, payloadMetadata)
                self.log.debug("New chunk for file {f} received.".format(f=targetFilepath))

                #append payload to file
                #TODO: save message to file using a thread (avoids blocking)
                try:
                    self.fileDescriptors[targetFilepath].write(payload)
                # File was not open
                except KeyError:
                    try:
                        self.fileDescriptors[targetFilepath] = open(targetFilepath, "wb")
                        self.fileDescriptors[targetFilepath].write(payload)
                    except IOError, e:
                        # errno.ENOENT == "No such file or directory"
                        if e.errno == errno.ENOENT:
                            try:
                                #TODO do not create commissioning, current, local
                                targetPath = self.__generateTargetPath(targetBasePath, payloadMetadata)
                                os.makedirs(targetPath)

                                self.fileDescriptors[targetFilepath] = open(targetFilepath, "wb")
                                self.log.info("New target directory created: {p}".format(p=targetPath))
                                self.fileDescriptors[targetFilepath].write(payload)
                            except:
                                self.log.error("Unable to save payload to file: '{p}'".format(p=targetFilepath), exc_info=True)
                                self.log.debug("targetPath:{p}".format(p=targetPath))
                        else:
                            self.log.error("Failed to append payload to file: '{p}'".format(p=targetFilepath), exc_info=True)
                except KeyboardInterrupt:
                    self.log.info("KeyboardInterrupt detected. Unable to append multipart-content to file.")
                    break
                except:
                    self.log.error("Failed to append payload to file: '{p}'".format(p=targetFilepath), exc_info=True)

                if len(payload) < payloadMetadata["chunkSize"] :
                    #indicated end of file. Leave loop
                    filename    = self.generateTargetFilepath(targetBasePath, payloadMetadata)
                    fileModTime = payloadMetadata["fileModTime"]

                    self.fileDescriptors[targetFilepath].close()
                    del self.fileDescriptors[targetFilepath]

                    self.log.info("New file with modification time {t} received and saved: {n}".format(t=fileModTime, n=filename))
                    break


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
            targetPath = os.path.normpath(os.path.join(basePath, relativePath))

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
            if self.fileOpSocket:
                self.log.info("closing fileOpSocket...")
                self.fileOpSocket.close(linger=0)
                self.fileOpSocket = None
            if self.controlSocket:
                self.log.info("closing controlSocket...")
                self.controlSocket.close(linger=0)
                self.controlSocket = None

                controlConStr = "{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="control_API")
                try:
                    os.remove(controlConStr)
                    self.log.debug("Removed ipc socket: {p}".format(p=controlConStr))
                except OSError:
                    self.log.warning("Could not remove ipc socket: {p}".format(p=controlConStr))
                except:
                    self.log.warning("Could not remove ipc socket: {p}".format(p=controlConStr), exc_info=True)
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        if self.auth:
            try:
                self.auth.stop()
                self.auth = None
                self.log.info("Stopping authentication thread...done.")
            except:
                self.log.error("Stopping authentication thread...done.", exc_info=True)

        for target in self.fileDescriptors:
            self.fileDescriptors[target].close()
            self.log.warning("Not all chunks were received for file {t}".format(t=target))

        if self.fileDescriptors:
            self.fileDescriptors = dict()

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


    def forceStop (self, targets):

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
            signal     = "STOP_STREAM"
        elif self.connectionType == "streamMetadata":
            signalPort = self.signalPort
            signal     = "STOP_STREAM_METADATA"
        elif self.connectionType == "queryNext":
            signalPort = self.signalPort
            signal     = "STOP_QUERY_NEXT"
        elif self.connectionType == "queryMetadata":
            signalPort = self.signalPort
            signal     = "STOP_QUERY_METADATA"

        self.log.debug("Create socket for signal exchange...")


        if self.signalHost and not self.signalSocket:
            self.__createSignalSocket(signalPort)
        elif not self.signalHost:
            self.stop()
            raise ConnectionFailed("No host to send signal to specified." )

        self.__setTargets (targets)

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


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


