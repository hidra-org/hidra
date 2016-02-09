__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'

import time
import zmq
import logging
import os
import sys
import traceback
import copy
from multiprocessing import Process
from WorkerProcess import WorkerProcess

#path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SHARED_PATH = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helperScript

#
#  --------------------------  class: SignalHandler  --------------------------------------
#
class SignalHandler():
    def __init__(self,
                 whiteList,
                 comPort, signalFwPort, requestPort,
                 context = None):

        # to get the logging only handling this class
        log                 = None


        self.context         = context or zmq.Context()
        self.localhost       = "127.0.0.1"
        self.extIp           = "0.0.0.0"
        self.comPort         = comPort
        self.signalFwPort    = signalFwPort
        self.requestPort     = requestPort
        self.openConnections = []

        self.openRequVari    = []
        self.openRequPerm    = []
        self.allowedQueries  = []

        self.whiteList       = []

        #remove .desy.de from hostnames
        for host in whiteList:
            if host.endswith(".desy.de"):
                self.whiteList.append(host[:-8])
            else:
                self.whiteList.append(host)

        # sockets
        self.comSocket      = None
        self.signalFwSocket = None
        self.requestSocket  = None

        self.log = self.getLogger()
        self.log.debug("Init")

        self.createSockets()

        try:
            self.run()
        except KeyboardInterrupt:
            pass
        except:
            trace = traceback.format_exc()
            self.log.info("Stopping signalHandler due to unknown error condition.")
            self.log.debug("Error was: " + str(trace))


    def getLogger(self):
        logger = logging.getLogger("SignalHandler")
        return logger


    def createSockets(self):

        # create zmq socket for signal communication with receiver
        self.comSocket = self.context.socket(zmq.REP)
        connectionStr  = "tcp://{ip}:{port}".format(ip=self.extIp, port=self.comPort)
        try:
            self.comSocket.bind(connectionStr)
            self.log.info("comSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start comSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        self.signalFwSocket = self.context.socket(zmq.REP)
        connectionStr       = "tcp://{ip}:{port}".format(ip=self.localhost, port=self.signalFwPort)
        try:
            self.signalFwSocket.bind(connectionStr)
            self.log.debug("signalFwSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start signalFwSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # create socket to receive requests
        self.requestSocket = self.context.socket(zmq.PULL)
        connectionStr      = "tcp://{ip}:{port}".format(ip=self.extIp, port=self.requestPort)
        try:
            self.requestSocket.bind(connectionStr)
            self.log.debug("requestSocket started (bind) for '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start requestSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # Poller to distinguish between start/stop signals and queries for the next set of signals
        self.poller = zmq.Poller()
        self.poller.register(self.comSocket, zmq.POLLIN)
        self.poller.register(self.signalFwSocket, zmq.POLLIN)
        self.poller.register(self.requestSocket, zmq.POLLIN)


    def run(self):
        #run loop, and wait for incoming messages
        self.log.debug("Waiting for new signals or requests.")
        while True:
            socks = dict(self.poller.poll())

            if self.signalFwSocket in socks and socks[self.signalFwSocket] == zmq.POLLIN:

                try:
                    incomingMessage = self.signalFwSocket.recv()
                    if incomingMessage == "STOP":
                        self.signalFwSocket.send(incomingMessage)
                        break
                    self.log.debug("New request for signals received.")

                    openRequests = self.openRequPerm + self.openRequVari
                    self.openRequVari = []
                    if openRequests:
                        self.signalFwSocket.send_multipart(openRequests)
                        self.log.debug("Answered to request: " + str(openRequests))
                    else:
                        openRequests = ["None"]
                        self.signalFwSocket.send_multipart(openRequests)
                        self.log.debug("Answered to request: " + str(openRequests))
                except Exception, e:
                    self.log.error("Failed to receive/answer new signal requests.")
                    trace = traceback.format_exc()
                    self.log.debug("Error was: " + str(trace))
                continue

            if self.comSocket in socks and socks[self.comSocket] == zmq.POLLIN:

                incomingMessage = self.comSocket.recv_multipart()
                self.log.debug("Received signal: " + str(incomingMessage) )

                checkStatus, signal, host, port = self.checkSignal(incomingMessage)
                if not checkStatus:
                    continue

                self.reactToSignal(signal, host, port)

            if self.requestSocket in socks and socks[self.requestSocket] == zmq.POLLIN:

                incomingMessage = self.requestSocket.recv_multipart()
                self.log.debug("Received request: " + str(incomingMessage) )

                if incomingMessage[1] in self.allowedQueries:
                    self.openRequVari.append(incomingMessage[1])
                    self.log.debug("Add to openRequVari: " + incomingMessage[1] )


    def checkSignal(self, incomingMessage):

        if len(incomingMessage) != 4:

            log.info("Received signal is of the wrong format")
            log.debug("Received signal is too short or too long: " + str(incomingMessage))
            return False, None, None, None

        else:

            version, signal, host, port = incomingMessage

            if host.startswith("["):
                # remove "['" and "']" at the beginning and the end
                host = host[2:-2].split("', '")
            else:
                host = [host]

            if port.startswith("["):
                port = port[2:-2].split("', '")
            else:
                port = [port]

            if version:
                if helperScript.checkVersion(version, self.log):
                    self.log.debug("Versions are compatible: " + str(version))
                else:
                    self.log.debug("Version are not compatible")
                    self.sendResponse("VERSION_CONFLICT")
                    return False, None, None, None

            if signal and host and port :

                # Checking signal sending host
                self.log.debug("Check if signal sending host is in WhiteList...")
                if helperScript.checkHost(host, self.whiteList, self.log):
                    self.log.debug("Hosts are allowed to connect.")
                    self.log.debug("hosts: " + str(host))
                else:
                    self.log.debug("One of the hosts is not allowed to connect.")
                    self.log.debug("hosts: " + str(host))
                    self.sendResponse("NO_VALID_HOST")
                    return False, None, None, None

        return True, signal, host, port


    def sendResponse(self, signal):
            self.log.debug("send confirmation back to receiver: " + str(signal) )
            self.comSocket.send(signal, zmq.NOBLOCK)


    def reactToSignal(self, signal, host, port):

        # React to signal
        if signal == "START_STREAM":
            #FIXME
            host = host[0]
            port = port[0]
            socketId = host + ":" + port
            self.log.info("Received signal: " + signal + " to host " + str(socketId))

            if socketId in self.openRequPerm:
                self.log.info("Connection to " + str(socketId) + " is already open")
                self.sendResponse("CONNECTION_ALREADY_OPEN")
            else:
                # send signal back to receiver
                self.sendResponse(signal)
                self.log.debug("Send response back: " + str(signal))
                self.openRequPerm.append(socketId)

            return

        elif signal == "STOP_STREAM":
            #FIXME
            host = host[0]
            port = port[0]
            socketId = host + ":" + port
            self.log.info("Received signal: " + signal + " to host " + str(socketId))

            if socketId in self.openRequPerm:
                # send signal back to receiver
                self.sendResponse(signal)
                self.log.debug("Send response back: " + str(signal))
                self.openRequPerm.remove(socketId)
            else:
                self.log.info("No connection to close was found for " + str(socketId))
                self.sendResponse("NO_OPEN_CONNECTION_FOUND")

            return

        elif signal == "START_QUERY_NEXT":
            self.log.info("Received signal to enable querying for data for hosts: " + str(host))
            connectionFound = False
            tmpAllowed = []
            for h in host:
                for p in port:
                    socketId = h + ":" + p
                    if socketId in self.allowedQueries:
                        connectionFound = True
                        self.log.info("Connection to " + str(socketId) + " is already open")
                        self.sendResponse("CONNECTION_ALREADY_OPEN")
                    elif socketId not in tmpAllowed:
                        tmpAllowed.append(socketId)
                    else:
                        #TODO send notification (double entries in START_QUERY_NEXT) back?
                        pass

            if not connectionFound:
                # send signal back to receiver
                self.sendResponse(signal)
                self.allowedQueries += tmpAllowed
                self.log.debug("Send response back: " + str(signal))

            return

        elif signal == "STOP_QUERY_NEXT":
            self.log.info("Received signal to disable querying for data for hosts: " + str(host))
            connectionNotFound = False
            for h in host:
                for p in port:
                    socketId = h + ":" + p
                    if socketId in self.allowedQueries:
                        self.allowedQueries.remove(socketId)
                    else:
                        connectionNotFound = True

            if connectionNotFound:
                self.log.info("No connection to close was found for " + str(socketId))
                self.sendResponse("NO_OPEN_CONNECTION_FOUND")
            else:
                # send signal back to receiver
                self.sendResponse(signal)
                self.log.debug("Send response back: " + str(signal))

            return

        else:
            self.log.info("Received signal from host " + str(host) + " unkown: " + str(signal))
            self.sendResponse("NO_VALID_SIGNAL")


    def stop(self):
        self.log.debug("Closing sockets")
        self.comSocket.close(0)
        self.signalFwSocket.close(0)
        self.requestSocket.close(0)


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


if __name__ == '__main__':
    from multiprocessing import Process
    import time

    helperScript.initLogging("/space/projects/live-viewer/logs/signalHandler.log", verbose=True, onScreenLogLevel="debug")


    whiteList     = ["localhost", "zitpcx19282"]
    comPort       = "6000"
    requestFwPort = "6001"
    requestPort   = "6002"
    signalHandlerProcess = Process ( target = SignalHandler, args = (whiteList, comPort, requestFwPort, requestPort) )

    signalHandlerProcess.start()


    def sendSignal(socket, signal, port):
        sendMessage = ["0.0.1",  signal, "zitpcx19282", port]
        socket.send_multipart(sendMessage)
        receivedMessage = socket.recv()
        logging.info("=== Responce : " + receivedMessage )

    def sendRequest(socket, socketId):
        sendMessage = ["NEXT", socketId]
        socket.send_multipart(sendMessage)
        logging.info("=== request sent: " + str(sendMessage))


    def getRequests(socket):
        socket.send("")
        requests = socket.recv_multipart()
        logging.info("=== Requests: " + str(requests))


    context         = zmq.Context.instance()
    comSocket       = context.socket(zmq.REQ)
    connectionStr   = "tcp://zitpcx19282:" + comPort
    comSocket.connect(connectionStr)
    logging.info("=== comSocket connected to " + connectionStr)

    requestSocket   = context.socket(zmq.PUSH)
    connectionStr   = "tcp://zitpcx19282:" + requestPort
    requestSocket.connect(connectionStr)
    logging.info("=== requestSocket connected to " + connectionStr)

    requestFwSocket = context.socket(zmq.REQ)
    connectionStr   = "tcp://localhost:" + requestFwPort
    requestFwSocket.connect(connectionStr)
    logging.info("=== requestFwSocket connected to " + connectionStr)



    sendSignal(comSocket, "START_STREAM", "6003")
    getRequests(requestFwSocket)

    sendSignal(comSocket, "START_STREAM", "6004")
    getRequests(requestFwSocket)

    sendSignal(comSocket, "STOP_STREAM", "6003")
    getRequests(requestFwSocket)

    sendRequest(requestSocket, "zitpcx19282:6006")
    getRequests(requestFwSocket)

    sendSignal(comSocket, "START_QUERY_NEXT", "6005")
    getRequests(requestFwSocket)

    sendRequest(requestSocket, "zitpcx19282:6005")
    getRequests(requestFwSocket)
    getRequests(requestFwSocket)


    requestFwSocket.send("STOP")
    requests = requestFwSocket.recv()
    logging.debug("=== Requests: " + requests)

    signalHandlerProcess.join()

    comSocket.close(0)
    requestSocket.close(0)
    requestFwSocket.close(0)
    context.destroy()


