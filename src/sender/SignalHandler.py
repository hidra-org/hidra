__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

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

    def __init__ (self, whiteList,
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
        self.comSocket       = None
        self.requestFwSocket = None
        self.requestSocket   = None

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


    def getLogger (self):
        logger = logging.getLogger("SignalHandler")
        return logger


    def createSockets (self):

        # create zmq socket for signal communication with receiver
        self.comSocket = self.context.socket(zmq.REP)
        connectionStr  = "tcp://{ip}:{port}".format(ip=self.extIp, port=self.comPort)
        try:
            self.comSocket.bind(connectionStr)
            self.log.info("Start comSocket (bind): '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start comSocket (bind): '" + connectionStr + "'")
            self.log.debug("Error was:" + str(e))

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        self.requestFwSocket = self.context.socket(zmq.REP)
        connectionStr       = "tcp://{ip}:{port}".format(ip=self.localhost, port=self.signalFwPort)
        try:
            self.requestFwSocket.bind(connectionStr)
            self.log.info("Start requestFwSocket (bind): '" + connectionStr + "'")
        except Exception as e:
            self.log.error("Failed to start requestFwSocket (bind): '" + connectionStr + "'")
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
        self.poller.register(self.requestFwSocket, zmq.POLLIN)
        self.poller.register(self.requestSocket, zmq.POLLIN)


    def run (self):
        #run loop, and wait for incoming messages
        self.log.debug("Waiting for new signals or requests.")
        while True:
            socks = dict(self.poller.poll())

            if self.requestFwSocket in socks and socks[self.requestFwSocket] == zmq.POLLIN:

                try:
                    incomingMessage = self.requestFwSocket.recv()
                    if incomingMessage == "STOP":
                        self.requestFwSocket.send(incomingMessage)
                        time.sleep(0.1)
                        break
                    self.log.debug("New request for signals received.")

                    openRequests = copy.deepcopy(self.openRequPerm)
                    for requestSet in self.openRequVari:
                        if requestSet:
                            tmp = requestSet.pop(0)
                            openRequests.append(tmp)

                    if openRequests:
                        self.requestFwSocket.send(str(openRequests))
                        self.log.debug("Answered to request: " + str(openRequests))
                    else:
                        openRequests = ["None"]
                        self.requestFwSocket.send(str(openRequests))
                        self.log.debug("Answered to request: " + str(openRequests))
                except Exception, e:
                    self.log.error("Failed to receive/answer new signal requests.")
                    trace = traceback.format_exc()
                    self.log.debug("Error was: " + str(trace))
#                continue

            if self.comSocket in socks and socks[self.comSocket] == zmq.POLLIN:
                self.log.debug("")
                self.log.debug("comSocket")
                self.log.debug("")

                incomingMessage = self.comSocket.recv_multipart()
                self.log.debug("Received signal: " + str(incomingMessage) )

                checkStatus, signal, target = self.checkSignal(incomingMessage)
                if checkStatus:
                    self.reactToSignal(signal, target)

            if self.requestSocket in socks and socks[self.requestSocket] == zmq.POLLIN:
                self.log.debug("")
                self.log.debug("!!!! requestSocket !!!!")
                self.log.debug("")


                incomingMessage = self.requestSocket.recv_multipart()
                self.log.debug("Received request: " + str(incomingMessage) )

                for index in range(len(self.allowedQueries)):
                    if incomingMessage[1] in self.allowedQueries[index]:
                        self.openRequVari[index].append(incomingMessage[1])
                        self.log.debug("Add to openRequVari: " + incomingMessage[1] )



    def checkSignal (self, incomingMessage):

        if len(incomingMessage) != 3:

            self.log.info("Received signal is of the wrong format")
            self.log.debug("Received signal is too short or too long: " + str(incomingMessage))
            return False, None, None, None

        else:

            version, signal, target = incomingMessage

            if target.startswith("["):
                # remove "['" and "']" at the beginning and the end
                target = target[2:-2].split("', '")
            else:
                target = [target]

            host = [t.split(":")[0] for t in target]

            if version:
                if helperScript.checkVersion(version, self.log):
                    self.log.debug("Versions are compatible: " + str(version))
                else:
                    self.log.debug("Version are not compatible")
                    self.sendResponse("VERSION_CONFLICT")
                    return False, None, None, None

            if signal and host:

                # Checking signal sending host
                self.log.debug("Check if host to send data to are in WhiteList...")
                if helperScript.checkHost(host, self.whiteList, self.log):
                    self.log.debug("Hosts are allowed to connect.")
                    self.log.debug("hosts: " + str(host))
                else:
                    self.log.debug("One of the hosts is not allowed to connect.")
                    self.log.debug("hosts: " + str(host))
                    self.sendResponse("NO_VALID_HOST")
                    return False, None, None, None

        return True, signal, target


    def sendResponse (self, signal):
            self.log.debug("send confirmation back to receiver: " + str(signal) )
            self.comSocket.send(signal, zmq.NOBLOCK)


    def reactToSignal (self, signal, socketIds):

        # React to signal
        if signal == "START_STREAM":
            #FIXME
            socketId = socketIds[0]
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
            socketId = socketIds[0]
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
            self.log.info("Received signal to enable querying for data for hosts: " + str(socketIds))
            connectionFound = False
            tmpAllowed = []
            for socketId in socketIds:
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
                self.allowedQueries.append(sorted(tmpAllowed))
                self.openRequVari.append([])
                self.log.debug("Send response back: " + str(signal))

            return

        elif signal == "STOP_QUERY_NEXT":
            self.log.info("Received signal to disable querying for data for hosts: " + str(socketIds))
            connectionNotFound = False
            tmpRemove = []
            for socketId in socketIds:
                if socketId in self.allowedQueries:
                    tmpRemove.append(socketId)
                else:
                    connectionNotFound = True

            if connectionNotFound:
                self.log.info("No connection to close was found for " + str(socketId))
                self.sendResponse("NO_OPEN_CONNECTION_FOUND")
            else:
                # send signal back to receiver
                self.sendResponse(signal)
                indexToRemove = self.allowedQueries.index(sorted(tmpRemove))
                self.allowedQueries.pop(i)
                self.openRequVari.pop(i)
                self.log.debug("Send response back: " + str(signal))

            return

        else:
            self.log.info("Received signal from host " + str(host) + " unkown: " + str(signal))
            self.sendResponse("NO_VALID_SIGNAL")


    def stop (self):
        self.log.debug("Closing sockets")
        self.comSocket.close(0)
        self.requestFwSocket.close(0)
        self.requestSocket.close(0)


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


if __name__ == '__main__':

    from multiprocessing import Process
    import time

    class requestPuller():
        def __init__ (self, requestFwPort, context = None):
            self.context         = context or zmq.Context.instance()
            self.requestFwSocket = self.context.socket(zmq.REQ)
            connectionStr   = "tcp://localhost:" + requestFwPort
            self.requestFwSocket.connect(connectionStr)
            logging.info("[getRequests] requestFwSocket started (connect) for '" + connectionStr + "'")

            self.run()


        def run (self):
            logging.info("[getRequests] Start run")
            while True:
                self.requestFwSocket.send("")
                logging.info("[getRequests] send")
                requests = self.requestFwSocket.recv()
                logging.info("[getRequests] Requests: " + str(requests))
                time.sleep(0.25)

        def __exit__(self):
            self.requestFwSocket.close(0)
            self.context.destroy()

#        def __del__(self):
#            self.requestFwSocket.close(0)
#            self.context.destroy()


    helperScript.initLogging("/space/projects/live-viewer/logs/signalHandler.log", verbose=True, onScreenLogLevel="debug")


    whiteList     = ["localhost", "zitpcx19282"]
    comPort       = "6000"
    requestFwPort = "6001"
    requestPort   = "6002"
    signalHandlerProcess = Process ( target = SignalHandler, args = (whiteList, comPort, requestFwPort, requestPort) )
    signalHandlerProcess.start()

    requestPullerProcess = Process ( target = requestPuller, args = (requestFwPort, ) )
    requestPullerProcess.start()


    def sendSignal(socket, signal, ports):
        logging.info("=== sendSignal : " + signal + ", " + str(ports))
        sendMessage = ["0.0.1",  signal]
        targets = []
        if type(ports) == list:
            for port in ports:
                targets += ["zitpcx19282:" + port]
        else:
            targets += ["zitpcx19282:" + ports]
        sendMessage.append(str(targets))
        socket.send_multipart(sendMessage)
        receivedMessage = socket.recv()
        logging.info("=== Responce : " + receivedMessage )

    def sendRequest(socket, socketId):
        sendMessage = ["NEXT", socketId]
        logging.info("=== sendRequest: " + str(sendMessage))
        socket.send_multipart(sendMessage)
        logging.info("=== request sent: " + str(sendMessage))


    def getRequests(socket):
        logging.info("=== getRequests")
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

    time.sleep(3)

    sendSignal(comSocket, "START_STREAM", "6003")

    sendSignal(comSocket, "START_STREAM", "6004")

    sendSignal(comSocket, "STOP_STREAM", "6003")

    sendRequest(requestSocket, "zitpcx19282:6006")

    sendSignal(comSocket, "START_QUERY_NEXT", ["6005", "6006"])

    sendRequest(requestSocket, "zitpcx19282:6005")
    sendRequest(requestSocket, "zitpcx19282:6005")

    time.sleep(1)


    requestFwSocket.send("STOP")
    requests = requestFwSocket.recv()
    logging.debug("=== Stop: " + requests)

    signalHandlerProcess.join()
    requestPullerProcess.terminate()

    comSocket.close(0)
    requestSocket.close(0)
    requestFwSocket.close(0)
    context.destroy()

