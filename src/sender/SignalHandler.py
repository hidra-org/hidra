__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import time
import zmq
import logging
import os
import sys
import traceback
import copy
import cPickle
from logutils.queue import QueueHandler


#path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( '__file__' ) )))
#    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
#SHARED_PATH = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) + os.sep + "shared"
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

import helpers

#
#  --------------------------  class: SignalHandler  --------------------------------------
#
class SignalHandler():

    def __init__ (self, whiteList, comPort, signalFwPort, requestPort,
                  logQueue, context = None):

        # to get the logging only handling this class
        log                  = None

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

        # Send all logs to the main process
        self.log = self.getLogger(logQueue)
        self.log.debug("SignalHandler started (PID " + str(os.getpid()) + ").")

        self.createSockets()

        try:
            self.run()
        except KeyboardInterrupt:
            self.stop()
        except:
            self.log.error("Stopping signalHandler due to unknown error condition.", exc_info=True)
            self.stop()


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("SignalHandler")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def createSockets (self):

        # create zmq socket for signal communication with receiver
        self.comSocket = self.context.socket(zmq.REP)
        connectionStr  = "tcp://{ip}:{port}".format(ip=self.extIp, port=self.comPort)
        try:
            self.comSocket.bind(connectionStr)
            self.log.info("Start comSocket (bind): '" + connectionStr + "'")
        except:
            self.log.error("Failed to start comSocket (bind): '" + connectionStr + "'", exc_info=True)

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        self.requestFwSocket = self.context.socket(zmq.REP)
        connectionStr       = "tcp://{ip}:{port}".format(ip=self.localhost, port=self.signalFwPort)
        try:
            self.requestFwSocket.bind(connectionStr)
            self.log.info("Start requestFwSocket (bind): '" + connectionStr + "'")
        except:
            self.log.error("Failed to start requestFwSocket (bind): '" + connectionStr + "'", exc_info=True)

        # create socket to receive requests
        self.requestSocket = self.context.socket(zmq.PULL)
        connectionStr      = "tcp://{ip}:{port}".format(ip=self.extIp, port=self.requestPort)
        try:
            self.requestSocket.bind(connectionStr)
            self.log.debug("requestSocket started (bind) for '" + connectionStr + "'")
        except:
            self.log.error("Failed to start requestSocket (bind): '" + connectionStr + "'", exc_info=True)

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
                        self.requestFwSocket.send(cPickle.dumps(openRequests))
                        self.log.debug("Answered to request: " + str(openRequests))
                    else:
                        openRequests = ["None"]
                        self.requestFwSocket.send(cPickle.dumps(openRequests))
                        self.log.debug("Answered to request: " + str(openRequests))
                except:
                    self.log.error("Failed to receive/answer new signal requests.", exc_info=True)
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
                    for i in range(len(self.allowedQueries[index])):
                        if incomingMessage[1] == self.allowedQueries[index][i][0]:
                            self.openRequVari[index].append(self.allowedQueries[index][i])
                            self.log.debug("Add to openRequVari: " + str(self.allowedQueries[index][i]) )


    def checkSignal (self, incomingMessage):

        if len(incomingMessage) != 3:

            self.log.info("Received signal is of the wrong format")
            self.log.debug("Received signal is too short or too long: " + str(incomingMessage))
            return False, None, None, None

        else:

            version, signal, target = incomingMessage
            target = cPickle.loads(target)

            host = [t[0].split(":")[0] for t in target]

            if version:
                if helpers.checkVersion(version, self.log):
                    self.log.debug("Versions are compatible: " + str(version))
                else:
                    self.log.debug("Version are not compatible")
                    self.sendResponse("VERSION_CONFLICT")
                    return False, None, None, None

            if signal and host:

                # Checking signal sending host
                self.log.debug("Check if host to send data to are in WhiteList...")
                if helpers.checkHost(host, self.whiteList, self.log):
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
            self.log.info("Received signal: " + signal + " to host " + str(socketId) + \
                          " with priority " + str(socketId[1]))

            if socketId in [i[0] for i in self.openRequPerm]:
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
            socketId = socketIds[0][0]

            self.log.info("Received signal: " + signal + " to host " + str(socketId[0]))

            if socketId in [i[0] for i in self.openRequPerm]:
                # send signal back to receiver
                self.sendResponse(signal)
                self.log.debug("Send response back: " + str(signal))

                for element in self.openRequPerm:
                    if element[0] == socketId:
                        self.openRequPerm.remove(element)
            else:
                self.log.info("No connection to close was found for " + str(socketId))
                self.log.debug("self.openReqPerm=" + str(self.openRequPerm))
                self.sendResponse("NO_OPEN_CONNECTION_FOUND")

            return

        elif signal == "START_QUERY_NEXT":
            self.log.info("Received signal to enable querying for data for hosts: " + str(socketIds))
            connectionFound = False
            tmpAllowed = []
            for socketConf in socketIds:
                socketId = socketConf[0]
                if socketId in [ i[0] for i in self.allowedQueries]:
                    connectionFound = True
                    self.log.info("Connection to " + str(socketId) + " is already open")
                    self.sendResponse("CONNECTION_ALREADY_OPEN")
                elif socketId not in [ i[0] for i in tmpAllowed]:
                    tmpAllowed.append(socketConf)
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
            tmpRemoveIndex = []
            found = False
            for socketConf in socketIds:
                socketId = socketConf[0]
                for i in range(len(self.allowedQueries)):
                    for j in range(len(self.allowedQueries[i])):
                        if socketId == self.allowedQueries[i][j][0]:
                            tmpRemoveIndex.append([i,j])
                            found = True
                if not found:
                    connectionNotFound = True

            if connectionNotFound:
                self.sendResponse("NO_OPEN_CONNECTION_FOUND")
                self.log.info("No connection to close was found for " + str(socketConf))
            else:
                # send signal back to receiver
                self.sendResponse(signal)
                self.log.debug("Send response back: " + str(signal))
                for i, j in tmpRemoveIndex:
                    self.log.debug("Remove " + str(self.allowedQueries[i][j]) + " from allowedQueries.")
                    socketId = self.allowedQueries[i].pop(j)[0]

                    self.openRequVari =  [ [ b for b in  self.openRequVari[a] if socketId != b[0] ] for a in range(len(self.openRequVari)) ]
                    self.log.debug("Remove all occurences from " + str(socketId) + " from openRequVari.")

                    if not self.allowedQueries[i]:
                        del self.allowedQueries[i]
                        del self.openRequVari[i]

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


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class requestPuller():
    def __init__ (self, requestFwPort, logQueue, context = None):

        self.log = self.getLogger(logQueue)

        self.context         = context or zmq.Context.instance()
        self.requestFwSocket = self.context.socket(zmq.REQ)
        connectionStr   = "tcp://localhost:" + requestFwPort
        self.requestFwSocket.connect(connectionStr)
        self.log.info("[getRequests] requestFwSocket started (connect) for '" + connectionStr + "'")

        self.run()

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("requestPuller")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def run (self):
        self.log.info("[getRequests] Start run")
        while True:
            try:
                self.requestFwSocket.send("")
                self.log.info("[getRequests] send")
                requests = cPickle.loads(self.requestFwSocket.recv())
                self.log.info("[getRequests] Requests: " + str(requests))
                time.sleep(0.25)
            except Exception as e:
                self.log.error(str(e), exc_info=True)
                break

    def __exit__(self):
        self.requestFwSocket.close(0)
        self.context.destroy()


if __name__ == '__main__':
    from multiprocessing import Process, freeze_support, Queue
    import time

    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows

    whiteList     = ["localhost", "zitpcx19282"]
    comPort       = "6000"
    requestFwPort = "6001"
    requestPort   = "6002"

    logfile  = BASE_PATH + os.sep + "logs" + os.sep + "signalHandler.log"
    logsize  = 10485760

    logQueue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose=True, onScreenLogLevel="debug")

    # Start queue listener using the stream handler above
    logQueueListener    = helpers.CustomQueueListener(logQueue, h1, h2)
    logQueueListener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG) # Log level = DEBUG
    qh = QueueHandler(logQueue)
    root.addHandler(qh)


    signalHandlerProcess = Process ( target = SignalHandler, args = (whiteList, comPort, requestFwPort, requestPort, logQueue) )
    signalHandlerProcess.start()

    requestPullerProcess = Process ( target = requestPuller, args = (requestFwPort, logQueue) )
    requestPullerProcess.start()


    def sendSignal(socket, signal, ports, prio = None):
        logging.info("=== sendSignal : " + signal + ", " + str(ports))
        sendMessage = ["0.0.1",  signal]
        targets = []
        if type(ports) == list:
            for port in ports:
                targets.append(["zitpcx19282:" + port, prio])
        else:
            targets.append(["zitpcx19282:" + ports, prio])
        targets = cPickle.dumps(targets)
        sendMessage.append(targets)
        socket.send_multipart(sendMessage)
        receivedMessage = socket.recv()
        logging.info("=== Responce : " + receivedMessage )

    def sendRequest(socket, socketId):
        sendMessage = ["NEXT", socketId]
        logging.info("=== sendRequest: " + str(sendMessage))
        socket.send_multipart(sendMessage)
        logging.info("=== request sent: " + str(sendMessage))


    context         = zmq.Context.instance()

    comSocket       = context.socket(zmq.REQ)
    connectionStr   = "tcp://localhost:" + comPort
    comSocket.connect(connectionStr)
    logging.info("=== comSocket connected to " + connectionStr)

    requestSocket   = context.socket(zmq.PUSH)
    connectionStr   = "tcp://localhost:" + requestPort
    requestSocket.connect(connectionStr)
    logging.info("=== requestSocket connected to " + connectionStr)

    requestFwSocket = context.socket(zmq.REQ)
    connectionStr   = "tcp://localhost:" + requestFwPort
    requestFwSocket.connect(connectionStr)
    logging.info("=== requestFwSocket connected to " + connectionStr)

    time.sleep(1)

    sendSignal(comSocket, "START_STREAM", "6003", 1)

    sendSignal(comSocket, "START_STREAM", "6004", 0)

    sendSignal(comSocket, "STOP_STREAM", "6003")

    sendRequest(requestSocket, "zitpcx19282:6006")

    sendSignal(comSocket, "START_QUERY_NEXT", ["6005", "6006"], 2)

    sendRequest(requestSocket, "zitpcx19282:6005")
    sendRequest(requestSocket, "zitpcx19282:6005")
    sendRequest(requestSocket, "zitpcx19282:6006")

    time.sleep(0.5)


    sendRequest(requestSocket, "zitpcx19282:6005")
    sendSignal(comSocket, "STOP_QUERY_NEXT", "6005", 2)

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

    logQueue.put_nowait(None)
    logQueueListener.stop()


