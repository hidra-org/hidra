__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import time
import zmq
import logging
import os
import sys
import traceback
import copy
import cPickle


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

from logutils.queue import QueueHandler
import helpers

#
#  --------------------------  class: SignalHandler  --------------------------------------
#
class SignalHandler():

    def __init__ (self, controlPort, whiteList, comPort, signalFwPort, requestPort,
                  logQueue, context = None):

        # to get the logging only handling this class
        log                  = None

        # Send all logs to the main process
        self.log = self.getLogger(logQueue)
        self.log.debug("SignalHandler started (PID " + str(os.getpid()) + ").")

        self.localhost       = "127.0.0.1"
        self.extIp           = "0.0.0.0"

        self.comPort         = comPort
        self.signalFwPort    = signalFwPort
        self.requestPort     = requestPort
        self.controlPort     = controlPort

        self.openConnections = []
        self.forwardSignal   = []

        self.openRequVari    = []
        self.openRequPerm    = []
        self.allowedQueries  = []
        self.nextRequNode    = []  # to rotate through the open permanent requests

        self.whiteList       = []

        #remove .desy.de from hostnames
        for host in whiteList:
            if host.endswith(".desy.de"):
                self.whiteList.append(host[:-8])
            else:
                self.whiteList.append(host)

        # sockets
        self.controlSocket   = None
        self.comSocket       = None
        self.requestFwSocket = None
        self.requestSocket   = None

        self.log.debug("Registering ZMQ context")
        # remember if the context was created outside this class or not
        if context:
            self.context    = context
            self.extContext = True
        else:
            self.context    = zmq.Context()
            self.extContext = False

        self.createSockets()

        try:
            self.run()
        except KeyboardInterrupt:
            pass
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

        # socket to get control signals from
        self.controlSocket = self.context.socket(zmq.SUB)
        connectionStr  = "tcp://{ip}:{port}".format(ip=self.localhost, port=self.controlPort)
        try:
            self.controlSocket.connect(connectionStr)
            self.log.info("Start controlSocket (connect): '" + connectionStr + "'")
        except:
            self.log.error("Failed to start controlSocket (connect): '" + connectionStr + "'", exc_info=True)

        self.controlSocket.setsockopt(zmq.SUBSCRIBE, "control")

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
        self.poller.register(self.controlSocket, zmq.POLLIN)
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

                    #TODO do this the right way
                    if incomingMessage == b"STOP":
                        self.requestFwSocket.send([incomingMessage])
                        time.sleep(0.1)
                        break

                    self.log.debug("New request for signals received.")

                    openRequests = []

#                    openRequests = copy.deepcopy(self.openRequPerm)
                    for requestSet in self.openRequPerm:
                        if requestSet:
                            index = self.openRequPerm.index(requestSet)
                            tmp = requestSet[self.nextRequNode[index]]
                            openRequests.append(copy.deepcopy(tmp))
                            # distribute in round-robin order
                            self.nextRequNode[index] = (self.nextRequNode[index] + 1) % len(requestSet)

                    for requestSet in self.openRequVari:
                        if requestSet:
                            tmp = requestSet.pop(0)
                            openRequests.append(tmp)

                    if openRequests:
                        self.requestFwSocket.send_multipart(["", cPickle.dumps(openRequests)])
                        self.log.debug("Answered to request: " + str([self.forwardSignal, openRequests]))
                    else:
                        openRequests = ["None"]
                        self.requestFwSocket.send_multipart(["", cPickle.dumps(openRequests)])
                        self.log.debug("Answered to request: " + str([self.forwardSignal, openRequests]))

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

                continue

            if self.requestSocket in socks and socks[self.requestSocket] == zmq.POLLIN:
                self.log.debug("")
                self.log.debug("!!!! requestSocket !!!!")
                self.log.debug("")

                incomingMessage = self.requestSocket.recv_multipart()
                self.log.debug("Received request: " + str(incomingMessage) )

                for index in range(len(self.allowedQueries)):
                    for i in range(len(self.allowedQueries[index])):

                        if ".desy.de:" in incomingMessage[1]:
                            incomingMessage[1] = incomingMessage[1].replace(".desy.de:", ":")

                        incomingSocketId = incomingMessage[1]

                        if incomingSocketId == self.allowedQueries[index][i][0]:
                            self.openRequVari[index].append(self.allowedQueries[index][i])
                            self.log.debug("Add to openRequVari: " + str(self.allowedQueries[index][i]) )

            if self.controlSocket in socks and socks[self.controlSocket] == zmq.POLLIN:

                try:
                    message = self.controlSocket.recv_multipart()
                    self.log.debug("Control signal received.")
                except:
                    self.log.error("Waiting for control signal...failed", exc_info=True)
                    continue

                # remove subsription topic
                del message[0]

                if message[0] == b"EXIT":
                    self.log.debug("Requested to shutdown.")
                    break
                else:
                    self.log.error("Unhandled control signal received: " + str(message[0]))


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
                    return False, None, None

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
                    return False, None, None

        return True, signal, target


    def sendResponse (self, signal):
            self.log.debug("Send response back: " + str(signal))
            self.comSocket.send(signal, zmq.NOBLOCK)


    def __startSignal(self, signal, sendType, socketIds, listToCheck, variList, correspList):

        connectionFound = False
        tmpAllowed = []

        for socketConf in socketIds:

            if ".desy.de:" in socketConf[0]:
                socketConf[0] = socketConf[0].replace(".desy.de:",":")

            socketId = socketConf[0]

            self.log.debug("socketId: " + str(socketId))
            flatlist = [ i[0] for i in [j for sublist in listToCheck for j in sublist]]
            self.log.debug("flatlist: " + str(flatlist))

            if socketId in flatlist:
                connectionFound = True
                self.log.info("Connection to " + str(socketId) + " is already open")
            elif socketId not in [ i[0] for i in tmpAllowed]:
                tmpSocketConf = socketConf + [sendType]
                tmpAllowed.append(tmpSocketConf)
            else:
                #TODO send notification (double entries in START_QUERY_NEXT) back?
                pass

        if not connectionFound:
            # send signal back to receiver
            self.sendResponse(signal)
            listToCheck.append(copy.deepcopy(sorted(tmpAllowed)))
            if correspList != None:
                correspList.append(0)
            del tmpAllowed

            if variList != None:
                variList.append([])
        else:
            # send error back to receiver
            self.sendResponse("CONNECTION_ALREADY_OPEN")


    def __stopSignal(self, signal, socketIds, listToCheck, variList, correspList):

        connectionNotFound = False
        tmpRemoveIndex = []
        tmpRemoveElement = []
        found = False

        for socketConf in socketIds:

            if ".desy.de:" in socketConf[0]:
                socketConf[0] = socketConf[0].replace(".desy.de:",":")

            socketId = socketConf[0]

            for sublist in listToCheck:
                for element in sublist:
                    if socketId == element[0]:
                        tmpRemoveElement.append(element)
                        found = True
            if not found:
                connectionNotFound = True

        if connectionNotFound:
            self.sendResponse("NO_OPEN_CONNECTION_FOUND")
            self.log.info("No connection to close was found for " + str(socketConf))
        else:
            # send signal back to receiver
            self.sendResponse(signal)

            for element in tmpRemoveElement:

                socketId = element[0]

                if variList != None:
                    variList =  [ [ b for b in  variList[a] if socketId != b[0] ] for a in range(len(variList)) ]
                    self.log.debug("Remove all occurences from " + str(socketId) + " from variable request list.")

                for i in range(len(listToCheck)):
                    if element in listToCheck[i]:
                        listToCheck[i].remove(element)
                        self.log.debug("Remove " + str(socketId) + " from pemanent request/allowed list.")

                        if not listToCheck[i]:
                            tmpRemoveIndex.append(i)
                            if variList != None:
                                del variList[i]
                            if correspList != None:
                                correspList.pop(i)
                        else:
                            if correspList != None:
                                correspList[i] = correspList[i] % len(listToCheck[i])

                for index in tmpRemoveIndex:
                    del listToCheck[index]

            # send signal to TaskManager
            helpers.globalObjects.controlSocket.send_multipart(["signal", "CLOSE_SOCKETS", cPickle.dumps(socketIds)])

        return listToCheck, variList, correspList


    def reactToSignal (self, signal, socketIds):

        ###########################
        ##      START_STREAM     ##
        ###########################
        if signal == "START_STREAM":
            self.log.info("Received signal: " + signal + " for hosts " + str(socketIds))

            self.__startSignal(signal, "data", socketIds, self.openRequPerm, None, self.nextRequNode)

            return

        ###########################
        ## START_STREAM_METADATA ##
        ###########################
        elif signal == "START_STREAM_METADATA":
            self.log.info("Received signal: " + signal + " for hosts " + str(socketIds))

            self.__startSignal(signal, "metadata", socketIds, self.openRequPerm, None, self.nextRequNode)

            return

        ###########################
        ##      STOP_STREAM      ##
        ## STOP_STREAM_METADATA  ##
        ###########################
        elif signal == "STOP_STREAM" or signal == "STOP_STREAM_METADATA":
            self.log.info("Received signal: " + signal + " for host " + str(socketIds))

            self.openRequPerm, nonetmp, self.nextRequNode = self.__stopSignal(signal, socketIds, self.openRequPerm, None, self.nextRequNode)

            return


        ###########################
        ##      START_QUERY      ##
        ###########################
        elif signal == "START_QUERY_NEXT":
            self.log.info("Received signal: " + signal + " for hosts " + str(socketIds))

            self.__startSignal(signal, "data", socketIds, self.allowedQueries, self.openRequVari, None)

            return

        ###########################
        ## START_QUERY_METADATA  ##
        ###########################
        elif signal == "START_QUERY_METADATA":
            self.log.info("Received signal: " + signal + " for hosts " + str(socketIds))

            self.__startSignal(signal, "metadata", socketIds, self.allowedQueries, self.openRequVari, None)

            return

        ###########################
        ##      STOP_QUERY       ##
        ## STOP_QUERY_METADATA   ##
        ###########################
        elif signal == "STOP_QUERY_NEXT" or signal == "STOP_QUERY_METADATA":
            self.log.info("Received signal: " + signal + " for hosts " + str(socketIds))

            self.allowedQueries, self.openRequVari, nonetmp = self.__stopSignal(signal, socketIds, self.allowedQueries, self.openRequVari, None)

            return


        else:
            self.log.info("Received signal from host " + str(host) + " unkown: " + str(signal))
            self.sendResponse("NO_VALID_SIGNAL")


    def stop (self):

        self.log.debug("Closing sockets")
        if self.comSocket:
            self.comSocket.close(0)
            self.comSocket = None
        if self.requestFwSocket:
            self.requestFwSocket.close(0)
            self.requestFwSocket = None
        if self.requestSocket:
            self.requestSocket.close(0)
            self.requestSocket = None
        if self.controlSocket:
            self.controlSocket.close(0)
            self.controlSocket = None
        if not self.extContext and self.context:
            self.context.destroy(0)
            self.context = None


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


