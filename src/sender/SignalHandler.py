__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import time
import zmq
import zmq.devices
import logging
import os
import sys
import traceback
import copy
import cPickle
import threading


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

    def __init__ (self, controlPubConId, controlSubConId, whiteList, comConId, requestFwConId, requestConId,
                  logQueue, context = None):

        # to get the logging only handling this class
        log                  = None

        # Send all logs to the main process
        self.log = self.getLogger(logQueue)

        self.currentPID       = os.getpid()
        self.log.debug("SignalHandler started (PID " + str(self.currentPID) + ").")

        self.controlPubConId  = controlPubConId
        self.controlSubConId  = controlSubConId
        self.comConId         = comConId
        self.requestFwConId   = requestFwConId
        self.requestConId     = requestConId

        self.openConnections  = []
        self.forwardSignal    = []

        self.openRequVari     = []
        self.openRequPerm     = []
        self.allowedQueries   = []
        self.nextRequNode     = []  # to rotate through the open permanent requests

        self.whiteList        = []

        #remove .desy.de from hostnames
        for host in whiteList:
            if host.endswith(".desy.de"):
                self.whiteList.append(host[:-8])
            else:
                self.whiteList.append(host)

        # sockets
        self.controlPubSocket = None
        self.controlSubSocket = None
        self.comSocket        = None
        self.requestFwSocket  = None
        self.requestSocket    = None

        # remember if the context was created outside this class or not
        if context:
            self.context    = context
            self.extContext = True
        else:
            self.log.info("Registering ZMQ context")
            self.context    = zmq.Context()
            self.extContext = False

        try:
            self.createSockets()

            self.run()
        except zmq.ZMQError:
            pass
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

        # socket to send control signals to
        try:
            self.controlPubSocket = self.context.socket(zmq.PUB)
            self.controlPubSocket.connect(self.controlPubConId)
            self.log.info("Start controlPubSocket (connect): '" + self.controlPubConId + "'")
        except:
            self.log.error("Failed to start controlPubSocket (connect): '" + self.controlPubConId + "'", exc_info=True)
            raise

        # socket to get control signals from
        try:
            self.controlSubSocket = self.context.socket(zmq.SUB)
            self.controlSubSocket.connect(self.controlSubConId)
            self.log.info("Start controlSubSocket (connect): '" + self.controlSubConId + "'")
        except:
            self.log.error("Failed to start controlSubSocket (connect): '" + self.controlSubConId + "'", exc_info=True)
            raise

        self.controlSubSocket.setsockopt(zmq.SUBSCRIBE, "control")

        # create zmq socket for signal communication with receiver
        try:
            self.comSocket = self.context.socket(zmq.REP)
            self.comSocket.bind(self.comConId)
            self.log.info("Start comSocket (bind): '" + self.comConId + "'")
        except:
            self.log.error("Failed to start comSocket (bind): '" + self.comConId + "'", exc_info=True)
            raise

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        try:
            self.requestFwSocket = self.context.socket(zmq.REP)
            self.requestFwSocket.bind(self.requestFwConId)
            self.log.info("Start requestFwSocket (bind): '" + self.requestFwConId + "'")
        except:
            self.log.error("Failed to start requestFwSocket (bind): '" + self.requestFwConId + "'", exc_info=True)
            raise

        # create socket to receive requests
        try:
            self.requestSocket = self.context.socket(zmq.PULL)
            self.requestSocket.bind(self.requestConId)
            self.log.info("requestSocket started (bind) for '" + self.requestConId + "'")
        except:
            self.log.error("Failed to start requestSocket (bind): '" + self.requestConId + "'", exc_info=True)
            raise

        # Poller to distinguish between start/stop signals and queries for the next set of signals
        self.poller = zmq.Poller()
        self.poller.register(self.controlSubSocket, zmq.POLLIN)
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
                    self.log.debug("New request for signals received.")

                    openRequests = []

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
                        self.requestFwSocket.send(cPickle.dumps(openRequests))
                        self.log.debug("Answered to request: " + str(openRequests))
                    else:
                        openRequests = ["None"]
                        self.requestFwSocket.send(cPickle.dumps(openRequests))
                        self.log.debug("Answered to request: " + str(openRequests))

                except:
                    self.log.error("Failed to receive/answer new signal requests.", exc_info=True)

            if self.comSocket in socks and socks[self.comSocket] == zmq.POLLIN:

                incomingMessage = self.comSocket.recv_multipart()
                self.log.debug("Received signal: " + str(incomingMessage) )

                checkFailed, signal, target = self.checkSignalInverted(incomingMessage)
                if not checkFailed:
                    self.reactToSignal(signal, target)
                else:
                    self.sendResponse(checkFailed)

            if self.requestSocket in socks and socks[self.requestSocket] == zmq.POLLIN:

                incomingMessage = self.requestSocket.recv_multipart()
                self.log.debug("Received request: " + str(incomingMessage) )

                if incomingMessage[0] == "NEXT":

                    if ".desy.de:" in incomingMessage[1]:
                        incomingMessage[1] = incomingMessage[1].replace(".desy.de:", ":")

                    incomingSocketId = incomingMessage[1]

                    for index in range(len(self.allowedQueries)):
                        for i in range(len(self.allowedQueries[index])):
                            if incomingSocketId == self.allowedQueries[index][i][0]:
                                self.openRequVari[index].append(self.allowedQueries[index][i])
                                self.log.info("Add to open requests: " + str(self.allowedQueries[index][i]) )

                elif incomingMessage[0] == "CANCEL":

                    if ".desy.de:" in incomingMessage[1]:
                        incomingMessage[1] = incomingMessage[1].replace(".desy.de:", ":")

                    incomingSocketId = incomingMessage[1]

                    self.openRequVari =  [ [ b for b in  self.openRequVari[a] if incomingSocketId != b[0] ] for a in range(len(self.openRequVari)) ]
                    self.log.info("Remove all occurences from " + str(incomingSocketId) + " from variable request list.")

                else:
                    self.log.info("Request not supported.")


            if self.controlSubSocket in socks and socks[self.controlSubSocket] == zmq.POLLIN:

                try:
                    message = self.controlSubSocket.recv_multipart()
                    self.log.debug("Control signal received.")
                except:
                    self.log.error("Waiting for control signal...failed", exc_info=True)
                    continue

                # remove subsription topic
                del message[0]

                if message[0] == b"EXIT":
                    self.log.info("Requested to shutdown.")
                    break
                else:
                    self.log.error("Unhandled control signal received: " + str(message[0]))


    def checkSignalInverted (self, incomingMessage):

        if len(incomingMessage) != 3:

            self.log.info("Received signal is of the wrong format")
            self.log.debug("Received signal is too short or too long: " + str(incomingMessage))
            return "NO_VALID_SIGNAL", None, None

        else:

            version, signal, target = incomingMessage
            target = cPickle.loads(target)

            try:
                host = [t[0].split(":")[0] for t in target]
            except:
                return "NO_VALID_SIGNAL", None, None

            if version:
                if helpers.checkVersion(version, self.log):
                    self.log.debug("Versions are compatible: " + str(version))
                else:
                    self.log.debug("Version are not compatible")
                    return "VERSION_CONFLICT", None, None

            if signal and host:

                # Checking signal sending host
                self.log.debug("Check if host to send data to are in WhiteList...")
                if helpers.checkHost(host, self.whiteList, self.log):
                    self.log.debug("Hosts are allowed to connect.")
                    self.log.debug("hosts: " + str(host))
                else:
                    self.log.debug("One of the hosts is not allowed to connect.")
                    self.log.debug("hosts: " + str(host))
                    return "NO_VALID_HOST", None, None

        return False, signal, target


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
#            self.sendResponse("CONNECTION_ALREADY_OPEN")
            # "reopen" the connection and confirm to receiver
            self.sendResponse(signal)


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
            controlPubSocket.send_multipart(["signal", "CLOSE_SOCKETS", cPickle.dumps(socketIds)])

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
        self.log.debug("Closing sockets for SignalHandler")
        if self.comSocket:
            self.log.info("Closing comSocket")
            self.comSocket.close(0)
            self.comSocket = None

        if self.requestFwSocket:
            self.log.info("Closing requestFwSocket")
            self.requestFwSocket.close(0)
            self.requestFwSocket = None

        if self.requestSocket:
            self.log.info("Closing requestSocket")
            self.requestSocket.close(0)
            self.requestSocket = None

        if self.controlPubSocket:
            self.log.info("Closing controlPubSocket")
            self.controlPubSocket.close(0)
            self.controlPubSocket = None

        if self.controlSubSocket:
            self.log.info("Closing controlSubSocket")
            self.controlSubSocket.close(0)
            self.controlSubSocket = None

        if not self.extContext and self.context:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class requestPuller():
    def __init__ (self, requestFwConId, logQueue, context = None):

        self.log = self.getLogger(logQueue)

        self.context         = context or zmq.Context.instance()
        self.requestFwSocket = self.context.socket(zmq.REQ)
        self.requestFwSocket.connect(requestFwConId)
        self.log.info("[getRequests] requestFwSocket started (connect) for '" + requestFwConId + "'")

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
    import threading
    from version import __version__

    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows

    whiteList     = ["localhost", "zitpcx19282"]

    localhost       = "127.0.0.1"
    extIp           = "0.0.0.0"

    controlPort     = "7000"
    comPort         = "6000"
    requestPort     = "6002"

    currentPID      = os.getpid()

    controlPubConId = "ipc://{pid}_{id}".format(pid=currentPID, id="controlPub")
    controlSubConId = "ipc://{pid}_{id}".format(pid=currentPID, id="controlSub")
    comConId        = "tcp://{ip}:{port}".format(ip=extIp,     port=comPort)
    requestFwConId  = "ipc://{pid}_{id}".format(pid=currentPID, id="requestFw")
    requestConId    = "tcp://{ip}:{port}".format(ip=extIp,     port=requestPort)

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

    # Register context
    context = zmq.Context()

    # initiate forwarder for control signals (multiple pub, multiple sub)
    device = zmq.devices.ThreadDevice(zmq.FORWARDER, zmq.SUB, zmq.PUB)
    device.bind_in(controlPubConId)
    device.bind_out(controlSubConId)
    device.setsockopt_in(zmq.SUBSCRIBE, b"")
    device.start()

    # create control socket
    controlPubSocket = context.socket(zmq.PUB)
    controlPubSocket.connect(controlPubConId)
    logging.info("=== controlPubSocket connect to: '" + controlPubConId + "'")

    signalHandlerPr = threading.Thread ( target = SignalHandler, args = (controlPubConId, controlSubConId, whiteList, comConId, requestFwConId, requestConId, logQueue, context) )
    signalHandlerPr.start()

    requestPullerPr = Process ( target = requestPuller, args = (requestFwConId, logQueue) )
    requestPullerPr.start()


    def sendSignal(socket, signal, ports, prio = None):
        logging.info("=== sendSignal : " + signal + ", " + str(ports))
        sendMessage = [__version__,  signal]
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


    comSocket       = context.socket(zmq.REQ)
    comSocket.connect(comConId)
    logging.info("=== comSocket connected to " + comConId)

    requestSocket   = context.socket(zmq.PUSH)
    requestSocket.connect(requestConId)
    logging.info("=== requestSocket connected to " + requestConId)

    requestFwSocket = context.socket(zmq.REQ)
    requestFwSocket.connect(requestFwConId)
    logging.info("=== requestFwSocket connected to " + requestFwConId)

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


    controlPubSocket.send_multipart(["control", "EXIT"])
    logging.debug("=== EXIT")

    signalHandlerPr.join()
    requestPullerPr.terminate()

    controlPubSocket.close(0)

    comSocket.close(0)
    requestSocket.close(0)
    requestFwSocket.close(0)

    context.destroy()

    logQueue.put_nowait(None)
    logQueueListener.stop()


