from __future__ import unicode_literals

import time
import zmq
import zmq.devices
import logging
import os
import sys
import copy
import json


# path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
try:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.realpath(__file__))))
except:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.realpath('__file__'))))
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)
del SHARED_PATH

from logutils.queue import QueueHandler
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


#
#  --------------------------  class: SignalHandler  --------------------------
#
class SignalHandler():

    def __init__(self, controlPubConId, controlSubConId, whiteList, comConId,
                 requestFwConId, requestConId, logQueue, context=None):

        # Send all logs to the main process
        self.log = self.get_logger(logQueue)

        self.currentPID = os.getpid()
        self.log.debug("SignalHandler started (PID {pid})."
                       .format(pid=self.currentPID))

        self.controlPubConId = controlPubConId
        self.controlSubConId = controlSubConId
        self.comConId = comConId
        self.requestFwConId = requestFwConId
        self.requestConId = requestConId

        self.openConnections = []
        self.forwardSignal = []

        self.openRequVari = []
        self.openRequPerm = []
        self.allowedQueries = []
        # to rotate through the open permanent requests
        self.nextRequNode = []

        self.whiteList = []

        for host in whiteList:
            self.whiteList.append(host.replace(".desy.de", ""))

        # sockets
        self.controlPubSocket = None
        self.controlSubSocket = None
        self.comSocket = None
        self.requestFwSocket = None
        self.requestSocket = None

        # remember if the context was created outside this class or not
        if context:
            self.context = context
            self.extContext = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.extContext = False

        try:
            self.create_sockets()

            self.run()
        except zmq.ZMQError:
            self.log.error("Stopping signalHandler due to ZMQError.",
                           exc_info=True)
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping SignalHandler due to unknown error "
                           "condition.", exc_info=True)
        finally:
            self.stop()

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("SignalHandler")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def create_sockets(self):

        # socket to send control signals to
        try:
            self.controlPubSocket = self.context.socket(zmq.PUB)
            self.controlPubSocket.connect(self.controlPubConId)
            self.log.info("Start controlPubSocket (connect): '{0}'"
                          .format(self.controlPubConId))
        except:
            self.log.error("Failed to start controlPubSocket (connect): '{0}'"
                           .format(self.controlPubConId), exc_info=True)
            raise

        # socket to get control signals from
        try:
            self.controlSubSocket = self.context.socket(zmq.SUB)
            self.controlSubSocket.connect(self.controlSubConId)
            self.log.info("Start controlSubSocket (connect): '{0}'"
                          .format(self.controlSubConId))
        except:
            self.log.error("Failed to start controlSubSocket (connect): '{0}'"
                           .format(self.controlSubConId), exc_info=True)
            raise

        self.controlSubSocket.setsockopt_string(zmq.SUBSCRIBE, u"control")

        # create zmq socket for signal communication with receiver
        try:
            self.comSocket = self.context.socket(zmq.REP)
            self.comSocket.bind(self.comConId)
            self.log.info("Start comSocket (bind): '{0}'"
                          .format(self.comConId))
        except:
            self.log.error("Failed to start comSocket (bind): '{0}'"
                           .format(self.comConId), exc_info=True)
            raise

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        try:
            self.requestFwSocket = self.context.socket(zmq.REP)
            self.requestFwSocket.bind(self.requestFwConId)
            self.log.info("Start requestFwSocket (bind): '{0}'"
                          .format(self.requestFwConId))
        except:
            self.log.error("Failed to start requestFwSocket (bind): '{0}'"
                           .format(self.requestFwConId), exc_info=True)
            raise

        # create socket to receive requests
        try:
            self.requestSocket = self.context.socket(zmq.PULL)
            self.requestSocket.bind(self.requestConId)
            self.log.info("requestSocket started (bind) for '{0}'"
                          .format(self.requestConId))
        except:
            self.log.error("Failed to start requestSocket (bind): '{0}'"
                           .format(self.requestConId), exc_info=True)
            raise

        # Poller to distinguish between start/stop signals and queries for the
        # next set of signals
        self.poller = zmq.Poller()
        self.poller.register(self.controlSubSocket, zmq.POLLIN)
        self.poller.register(self.comSocket, zmq.POLLIN)
        self.poller.register(self.requestFwSocket, zmq.POLLIN)
        self.poller.register(self.requestSocket, zmq.POLLIN)

    def run(self):
        # run loop, and wait for incoming messages
        self.log.debug("Waiting for new signals or requests.")
        while True:
            socks = dict(self.poller.poll())

            ######################################
            # incoming request from TaskProvider #
            ######################################
            if (self.requestFwSocket in socks
                    and socks[self.requestFwSocket] == zmq.POLLIN):

                try:
                    inMessage = self.requestFwSocket.recv_multipart()
                    if inMessage[0] == b"GET_REQUESTS":
                        self.log.debug("New request for signals received.")
                        filename = json.loads(inMessage[1].decode("utf-8"))
                        openRequests = []

                        for requestSet in self.openRequPerm:
                            if requestSet:
                                index = self.openRequPerm.index(requestSet)
                                tmp = requestSet[self.nextRequNode[index]]
                                # Check if filename suffix matches requested
                                # suffix
                                if filename.endswith(tuple(tmp[2])):
                                    openRequests.append(copy.deepcopy(tmp))
                                    # distribute in round-robin order
                                    self.nextRequNode[index] = (
                                        (self.nextRequNode[index] + 1)
                                        % len(requestSet)
                                        )

                        for requestSet in self.openRequVari:
                            # Check if filename suffix matches requested suffix
                            if (requestSet
                                    and filename.endswith(
                                        tuple(requestSet[0][2]))):
                                tmp = requestSet.pop(0)
                                openRequests.append(tmp)

                        if openRequests:
                            self.requestFwSocket.send_string(
                                json.dumps(openRequests))
                            self.log.debug("Answered to request: {0}"
                                           .format(openRequests))
                            self.log.debug("openRequVari: {0}"
                                           .format(self.openRequVari))
                            self.log.debug("allowedQueries: {0}"
                                           .format(self.allowedQueries))
                        else:
                            openRequests = ["None"]
                            self.requestFwSocket.send_string(
                                json.dumps(openRequests))
                            self.log.debug("Answered to request: {0}"
                                           .format(openRequests))
                            self.log.debug("openRequVari: {0}"
                                           .format(self.openRequVari))
                            self.log.debug("allowedQueries: {0}"
                                           .format(self.allowedQueries))

                except:
                    self.log.error("Failed to receive/answer new signal "
                                   "requests", exc_info=True)

            ######################################
            #  start/stop command from external  #
            ######################################
            if (self.comSocket in socks
                    and socks[self.comSocket] == zmq.POLLIN):

                inMessage = self.comSocket.recv_multipart()
                self.log.debug("Received signal: {0}".format(inMessage))

                checkFailed, signal, target = (
                    self.check_signal_inverted(inMessage)
                    )
                if not checkFailed:
                    self.react_to_signal(signal, target)
                else:
                    self.send_response(checkFailed)

            ######################################
            #        request from external       #
            ######################################
            if (self.requestSocket in socks
                    and socks[self.requestSocket] == zmq.POLLIN):

                inMessage = self.requestSocket.recv_multipart()
                self.log.debug("Received request: {0}".format(inMessage))

                if inMessage[0] == b"NEXT":
                    incomingSocketId = (
                        inMessage[1]
                        .decode("utf-8")
                        .replace(".desy.de:", ":")
                        )

                    for index in range(len(self.allowedQueries)):
                        for i in range(len(self.allowedQueries[index])):
                            if (incomingSocketId
                                    == self.allowedQueries[index][i][0]):
                                self.openRequVari[index].append(
                                    self.allowedQueries[index][i])
                                self.log.info("Add to open requests: {0}"
                                              .format(self.allowedQueries[
                                                  index][i]))

                elif inMessage[0] == b"CANCEL":
                    incomingSocketId = (
                        inMessage[1]
                        .decode("utf-8")
                        .replace(".desy.de:", ":")
                        )

                    still_requested = []
                    for a in range(len(self.openRequVari)):
                        vari_per_group = []
                        for b in self.openRequVari[a]:
                            if incomingSocketId != b[0]:
                                vari_per_group.append(b)

                        still_requested.append(vari_per_group)

                    self.openRequVari = still_requested

#                    self.openRequVari_old = [
#                        [
#                            b
#                            for b in self.openRequVari[a]
#                            if incomingSocketId != b[0]]
#                        for a in range(len(self.openRequVari))]

#                    self.log.debug("openRequVari_old ={0}"
#                                   .format(self.openRequVari_old))
#                    self.log.debug("openRequVari     ={0}"
#                                   .format(self.openRequVari))

                    self.log.info("Remove all occurences from {0} from "
                                  "variable request list."
                                  .format(incomingSocketId))

                else:
                    self.log.info("Request not supported.")

            ######################################
            #   control commands from internal   #
            ######################################
            if (self.controlSubSocket in socks
                    and socks[self.controlSubSocket] == zmq.POLLIN):

                try:
                    message = self.controlSubSocket.recv_multipart()
                    self.log.debug("Control signal received.")
                except:
                    self.log.error("Waiting for control signal...failed",
                                   exc_info=True)
                    continue

                # remove subsription topic
                del message[0]

                if message[0] == b"EXIT":
                    self.log.info("Requested to shutdown.")
                    break
                else:
                    self.log.error("Unhandled control signal received: {0}"
                                   .format(message[0]))

    def check_signal_inverted(self, inMessage):

        if len(inMessage) != 3:

            self.log.warning("Received signal is of the wrong format")
            self.log.debug("Received signal is too short or too long: {0}"
                           .format(inMessage))
            return b"NO_VALID_SIGNAL", None, None

        else:

            version, signal, target = (
                inMessage[0].decode("utf-8"),
                inMessage[1],
                inMessage[2].decode("utf-8")
                )
            target = json.loads(target)

            try:
                host = [t[0].split(":")[0] for t in target]
            except:
                return b"NO_VALID_SIGNAL", None, None

            if version:
                if helpers.check_version(version, self.log):
                    self.log.info("Versions are compatible")
                else:
                    self.log.warning("Version are not compatible")
                    return b"VERSION_CONFLICT", None, None

            if signal and host:

                # Checking signal sending host
                self.log.debug("Check if host to send data to are in "
                               "whiteList...")
                if helpers.check_host(host, self.whiteList, self.log):
                    self.log.info("Hosts are allowed to connect.")
                    self.log.debug("hosts: {0}".format(host))
                else:
                    self.log.warning("One of the hosts is not allowed to "
                                     "connect.")
                    self.log.debug("hosts: {0}".format(host))
                    return b"NO_VALID_HOST", None, None

        return False, signal, target

    def send_response(self, signal):
            self.log.debug("Send response back: {0}".format(signal))
            self.comSocket.send(signal, zmq.NOBLOCK)

    def __start_signal(self, signal, sendType, socketIds, listToCheck,
                       variList, correspList):

        # make host naming consistent
        for socketConf in socketIds:
            socketConf[0] = socketConf[0].replace(".desy.de:", ":")

        overwrite_index = None
        flatlist_nested = [set([j[0] for j in sublist])
                           for sublist in listToCheck]
        socketIds_flatlist = set([socketConf[0] for socketConf in socketIds])

        for i in flatlist_nested:
            # Check if socketIds is sublist of one entry of listToCheck
            if socketIds_flatlist.issubset(i):
                self.log.debug("socketIds already contained, override")
                overwrite_index = flatlist_nested.index(i)
            # Check if one entry of listToCheck is sublist in socketIds
            elif i.issubset(socketIds_flatlist):
                self.log.debug("socketIds is superset of already contained "
                               "set, override")
                overwrite_index = flatlist_nested.index(i)
            # TODO Mixture ?
            elif not socketIds_flatlist.isdisjoint(i):
                self.log.debug("socketIds is neither a subset nor superset "
                               "of already contained set")
                self.log.debug("Currently: no idea what to do with this.")
                self.log.debug("socketIds={0}".format(socketIds_flatlist))
                self.log.debug("flatlist_nested[i]={0}".format(i))

        if overwrite_index is not None:
            # overriding is necessary because the new request may contain
            # different parameters like monitored file suffix, priority or
            # connection type also this means the old socketId set should be
            # replaced in total and not only partially
            self.log.debug("overwrite_index={0}".format(overwrite_index))

            listToCheck[overwrite_index] = copy.deepcopy(
                sorted([i + [sendType] for i in socketIds]))
            if correspList is not None:
                correspList[overwrite_index] = 0

            if variList is not None:
                variList[overwrite_index] = []
        else:
            listToCheck.append(copy.deepcopy(
                sorted([i + [sendType] for i in socketIds])))
            if correspList is not None:
                correspList.append(0)

            if variList is not None:

                variList.append([])

        self.log.debug("after start handling: listToCheck={0}"
                       .format(listToCheck))

        # send signal back to receiver
        self.send_response(signal)

#        connectionFound = False
#        tmpAllowed = []
#        flatlist = [i[0] for i in
#                    [j for sublist in listToCheck for j in sublist]]
#        self.log.debug("flatlist: {0}".format(flatlist))

#        for socketConf in socketIds:
#
#            socketConf[0] = socketConf[0].replace(".desy.de:",":")
#
#            socketId = socketConf[0]
#            self.log.debug("socketId: {0}".format(socketId))
#
#            if socketId in flatlist:
#                connectionFound = True
#                self.log.info("Connection to {0} is already open"
#                              .format(socketId))
#            elif socketId not in [ i[0] for i in tmpAllowed]:
#                tmpSocketConf = socketConf + [sendType]
#                tmpAllowed.append(tmpSocketConf)
#            else:
#                # TODO send notification back?
#                # (double entries in START_QUERY_NEXT)
#                pass

#        if not connectionFound:
#            # send signal back to receiver
#            self.send_response(signal)
#            listToCheck.append(copy.deepcopy(sorted(tmpAllowed)))
#            if correspList != None:
#                correspList.append(0)
#            del tmpAllowed
#
#            if variList != None:
#                variList.append([])
#        else:
#            # send error back to receiver
# #           self.send_response("CONNECTION_ALREADY_OPEN")
#            # "reopen" the connection and confirm to receiver
#            self.send_response(signal)

    def __stop_signal(self, signal, socketIds, listToCheck, variList,
                      correspList):

        connectionNotFound = False
        tmpRemoveIndex = []
        tmpRemoveElement = []
        found = False

        for socketConf in socketIds:

            socketId = socketConf[0].replace(".desy.de:", ":")

            for sublist in listToCheck:
                for element in sublist:
                    if socketId == element[0]:
                        tmpRemoveElement.append(element)
                        found = True
            if not found:
                connectionNotFound = True

        if connectionNotFound:
            self.send_response(b"NO_OPEN_CONNECTION_FOUND")
            self.log.info("No connection to close was found for {0}"
                          .format(socketConf))
        else:
            # send signal back to receiver
            self.send_response(signal)

            for element in tmpRemoveElement:

                socketId = element[0]

                if variList is not None:
                    variList = [[b for b in variList[a] if socketId != b[0]]
                                for a in range(len(variList))]
                    self.log.debug("Remove all occurences from {0} from "
                                   "variable request list.".format(socketId))

                for i in range(len(listToCheck)):
                    if element in listToCheck[i]:
                        listToCheck[i].remove(element)
                        self.log.debug("Remove {0} from pemanent request "
                                       "allowed list.".format(socketId))

                        if not listToCheck[i]:
                            tmpRemoveIndex.append(i)
                            if variList is not None:
                                del variList[i]
                            if correspList is not None:
                                correspList.pop(i)
                        else:
                            if correspList is not None:
                                correspList[i] = (
                                    correspList[i] % len(listToCheck[i])
                                    )

                for index in tmpRemoveIndex:
                    del listToCheck[index]

            # send signal to TaskManager
            self.controlPubSocket.send_multipart(
                [b"signal", b"CLOSE_SOCKETS",
                    json.dumps(socketIds).encode("utf-8")])

        return listToCheck, variList, correspList

    def react_to_signal(self, signal, socketIds):

        ###########################
        #       START_STREAM      #
        ###########################
        if signal == b"START_STREAM":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socketIds))

            self.__start_signal(signal, "data", socketIds, self.openRequPerm,
                                None, self.nextRequNode)

            return

        ###########################
        #  START_STREAM_METADATA  #
        ###########################
        elif signal == b"START_STREAM_METADATA":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socketIds))

            self.__start_signal(signal, "metadata", socketIds,
                                self.openRequPerm, None,
                                self.nextRequNode)

            return

        ###########################
        #       STOP_STREAM       #
        #  STOP_STREAM_METADATA   #
        ###########################
        elif signal == b"STOP_STREAM" or signal == b"STOP_STREAM_METADATA":
            self.log.info("Received signal: {0} for host {1}"
                          .format(signal, socketIds))

            self.openRequPerm, nonetmp, self.nextRequNode = (
                self.__stop_signal(signal, socketIds, self.openRequPerm,
                                   None, self.nextRequNode)
                )

            return

        ###########################
        #       START_QUERY       #
        ###########################
        elif signal == b"START_QUERY_NEXT":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socketIds))

            self.__start_signal(signal, "data", socketIds,
                                self.allowedQueries, self.openRequVari,
                                None)

            return

        ###########################
        #  START_QUERY_METADATA   #
        ###########################
        elif signal == b"START_QUERY_METADATA":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socketIds))

            self.__start_signal(signal, "metadata", socketIds,
                                self.allowedQueries,
                                self.openRequVari, None)

            return

        ###########################
        #       STOP_QUERY        #
        #  STOP_QUERY_METADATA    #
        ###########################
        elif signal == b"STOP_QUERY_NEXT" or signal == b"STOP_QUERY_METADATA":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socketIds))

            self.allowedQueries, self.openRequVari, nonetmp = (
                self.__stop_signal(signal, socketIds, self.allowedQueries,
                                   self.openRequVari, None)
                )

            return

        else:
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socketIds))
            self.send_response(b"NO_VALID_SIGNAL")

    def stop(self):
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

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class RequestPuller():
    def __init__(self, requestFwConId, logQueue, context=None):

        self.log = self.get_logger(logQueue)

        self.context = context or zmq.Context.instance()
        self.requestFwSocket = self.context.socket(zmq.REQ)
        self.requestFwSocket.connect(requestFwConId)
        self.log.info("[getRequests] requestFwSocket started (connect) for "
                      "'{0}'".format(requestFwConId))

        self.run()

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("RequestPuller")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def run(self):
        self.log.info("[getRequests] Start run")
        while True:
            try:
                self.requestFwSocket.send_multipart([b"GET_REQUESTS"])
                self.log.info("[getRequests] send")
                requests = (
                    json.loads(
                        self.requestFwSocket.recv().decode("utf-8"))
                    )
                self.log.info("[getRequests] Requests: {0}".format(requests))
                time.sleep(0.25)
            except Exception as e:
                self.log.error(str(e), exc_info=True)
                break

    def __exit__(self):
        self.requestFwSocket.close(0)
        self.context.destroy()


if __name__ == '__main__':
    from multiprocessing import Process, freeze_support, Queue
    import threading
    from version import __version__

    # see https://docs.python.org/2/library/multiprocessing.html#windows
    freeze_support()

    whiteList = ["localhost", "zitpcx19282"]

    localhost = "127.0.0.1"
    extIp = "0.0.0.0"

    controlPort = "7000"
    comPort = "6000"
    requestPort = "6002"

    currentPID = os.getpid()

    controlPubConId = "ipc://{pid}_{id}".format(
        pid=currentPID, id="controlPub")
    controlSubConId = "ipc://{pid}_{id}".format(
        pid=currentPID, id="controlSub")
    comConId = "tcp://{ip}:{port}".format(ip=extIp, port=comPort)
    requestFwConId = "ipc://{pid}_{id}".format(
        pid=currentPID, id="requestFw")
    requestConId = "tcp://{ip}:{port}".format(ip=extIp, port=requestPort)

    logfile = os.path.join(BASE_PATH, "logs", "signalHandler.log")
    logsize = 10485760

    logQueue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True,
                                      onScreenLogLevel="debug")

    # Start queue listener using the stream handler above
    logQueueListener = helpers.CustomQueueListener(logQueue, h1, h2)
    logQueueListener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
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
    logging.info("=== controlPubSocket connect to: '{0}'"
                 .format(controlPubConId))

    signalHandlerPr = threading.Thread(
        target=SignalHandler,
        args=(controlPubConId, controlSubConId, whiteList, comConId,
              requestFwConId, requestConId, logQueue, context))
    signalHandlerPr.start()

    requestPullerPr = Process(
        target=RequestPuller,
        args=(requestFwConId, logQueue))
    requestPullerPr.start()

    def send_signal(socket, signal, ports, prio=None):
        logging.info("=== send_signal : {s}, {p}".format(s=signal, p=ports))
        sendMessage = [__version__,  signal]
        targets = []
        if type(ports) == list:
            for port in ports:
                targets.append(["zitpcx19282:{0}".format(port), prio])
        else:
            targets.append(["zitpcx19282:{0}".format(ports), prio])
        targets = json.dumps(targets).encode("utf-8")
        sendMessage.append(targets)
        socket.send_multipart(sendMessage)
        receivedMessage = socket.recv()
        logging.info("=== Responce : {0}".format(receivedMessage))

    def sendRequest(socket, socketId):
        sendMessage = [b"NEXT", socketId.encode('utf-8')]
        logging.info("=== sendRequest: {0}".format(sendMessage))
        socket.send_multipart(sendMessage)
        logging.info("=== request sent: {0}".format(sendMessage))

    def cancelRequest(socket, socketId):
        sendMessage = [b"CANCEL", socketId.encode('utf-8')]
        logging.info("=== sendRequest: {0}".format(sendMessage))
        socket.send_multipart(sendMessage)
        logging.info("=== request sent: {0}".format(sendMessage))

    comSocket = context.socket(zmq.REQ)
    comSocket.connect(comConId)
    logging.info("=== comSocket connected to {0}".format(comConId))

    requestSocket = context.socket(zmq.PUSH)
    requestSocket.connect(requestConId)
    logging.info("=== requestSocket connected to {0}". format(requestConId))

    requestFwSocket = context.socket(zmq.REQ)
    requestFwSocket.connect(requestFwConId)
    logging.info("=== requestFwSocket connected to {0}"
                 .format(requestFwConId))

    time.sleep(1)

    send_signal(comSocket, b"START_STREAM", "6003", 1)

    send_signal(comSocket, b"START_STREAM", "6004", 0)

    send_signal(comSocket, b"STOP_STREAM", "6003")

    sendRequest(requestSocket, "zitpcx19282:6006")

    send_signal(comSocket, b"START_QUERY_NEXT", ["6005", "6006"], 2)

    sendRequest(requestSocket, b"zitpcx19282:6005")
    sendRequest(requestSocket, b"zitpcx19282:6005")
    sendRequest(requestSocket, b"zitpcx19282:6006")

    cancelRequest(requestSocket, b"zitpcx19282:6005")

    time.sleep(0.5)

    sendRequest(requestSocket, "zitpcx19282:6005")
    send_signal(comSocket, b"STOP_QUERY_NEXT", "6005", 2)

    time.sleep(1)

    controlPubSocket.send_multipart([b"control", b"EXIT"])
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
