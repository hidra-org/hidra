from __future__ import unicode_literals

import time
import zmq
import zmq.devices
import logging
import os
import copy
import json

from __init__ import BASE_PATH
from logutils.queue import QueueHandler
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


#
#  --------------------------  class: SignalHandler  --------------------------
#
class SignalHandler():

    def __init__(self, control_pub_con_id, control_sub_con_id, whitelist,
                 com_con_id, request_fw_con_id, request_con_id, log_queue,
                 context=None):

        # Send all logs to the main process
        self.log = self.get_logger(log_queue)

        self.current_pid = os.getpid()
        self.log.debug("SignalHandler started (PID {0})."
                       .format(self.current_pid))

        self.control_pub_con_id = control_pub_con_id
        self.control_sub_con_id = control_sub_con_id
        self.com_con_id = com_con_id
        self.request_fw_con_id = request_fw_con_id
        self.request_con_id = request_con_id

        self.open_connections = []

        self.open_requ_vari = []
        self.open_requ_perm = []
        self.allowed_queries = []
        # to rotate through the open permanent requests
        self.next_requ_node = []

        self.whitelist = []

        for host in whitelist:
            self.whitelist.append(host.replace(".desy.de", ""))

        # sockets
        self.control_pub_socket = None
        self.control_sub_socket = None
        self.com_socket = None
        self.request_fw_socket = None
        self.request_socket = None

        # remember if the context was created outside this class or not
        if context:
            self.context = context
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

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
            self.control_pub_socket = self.context.socket(zmq.PUB)
            self.control_pub_socket.connect(self.control_pub_con_id)
            self.log.info("Start control_pub_socket (connect): '{0}'"
                          .format(self.control_pub_con_id))
        except:
            self.log.error("Failed to start control_pub_socket (connect): "
                           "'{0}'".format(self.control_pub_con_id),
                           exc_info=True)
            raise

        # socket to get control signals from
        try:
            self.control_sub_socket = self.context.socket(zmq.SUB)
            self.control_sub_socket.connect(self.control_sub_con_id)
            self.log.info("Start control_sub_socket (connect): '{0}'"
                          .format(self.control_sub_con_id))
        except:
            self.log.error("Failed to start control_sub_socket (connect): "
                           "'{0}'".format(self.control_sub_con_id),
                           exc_info=True)
            raise

        self.control_sub_socket.setsockopt_string(zmq.SUBSCRIBE, u"control")

        # create zmq socket for signal communication with receiver
        try:
            self.com_socket = self.context.socket(zmq.REP)
            self.com_socket.bind(self.com_con_id)
            self.log.info("Start com_socket (bind): '{0}'"
                          .format(self.com_con_id))
        except:
            self.log.error("Failed to start com_socket (bind): '{0}'"
                           .format(self.com_con_id), exc_info=True)
            raise

        # setting up router for load-balancing worker-processes.
        # each worker-process will handle a file event
        try:
            self.request_fw_socket = self.context.socket(zmq.REP)
            self.request_fw_socket.bind(self.request_fw_con_id)
            self.log.info("Start request_fw_socket (bind): '{0}'"
                          .format(self.request_fw_con_id))
        except:
            self.log.error("Failed to start request_fw_socket (bind): '{0}'"
                           .format(self.request_fw_con_id), exc_info=True)
            raise

        # create socket to receive requests
        try:
            self.request_socket = self.context.socket(zmq.PULL)
            self.request_socket.bind(self.request_con_id)
            self.log.info("request_socket started (bind) for '{0}'"
                          .format(self.request_con_id))
        except:
            self.log.error("Failed to start request_socket (bind): '{0}'"
                           .format(self.request_con_id), exc_info=True)
            raise

        # Poller to distinguish between start/stop signals and queries for the
        # next set of signals
        self.poller = zmq.Poller()
        self.poller.register(self.control_sub_socket, zmq.POLLIN)
        self.poller.register(self.com_socket, zmq.POLLIN)
        self.poller.register(self.request_fw_socket, zmq.POLLIN)
        self.poller.register(self.request_socket, zmq.POLLIN)

    def run(self):
        # run loop, and wait for incoming messages
        self.log.debug("Waiting for new signals or requests.")
        while True:
            socks = dict(self.poller.poll())

            ######################################
            # incoming request from TaskProvider #
            ######################################
            if (self.request_fw_socket in socks
                    and socks[self.request_fw_socket] == zmq.POLLIN):

                try:
                    in_message = self.request_fw_socket.recv_multipart()
                    if in_message[0] == b"GET_REQUESTS":
                        self.log.debug("New request for signals received.")
                        filename = json.loads(in_message[1].decode("utf-8"))
                        open_requests = []

                        for request_set in self.open_requ_perm:
                            if request_set:
                                index = self.open_requ_perm.index(request_set)
                                tmp = request_set[self.next_requ_node[index]]
                                # Check if filename suffix matches requested
                                # suffix
                                if filename.endswith(tuple(tmp[2])):
                                    open_requests.append(copy.deepcopy(tmp))
                                    # distribute in round-robin order
                                    self.next_requ_node[index] = (
                                        (self.next_requ_node[index] + 1)
                                        % len(request_set)
                                        )

                        for request_set in self.open_requ_vari:
                            # Check if filename suffix matches requested suffix
                            if (request_set
                                    and filename.endswith(
                                        tuple(request_set[0][2]))):
                                tmp = request_set.pop(0)
                                open_requests.append(tmp)

                        if open_requests:
                            self.request_fw_socket.send_string(
                                json.dumps(open_requests))
                            self.log.debug("Answered to request: {0}"
                                           .format(open_requests))
                            self.log.debug("open_requ_vari: {0}"
                                           .format(self.open_requ_vari))
                            self.log.debug("allowed_queries: {0}"
                                           .format(self.allowed_queries))
                        else:
                            open_requests = ["None"]
                            self.request_fw_socket.send_string(
                                json.dumps(open_requests))
                            self.log.debug("Answered to request: {0}"
                                           .format(open_requests))
                            self.log.debug("open_requ_vari: {0}"
                                           .format(self.open_requ_vari))
                            self.log.debug("allowed_queries: {0}"
                                           .format(self.allowed_queries))

                except:
                    self.log.error("Failed to receive/answer new signal "
                                   "requests", exc_info=True)

            ######################################
            #  start/stop command from external  #
            ######################################
            if (self.com_socket in socks
                    and socks[self.com_socket] == zmq.POLLIN):

                in_message = self.com_socket.recv_multipart()
                self.log.debug("Received signal: {0}".format(in_message))

                check_failed, signal, target = (
                    self.check_signal_inverted(in_message)
                    )
                if not check_failed:
                    self.react_to_signal(signal, target)
                else:
                    self.send_response(check_failed)

            ######################################
            #        request from external       #
            ######################################
            if (self.request_socket in socks
                    and socks[self.request_socket] == zmq.POLLIN):

                in_message = self.request_socket.recv_multipart()
                self.log.debug("Received request: {0}".format(in_message))

                if in_message[0] == b"NEXT":
                    incoming_socket_id = (
                        in_message[1]
                        .decode("utf-8")
                        .replace(".desy.de:", ":")
                        )

                    for index in range(len(self.allowed_queries)):
                        for i in range(len(self.allowed_queries[index])):
                            if (incoming_socket_id
                                    == self.allowed_queries[index][i][0]):
                                self.open_requ_vari[index].append(
                                    self.allowed_queries[index][i])
                                self.log.info("Add to open requests: {0}"
                                              .format(self.allowed_queries[
                                                  index][i]))

                elif in_message[0] == b"CANCEL":
                    incoming_socket_id = (
                        in_message[1]
                        .decode("utf-8")
                        .replace(".desy.de:", ":")
                        )

                    still_requested = []
                    for a in range(len(self.open_requ_vari)):
                        vari_per_group = []
                        for b in self.open_requ_vari[a]:
                            if incoming_socket_id != b[0]:
                                vari_per_group.append(b)

                        still_requested.append(vari_per_group)

                    self.open_requ_vari = still_requested

#                    self.open_requ_vari_old = [
#                        [
#                            b
#                            for b in self.open_requ_vari[a]
#                            if incoming_socket_id != b[0]]
#                        for a in range(len(self.open_requ_vari))]

#                    self.log.debug("open_requ_vari_old ={0}"
#                                   .format(self.open_requ_vari_old))
#                    self.log.debug("open_requ_vari     ={0}"
#                                   .format(self.open_requ_vari))

                    self.log.info("Remove all occurences from {0} from "
                                  "variable request list."
                                  .format(incoming_socket_id))

                else:
                    self.log.info("Request not supported.")

            ######################################
            #   control commands from internal   #
            ######################################
            if (self.control_sub_socket in socks
                    and socks[self.control_sub_socket] == zmq.POLLIN):

                try:
                    message = self.control_sub_socket.recv_multipart()
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

    def check_signal_inverted(self, in_message):

        if len(in_message) != 3:

            self.log.warning("Received signal is of the wrong format")
            self.log.debug("Received signal is too short or too long: {0}"
                           .format(in_message))
            return b"NO_VALID_SIGNAL", None, None

        else:

            version, signal, target = (
                in_message[0].decode("utf-8"),
                in_message[1],
                in_message[2].decode("utf-8")
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
                               "whitelist...")
                if helpers.check_host(host, self.whitelist, self.log):
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
            self.com_socket.send(signal, zmq.NOBLOCK)

    def __start_signal(self, signal, send_type, socket_ids, list_to_check,
                       vari_list, corresp_list):

        # make host naming consistent
        for socket_conf in socket_ids:
            socket_conf[0] = socket_conf[0].replace(".desy.de:", ":")

        overwrite_index = None
        flatlist_nested = [set([j[0] for j in sublist])
                           for sublist in list_to_check]
        socket_ids_flatlist = set([socket_conf[0]
                                   for socket_conf in socket_ids])

        for i in flatlist_nested:
            # Check if socket_ids is sublist of one entry of list_to_check
            if socket_ids_flatlist.issubset(i):
                self.log.debug("socket_ids already contained, override")
                overwrite_index = flatlist_nested.index(i)
            # Check if one entry of list_to_check is sublist in socket_ids
            elif i.issubset(socket_ids_flatlist):
                self.log.debug("socket_ids is superset of already contained "
                               "set, override")
                overwrite_index = flatlist_nested.index(i)
            # TODO Mixture ?
            elif not socket_ids_flatlist.isdisjoint(i):
                self.log.debug("socket_ids is neither a subset nor superset "
                               "of already contained set")
                self.log.debug("Currently: no idea what to do with this.")
                self.log.debug("socket_ids={0}".format(socket_ids_flatlist))
                self.log.debug("flatlist_nested[i]={0}".format(i))

        if overwrite_index is not None:
            # overriding is necessary because the new request may contain
            # different parameters like monitored file suffix, priority or
            # connection type also this means the old socket_id set should be
            # replaced in total and not only partially
            self.log.debug("overwrite_index={0}".format(overwrite_index))

            list_to_check[overwrite_index] = copy.deepcopy(
                sorted([i + [send_type] for i in socket_ids]))
            if corresp_list is not None:
                corresp_list[overwrite_index] = 0

            if vari_list is not None:
                vari_list[overwrite_index] = []
        else:
            list_to_check.append(copy.deepcopy(
                sorted([i + [send_type] for i in socket_ids])))
            if corresp_list is not None:
                corresp_list.append(0)

            if vari_list is not None:

                vari_list.append([])

        self.log.debug("after start handling: list_to_check={0}"
                       .format(list_to_check))

        # send signal back to receiver
        self.send_response(signal)

#        connection_found = False
#        tmp_allowed = []
#        flatlist = [i[0] for i in
#                    [j for sublist in list_to_check for j in sublist]]
#        self.log.debug("flatlist: {0}".format(flatlist))

#        for socket_conf in socket_ids:
#
#            socket_conf[0] = socket_conf[0].replace(".desy.de:",":")
#
#            socket_id = socket_conf[0]
#            self.log.debug("socket_id: {0}".format(socket_id))
#
#            if socket_id in flatlist:
#                connection_found = True
#                self.log.info("Connection to {0} is already open"
#                              .format(socket_id))
#            elif socket_id not in [ i[0] for i in tmp_allowed]:
#                tmp_socket_conf = socket_conf + [send_type]
#                tmp_allowed.append(tmp_socket_conf)
#            else:
#                # TODO send notification back?
#                # (double entries in START_QUERY_NEXT)
#                pass

#        if not connection_found:
#            # send signal back to receiver
#            self.send_response(signal)
#            list_to_check.append(copy.deepcopy(sorted(tmp_allowed)))
#            if corresp_list != None:
#                corresp_list.append(0)
#            del tmp_allowed
#
#            if vari_list != None:
#                vari_list.append([])
#        else:
#            # send error back to receiver
# #           self.send_response("CONNECTION_ALREADY_OPEN")
#            # "reopen" the connection and confirm to receiver
#            self.send_response(signal)

    def __stop_signal(self, signal, socket_ids, list_to_check, vari_list,
                      corresp_list):

        connection_not_found = False
        tmp_remove_index = []
        tmp_remove_element = []
        found = False

        for socket_conf in socket_ids:

            socket_id = socket_conf[0].replace(".desy.de:", ":")

            for sublist in list_to_check:
                for element in sublist:
                    if socket_id == element[0]:
                        tmp_remove_element.append(element)
                        found = True
            if not found:
                connection_not_found = True

        if connection_not_found:
            self.send_response(b"NO_OPEN_CONNECTION_FOUND")
            self.log.info("No connection to close was found for {0}"
                          .format(socket_conf))
        else:
            # send signal back to receiver
            self.send_response(signal)

            for element in tmp_remove_element:

                socket_id = element[0]

                if vari_list is not None:
                    vari_list = [[b for b in vari_list[a] if socket_id != b[0]]
                                 for a in range(len(vari_list))]
                    self.log.debug("Remove all occurences from {0} from "
                                   "variable request list.".format(socket_id))

                for i in range(len(list_to_check)):
                    if element in list_to_check[i]:
                        list_to_check[i].remove(element)
                        self.log.debug("Remove {0} from pemanent request "
                                       "allowed list.".format(socket_id))

                        if not list_to_check[i]:
                            tmp_remove_index.append(i)
                            if vari_list is not None:
                                del vari_list[i]
                            if corresp_list is not None:
                                corresp_list.pop(i)
                        else:
                            if corresp_list is not None:
                                corresp_list[i] = (
                                    corresp_list[i] % len(list_to_check[i])
                                    )

                for index in tmp_remove_index:
                    del list_to_check[index]

            # send signal to TaskManager
            self.control_pub_socket.send_multipart(
                [b"signal", b"CLOSE_SOCKETS",
                    json.dumps(socket_ids).encode("utf-8")])

        return list_to_check, vari_list, corresp_list

    def react_to_signal(self, signal, socket_ids):

        ###########################
        #       START_STREAM      #
        ###########################
        if signal == b"START_STREAM":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socket_ids))

            self.__start_signal(signal, "data", socket_ids,
                                self.open_requ_perm, None,
                                self.next_requ_node)

            return

        ###########################
        #  START_STREAM_METADATA  #
        ###########################
        elif signal == b"START_STREAM_METADATA":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socket_ids))

            self.__start_signal(signal, "metadata", socket_ids,
                                self.open_requ_perm, None,
                                self.next_requ_node)

            return

        ###########################
        #       STOP_STREAM       #
        #  STOP_STREAM_METADATA   #
        ###########################
        elif signal == b"STOP_STREAM" or signal == b"STOP_STREAM_METADATA":
            self.log.info("Received signal: {0} for host {1}"
                          .format(signal, socket_ids))

            self.open_requ_perm, nonetmp, self.next_requ_node = (
                self.__stop_signal(signal, socket_ids, self.open_requ_perm,
                                   None, self.next_requ_node)
                )

            return

        ###########################
        #       START_QUERY       #
        ###########################
        elif signal == b"START_QUERY_NEXT":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socket_ids))

            self.__start_signal(signal, "data", socket_ids,
                                self.allowed_queries, self.open_requ_vari,
                                None)

            return

        ###########################
        #  START_QUERY_METADATA   #
        ###########################
        elif signal == b"START_QUERY_METADATA":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socket_ids))

            self.__start_signal(signal, "metadata", socket_ids,
                                self.allowed_queries,
                                self.open_requ_vari, None)

            return

        ###########################
        #       STOP_QUERY        #
        #  STOP_QUERY_METADATA    #
        ###########################
        elif signal == b"STOP_QUERY_NEXT" or signal == b"STOP_QUERY_METADATA":
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socket_ids))

            self.allowed_queries, self.open_requ_vari, nonetmp = (
                self.__stop_signal(signal, socket_ids, self.allowed_queries,
                                   self.open_requ_vari, None)
                )

            return

        else:
            self.log.info("Received signal: {0} for hosts {1}"
                          .format(signal, socket_ids))
            self.send_response(b"NO_VALID_SIGNAL")

    def stop(self):
        self.log.debug("Closing sockets for SignalHandler")
        if self.com_socket:
            self.log.info("Closing com_socket")
            self.com_socket.close(0)
            self.com_socket = None

        if self.request_fw_socket:
            self.log.info("Closing request_fw_socket")
            self.request_fw_socket.close(0)
            self.request_fw_socket = None

        if self.request_socket:
            self.log.info("Closing request_socket")
            self.request_socket.close(0)
            self.request_socket = None

        if self.control_pub_socket:
            self.log.info("Closing control_pub_socket")
            self.control_pub_socket.close(0)
            self.control_pub_socket = None

        if self.control_sub_socket:
            self.log.info("Closing control_sub_socket")
            self.control_sub_socket.close(0)
            self.control_sub_socket = None

        if not self.ext_context and self.context:
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
    def __init__(self, request_fw_con_id, log_queue, context=None):

        self.log = self.get_logger(log_queue)

        self.context = context or zmq.Context.instance()
        self.request_fw_socket = self.context.socket(zmq.REQ)
        self.request_fw_socket.connect(request_fw_con_id)
        self.log.info("[getRequests] request_fw_socket started (connect) for "
                      "'{0}'".format(request_fw_con_id))

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
                self.request_fw_socket.send_multipart([b"GET_REQUESTS"])
                self.log.info("[getRequests] send")
                requests = (
                    json.loads(
                        self.request_fw_socket.recv().decode("utf-8"))
                    )
                self.log.info("[getRequests] Requests: {0}".format(requests))
                time.sleep(0.25)
            except Exception as e:
                self.log.error(str(e), exc_info=True)
                break

    def __exit__(self):
        self.request_fw_socket.close(0)
        self.context.destroy()


if __name__ == '__main__':
    from multiprocessing import Process, freeze_support, Queue
    import threading
    from _version import __version__

    # see https://docs.python.org/2/library/multiprocessing.html#windows
    freeze_support()

    whitelist = ["localhost", "zitpcx19282"]

    localhost = "127.0.0.1"
    ext_ip = "0.0.0.0"

    control_port = "7000"
    com_port = "6000"
    request_port = "6002"

    current_pid = os.getpid()

    control_pub_path = "{0}_{1}".format(current_pid, "controlPub")
    control_pub_con_id = "ipc://{0}".format(control_pub_path)

    control_sub_path = "{0}_{1}".format(current_pid, "controlSub")
    control_sub_con_id = "ipc://{0}".format(control_sub_path)

    com_con_id = "tcp://{0}:{1}".format(ext_ip, com_port)
    request_fw_con_id = ("ipc://{0}_{1}"
                         .format(current_pid, "requestFw"))
    request_con_id = "tcp://{0}:{1}".format(ext_ip, request_port)

    logfile = os.path.join(BASE_PATH, "logs", "signalhandler.log")
    logsize = 10485760

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True,
                                      onscreen_log_level="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = helpers.CustomQueueListener(log_queue, h1, h2)
    log_queue_listener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    # Register context
    context = zmq.Context()

    # initiate forwarder for control signals (multiple pub, multiple sub)
    device = zmq.devices.ThreadDevice(zmq.FORWARDER, zmq.SUB, zmq.PUB)
    device.bind_in(control_pub_con_id)
    device.bind_out(control_sub_con_id)
    device.setsockopt_in(zmq.SUBSCRIBE, b"")
    device.start()

    # create control socket
    control_pub_socket = context.socket(zmq.PUB)
    control_pub_socket.connect(control_pub_con_id)
    logging.info("=== control_pub_socket connect to: '{0}'"
                 .format(control_pub_con_id))

    signalhandler_pr = threading.Thread(
        target=SignalHandler,
        args=(control_pub_con_id, control_sub_con_id, whitelist, com_con_id,
              request_fw_con_id, request_con_id, log_queue, context))
    signalhandler_pr.start()

    request_puller_pr = Process(
        target=RequestPuller,
        args=(request_fw_con_id, log_queue))
    request_puller_pr.start()

    def send_signal(socket, signal, ports, prio=None):
        logging.info("=== send_signal : {0}, {1}".format(signal, ports))
        send_message = [__version__, signal]
        targets = []
        if type(ports) == list:
            for port in ports:
                targets.append(["zitpcx19282:{0}".format(port), prio])
        else:
            targets.append(["zitpcx19282:{0}".format(ports), prio])
        targets = json.dumps(targets).encode("utf-8")
        send_message.append(targets)
        socket.send_multipart(send_message)
        received_message = socket.recv()
        logging.info("=== Responce : {0}".format(received_message))

    def send_request(socket, socket_id):
        send_message = [b"NEXT", socket_id.encode('utf-8')]
        logging.info("=== send_request: {0}".format(send_message))
        socket.send_multipart(send_message)
        logging.info("=== request sent: {0}".format(send_message))

    def cancel_request(socket, socket_id):
        send_message = [b"CANCEL", socket_id.encode('utf-8')]
        logging.info("=== send_request: {0}".format(send_message))
        socket.send_multipart(send_message)
        logging.info("=== request sent: {0}".format(send_message))

    com_socket = context.socket(zmq.REQ)
    com_socket.connect(com_con_id)
    logging.info("=== com_socket connected to {0}".format(com_con_id))

    request_socket = context.socket(zmq.PUSH)
    request_socket.connect(request_con_id)
    logging.info("=== request_socket connected to {0}". format(request_con_id))

    request_fw_socket = context.socket(zmq.REQ)
    request_fw_socket.connect(request_fw_con_id)
    logging.info("=== request_fw_socket connected to {0}"
                 .format(request_fw_con_id))

    time.sleep(1)

    send_signal(com_socket, b"START_STREAM", "6003", 1)

    send_signal(com_socket, b"START_STREAM", "6004", 0)

    send_signal(com_socket, b"STOP_STREAM", "6003")

    send_request(request_socket, "zitpcx19282:6006")

    send_signal(com_socket, b"START_QUERY_NEXT", ["6005", "6006"], 2)

    send_request(request_socket, b"zitpcx19282:6005")
    send_request(request_socket, b"zitpcx19282:6005")
    send_request(request_socket, b"zitpcx19282:6006")

    cancel_request(request_socket, b"zitpcx19282:6005")

    time.sleep(0.5)

    send_request(request_socket, "zitpcx19282:6005")
    send_signal(com_socket, b"STOP_QUERY_NEXT", "6005", 2)

    time.sleep(1)

    control_pub_socket.send_multipart([b"control", b"EXIT"])
    logging.debug("=== EXIT")

    signalhandler_pr.join()
    request_puller_pr.terminate()

    control_pub_socket.close(0)

    com_socket.close(0)
    request_socket.close(0)
    request_fw_socket.close(0)

    context.destroy()

    try:
        os.remove(control_pub_path)
        logging.debug("Removed ipc socket: {0}".format(control_pub_path))
    except OSError:
        logging.warning("Could not remove ipc socket: {0}"
                         .format(control_pub_path))
    except:
        logging.warning("Could not remove ipc socket: {0}"
                        .format(control_pub_path), exc_info=True)

    try:
        os.remove(control_sub_path)
        logging.debug("Removed ipc socket: {0}".format(control_sub_path))
    except OSError:
        logging.warning("Could not remove ipc socket: {0}"
                        .format(control_sub_path))
    except:
        logging.warning("Could not remove ipc socket: {0}"
                        .format(control_sub_path), exc_info=True)


    log_queue.put_nowait(None)
    log_queue_listener.stop()
