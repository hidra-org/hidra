from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import zmq
import zmq.devices
import os
import copy
import json
import re

from __init__ import BASE_PATH  # noqa F401
from _version import __version__
import utils
from hidra import convert_suffix_list_to_regex

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class SignalHandler(object):

    def __init__(self,
                 config,
                 endpoints,
                 whitelist,
                 ldapuri,
                 log_queue,
                 context=None):

        self.config = config
        self.endpoints = endpoints

        self.log = None

        self.open_requ_vari = []
        self.open_requ_perm = []
        self.allowed_queries = []
        # to rotate through the open permanent requests
        self.next_requ_node = []

        self.whitelist = None
        self.open_connections = []

        self.context = None
        self.ext_context = None
        self.socket = None
        self.poller = None

        self.control_pub_socket = None
        self.control_sub_socket = None
        self.com_socket = None
        self.request_fw_socket = None
        self.request_socket = None

        self.setup(log_queue, context, whitelist, ldapuri)

        try:
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

    def setup(self, log_queue, context, whitelist, ldapuri):
        # Send all logs to the main process
        self.log = utils.get_logger("SignalHandler", log_queue)
        self.log.debug("SignalHandler started (PID {}).".format(os.getpid()))

        self.whitelist = utils.extend_whitelist(whitelist, ldapuri, self.log)

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
        except:
            self.log.error("Cannot create sockets", exc_info=True)
            self.stop()

    def start_socket(self, name, sock_type, sock_con, endpoint):
        """Wrapper of utils.start_socket
        """

        return utils.start_socket(
            name=name,
            sock_type=sock_type,
            sock_con=sock_con,
            endpoint=endpoint,
            context=self.context,
            log=self.log
        )

    def create_sockets(self):

        # socket to send control signals to
        self.control_pub_socket = self.start_socket(
            name="control_pub_socket",
            sock_type=zmq.PUB,
            sock_con="connect",
            endpoint=self.endpoints.control_pub_con
        )

        # socket to get control signals from
        self.control_sub_socket = self.start_socket(
            name="control_sub_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.control_sub_con
        )

        self.control_sub_socket.setsockopt_string(zmq.SUBSCRIBE, u"control")

        # socket to forward requests
        self.request_fw_socket = self.start_socket(
            name="request_fw_socket",
            sock_type=zmq.REP,
            sock_con="bind",
            endpoint=self.endpoints.request_fw_bind
        )

        if self.whitelist != []:
            # create zmq socket for signal communication with receiver
            self.com_socket = self.start_socket(
                name="com_socket",
                sock_type=zmq.REP,
                sock_con="bind",
                endpoint=self.endpoints.com_bind
            )

            # create socket to receive requests
            self.request_socket = self.start_socket(
                name="request_socket",
                sock_type=zmq.PULL,
                sock_con="bind",
                endpoint=self.endpoints.request_bind
            )
        else:
            self.log.info("Socket com_socket and request_socket not started "
                          "since there is no host allowed to connect")

        # Poller to distinguish between start/stop signals and queries for the
        # next set of signals
        self.poller = zmq.Poller()
        self.poller.register(self.control_sub_socket, zmq.POLLIN)
        self.poller.register(self.request_fw_socket, zmq.POLLIN)
        if self.whitelist != []:
            self.poller.register(self.com_socket, zmq.POLLIN)
            self.poller.register(self.request_socket, zmq.POLLIN)

    def run(self):
        """
        possible incomming signals:
        com_socket
            (start/stop command from external)
            START_STREAM: Add request for all incoming data packets
                          (no  further requests needed)
            STOP_STREAM: Remove assignment for all incoming data packets
            START_STREAM_METADATA: Add request for metadata only of all
                                   incoming data packets
                                   (no  further requests needed)
            STOP_STREAM_METADATA: Remove assignment for metadata of all
                                  incoming data packets
            START_QUERY_NEXT: Enable requests for individual data packets
            STOP_QUERY_NEXT: Disable requests for individual data packets
            START_QUERY_METADATA: Enable requests for metadata of individual
                                  data packets
            STOP_QUERY_METADATA: Disable requests for metadata of individual
                                 data packets

        request_socket
            (requests from external)
            NEXT: Request for the next incoming data packet
            CANCEL: Cancel the previous request

        request_fw_socket
            (internal forwarding of requests which came fromexternal)
            GET_REQUESTS: TaskProvider asks to get the next set of open
                          requests

        control_sub_socket
            (internal control messages)
            SLEEP: receiver is currently not available
                   -> this does not affect this class
            WAKEUP: receiver is back online
                    -> this does not affect this class
            EXIT: shutdown everything
        """

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

                                # [<host:port>, <prio>, <suffix_list>,
                                #  <metadata|data>]
                                socket_id, prio, pattern, send_type = (
                                    request_set[self.next_requ_node[index]])

                                # Check if filename matches requested
                                # regex
                                if pattern.match(filename) is not None:
                                    # do not send pattern
                                    open_requests.append([socket_id,
                                                          prio,
                                                          send_type])

                                    # distribute in round-robin order
                                    self.next_requ_node[index] = (
                                        (self.next_requ_node[index] + 1)
                                        % len(request_set)
                                    )

                        for request_set in self.open_requ_vari:
                            # Check if filename suffix matches requested suffix
                            if (request_set
                                    and (request_set[0][2].match(filename)
                                         is not None)):
                                socket_id, prio, pattern, send_type = (
                                    request_set.pop(0))
                                # do not send pattern
                                open_requests.append([socket_id,
                                                      prio,
                                                      send_type])

                        if open_requests:
                            self.request_fw_socket.send_string(
                                json.dumps(open_requests))
                            self.log.debug("Answered to request: {}"
                                           .format(open_requests))
                            self.log.debug("open_requ_vari: {}"
                                           .format(self.open_requ_vari))
                            self.log.debug("allowed_queries: {}"
                                           .format(self.allowed_queries))
                        else:
                            open_requests = ["None"]
                            self.request_fw_socket.send_string(
                                json.dumps(open_requests))
                            self.log.debug("Answered to request: {}"
                                           .format(open_requests))
                            self.log.debug("open_requ_vari: {}"
                                           .format(self.open_requ_vari))
                            self.log.debug("allowed_queries: {}"
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
                self.log.debug("Received signal: {}".format(in_message))

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
                self.log.debug("Received request: {}".format(in_message))

                if in_message[0] == b"NEXT":
                    incoming_socket_id = utils.convert_socket_to_fqdn(
                        in_message[1].decode("utf-8"), self.log)

                    for index in range(len(self.allowed_queries)):
                        for i in range(len(self.allowed_queries[index])):
                            if (incoming_socket_id
                                    == self.allowed_queries[index][i][0]):
                                self.open_requ_vari[index].append(
                                    self.allowed_queries[index][i])
                                self.log.info("Add to open requests: {}"
                                              .format(self.allowed_queries[
                                                  index][i]))

                elif in_message[0] == b"CANCEL":
                    incoming_socket_id = utils.convert_socket_to_fqdn(
                        in_message[1].decode("utf-8"), self.log)

                    still_requested = []
                    for a in range(len(self.open_requ_vari)):
                        vari_per_group = []
                        for b in self.open_requ_vari[a]:
                            if incoming_socket_id != b[0]:
                                vari_per_group.append(b)

                        still_requested.append(vari_per_group)

                    self.open_requ_vari = still_requested

                    self.log.info("Remove all occurences from {} from "
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
                    # self.log.debug("Control signal received.")
                except:
                    self.log.error("Waiting for control signal...failed",
                                   exc_info=True)
                    continue

                # remove subscription topic
                del message[0]

                if message[0] == b"EXIT":
                    self.log.info("Requested to shutdown.")
                    break
                elif message[0] == b"SLEEP":
                    # self.log.debug("Received sleep signal. Do nothing.")
                    continue
                elif message[0] == b"WAKEUP":
                    self.log.debug("Received wakeup signal. Do nothing.")
                    continue
                else:
                    self.log.error("Unhandled control signal received: {}"
                                   .format(message[0]))

    def check_signal_inverted(self, in_message):

        if len(in_message) != 3:

            self.log.warning("Received signal is of the wrong format")
            self.log.debug("Received signal is too short or too long: {}"
                           .format(in_message))
            return [b"NO_VALID_SIGNAL"], None, None

        else:

            version, signal, target = (
                in_message[0].decode("utf-8"),
                in_message[1],
                in_message[2].decode("utf-8")
            )
            target = json.loads(target)

            target = utils.convert_socket_to_fqdn(target, self.log)

            try:
                host = [t[0].split(":")[0] for t in target]
            except:
                return [b"NO_VALID_SIGNAL"], None, None

            if version:
                if utils.check_version(version, self.log):
                    self.log.info("Versions are compatible")
                else:
                    self.log.warning("Version are not compatible")
                    return [b"VERSION_CONFLICT", __version__], None, None

            if signal and host:

                # Checking signal sending host
                self.log.debug("Check if host to send data to are in "
                               "whitelist...")
                if utils.check_host(host, self.whitelist, self.log):
                    self.log.info("Hosts are allowed to connect.")
                    self.log.debug("hosts: {}".format(host))
                else:
                    self.log.warning("One of the hosts is not allowed to "
                                     "connect.")
                    self.log.debug("hosts: {}".format(host))
                    self.log.debug("whitelist: {}".format(self.whitelist))
                    return [b"NO_VALID_HOST"], None, None

        return False, signal, target

    def send_response(self, signal):
        if type(signal) != list:
            signal = [signal]

        self.log.debug("Send response back: {}".format(signal))
        self.com_socket.send_multipart(signal, zmq.NOBLOCK)

    def __start_signal(self, signal, send_type, socket_ids, list_to_check,
                       vari_list, corresp_list):

        socket_ids = utils.convert_socket_to_fqdn(socket_ids,
                                                  self.log)

        # socket_ids is of the format [[<host>, <prio>, <suffix>], ...]
        for socket_conf in socket_ids:
            # for compatibility with API versions 3.1.2 or older
            self.log.debug("suffix={}".format(socket_conf[2]))
            socket_conf[2] = convert_suffix_list_to_regex(socket_conf[2],
                                                          suffix=True,
                                                          compile_regex=False,
                                                          log=self.log)

        overwrite_index = None
        # [set(<host>, <host>, ...), set(...), ...] created from list_to_check
        flatlist_nested = [set([j[0] for j in sublist])
                           for sublist in list_to_check]
        # set(<host>, <host>, ...) created from target list (=socket_ids)
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
                self.log.debug("socket_ids={}".format(socket_ids_flatlist))
                self.log.debug("flatlist_nested[i]={}".format(i))

        if overwrite_index is not None:
            # overriding is necessary because the new request may contain
            # different parameters like monitored file suffix, priority or
            # connection type also this means the old socket_id set should be
            # replaced in total and not only partially
            self.log.debug("overwrite_index={}".format(overwrite_index))

            list_to_check[overwrite_index] = copy.deepcopy(
                sorted([i + [send_type] for i in socket_ids]))

            # compile regex
            # This cannot be done before because deepcopy does not support for
            # python versions < 3.7, see http://bugs.python.org/issue10076
            for socket_conf in list_to_check[overwrite_index]:
                socket_conf[2] = re.compile(socket_conf[2])

            if corresp_list is not None:
                corresp_list[overwrite_index] = 0

            if vari_list is not None:
                vari_list[overwrite_index] = []
        else:
            list_to_check.append(copy.deepcopy(
                sorted([i + [send_type] for i in socket_ids])))

            # compile regex
            # This cannot be done before because deepcopy does not support for
            # python versions < 3.7, see http://bugs.python.org/issue10076
            for socket_conf in list_to_check[-1]:
                socket_conf[2] = re.compile(socket_conf[2])

            if corresp_list is not None:
                corresp_list.append(0)

            if vari_list is not None:

                vari_list.append([])

        self.log.debug("after start handling: list_to_check={}"
                       .format(list_to_check))

        # send signal back to receiver
        self.send_response([signal])

#        connection_found = False
#        tmp_allowed = []
#        flatlist = [i[0] for i in
#                    [j for sublist in list_to_check for j in sublist]]
#        self.log.debug("flatlist: {0}".format(flatlist))

#        for socket_conf in socket_ids:
#
#            socket_conf[0] = socket.getfqdn(socket_conf[0])
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
#            self.send_response([signal])
#            list_to_check.append(copy.deepcopy(sorted(tmp_allowed)))
#            if corresp_list != None:
#                corresp_list.append(0)
#            del tmp_allowed
#
#            if vari_list != None:
#                vari_list.append([])
#        else:
#            # send error back to receiver
# #           self.send_response(["CONNECTION_ALREADY_OPEN"])
#            # "reopen" the connection and confirm to receiver
#            self.send_response([signal])

    def __stop_signal(self, signal, socket_ids, list_to_check, vari_list,
                      corresp_list):

        connection_not_found = False
        tmp_remove_index = []
        tmp_remove_element = []
        found = False

        socket_ids = utils.convert_socket_to_fqdn(socket_ids,
                                                  self.log)

        for socket_conf in socket_ids:

            socket_id = socket_conf[0]

            for sublist in list_to_check:
                for element in sublist:
                    if socket_id == element[0]:
                        tmp_remove_element.append(element)
                        found = True
            if not found:
                connection_not_found = True

        if connection_not_found:
            self.send_response([b"NO_OPEN_CONNECTION_FOUND"])
            self.log.info("No connection to close was found for {}"
                          .format(socket_conf))
        else:
            # send signal back to receiver
            self.send_response([signal])

            for element in tmp_remove_element:

                socket_id = element[0]

                if vari_list is not None:
                    vari_list = [[b for b in vari_list[a] if socket_id != b[0]]
                                 for a in range(len(vari_list))]
                    self.log.debug("Remove all occurences from {} from "
                                   "variable request list.".format(socket_id))

                for i in range(len(list_to_check)):
                    if element in list_to_check[i]:
                        list_to_check[i].remove(element)
                        self.log.debug("Remove {} from pemanent request "
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
        if signal == b"GET_VERSION":
            self.log.info("Received signal: {}".format(signal))

            self.send_response([signal, __version__])
            return

        ###########################
        #       START_STREAM      #
        ###########################
        elif signal == b"START_STREAM":
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))

            self.__start_signal(signal, "data", socket_ids,
                                self.open_requ_perm, None,
                                self.next_requ_node)

            return

        ###########################
        #  START_STREAM_METADATA  #
        ###########################
        elif signal == b"START_STREAM_METADATA":
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))
            if not self.config["store_data"]:
                self.log.debug("Send notification that store_data is disabled")
                self.send_response([b"STORING_DISABLED", __version__])
            else:
                self.__start_signal(signal, "metadata", socket_ids,
                                    self.open_requ_perm, None,
                                    self.next_requ_node)

            return

        ###########################
        #       STOP_STREAM       #
        #  STOP_STREAM_METADATA   #
        ###########################
        elif signal == b"STOP_STREAM" or signal == b"STOP_STREAM_METADATA":
            self.log.info("Received signal: {} for host {}"
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
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))

            self.__start_signal(signal, "data", socket_ids,
                                self.allowed_queries, self.open_requ_vari,
                                None)

            return

        ###########################
        #  START_QUERY_METADATA   #
        ###########################
        elif signal == b"START_QUERY_METADATA":
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))

            if not self.config["store_data"]:
                self.log.debug("Send notification that store_data is disabled")
                self.send_response([b"STORING_DISABLED", __version__])
            else:
                self.__start_signal(signal, "metadata", socket_ids,
                                    self.allowed_queries,
                                    self.open_requ_vari, None)

            return

        ###########################
        #       STOP_QUERY        #
        #  STOP_QUERY_METADATA    #
        ###########################
        elif signal == b"STOP_QUERY_NEXT" or signal == b"STOP_QUERY_METADATA":
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))

            self.allowed_queries, self.open_requ_vari, nonetmp = (
                self.__stop_signal(signal, socket_ids, self.allowed_queries,
                                   self.open_requ_vari, None)
            )

            return

        else:
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))
            self.send_response([b"NO_VALID_SIGNAL"])

    def stop_socket(self, name, socket=None):
        """Wrapper for utils.stop_socket.
        """

        if socket is None:
            socket = getattr(self, name)

        utils.stop_socket(name=name, socket=socket, log=self.log)

    def stop(self):
        self.log.debug("Closing sockets for SignalHandler")

        self.stop_socket(name="com_socket")
        self.stop_socket(name="request_socket")
        self.stop_socket(name="request_fw_socket")
        self.stop_socket(name="control_pub_socket")
        self.stop_socket(name="control_sub_socket")

        if not self.ext_context and self.context:
            self.log.info("Destroying context")
            self.context.term()
#            self.context.destroy(0)
            self.context = None

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
