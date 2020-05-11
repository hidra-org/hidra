# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""
This module implements the signal handler class.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

from collections import namedtuple
import copy
import datetime
import json
import os
import re
import zmq
import zmq.devices

from base_class import Base

from hidra import convert_suffix_list_to_regex, __version__, FormatError
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


# SignalhandlerSockets = namedtuple(
#    "SignalhandlerSockets", [
#        "control_pub",
#        "control_sub",
#        "request_fw",
#        "request",
#        "com"
#    ]
# )


UnpackedMessage = namedtuple(
    "UnpackedMessage", [
        "check_successful",
        "response",
        "appid",
        "signal",
        "targets"
    ]
)


TargetProperties = namedtuple(
    "TargetProperties", [
        "targets",
        "appid",
        "time_registered"
    ]
)


class SignalHandler(Base):
    """React to signals from outside.
    """

    def __init__(self,
                 config,
                 endpoints,
                 whitelist,
                 ldapuri,
                 log_queue,
                 log_level,
                 stop_request,
                 context=None):

        super().__init__()

        # needed to initialize base_class
        self.config_all = config
        # signal handler does not have a stripped down config
        self.config = config
        self.endpoints = endpoints
        self.stop_request = stop_request
        self.stopped = None

        self.log = None

        self.vari_requests = []
        self.registered_streams = []
        self.registered_queries = []
        # to rotate through the open permanent requests
        self.perm_requests = []

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

        self.init_args = {
            "whitelist": whitelist,
            "ldapuri": ldapuri,
            "context": context,
            "log_queue": log_queue,
            "log_level": log_level
        }

    def setup(self):
        """Initializes parameters and creates sockets.
        """

        whitelist = self.init_args["whitelist"]
        ldapuri = self.init_args["ldapuri"]
        context = self.init_args["context"]
        log_queue = self.init_args["log_queue"]
        log_level = self.init_args["log_level"]

        # Send all logs to the main process
        self.log = utils.get_logger("SignalHandler",
                                    queue=log_queue,
                                    log_level=log_level)
        self.log.info("SignalHandler started (PID %s).", os.getpid())

        self.whitelist = utils.extend_whitelist(whitelist, ldapuri, self.log)

        # remember if the context was created outside this class or not
        if context:
            self.context = context
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

        if self.config["general"]["use_statserver"]:
            self.setup_stats_collection()

        try:
            self.create_sockets()
        except Exception:
            self.log.error("Cannot create sockets", exc_info=True)
            self.stop()
            raise

    def stats_config(self):
        """Extend the stats_config function of the Base class
        """
        conf = super().stats_config()
        conf["com_socket"] = ["general", "com_port"]
        conf["request_socket"] = ["general", "request_port"]

        return conf

    def create_sockets(self):
        """Create ZMQ sockets.
        """

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

        # just anther name for the same socket to be able to use the base
        # class check_control_socket method
        self.control_socket = self.control_sub_socket

        # socket to forward requests
        self.request_fw_socket = self.start_socket(
            name="request_fw_socket",
            sock_type=zmq.REP,
            sock_con="bind",
            endpoint=self.endpoints.request_fw_bind
        )

        # needs to be explicit check to [] because None would also fail
        if self.whitelist != []:
            com_endpt_split = self.endpoints.com_bind.split(":")
            use_random_com = len(com_endpt_split) == 2
            self.log.debug("use random port for request: %s", use_random_com)

            request_endpt_split = self.endpoints.request_bind.split(":")
            use_random_request = len(request_endpt_split) == 2
            self.log.debug("use random port for com: %s", use_random_request)

            # create zmq socket for signal communication with receiver
            self.com_socket = self.start_socket(
                name="com_socket",
                sock_type=zmq.REP,
                sock_con="bind",
                endpoint=self.endpoints.com_bind,
                # TODO this is a hack
                random_port=use_random_com
            )

            # create socket to receive requests
            self.request_socket = self.start_socket(
                name="request_socket",
                sock_type=zmq.PULL,
                sock_con="bind",
                endpoint=self.endpoints.request_bind,
                # TODO this is a hack
                random_port=use_random_request
            )
        else:
            self.log.info("Socket com_socket and request_socket not started "
                          "since there is no host allowed to connect")

        # Poller to distinguish between start/stop signals and queries for the
        # next set of signals
        self.poller = zmq.Poller()
        self.poller.register(self.control_sub_socket, zmq.POLLIN)
        self.poller.register(self.request_fw_socket, zmq.POLLIN)
        # needs to be explicit check to [] because None would also fail
        if self.whitelist != []:
            self.poller.register(self.com_socket, zmq.POLLIN)
            self.poller.register(self.request_socket, zmq.POLLIN)

    def run(self):
        """Calling run method and react to exceptions.
        """

        self.setup()

        try:
            self.stopped = False
            self._run()
        except zmq.ZMQError:
            self.log.error("Stopping signalHandler due to ZMQError.",
                           exc_info=True)
        except KeyboardInterrupt:
            pass
        except Exception:
            self.log.error("Stopping SignalHandler due to unknown error "
                           "condition.", exc_info=True)
        finally:
            self.stopped = True
            self.stop()

    def _run(self):
        """React to incoming signals.

        Possible incoming signals:
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
            START_QUERY_NEXT_METADATA: Enable requests for metadata of
                                       individual data packets
            STOP_QUERY_NEXT_METADATA: Disable requests for metadata of
                                      individual data packets

        request_socket
            (requests from external)
            NEXT: Request for the next incoming data packet
            CANCEL: Cancel the previous request

        request_fw_socket
            (internal forwarding of requests which came from external)
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
        while not self.stop_request.is_set():
            socks = dict(self.poller.poll())

            # ----------------------------------------------------------------
            # incoming request from TaskProvider
            # ----------------------------------------------------------------
            if (self.request_fw_socket in socks
                    and socks[self.request_fw_socket] == zmq.POLLIN):

                self._handle_request_task_provider()

            # ----------------------------------------------------------------
            # start/stop command from external
            # ----------------------------------------------------------------
            if (self.com_socket in socks
                    and socks[self.com_socket] == zmq.POLLIN):

                in_message = self.com_socket.recv_multipart()
                self.log.debug("Received signal: %s", in_message)

                unpacked_message = self.check_signal(in_message)

                if unpacked_message.check_successful:
                    try:
                        self.react_to_signal(unpacked_message)
                    except FormatError:
                        self.send_response([b"NO_VALID_SIGNAL"])
                else:
                    self.send_response(unpacked_message.response)

            # ----------------------------------------------------------------
            # request from external
            # ----------------------------------------------------------------
            if (self.request_socket in socks
                    and socks[self.request_socket] == zmq.POLLIN):

                in_message = self.request_socket.recv_multipart()
                self.log.debug("Received request: %s", in_message)

                if in_message[0] == b"NEXT":
                    self._handle_request_external_next(in_message)
                elif in_message[0] == b"CANCEL":
                    self._handle_request_external_cancel(in_message)
                else:
                    self.log.info("Request not supported.")

            # ----------------------------------------------------------------
            # control commands from internal
            # ----------------------------------------------------------------
            if (self.control_sub_socket in socks
                    and socks[self.control_sub_socket] == zmq.POLLIN):

                # the exit signal should become effective
                if self.check_control_signal():
                    break

    def _handle_request_task_provider(self):
        in_message = None
        try:
            in_message = self.request_fw_socket.recv_multipart()

            if in_message[0] != b"GET_REQUESTS":
                self.log.debug("in_message=%s", in_message)
                self.log.error("Failed to receive/answer new signal requests: "
                               "incoming message not supported")
                return

            self.log.debug("New request for signals received.")
            filename = json.loads(in_message[1].decode("utf-8"))
            open_requests = []

            for i, trgt_prop in enumerate(self.registered_streams):
                request_set = trgt_prop.targets

                if request_set:
                    # [<host:port>, <prio>, <suffix_list>, <metadata|data>]
                    socket_id, prio, pattern, send_type = (
                        request_set[self.perm_requests[i]])

                    # Check if filename matches requested regex
                    if pattern.match(filename) is not None:
                        # do not send pattern
                        open_requests.append([socket_id, prio, send_type])

                        # distribute in round-robin order
                        self.perm_requests[i] = (
                            (self.perm_requests[i] + 1) % len(request_set)
                        )

            for request_set in self.vari_requests:
                # Check if filename suffix matches requested suffix
                if (request_set
                        and (request_set[0][2].match(filename) is not None)):
                    socket_id, prio, pattern, send_type = request_set.pop(0)
                    # do not send pattern
                    open_requests.append([socket_id, prio, send_type])

            if not open_requests:
                open_requests = ["None"]
            self.request_fw_socket.send_string(json.dumps(open_requests))

            self.log.debug("Answered to request: %s", open_requests)
            self.log.debug("vari_requests: %s", self.vari_requests)
            self.log.debug("registered_queries: %s", self.registered_queries)
            self.log.debug("registered_streams: %s", self.registered_streams)

        except Exception:
            self.log.debug("in_message=%s", in_message)
            self.log.error("Failed to receive/answer new signal requests",
                           exc_info=True)

    def _handle_request_external_next(self, in_message):
        socket_id = utils.convert_socket_to_fqdn(
            in_message[1].decode("utf-8"), self.log
        )

        # determine if the socket id is contained in the TargetProperties
        # -> True is so, False otherwise (each TargetProperty checked
        #    independently
        res = [[i[0] == socket_id for i in query_set.targets]
               for query_set in self.registered_queries]

        # determine the registered queries where the socket id is contained
        # (time, target set index, target index)
        possible_queries = [
            (self.registered_queries[i].time_registered, i, j)
            for i, tset in enumerate(res)
            for j, trgt in enumerate(tset)
            if trgt
        ]

        # only add the request to the newest one (others might be left overs)

        # identify which one is the newest
        try:
            idx_newest = possible_queries.index(max(possible_queries))
        except ValueError:
            # target not found in possible_queries
            self.log.debug("No registration found for query")
            return

        newest = possible_queries[idx_newest]

        # Add request
        request = self.registered_queries[newest[1]].targets[newest[2]]
        self.vari_requests[newest[1]].append(request)
        self.log.info("Add to open requests: %s", request)

        # avoid duplicates -> remove old registered queries this cannot be done
        # when the START signal arrives because putting it in there would e.g.
        # break in the following case:
        # - app1 connects and gets data normally
        # - app2 (duplicate of app1, i.e. same host + port to receive data on)
        #   tries to connect but fails when establishing the sockets
        # -> putting the cleanup in the start would break app1 once app2 is
        #    started
        if len(possible_queries) > 1:

            # only the left overs
            del possible_queries[idx_newest]

            # get the indexes to remove first and then remove them in backwards
            # order to prevent  index shifting
            # e.g. list(a, b, c), remove index 1 and 2
            # -> remove index 1 -> list(a, c)
            #    remove index 2 -> error
            idxs_to_remove = [query[1] for query in possible_queries]
            idxs_to_remove.sort(reverse=True)
            self.log.debug("idxs_to_remove=%s", idxs_to_remove)

            # left overs found
            try:
                for i in idxs_to_remove:
                    self.log.debug("Remove leftover/duplicate registered "
                                   "query %s ", self.registered_queries[i])
                    del self.vari_requests[i]
                    del self.registered_queries[i]
            except Exception:
                self.log.debug("i=%s", i)
                self.log.debug("registered_queries=%s",
                               self.registered_queries)
                self.log.error("Could not remove leftover/duplicate query",
                               exc_info=True)
                raise

    def _handle_request_external_cancel(self, in_message):
        socket_id = utils.convert_socket_to_fqdn(in_message[1].decode("utf-8"),
                                                 self.log)

        # socket_conf is of the form
        # [<host>:<port>, <prio>, <regex>, data|metadata]
        self.vari_requests = [
            [socket_conf for socket_conf in request_set
             if socket_id != socket_conf[0]]
            for request_set in self.vari_requests
        ]

        self.log.info("Remove all occurrences from %s from variable request "
                      "list.", socket_id)

    def _react_to_sleep_signal(self, message):
        """Overwrite the base class reaction method to sleep signal.
        """

        # Do not react on sleep signals.
        pass

    def check_signal(self, in_message):
        """Unpack and check incoming message.

        Args:
            in_message: Message to unpack.

        Return:
            A tuple containing:
                - Entry if the check failed:
                    - False if the everything was OK.
                    - A response message if the test failed. Options:
                        - VERSION_CONFLICT
                        - NO_VALID_SIGNAL
                        - NO_VALID_HOST
                - The signal contained in the message.
                - The targets extracted from the message.
        """

        if len(in_message) != 4:
            self.log.warning("Received signal is of the wrong format")
            self.log.debug("Received signal is too short or too long: %s",
                           in_message)
            return UnpackedMessage(
                check_successful=False,
                response=[b"NO_VALID_SIGNAL"],
                appid=None,
                signal=None,
                targets=None
            )

        try:
            version, appid, signal, targets = (
                in_message[0].decode("utf-8"),
                in_message[1].decode("utf-8"),
                in_message[2],
                in_message[3].decode("utf-8")
            )
            targets = json.loads(targets)

            targets = utils.convert_socket_to_fqdn(targets, self.log)

            host = [t[0].split(":")[0] for t in targets]
            self.log.debug("host %s", host)
        except Exception:
            self.log.debug("No valid signal received", exc_info=True)
            return UnpackedMessage(
                check_successful=False,
                response=[b"NO_VALID_SIGNAL"],
                appid=None,
                signal=None,
                targets=None
            )

        if version:
            if utils.check_version(version, self.log):
                self.log.info("Versions are compatible")
            else:
                self.log.warning("Versions are not compatible")
                return UnpackedMessage(
                    check_successful=False,
                    response=[b"VERSION_CONFLICT", __version__],
                    appid=None,
                    signal=None,
                    targets=None
                )

        if signal and host:
            # Checking signal sending host
            self.log.debug("Check if host to send data to are in whitelist...")
            if utils.check_host(host, self.whitelist, self.log):
                self.log.info("Hosts are allowed to connect.")
                self.log.debug("hosts: %s", host)
            else:
                self.log.warning("One of the hosts is not allowed to connect.")
                self.log.debug("hosts: %s", host)
                self.log.debug("whitelist: %s", self.whitelist)
                return UnpackedMessage(
                    check_successful=False,
                    response=[b"NO_VALID_HOST"],
                    appid=None,
                    signal=None,
                    targets=None
                )

        return UnpackedMessage(
            check_successful=True,
            response=None,
            appid=appid,
            signal=signal,
            targets=targets
        )

    def send_response(self, signal):
        """Send response back.
        """

        if not isinstance(signal, list):
            signal = [signal]

        self.log.debug("Send response back: %s", signal)
        self.com_socket.send_multipart(signal, zmq.NOBLOCK)

    def _start_signal(self,
                      signal,
                      send_type,
                      appid,
                      socket_ids,
                      registered_ids,
                      vari_requests,
                      perm_requests):
        """Register socket ids and updated related lists accordingly.

        Updated registered_ids, vari_requests and perm_requests in place and
        send confirmation back.

        Args:
            signal: Signal to send after finishing
            send_type: The type of data the socket ids should get.
            appid: The application ID to identify where the signal came from.
            socket_ids: Socket ids to be registered.
            registered_ids: Already registered socket ids.
            vari_requests: List of open requests (query mode).
            perm_requests: List of next node number to serve (stream mode).
        """

        socket_ids = utils.convert_socket_to_fqdn(socket_ids,
                                                  self.log)

        # Convert suffixes to regex
        # for compatibility with API versions 3.1.2 or older
        # socket_ids is of the format [[<host>, <prio>, <suffix>], ...]
        for socket_conf in socket_ids:
            self.log.debug("suffix=%s", socket_conf[2])
            socket_conf[2] = convert_suffix_list_to_regex(socket_conf[2],
                                                          suffix=True,
                                                          compile_regex=False,
                                                          log=self.log)

        targets = copy.deepcopy(
            sorted([i + [send_type] for i in socket_ids])
        )
        # compile regex
        # This cannot be done before because deepcopy does not support it
        # for python versions < 3.7, see http://bugs.python.org/issue10076
        for socket_conf in targets:
            try:
                socket_conf[2] = re.compile(socket_conf[2])
            except Exception:
                self.log.error("Error message was:", exc_info=True)
                raise FormatError("Could not compile regex '{}'"
                                  .format(socket_conf[2]))

        current_time = datetime.datetime.now().isoformat()
        targetset = TargetProperties(targets=targets,
                                     appid=appid,
                                     time_registered=current_time)

        overwrite_index = None
        for i, target_properties in enumerate(registered_ids):
            if target_properties.appid != appid:
                continue

            # the registered disjoint socket ids for each node set
            # set(<host>:<port>, <host>:<port>, ...)
            targets_flatlist = set(
                [j[0] for j in target_properties.targets]
            )

            # the disjoint socket_ids to be register
            # "set" is used to eliminated duplications
            # set(<host>:<port>, <host>:<port>, ...) created from socket_ids
            socket_ids_flatlist = set([socket_conf[0]
                                       for socket_conf in socket_ids])

            # If the socket_ids of the node set to be register are either a
            # subset or a superset of an already registered node set
            # overwrite the old one with it
            # new registration  | registered    | what to done
            # (h:p, h:p2)       |  (h:p)        |  overwrite: (h:p, h:p2)
            # (h:p              |  (h:p, h:p2)  |  overwrite: (h:p)
            # (h:p, h:p2)       |  (h:p, h:p3)  |  ?

            # Check if socket_ids is sublist of one entry of registered_ids
            # -> overwrite existing entry
            if socket_ids_flatlist.issubset(targets_flatlist):
                self.log.debug("socket_ids already contained, override")
                overwrite_index = i
            # Check if one entry of registered_ids is sublist in socket_ids
            # -> overwrite existing entry
            elif targets_flatlist.issubset(socket_ids_flatlist):
                self.log.debug("socket_ids is superset of already "
                               "contained set, override")
                overwrite_index = i
            # TODO Mixture ?
            elif not socket_ids_flatlist.isdisjoint(targets_flatlist):
                self.log.error("socket_ids is neither a subset nor "
                               "superset of already contained set")
                self.log.debug("Currently: no idea what to do with this.")
                self.log.debug("socket_ids=%s", socket_ids_flatlist)
                self.log.debug("registered_socketids=%s", targets_flatlist)

        if overwrite_index is None:
            registered_ids.append(targetset)

            if perm_requests is not None:
                perm_requests.append(0)

            if vari_requests is not None:
                vari_requests.append([])

        else:
            # overriding is necessary because the new request may contain
            # different parameters like monitored file suffix, priority or
            # connection type also this means the old socket_id set should be
            # replaced in total and not only partially
            self.log.debug("overwrite_index=%s", overwrite_index)

            registered_ids[overwrite_index] = targetset

            if perm_requests is not None:
                perm_requests[overwrite_index] = 0

            if vari_requests is not None:
                vari_requests[overwrite_index] = []

        self.log.debug("after start handling: registered_ids=%s",
                       registered_ids)

        # send signal back to receiver
        self.send_response([signal])

    def _stop_signal(self,
                     signal,
                     appid,
                     socket_ids,
                     registered_ids,
                     vari_requests,
                     perm_requests):
        """Unregister socket ids and updated related lists accordingly.

        Updated registered_ids, vari_requests and perm_requests in place and
        send confirmation back.

        Args:
            signal: Signal to send after finishing
            appid: The application ID to identify where the signal came from.
            socket_ids: Socket ids to be de-registered.
            registered_ids: Currently registered socket ids.
            vari_requests: List of open requests (query mode).
            perm_requests: List of next node number to serve (stream mode).
        """

        socket_ids = utils.convert_socket_to_fqdn(socket_ids,
                                                  self.log)

        if appid is None:
            # check all registered ids and ignore appid
            reg_to_check = [(i, target_prop)
                            for i, target_prop in enumerate(registered_ids)]
        else:
            reg_to_check = [(i, target_prop)
                            for i, target_prop in enumerate(registered_ids)
                            if target_prop.appid == appid]

        # list of socket configurations to remove (in format how they are
        # registered:
        # [[<host>:<port>, <prio>, <regex>, <end_type>],...],
        # this is needed because socket_ids only contain partial information:
        # [[<host>:<port>, <prio>, <regex uncompiled>]]
        to_remove = [reg_id
                     for socket_conf in socket_ids
                     for sublist in reg_to_check
                     for reg_id in sublist[1].targets
                     if socket_conf[0] == reg_id[0]]
        self.log.debug("to_remove %s", to_remove)

        if not to_remove:
            self.send_response([b"NO_OPEN_CONNECTION_FOUND"])
            self.log.info("No connection to close was found for %s",
                          socket_ids)
        else:
            # send signal back to receiver
            self.send_response([signal])

            self.log.debug("registered_ids %s", registered_ids)
            self.log.debug("vari_requests %s", vari_requests)
            self.log.debug("perm_requests %s", perm_requests)
            for i, target_properties in reg_to_check:
                self.log.debug("target_properties %s", target_properties)

                targets = target_properties.targets

                for reg_id in to_remove:
                    socket_id = reg_id[0]

                    try:
                        targets.remove(reg_id)
                        self.log.debug("De-register %s", socket_id)
                    except ValueError:
                        # reg_id is not contained in targets
                        # -> nothing to remove
                        continue

                    if not targets:
                        del registered_ids[i]

                        # remove open requests (queries)
                        if vari_requests is not None:
                            del vari_requests[i]
                        # remove open requests (streams)
                        if perm_requests is not None:
                            perm_requests.pop(i)
                    else:

                        if vari_requests is not None:
                            # vari requests is of the form
                            # [[[<host>:<port>, <prio>, <regex>, <end_type>],
                            #   ...],
                            #  ...]
                            vari_requests[i] = [
                                socket_conf
                                for socket_conf in vari_requests[i]
                                if socket_id != socket_conf[0]
                            ]

                            self.log.debug("Remove all occurrences from %s "
                                           "from variable request list.",
                                           socket_id)

                        # perm_requests is a list of node numbers to feed
                        # next i.e. index of the node inside of the node
                        # set whose request will be served next
                        # -> has to be updated because number of
                        # registered nodes changed
                        n_targets = len(registered_ids[i].targets)
                        if perm_requests is not None:
                            perm_requests[i] = perm_requests[i] % n_targets

            # send signal to TaskManager
            self.control_pub_socket.send_multipart(
                [b"signal",
                 b"CLOSE_SOCKETS",
                 json.dumps(socket_ids).encode("utf-8")]
            )

        return registered_ids, vari_requests, perm_requests

    def react_to_signal(self, unpacked_message):
        """React to external signal.

        Args:
            unpacked_message: Named tuple containing all signal information,
                              e.g.
                                check_successful=...
                                response=...
                                appid=...
                                signal=...
                                targets=...
        """

        signal = unpacked_message.signal
        appid = unpacked_message.appid
        socket_ids = unpacked_message.targets
        version = __version__.encode("utf-8")

        # --------------------------------------------------------------------
        # GET_VERSION
        # --------------------------------------------------------------------
        if signal == b"GET_VERSION":
            self.log.info("Received signal: %s", signal)

            self.send_response([signal, version])
            return
        else:
            self.log.info("Received signal: %s for hosts %s",
                          signal, socket_ids)

        # --------------------------------------------------------------------
        # START_STREAM
        # --------------------------------------------------------------------
        if signal == b"START_STREAM":

            self._start_signal(
                signal=signal,
                send_type="data",
                appid=appid,
                socket_ids=socket_ids,
                registered_ids=self.registered_streams,
                vari_requests=None,
                perm_requests=self.perm_requests
            )

            return

        # --------------------------------------------------------------------
        # START_STREAM_METADATA
        # --------------------------------------------------------------------
        elif signal == b"START_STREAM_METADATA":

            if not self.config["datafetcher"]["store_data"]:
                self.log.debug("Send notification that store_data is disabled")
                self.send_response([b"STORING_DISABLED", version])
            else:
                self._start_signal(
                    signal=signal,
                    send_type="metadata",
                    appid=appid,
                    socket_ids=socket_ids,
                    registered_ids=self.registered_streams,
                    vari_requests=None,
                    perm_requests=self.perm_requests
                )

            return

        # --------------------------------------------------------------------
        # STOP_STREAM
        # STOP_STREAM_METADATA
        # --------------------------------------------------------------------
        elif signal == b"STOP_STREAM" or signal == b"STOP_STREAM_METADATA":

            ret_val = self._stop_signal(
                signal=signal,
                appid=appid,
                socket_ids=socket_ids,
                registered_ids=self.registered_streams,
                vari_requests=None,
                perm_requests=self.perm_requests
            )

            self.registered_streams, _, self.perm_requests = ret_val

            return

        # --------------------------------------------------------------------
        # START_QUERY_NEXT
        # --------------------------------------------------------------------
        elif signal == b"START_QUERY_NEXT":

            self._start_signal(
                signal=signal,
                send_type="data",
                appid=appid,
                socket_ids=socket_ids,
                registered_ids=self.registered_queries,
                vari_requests=self.vari_requests,
                perm_requests=None
            )

            return

        # --------------------------------------------------------------------
        # START_QUERY_NEXT_METADATA
        # --------------------------------------------------------------------
        elif signal == b"START_QUERY_NEXT_METADATA":

            if not self.config["datafetcher"]["store_data"]:
                self.log.debug("Send notification that store_data is disabled")
                self.send_response([b"STORING_DISABLED", version])
            else:
                self._start_signal(
                    signal=signal,
                    send_type="metadata",
                    appid=appid,
                    socket_ids=socket_ids,
                    registered_ids=self.registered_queries,
                    vari_requests=self.vari_requests,
                    perm_requests=None
                )

            return

        # --------------------------------------------------------------------
        #  STOP_QUERY_NEXT
        #  STOP_QUERY_NEXT_METADATA
        # --------------------------------------------------------------------
        elif (signal == b"STOP_QUERY_NEXT"
              or signal == b"STOP_QUERY_NEXT_METADATA"):

            ret_val = self._stop_signal(
                signal=signal,
                appid=appid,
                socket_ids=socket_ids,
                registered_ids=self.registered_queries,
                vari_requests=self.vari_requests,
                perm_requests=None
            )

            self.registered_queries, self.vari_requests, _ = ret_val

            return

        # --------------------------------------------------------------------
        # FORCE_STOP_STREAM
        # FORCE_STOP_STREAM_METADATA
        # --------------------------------------------------------------------
        elif (signal == b"FORCE_STOP_STREAM"
              or signal == b"FORCE_STOP_STREAM_METADATA"):

            ret_val = self._stop_signal(
                signal=signal,
                appid=None,
                socket_ids=socket_ids,
                registered_ids=self.registered_streams,
                vari_requests=None,
                perm_requests=self.perm_requests
            )

            self.registered_streams, _, self.perm_requests = ret_val

            return

        # --------------------------------------------------------------------
        #  FORCE_STOP_QUERY_NEXT
        #  FORCE_STOP_QUERY_NEXT_METADATA
        # --------------------------------------------------------------------
        elif (signal == b"FORCE_STOP_QUERY_NEXT"
              or signal == b"FORCE_STOP_QUERY_NEXT_METADATA"):

            ret_val = self._stop_signal(
                signal=signal,
                appid=None,
                socket_ids=socket_ids,
                registered_ids=self.registered_queries,
                vari_requests=self.vari_requests,
                perm_requests=None
            )

            self.registered_queries, self.vari_requests, _ = ret_val

            return

        else:
            self.send_response([b"NO_VALID_SIGNAL"])

    def stop(self):
        """Close sockets and clean up.
        """
        super().stop()

        self.stop_request.set()
        self.wait_for_stopped()

        self.log.debug("Closing sockets for SignalHandler")

        self.stop_socket(name="com_socket")
        self.stop_socket(name="request_socket")
        self.stop_socket(name="request_fw_socket")
        self.stop_socket(name="control_pub_socket")
        self.stop_socket(name="control_sub_socket")

        if not self.ext_context and self.context:
            self.log.info("Destroying context")
            # don't use term her because it hangs
            self.context.destroy(0)
            self.context = None


def run_signalhandler(**kwargs):
    """ Wrapper to run in a process or thread"""

    proc = SignalHandler(**kwargs)
    proc.run()
