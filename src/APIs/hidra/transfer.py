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
API to communicate with a hidra sender unit.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import copy
from distutils.version import LooseVersion
import errno
import json
import logging
import multiprocessing
import multiprocessing.queues
import os
import re
import socket as socket_m
import sys
import tempfile
import time
import zmq
from zmq.auth.thread import ThreadAuthenticator

try:
    from pathlib2 import Path
except ImportError:
    from pathlib import Path

from .utils._version import __version__
from .utils import (
    NotSupported,
    UsageError,
    FormatError,
    ConnectionFailed,
    VersionError,
    AuthenticationFailed,
    CommunicationFailed,
    DataSavingError,
    LoggingFunction,
    Base,
    zmq_msg_to_nparray
)
from .control import Control


def get_logger(logger_name,
               queue=False,
               log_level="debug"):
    """Send all logs to the main process.

    The worker configuration is done at the start of the worker process run.
    Note that on Windows you can't rely on fork semantics, so each process
    will run the logging configuration code when it starts.
    """

    # pylint: disable=redefined-variable-type

    log_level_lower = log_level.lower()

    if queue:
        from logutils.queue import QueueHandler

        # Create log and set handler to queue handle
        handler = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger(logger_name)
        logger.propagate = False
        logger.addHandler(handler)

        if log_level_lower == "debug":
            logger.setLevel(logging.DEBUG)
        elif log_level_lower == "info":
            logger.setLevel(logging.INFO)
        elif log_level_lower == "warning":
            logger.setLevel(logging.WARNING)
        elif log_level_lower == "error":
            logger.setLevel(logging.ERROR)
        elif log_level_lower == "critical":
            logger.setLevel(logging.CRITICAL)
    else:
        logger = LoggingFunction(log_level_lower)

    return logger


def generate_filepath(base_path, config_dict, add_filename=True):
    """
    Generates full path (including file name) where file will be saved to.

    """
    if not config_dict or base_path is None:
        return None

    if (config_dict["relative_path"] == ""
            or config_dict["relative_path"] is None):
        target_path = base_path

    else:
        # if the relative path starts with a slash path.join will consider it
        # as absolute path
        if config_dict["relative_path"].startswith("/"):
            rel_path = config_dict["relative_path"][1:]
        else:
            rel_path = config_dict["relative_path"]

        target_path = Path(os.path.join(base_path, rel_path)).as_posix()

    if add_filename:
        filepath = Path(os.path.join(target_path,
                                     config_dict["filename"])).as_posix()

        return filepath
    else:
        return target_path


def generate_filepath_synced(config_dict):
    """
    Generates all full paths (including file names) where file will be saved
    to.

    Returns:
        A list of the file paths as strings.
    """

    if not config_dict or "additional_info" not in config_dict:
        return None

    file_paths = []
    for info in config_dict["additional_info"]:
        file_paths.append(info["file_path"])

    return file_paths


def generate_file_identifier(config_dict):
    """
    Generates file identifier assembled out of relative path and file name.

    """
    if not config_dict:
        return None

    if (config_dict["relative_path"] == ""
            or config_dict["relative_path"] is None):
        file_id = config_dict["filename"]

    # if the relative path starts with a slash path.join will consider it
    # as absolute path
    elif config_dict["relative_path"].startswith("/"):
        file_id = os.path.join(config_dict["relative_path"][1:],
                               config_dict["filename"])
    else:
        file_id = os.path.join(config_dict["relative_path"],
                               config_dict["filename"])
    return file_id


def convert_suffix_list_to_regex(pattern,
                                 suffix=True,
                                 compile_regex=False,
                                 log=None):
    """
    Takes a list of suffixes and converts it into a corresponding regex
    If input is a string nothing is done.

    Args:
        pattern (list or string): a list of suffixes, regexes or a string
        suffix (boolean): if a list of regexes is given which should be merged
        compile_regex (boolean): if the regex should be compiled
        log: logging handler (optional)

    Returns:
        regex (regex object): compiled regular expression of the style
                              ".*(<suffix>|...)$ resp. (<regex>|<regex>)"

    Raises:
        FormatError if the given regex is not compilable.
    """

    # Convert list into regex
    if isinstance(pattern, list):

        if suffix:
            regex = ".*"
        else:
            regex = ""

        file_suffix = ""
        for i in pattern:
            if i:
                # not an empty string
                file_suffix += i
            file_suffix += "|"

        # remove the last "|"
        file_suffix = file_suffix[:-1]

        if file_suffix:
            regex += "({})$".format(file_suffix)

    # a regex was given
    else:
        regex = pattern

    if log:
        log.debug("converted regex=%s", regex)

    if compile_regex:
        try:
            return re.compile(regex)
        except Exception:
            raise FormatError("Error when compiling regex '{}'".format(regex))
    else:
        return regex


class Transfer(Base):
    """The main transfer class.
    """

    def __init__(self,
                 connection_type,
                 signal_host=None,
                 use_log=False,
                 context=None,
                 # TODO remove dirs_not_to_create here
                 dirs_not_to_create=None,
                 detector_id=None,
                 control_server_port=51000):

        super().__init__()

        self.connection_type = connection_type
        self.detector_id = detector_id
        self.control_server_port = control_server_port
        self.control_api_log_level = "warning"

        self.log = None

        self.current_pid = None
        self.ipc_dir = None

        self.appid = None

        self.signal_host = None
        self.socket_conf = {}
        self.data_socket_endpoint = None

        self.context = None
        self.ext_context = None
        self.is_ipv6 = False
        self.zmq_protocol = None
        self.data_con_style = None
        self.ip = None

        self.signal_socket = None
        self.request_socket = None
        self.file_op_socket = None
        self.status_check_socket = None
        self.data_socket = None
        self.confirmation_socket = None
        self.control_socket = None

        self.control = None
        self.poller = None
        self.auth = None

        self.targets = None
        self.supported_connections = None
        self.signal_exchanged = None
        self.started_connections = dict()
        self.status = None

        self.socket_response_timeout = None

        self.number_of_streams = None
        self.recvd_close_from = []
        self.reply_to_signal = False
        self.all_close_recvd = False

        self.file_descriptors = dict()

        self.file_opened = False
        self.callback_params = None
        self.open_callback = None
        self.read_callback = None
        self.close_callback = None

        self.stopped_everything = False
        self.generate_target_filepath = None
        self._remote_version = None

        self.init_args = {
            "signal_host": signal_host,
            "use_log": use_log,
            "context": context,
            "dirs_not_to_create": dirs_not_to_create
        }
        self._setup()

    def _setup(self):

        # pylint: disable=redefined-variable-type
        # pylint: disable=unidiomatic-typecheck

        self._setup_logging()

        # ZMQ applications always start by creating a context,
        # and then using that for creating sockets
        # (source: ZeroMQ, Messaging for Many Applications by Pieter Hintjens)
        if self.init_args["context"] is not None:
            self.context = self.init_args["context"]
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.current_pid = os.getpid()
        self.ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")

        # Add application id to prevent other application closing remote
        # connections from this application
        self.appid = str(self.current_pid)

        if self.init_args["signal_host"] is not None:
            self.signal_host = socket_m.getfqdn(self.init_args["signal_host"])

        ports = self._get_remote_ports()

        # TODO use IP of hostname?
        self.ip = "0.0.0.0"  # pylint: disable=invalid-name

        default_conf = {
            "protocol": "tcp",
            "ip": self.ip,
            "port": None,
            "ipc_file": None
        }

        # TCP socket configurations
        for sckt_type, port in ports.items():
            self.socket_conf[sckt_type] = copy.deepcopy(default_conf)
            self.socket_conf[sckt_type]["port"] = port

        self.socket_conf["signal"]["ip"] = self.signal_host
        self.socket_conf["request"]["ip"] = self.signal_host

        # IPC socket configurations
        default_conf["protocol"] = "ipc"
        default_conf["ip"] = None

        self.socket_conf["control"] = copy.deepcopy(default_conf)
        self.socket_conf["control"]["ipc_file"] = "control_API"

        self.is_ipv6 = False
        self.data_con_style = "bind"

        self.poller = zmq.Poller()

        self.supported_connections = [
            "STREAM",
            "STREAM_METADATA",
            "QUERY_NEXT",
            "QUERY_NEXT_METADATA",
            "NEXUS"
        ]

        if self.init_args["dirs_not_to_create"] is None:
            self.dirs_not_to_create = self.init_args["dirs_not_to_create"]
        else:
            self.dirs_not_to_create = tuple(
                self.init_args["dirs_not_to_create"]
            )

        self.status = [b"OK"]
        self.socket_response_timeout = 1000

        # In older api versions this was a class method
        # (further support for users)
        self.generate_target_filepath = generate_filepath

        if self.connection_type not in self.supported_connections:
            raise NotSupported("Chosen type of connection is not supported.")

    def _setup_logging(self):

        # print messages of certain level to screen
        log_levels = ["debug", "info", "warning", "error", "critical"]
        if self.init_args["use_log"] in log_levels:
            self.log = LoggingFunction(self.init_args["use_log"])
            self.control_api_log_level = self.init_args["use_log"]

        # use logutils queue
        # isinstance does not work here
        elif type(self.init_args["use_log"]) in [multiprocessing.Queue,
                                                 multiprocessing.queues.Queue]:
            self.log = get_logger("Transfer", self.init_args["use_log"])

        # use logging
        elif self.init_args["use_log"]:
            self.log = logging.getLogger("Transfer")  # pylint: disable=redefined-variable-type

        # use no logging at all
        elif self.init_args["use_log"] is None:
            self.log = LoggingFunction(None)

        # print everything to screen
        else:
            self.log = LoggingFunction("debug")

    def _setup_control_server_connection(self):

        if self.control is not None:
            # nothing to do
            return

        beamline = {
            "host": self.signal_host,
            "port": self.control_server_port
        }
        self.log.debug("Setup control server connection (%s)", beamline)

        self.control = Control(
            beamline=beamline,
            detector=self.detector_id,
            ldapuri="",
            netgroup_template="",
            use_log=self.control_api_log_level,
            do_check=False
        )

    def _get_remote_ports(self):

        ports = {
            "status_check": 50050,
            "file_op": 50050,
            "confirmation": 50053
        }

        if self.detector_id is None:
            ports["signal"] = 50000
            ports["request"] = 50001
            return ports

        self.log.info("Get ports from the control server")
        self._setup_control_server_connection()

        # com port
        answer = self.control.get("com_port")
        if answer == b"ERROR":
            raise CommunicationFailed("Error when receiving signal/com port")
        ports["signal"] = answer

        # request port
        answer = self.control.get("request_port")
        if answer == b"ERROR":
            raise CommunicationFailed("Error when receiving request port")
        ports["request"] = answer

        self.log.debug("ports=%s", ports)
        return ports

    def get_remote_version(self):
        """Retrieves the version of hidra to connect to.

        Return:
            The version as a string.
        """

        self._create_signal_socket()

        if self.targets is None:
            self.targets = []

        signal = b"GET_VERSION"
        message = self._send_signal(signal)

        # if there was no response or the response was of the wrong format,
        # the receiver should be shut down
        if message and message[0].startswith(signal):
            self.log.info("Received signal confirmation ...")
            return message[1]
        else:
            self.log.error("Invalid confirmation received...")
            self.log.debug("message=%s", message)
            return None

    def set_appid(self, appid):
        """Sets the application id to custom value.

        Args:
            appid: The value to which the appid should be set.
        """

        self.appid = str(appid)

    def get_appid(self):
        """Get the application id.

        Return:
            The value to which the application id is set to (as str).
        """

        return self.appid

    def initiate(self, targets):
        """Sets up the zmq part on the hidra side.

        Args:
            targets: The targets to enable
                     [host, port, prio, file type]
                     or [[host, port, prio, file type], ...]

        Raises:
            CommunicationFailed: The communication to the hidra sender failed.
        """

        if self.connection_type == "NEXUS":
            self.log.info("There is no need for a signal exchange for "
                          "connection type 'NEXUS'")
            return

        if not isinstance(targets, list):
            self.stop()
            self.log.debug("targets=%s", targets)
            raise FormatError("Argument 'targets' must be list.")

        if not self.context:
            self.context = zmq.Context()
            self.ext_context = False

        if self.poller is None:
            self.poller = zmq.Poller()

        signal = None
        # Signal exchange
        if self.connection_type == "STREAM":
            signal = b"START_STREAM"
        elif self.connection_type == "STREAM_METADATA":
            signal = b"START_STREAM_METADATA"
        elif self.connection_type == "QUERY_NEXT":
            signal = b"START_QUERY_NEXT"
        elif self.connection_type == "QUERY_NEXT_METADATA":
            signal = b"START_QUERY_NEXT_METADATA"

        self.log.debug("Create socket for signal exchange...")

        self._create_signal_socket()

        self._set_targets(targets)

        message = self._send_signal(signal)

        # if there was no response or the response was of the wrong format,
        # the receiver should be shut down
        if message and message[0].startswith(signal):
            self.log.info("Received signal confirmation ...")
            self.signal_exchanged = signal

        else:
            msg_extension = ""
            if self._check_control_server_exists():
                instances = self.control.do("get_instances")

                if instances:
                    # self.log.info("Available detector instances: %s",
                    #               ", ".join(instances))
                    msg_extension = (" Available detector instances: {}"
                                     .format(", ".join(instances)))

            raise CommunicationFailed("Sending start signal ...failed."
                                      + msg_extension)

        self._remote_version = self.get_remote_version()

    def _create_signal_socket(self):
        """Create socket to exchange signals with sender.

        To send a notification that a displayer is up and running, a
        communication socket is needed.
        """

        if not self.signal_host:
            self.stop()
            raise ConnectionFailed("No host to send signal to specified.")

        # time to wait for the sender to give a confirmation of the signal
        # self.signal_socket.RCVTIMEO = self.socket_response_timeout

        self.signal_socket = self._start_socket(
            name="signal_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint=self._get_endpoint(**self.socket_conf["signal"])
        )

        # using a Poller to implement the signal_socket timeout
        self.poller.register(self.signal_socket, zmq.POLLIN)

    def _set_targets(self, targets):
        self.targets = []

        if not isinstance(targets, list) or len(targets) < 1:
            self.stop()
            self.log.debug("targets=%s", targets)
            raise FormatError("Argument 'targets' is of wrong format. "
                              "Has to be a list.")

        # [host, port, prio]
        if (len(targets) == 3
                and not isinstance(targets[0], list)
                and not isinstance(targets[1], list)
                and not isinstance(targets[2], list)):
            host, port, prio = targets
            addr = "{}:{}".format(socket_m.getfqdn(host), port)
            self.targets = [[addr, prio, ".*"]]

        # [host, port, prio, suffixes]
        elif (len(targets) == 4
              and not isinstance(targets[0], list)
              and not isinstance(targets[1], list)
              and not isinstance(targets[2], list)
              and isinstance(targets[3], list)):
            host, port, prio, suffixes = targets

            regex = convert_suffix_list_to_regex(suffixes,
                                                 log=self.log)

            addr = "{}:{}".format(socket_m.getfqdn(host), port)
            self.targets = [[addr, prio, regex]]

        # [[host, port, prio], ...] or [[host, port, prio, suffixes], ...]
        else:
            for t in targets:  # pylint: disable=invalid-name
                len_t = len(t)
                if (isinstance(t, list)
                        and (len_t == 3 or len_t == 4)
                        and not isinstance(t[0], list)
                        and not isinstance(t[1], list)
                        and not isinstance(t[2], list)):

                    if len_t == 3:
                        host, port, prio = t
                        suffixes = [""]
                    else:
                        host, port, prio, suffixes = t

                    regex = convert_suffix_list_to_regex(suffixes,
                                                         log=self.log)

                    addr = "{}:{}".format(socket_m.getfqdn(host), port)
                    self.targets.append([addr, prio, regex])
                else:
                    self.stop()
                    self.log.debug("targets=%s", targets)
                    raise FormatError("Argument 'targets' is of wrong format.")

    def _send_signal(self, signal):

        if not signal:
            return

        # Send the signal that the communication infrastructure should be
        # established
        self.log.info("Sending Signal")

        send_message = [__version__.encode("utf-8"),
                        self.appid.encode('utf-8'),
                        signal]

        trg = json.dumps(self.targets).encode('utf-8')
        send_message.append(trg)

        self.log.debug("Signal: %s", send_message)
        try:
            self.signal_socket.send_multipart(send_message)
        except Exception:
            self.log.error("Could not send signal")
            raise

        message = None
        try:
            socks = dict(self.poller.poll(self.socket_response_timeout))
        except Exception:
            self.log.error("Could not poll for new message")
            raise

        # if there was a response
        if (self.signal_socket in socks
                and socks[self.signal_socket] == zmq.POLLIN):
            try:
                #  Get the reply.
                message = self.signal_socket.recv_multipart()
                self.log.info("Received answer to signal: %s", message)

            except Exception:
                self.log.error("Could not receive answer to signal")
                raise
        else:
            self.log.error("Timeout for signal response")

        # check correctness of message
        if message and message[0] == b"VERSION_CONFLICT":
            self.stop()
            raise VersionError(
                "Versions are conflicting. Sender version: {}, API version: {}"
                .format(message[1], __version__)
            )

        elif message and message[0] == b"NO_VALID_HOST":
            self.stop()
            raise AuthenticationFailed("Host is not allowed to connect.")

        elif message and message[0] == b"CONNECTION_ALREADY_OPEN":
            self.stop()
            raise CommunicationFailed("Connection is already open.")

        elif message and message[0] == b"STORING_DISABLED":
            self.stop()
            raise CommunicationFailed("Data storing is disabled on sender "
                                      "side")

        elif message and message[0] == b"NO_VALID_SIGNAL":
            self.stop()
            raise CommunicationFailed("Either the connection type is not "
                                      "supported for this kind of sender or "
                                      "the targets are of wrong format.")

        return message

    def _check_control_server_exists(self):
        try:
            self._setup_control_server_connection()
            exists = True
        except CommunicationFailed:
            exists = False

        return exists

    def _get_data_endpoint(self, data_socket_prop):
        """Determines the local ip, DNS name and socket IDs

        Args:
            data_socket_prop: information about the socket to receive incoming
                              data

                - can be a list of the form [<host>, <port>]
                - can be a <port>, then the local host is used as <host>

        Return:
            socket_id (str): Address identifier (using the DNS name) where
                             it is bind to for data receiving.
            endpoint (str): The endpoint to bind to for data receiving
                            (this has to use the ip)
        """

        # pylint: disable=redefined-variable-type

        host = ""
        port = ""

        # determine host (may be DNS name) and port
        if data_socket_prop:
            self.log.debug("Specified data_socket_prop: %s", data_socket_prop)

            if isinstance(data_socket_prop, list):
                if len(data_socket_prop) == 2:
                    host = data_socket_prop[0]
                    port = data_socket_prop[1]
                else:
                    self.log.debug("data_socket_prop=%s", data_socket_prop)
                    raise FormatError("Socket information have to be of the "
                                      "form [<host>, <port>].")
            else:
                host = socket_m.getfqdn()
                port = str(data_socket_prop)

        elif self.targets:
            if len(self.targets) == 1:
                host, port = self.targets[0][0].split(":")
            else:
                raise FormatError("Multiple possible ports. "
                                  "Please choose which one to use.")
        else:
            raise FormatError("No target specified.")

        # ZMQ transport protocol IPC has a different syntax than TCP
        if self.zmq_protocol == "ipc":
            socket_id = "{}/{}".format(host, port).encode("utf-8")
            addr = "{}/{}".format(host, port)
            endpoint = "{}://{}".format(self.zmq_protocol, addr)

            return socket_id, endpoint

        # determine IP to bind to
        ip_from_host = socket_m.gethostbyaddr(host)[2]
        if len(ip_from_host) == 1:
            self.ip = ip_from_host[0]
            self._update_ip()
        else:
            self.log.debug("ip_from_host=%s", ip_from_host)
            raise CommunicationFailed("IP is ambiguous")

        # determine socket identifier (might use DNS name)
        socket_id = "{}:{}".format(host, port).encode("utf-8")

        # Distinguish between IPv4 and IPv6 addresses
        try:
            socket_m.inet_aton(self.ip)
            self.log.info("IPv4 address detected: %s.", self.ip)
            self.is_ipv6 = False
        except socket_m.error:
            self.log.info("Address '%s' is not an IPv4 address, "
                          "assume it is an IPv6 address.", self.ip)
            self.is_ipv6 = True

        # determine socket endpoint to bind to (uses IP)
        endpoint = self._get_endpoint(
            protocol="tcp",
            ip=self.ip,
            port=port,
            ipc_file=None
        )

        return socket_id, endpoint

    def _update_ip(self):
        """Update socket configuration if ip has changed.
        """

        self.socket_conf["status_check"]["ip"] = self.ip
        self.socket_conf["file_op"]["ip"] = self.ip
        self.socket_conf["confirmation"]["ip"] = self.ip

    def _get_endpoint(self, protocol, ip, port, ipc_file):
        """Determines socket endpoint.

        Args:
            protocol: The zmq protocol to use
            ip: The ip to use.
            port: The port to use.
            ipc_file: Location of ipc files.

        Return:
            The zmq endpoint to use.
        """
        # pylint: disable=invalid-name

        if protocol == "tcp":
            addr = self._get_tcp_addr(ip, port)
        elif protocol == "ipc":
            addr = self._get_ipc_addr(ipc_file)
        else:
            raise NotSupported("{Protocol %s is not supported", protocol)

        return "{}://{}".format(protocol, addr)

    def _get_tcp_addr(self, ip, port):
        """Determines ipc socket addess.

        If the IP is an IPV6 address the appropriate ZMQ syntax is used.
        """
        # pylint: disable=invalid-name

        if self.is_ipv6:
            return "[{}]:{}".format(ip, port)
        else:
            return "{}:{}".format(ip, port)

    def _get_ipc_addr(self, ipc_file):
        """Determines ipc socket address.
        """

        return "{}/{}_{}".format(self.ipc_dir,
                                 self.current_pid,
                                 ipc_file)

    def start(self,
              endpoint=False,
              whitelist=None,
              protocol="tcp",
              data_con_style="bind"):
        """Sets up zmq part locally.

        Args:
            endpoint (optional): The zmq endpoint to use.
            whitelist (optional): The host which are allowed to send data.
            protocol (optional): The zmq protocol to use. Default is tcp.
            data_con_style (optional): The zmq connection style.
                                       Default is bind.
        """

        # check parameters
        if protocol not in ["tcp", "ipc"]:
            raise NotSupported("Protocol {} is not supported."
                               .format(protocol))

        if data_con_style not in ["bind", "connect"]:
            raise NotSupported("Connection style '{}' is not supported."
                               .format(data_con_style))

        self.zmq_protocol = protocol
        self.data_con_style = data_con_style

        # determine socket id and address
        socket_endpoint = None
        socket_id = None
        for i in ["STREAM", "QUERY_NEXT", "NEXUS"]:
            if i in self.started_connections:
                socket_id = self.started_connections[i]["id"]
                socket_endpoint = self.started_connections[i]["endpoint"]

        if socket_endpoint is not None:
            self.log.info("Reopening already started connection.")
        else:
            socket_id, socket_endpoint = self._get_data_endpoint(endpoint)

        # -- authentication and data socket -- #
        # remember the endpoint for reestablishment of the connection
        self.data_socket_endpoint = socket_endpoint
        self.log.debug("data_socket_endpoint=%s", self.data_socket_endpoint)

        self.register(whitelist)
        # ----------------------------------- #

        if self.connection_type in ["QUERY_NEXT", "QUERY_NEXT_METADATA"]:

            # --------- request socket ---------- #
            # An additional socket is needed to establish the data retrieving
            # mechanism
            self.request_socket = self._start_socket(
                name="request socket",
                sock_type=zmq.PUSH,
                sock_con="connect",
                endpoint=self._get_endpoint(**self.socket_conf["request"])
            )
            # ----------------------------------- #

            self.started_connections["QUERY_NEXT"] = {
                "id": socket_id,
                "endpoint": socket_endpoint
            }

        elif self.connection_type in ["NEXUS"]:

            # ------ file operation socket ------ #
            # Reuse status check socket to get signals to open and close
            # nexus files
            self.setopt("status_check")
            # ----------------------------------- #

            # --------- control socket ---------- #
            if not os.path.exists(self.ipc_dir):
                os.makedirs(self.ipc_dir)

            # Socket to retrieve control signals from control API
            self.control_socket = self._start_socket(
                name="internal controlling socket",
                sock_type=zmq.PULL,
                sock_con="bind",
                endpoint=self._get_endpoint(**self.socket_conf["control"])
            )

            self.poller.register(self.control_socket, zmq.POLLIN)
            # ---------------------------------- #

            self.started_connections["NEXUS"] = {
                "id": socket_id,
                "endpoint": socket_endpoint
            }
        else:
            self.started_connections["STREAM"] = {
                "id": socket_id,
                "endpoint": socket_endpoint
            }

    def setopt(self, option, value=None):
        """
        Args:
            option (str):
                "file_op": enable and configure socket to use for receiving
                           file operation commands for NEXUS use case
                "status_check": enable and configure socket to use for status
                                check requests
                "confirmation": enable and configure socket to use for
                                confirmation that individual data packets where
                                handled without problems
            value (optional):
                - int: port to be used for setup of specified option
                - list of len 2: ip and port to be used for setup of specified
                                 option [<ip>, <port>]
                - list of len 3: ip and port to be used for setup of specified
                                 option [<protocol>, <ip>, <port>]

        Raises:
            NotSupported: If option is not supported.
            FormatError: If value has the wrong format.
        """
        if option == "status_check":
            # TODO create Thread which handles this asynchronously
            if self.status_check_socket is not None:
                self.log.error("Status check is already enabled (used port: "
                               "%s)", self.socket_conf["status_check"]["port"])
                return

            # ------- status check socket ------ #
            # socket to get signals to get status check requests. this socket
            # is also used to get signals to open and close nexus files
            self._unpack_value(value, self.socket_conf["status_check"])
            endpoint = self._get_endpoint(**self.socket_conf["status_check"])

            self.status_check_socket = self._start_socket(
                name="status check socket",
                sock_type=zmq.REP,
                sock_con="bind",
                endpoint=endpoint,
                zap_domain=b"global",
                is_ipv6=self.is_ipv6
            )

            self.poller.register(self.status_check_socket, zmq.POLLIN)
            # ---------------------------------- #

        elif option == "file_op":
            if self.file_op_socket is not None:
                self.log.error("File operation is already enabled (used port: "
                               "%s)", self.socket_conf["file_op"]["port"])
                return

            # ------- status check socket ------ #
            # socket to get signals to get status check requests. this socket
            # is also used to get signals to open and close nexus files
            self._unpack_value(value, self.socket_conf["file_op"])
            endpoint = self._get_endpoint(**self.socket_conf["file_op"])

            self.file_op_socket = self._start_socket(
                name="file_op_socket",
                sock_type=zmq.REP,
                sock_con="bind",
                endpoint=endpoint,
                zap_domain=b"global",
                is_ipv6=self.is_ipv6,
            )

            self.poller.register(self.file_op_socket, zmq.POLLIN)
            # ---------------------------------- #

        elif option == "confirmation":
            if self.confirmation_socket is not None:
                self.log.error(
                    "Confirmation is already enabled (used port: %s)",
                    self.socket_conf["confirmation"]["port"]
                )
                return

#            self.confirmation_protocol = "tcp"
#            self.confirmation_ip = self.ip
#
#            if value is not None:
#                if type(value) == list:
#                    if len(value) == 2:
#                        self.confirmation_ip = value[0]
#                        self.confirmation_port = value[1]
#                    elif len(value) == 3:
#                        self.confirmation_protocol = value[0]
#                        self.confirmation_ip = value[1]
#                        self.confirmation_port = value[2]
#                    else:
#                        self.log.debug("value=%s", value)
#                        raise FormatError("Socket information have to be of "
#                                          "the form [<host>, <port>].")
#                else:
#                    self.confirmation_port = value
#
#            con_str = "{}://{}".format(
#                self.confirmation_protocol,
#                self.__get_socket_id(self.confirmation_ip,
#                                     self.confirmation_port))
#

            # ------- confirmation socket ------ #
            # to send the a confirmation to the sender that the data packages
            # was stored successfully
            self._unpack_value(value, self.socket_conf["confirmation"])
            endpoint = self._get_endpoint(**self.socket_conf["confirmation"])

            self.confirmation_socket = self._start_socket(
                name="confirmation socket",
                sock_type=zmq.PUB,
                sock_con="bind",
                endpoint=endpoint,
            )
            # ---------------------------------- #
        else:
            raise NotSupported("Option {} is not supported".format(option))

    def _unpack_value(self, value, prop):
        """Helper function to unpacks value.

        Args:
            value:
                - int: port to be used for setup of specified option
                - list of len 2: ip and port to be used for setup of specified
                                 option [<ip>, <port>]
                - list of len 3: ip and port to be used for setup of specified
                                 option [<protocol>, <ip>, <port>]

            prop (dict): Containing default values for protocol, ip and port.
                         These are overwritten with the content of value.
        """

        if value is not None:
            if isinstance(value, list):
                if len(value) == 2:
                    prop["ip"] = value[0]
                    prop["port"] = value[1]
                elif len(value) == 3:
                    prop["protocol"] = value[0]
                    prop["ip"] = value[1]
                    prop["port"] = value[2]
                else:
                    self.log.debug("value=%s", value)
                    raise FormatError(
                        "Socket information has to be of the form "
                        "[<host>, <port>]."
                    )
            else:
                prop["port"] = value

    def register(self, whitelist):
        """Registers a new whitelist and restart data socket.

        If the whitelist contains a netgroup whose content changed, the new
        hosts have to be registered and existing data sockets restarted.

        Args:
            whitelist: (None or list)
        """

        if whitelist is not None:
            if not isinstance(whitelist, list):
                self.log.debug("whitelist %s", whitelist)
                raise FormatError(
                    "Whitelist has to be a list of IPs/DNS names"
                )

            if self.auth is not None:
                # to add hosts to the whitelist the authentication thread has
                # to be stopped and the socket closed
                self.log.debug("Shutting down auth thread and data_socket")
                self.auth.stop()
                self.poller.unregister(self.data_socket)
                self.data_socket.close()

            self.log.debug("Starting auth thread")
            self.auth = ThreadAuthenticator(self.context)
            self.auth.start()

            if whitelist == []:
                # if auth.allow is not called for at least one host, all host
                # are allowed to connect.
                host = "localhost"
                ip = [socket_m.gethostbyname(host)]
                self.log.debug("Empty whitelist: Allowing host %s (%s)",
                               host, ip[0])
                self.auth.allow(ip[0])

            # receive data only from whitelisted nodes
            for host in whitelist:
                try:
                    # convert DNS names to IPs
                    if host == "localhost":
                        ip = [socket_m.gethostbyname(host)]
                    else:
                        # returns (hostname, aliaslist, ipaddrlist)
                        ip = socket_m.gethostbyaddr(host)[2]

                    self.log.debug("Allowing host %s (%s)", host, ip[0])
                    self.auth.allow(ip[0])
                # getaddrinfo error
                except socket_m.gaierror:
                    self.log.error("Could not get IP of host %s. Proceed.",
                                   host)
                except Exception:
                    self.log.error("Error was: ", exc_info=True)
                    raise AuthenticationFailed(
                        "Could not get IP of host {}".format(host)
                    )

        # Recreate the socket (now with the new whitelist enabled)
        self.data_socket = self._start_socket(
            name="data socket of type {}".format(self.connection_type),
            sock_type=zmq.PULL,
            sock_con=self.data_con_style,
            endpoint=self.data_socket_endpoint,
            zap_domain=b"global",
            is_ipv6=self.is_ipv6,
        )

        self.poller.register(self.data_socket, zmq.POLLIN)

    def read(self,
             callback_params,
             open_callback,
             read_callback,
             close_callback):
        """

        Args:
            callback_params:
            open_callback:
            read_callback:
            close_callback:
        """

        if (not self.connection_type == "NEXUS"
                or "NEXUS" not in self.started_connections):
            raise UsageError(
                "Wrong connection type (current: {}) or session not started."
                .format(self.connection_type)
            )

        self.callback_params = callback_params
        self.open_callback = open_callback
        self.read_callback = read_callback
        self.close_callback = close_callback

        run_loop = True

        while run_loop:
            self.log.debug("polling")
            try:
                socks = dict(self.poller.poll())
            except Exception:
                self.log.error("Could not poll for new message")
                raise

            # received signal from status check socket
            # (socket is also used for nexus signals)
            if (self.status_check_socket in socks
                    and socks[self.status_check_socket] == zmq.POLLIN):
                self.log.debug("status_check_socket is polling")

                message = self.status_check_socket.recv_multipart()
                self.log.debug("status_check_socket recv: %s", message)

                # request to close the open file
                if message[0] == b"CLOSE_FILE":
                    if self.all_close_recvd:
                        self.status_check_socket.send_multipart(message)
                        self.log.debug("status_check_socket (file operation) "
                                       "send: %s", message)
                        self.all_close_recvd = False

                        self.close_callback(self.callback_params,
                                            message)
                        break
                    else:
                        self.reply_to_signal = message

                # request to open a new file
                elif message[0] == b"OPEN_FILE":
                    self.status_check_socket.send_multipart(message)
                    self.log.debug("status_check_socket (file operation) "
                                   "send: %s", message)

                    try:
                        self.open_callback(self.callback_params, message[1])
                        self.file_opened = True
                    except Exception:
                        self.status_check_socket.send_multipart([b"ERROR"])
                        self.log.error("Not supported message received")

                # received not supported signal
                else:
                    self.status_check_socket.send_multipart([b"ERROR"])
                    self.log.error("Not supported message received")

            # received data
            if (self.data_socket in socks
                    and socks[self.data_socket] == zmq.POLLIN):
                self.log.debug("data_socket is polling")

                try:
                    multipart_message = self.data_socket.recv_multipart()
#                    self.log.debug("multipart_message=%s",
#                                   multipart_message[:100])
                except Exception:
                    self.log.error("Could not receive data due to unknown "
                                   "error.", exc_info=True)
                    continue

                if multipart_message[0] == b"ALIVE_TEST":
                    continue

                if len(multipart_message) < 2:
                    self.log.error("Received mutipart-message is too short. "
                                   "Either config or file content is missing.")
#                    self.log.debug("multipart_message=%s",
#                                   multipart_message[:100])
                    # TODO return errorcode

                try:
                    run_loop = self._react_on_message(multipart_message)
                except KeyboardInterrupt:
                    self.log.debug("Keyboard interrupt detected. "
                                   "Stopping to receive.")
                    raise
                except Exception:
                    self.log.error("Unknown error while receiving files. "
                                   "Need to abort.", exc_info=True)
#                    raise Exception("Unknown error while receiving files. "
#                                   "Need to abort.")

            # received control signal
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):
                self.log.debug("control_socket is polling")
                self.control_socket.recv()
#                self.log.debug("Control signal received. Stopping.")
                raise Exception("Control signal received. Stopping.")

    def _react_on_message(self, multipart_message):

        if multipart_message[0] == b"CLOSE_FILE":
            try:
                # filename = multipart_message[1]
                file_id = multipart_message[2]
            except Exception:
                self.log.error("Could not extract id from the "
                               "multipart-message", exc_info=True)
                self.log.debug("multipart-message: %s", multipart_message,
                               exc_info=True)
                raise

            self.recvd_close_from.append(file_id)
            self.log.debug("Received close-file signal from "
                           "DataDispatcher-%s", file_id)

            # get number of signals to wait for
            if not self.number_of_streams:
                self.number_of_streams = int(file_id.split("/")[1])

            # have all signals arrived?
            self.log.debug("self.recvd_close_from=%s, "
                           "self.number_of_streams=%s",
                           self.recvd_close_from, self.number_of_streams)
            if len(self.recvd_close_from) == self.number_of_streams:
                self.log.info("All close-file-signals arrived")
                if self.reply_to_signal:
                    self.status_check_socket.send_multipart(
                        self.reply_to_signal)
                    self.log.debug("status_check_socket (file operation) "
                                   "send: %s", self.reply_to_signal)

                    self.reply_to_signal = False
                    self.recvd_close_from = []

                    self.close_callback(self.callback_params,
                                        multipart_message)
                    return False
                else:
                    self.all_close_recvd = True

            else:
                self.log.info("self.recvd_close_from=%s, "
                              "self.number_of_streams=%s",
                              self.recvd_close_from, self.number_of_streams)

        else:
            # extract multipart message
            try:
                metadata = json.loads(multipart_message[0].decode("utf-8"))
            except Exception:
                # json.dumps of None results in 'null'
                if multipart_message[0] != 'null':
                    self.log.error("Could not extract metadata from the "
                                   "multipart-message.", exc_info=True)
                    self.log.debug("multipartmessage[0] = %s",
                                   multipart_message[0], exc_info=True)
                metadata = None

            # TODO validate multipart_message
            # (like correct dict-values for metadata)

            try:
                payload = multipart_message[1]
            except Exception:
                self.log.warning("An empty file was received within the "
                                 "multipart-message", exc_info=True)
                payload = None

            self.read_callback(self.callback_params, [metadata, payload])

        return True

    def get_chunk(self, timeout=None):
        """
        Receives or queries for chunks of the new files depending on the
        connection initialized.

        Args:
            timeout (optional): The time (in ms) to wait for new messages to
                               come before aborting.

        Returns:
            Either
            the newest data chunk
                (if connection type "QUERY_NEXT" or "STREAM" was chosen)
            the metadata of the newest data chunk
                (if connection type "QUERY_NEXT_METADATA" or "STREAM_METADATA"
                was chosen)

        """

        # query_metadata and stream_metadata are covered with this as well
        if ("STREAM" not in self.started_connections
                and "QUERY_NEXT" not in self.started_connections):
            self.log.error("Could not communicate, no connection was "
                           "initialized.")
            return None, None

        if "QUERY_NEXT" in self.started_connections:

            send_message = [b"NEXT",
                            self.started_connections["QUERY_NEXT"]["id"]]
            try:
                self.request_socket.send_multipart(send_message)
            except Exception:
                self.log.error("Could not send request to request_socket",
                               exc_info=True)
                return None, None

        timestamp = time.time()

        while True:
            # receive data
            try:
                socks = dict(self.poller.poll(timeout))
            except Exception:
                if self.stopped_everything:
                    self.log.debug("Stopping poller")
                    raise KeyboardInterrupt
                else:
                    self.log.error("Could not poll for new message")
                    raise

            # received signal from status check socket
            if (self.status_check_socket in socks
                    and socks[self.status_check_socket] == zmq.POLLIN):

                message = self.status_check_socket.recv_multipart()
#                self.log.debug("status_check_socket recv: %s", message)

                # request to close the open file
                if message[0] == b"STATUS_CHECK":
                    self.status_check_socket.send_multipart(self.status)
#                    self.log.debug("status_check_socket send: %s",
#                                   self.status)
                elif message[0] == b"RESET_STATUS":
                    self.status = [b"OK"]
                    self.log.debug("Reset request received. Status changed "
                                   "to: %s", self.status)
                    self.status_check_socket.send_multipart(self.status)
                # received not supported signal
                else:
                    self.status_check_socket.send_multipart([b"ERROR"])
                    self.log.error("Not supported message received")

            # if there was a response
            if (self.data_socket in socks
                    and socks[self.data_socket] == zmq.POLLIN):

                try:
                    multipart_message = self.data_socket.recv_multipart()
                except Exception:
                    self.log.error("Receiving data..failed.", exc_info=True)
                    return [None, None]

                if multipart_message[0] == b"ALIVE_TEST":
                    if timeout:
                        # measure how much time is left from the timeout value
                        # timeout is in ms, timestamp in s
                        timeout -= (time.time() - timestamp) * 1000
                    if timeout is not None and timeout < 0:
                        return [None, None]
                    else:
                        continue
                elif len(multipart_message) < 2:
                    self.log.error("Received mutipart-message is too short. "
                                   "Either config or file content is missing.")
                    self.log.debug("multipart_message=%s",
                                   multipart_message[:100])
                    return [None, None]

                # extract multipart message
                try:
                    metadata = json.loads(multipart_message[0].decode("utf-8"))
                except Exception:
                    self.log.error("Could not extract metadata from the "
                                   "multipart-message.", exc_info=True)
                    metadata = None

                # TODO validate multipart_message
                # (like correct dict-values for metadata)

                # this does not fail because length was already checked
                payload = multipart_message[1]

                return [metadata, payload]

            # no response was received
            else:
                # self.log.warning("Could not receive data in the given time.")

                if "QUERY_NEXT" in self.started_connections:
                    try:
                        self.request_socket.send_multipart(
                            [b"CANCEL",
                             self.started_connections["QUERY_NEXT"]["id"]])
                    except Exception:
                        self.log.error("Could not cancel the next query",
                                       exc_info=True)

                return [None, None]

    def check_file_closed(self, metadata, payload):
        """Checks if all chunks were received.

        Args:
            metadata: The metadata of the file.
            payload: The data of the file.

        Return:
            True if the payload was the last chunk of the file,
            False otherwise.
        """
        # pylint: disable=no-self-use
        # pylint: disable=invalid-name

        m = metadata  # pylint: disable=invalid-name

        # if arrays of numpy arrays are sent the max number of chunks is know
        # beforehand and thus can be used directly
        if "max_chunks" in m:
            return m["chunk_number"] == m["max_chunks"] - 1

        # Either the message is smaller than than expected (last chunk)
        # or the size of the origin file was a multiple of the
        # chunksize and this is the last expected chunk (chunk_number
        # starts with 0)
        return (
            len(payload) < m["chunksize"]
            or (m["filesize"] % m["chunksize"] == 0
                and m["filesize"] / m["chunksize"] == m["chunk_number"] + 1)
        )

    def get(self, timeout=None):
        """
        Receives or queries for new files depending on the connection
        initialized.

        Args:
            timeout (optional): The time (in ms) to wait for new messages to
                               come before stop waiting.

        Returns:
            Either
            the newest file
                (if connection type "QUERY_NEXT" or "STREAM" was chosen)
            the metadata of the newest file
                (if connection type "QUERY_NEXT_METADATA" or "STREAM_METADATA"
                was chosen)

        """
        run_loop = True
        all_received = {}

        # save all chunks to file
        while run_loop:

            # --------------------------------------------------------------------
            # receive data
            # --------------------------------------------------------------------
            try:
                # timeout (in ms) to be able to react on system signals
                [metadata, payload] = self.get_chunk(timeout=timeout)
            except KeyboardInterrupt:
                raise
            except Exception:
                if self.stopped_everything:
                    break
                else:
                    self.log.error("Getting data failed.", exc_info=True)
                    raise

            if metadata is None and payload is None:
                self.log.info("Reached timeout")
                return metadata, payload

            if "METADATA" in self.connection_type:
                return metadata, payload

            file_id = generate_file_identifier(metadata)

            # --------------------------------------------------------------------
            # keep track of chunks + check_number
            # --------------------------------------------------------------------
            if file_id not in all_received or metadata["chunk_number"] == 0:
                # "Reopen" file {}"
                all_received[file_id] = {
                    "metadata": metadata,
                    "data": [],
                }

            all_received[file_id]["data"].append(copy.deepcopy(payload))

            # --------------------------------------------------------------------
            # return closed file
            # --------------------------------------------------------------------
            if self.check_file_closed(metadata, payload):
                # for convenience
                received = all_received[file_id]

                # indicates end of file. Leave loop
                self.log.info("New file with modification time %s received",
                              received["metadata"]["file_mod_time"])

                try:
                    # highlight that the metadata does not correspond to only
                    # one chunk anymore
                    metadata = received["metadata"]
                    metadata["chunk_number"] = None

                    if ("type" in metadata
                            and metadata["type"] == "numpy_array_list"):
                        m = metadata["additional_info"]
                        received["data"] = [
                            zmq_msg_to_nparray(data=msg, array_metadata=m[i])
                            for i, msg in enumerate(received["data"])
                        ]
                    # when handling files
                    else:
                        # merge the data again
                        received["data"] = b"".join(received["data"])
                except Exception:
                    self.log.error("Something went wrong when merging chunks",
                                   exc_info=True)
                    raise

                return received["metadata"], received["data"]

    def store_chunk(self,
                    descriptors,
                    filepath,
                    payload,
                    base_path,
                    metadata):
        """Writes the data chunk into a file.

        Args:
            descriptors: All open file descriptors
            filepath: The path of the file to which the chunk should be stored.
            payload: The data to store.
            base_path: The base path under which the file should be stored
            metadata: The metadata received together with the data.
        """

        # --------------------------------------------------------------------
        # check chunk_number
        # --------------------------------------------------------------------
        # if chunk_number == 0 open file
        # if not, and file is not open throw DataSavingError
        # if file is open,
        #      - and chunk_number did not change compared to last one -> ignore
        #      - and chunk_number < last chunk number -> either reopen (if 0)
        #                                                or throw error

        try:
            # check if file is open
            desc = descriptors[filepath]
        except KeyError:
            # no
            if metadata["chunk_number"] != 0:
                self.log.debug("File not open but chunk_number not 0")
                raise DataSavingError(
                    "Missing beginning of file. Do not open file."
                )

        write_chunk = True

        # --------------------------------------------------------------------
        # open file
        # --------------------------------------------------------------------
        try:
            desc = descriptors[filepath]
        except KeyError:
            try:
                descriptors[filepath] = {
                    "file": open(filepath, "wb"),
                    "last_chunk_number": None
                }
                desc = descriptors[filepath]
            except IOError as excp:
                # errno.ENOENT == "No such file or directory"
                if excp.errno == errno.ENOENT:
                    target_path = None
                    try:
                        rel_path = metadata["relative_path"]

                        # do not create directories defined as immutable,
                        # e.g.commissioning, current and local
                        dirs = self.dirs_not_to_create
                        if (dirs is not None
                                and rel_path in dirs):
                            self.log.error("Unable to write file '%s': "
                                           "Directory %s is not available",
                                           filepath, rel_path)
                            raise

                        target_path = generate_filepath(base_path,
                                                        metadata,
                                                        add_filename=False)
                        os.makedirs(target_path)

                        descriptors[filepath] = {
                            "file": open(filepath, "wb"),
                            "last_chunk_number": None
                        }
                        desc = descriptors[filepath]
                        self.log.info("New target directory created: %s",
                                      target_path)
                    except Exception:
                        self.log.error("Unable to open file: '%s'", filepath,
                                       exc_info=True)
                        self.log.debug("target_path:%s", target_path)
                        raise
                else:
                    self.log.error("Failed to open file: '%s'", filepath,
                                   exc_info=True)
                    raise
            except Exception:
                self.log.error("Failed to open file: '%s'", filepath,
                               exc_info=True)
                raise

        # --------------------------------------------------------------------
        # check chunk_number
        # --------------------------------------------------------------------
        if desc["last_chunk_number"] is None:
            pass

        elif metadata["chunk_number"] == desc["last_chunk_number"]:
            # ignore chunk, was already written
            self.log.info("Ignore identical chunk %s for file %s",
                          metadata["chunk_number"], filepath)
            write_chunk = False

        elif metadata["chunk_number"] < desc["last_chunk_number"]:

            if metadata["chunk_number"] == 0:
                self.log.debug("Reopen file %s", filepath)
                # close the not finished file
                desc["file"].close()

                desc["file"] = open(filepath, "wb")
                desc["last_chunk_number"] = None
            else:
                self.log.debug("chunk_number=%s", metadata["chunk_number"])
                raise DataSavingError(
                    "Failed to reopen file '{}': Received a not matching "
                    "chunk_number".format(filepath)
                )

        # --------------------------------------------------------------------
        # write data
        # --------------------------------------------------------------------
        try:
            if write_chunk:
                desc["file"].write(payload)
                desc["last_chunk_number"] = metadata["chunk_number"]
        except KeyboardInterrupt:
            # save the data in the file before quitting
            self.log.debug("KeyboardInterrupt received while writing data")
            self.log.debug("Closing file %s", filepath)
            descriptors[filepath]["file"].close()
            del descriptors[filepath]
            raise
        except Exception:
            self.log.error("Failed to append payload to file: '%s'", filepath,
                           exc_info=True)
            self.log.debug("Closing file %s", filepath)
            descriptors[filepath]["file"].close()
            del descriptors[filepath]
            raise

        # --------------------------------------------------------------------
        # send confirmation
        # --------------------------------------------------------------------
        if ("confirmation_required" in metadata
                and metadata["confirmation_required"]):
            file_id = generate_file_identifier(metadata)
            # send confirmation
            try:
                topic = metadata["confirmation_required"].encode()

                try:
                    remote_version = LooseVersion(metadata["version"])
                except KeyError:
                    remote_version = None

                # to ensure backwards compatibility with 4.0.x versions
                # use LooseVersion because otherwise a test like
                # 4.0.10 <= 4.0.7 fails
                if (remote_version is None
                        or remote_version <= LooseVersion("4.0.7")):
                    message = [topic,
                               file_id.encode("utf-8")]
                else:
                    message = [topic,
                               file_id.encode("utf-8"),
                               str(metadata["chunk_number"]).encode("utf-8")]

                self.confirmation_socket.send_multipart(message)
                self.log.debug("Sending confirmation for chunk %s of "
                               "file '%s' to %s", metadata["chunk_number"],
                               file_id, topic)
            except Exception:
                if self.confirmation_socket is None:
                    self.log.error("Correct data handling is requested to "
                                   "be confirmed. Please enable option "
                                   "'confirmation'")
                    raise UsageError("Option 'confirmation' is not "
                                     "enabled")
                else:
                    raise

        # --------------------------------------------------------------------
        # close file
        # --------------------------------------------------------------------
        if self.check_file_closed(metadata, payload):
            # indicates end of file. Leave loop
            try:
                descriptors[filepath]["file"].close()
                del descriptors[filepath]

                self.log.info("New file with modification time %s received "
                              "and saved: %s", metadata["file_mod_time"],
                              filepath)
            except Exception:
                self.log.error("File could not be closed: %s", filepath,
                               exc_info=True)
                raise
            return False
        else:
            return True

    def store(self, target_base_path, timeout=None):
        """Writes all data belonging to one file to disc.

        Args:
            target_base_path: The base path under which the file possible
                              subdirectories should be created.
            timeout (optional): The time (in ms) to wait for new messages to
                               come before aborting.
        """

        run_loop = True

        # save all chunks to file
        while run_loop:

            try:
                # timeout (in ms) to be able to react on system signals
                [metadata, payload] = self.get_chunk(timeout)
            except KeyboardInterrupt:
                raise
            except Exception:
                if self.stopped_everything:
                    break
                else:
                    self.log.error("Getting data failed.", exc_info=True)
                    raise

            if metadata is None and payload is None:
                # self.log.debug("No data received. Break loop")
                break

            try:
                chunk_number = metadata["chunk_number"]
            except KeyError:
                chunk_number = None

            # generate target filepath
            target_filepath = generate_filepath(target_base_path, metadata)
            self.log.debug("New chunk (%s) for file %s received.",
                           chunk_number, target_filepath)

            # TODO: save message to file using a thread (avoids blocking)
            try:
                run_loop = self.store_chunk(
                    descriptors=self.file_descriptors,
                    filepath=target_filepath,
                    payload=payload,
                    base_path=target_base_path,
                    metadata=metadata
                )
            except Exception:
                self.log.debug("Stopping data storing loop")

                # returns a tuple (type, value, traceback)
                exc_type, exc_value = sys.exc_info()[:2]

                # duplicates error message in log
#                self.log.error(exc_value, exc_info=True)

                self.status = [b"ERROR",
                               str(exc_type).encode("utf-8"),
                               str(exc_value).encode("utf-8")]
                self.log.debug("Status changed to: %s", self.status)

                break

    def stop(self):
        """
        * Close open file handler to prevent file corruption
        * Send signal that the application is quitting
        * Close ZMQ connections
        * Destroying context

        """

        # Close open file handler to prevent file corruption
        for target in list(self.file_descriptors):
            try:
                self.file_descriptors[target]["file"].close()
                self.log.warning("Not all chunks were received for file %s",
                                 target)
            except KeyError:
                pass
            del self.file_descriptors[target]

        # Send signal that the application is quitting
        if self.signal_socket and self.signal_exchanged:
            self.log.info("Sending close signal")
            signal = None
            if ("STREAM" in self.started_connections
                    or b"STREAM" in self.signal_exchanged):
                signal = b"STOP_STREAM"
            elif ("QUERY_NEXT" in self.started_connections
                  or b"QUERY_NEXT" in self.signal_exchanged):
                signal = b"STOP_QUERY_NEXT"

            self._send_signal(signal)
            # TODO: need to check correctness of signal?
#            message = self._send_signal(signal)

            try:
                del self.started_connections["STREAM"]
            except KeyError:
                pass
            try:
                del self.started_connections["QUERY_NEXT"]
            except KeyError:
                pass

        # unregister sockets from poller
        self.poller = None

        # Close ZMQ connections
        try:
            self._stop_socket(name="signal_socket")
            self._stop_socket(name="data_socket")
            self._stop_socket(name="request_socket")
            self._stop_socket(name="status_check_socket")
            self._stop_socket(name="confirmation_socket")
            self._stop_socket(name="control_socket")

            # remove ipc remainings
            if self.control_socket is not None:
                control_addr = self._get_ipc_addr(
                    ipc_file=self.socket_conf["control"]["ipc_file"]
                )
                try:
                    os.remove(control_addr)
                    self.log.debug("Removed ipc address: %s", control_addr)
                except OSError:
                    self.log.warning("Could not remove ipc address: %s",
                                     control_addr)
                except Exception:
                    self.log.warning("Could not remove ipc address: %s",
                                     control_addr, exc_info=True)
        except Exception:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        # stopping authentication thread
        if self.auth is not None:
            try:
                self.auth.stop()
                self.auth = None
                self.log.info("Stopping authentication thread...done.")
            except Exception:
                self.log.error("Error when stopping authentication thread.",
                               exc_info=True)

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.ext_context and self.context is not None:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy()
#                self.context.term()
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except Exception:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)

        self.stopped_everything = True

    def force_stop(self, targets):
        """Stops the open connection on the hidra side.

        This is used for example when the former transfer process did not stop
        and de-register properly.

        Args
            targets: The targets to enable.
                     [[host, port, prio, file type], ...]
        """

        if not isinstance(targets, list):
            self.stop()
            raise FormatError("Argument 'targets' must be list.")

        if self.context is None:
            self.context = zmq.Context()
            self.ext_context = False

        signal = None
        # Signal exchange
        if self.connection_type == "STREAM":
            signal = b"FORCE_STOP_STREAM"
        elif self.connection_type == "STREAM_METADATA":
            signal = b"FORCE_STOP_STREAM_METADATA"
        elif self.connection_type == "QUERY_NEXT":
            signal = b"FORCE_STOP_QUERY_NEXT"
        elif self.connection_type == "QUERY_NEXT_METADATA":
            signal = b"FORCE_STOP_QUERY_NEXT_METADATA"

        self.log.debug("Create socket for signal exchange...")

        if self.signal_socket is None:
            self._create_signal_socket()

        self._set_targets(targets)

        message = self._send_signal(signal)

        # if there was no response or the response was of the wrong format,
        # the receiver should be shut down
        if message and message[0].startswith(signal):
            self.log.info("Received confirmation ...")

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()
