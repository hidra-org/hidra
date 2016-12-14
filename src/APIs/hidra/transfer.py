# API to communicate with a data transfer unit

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import


import zmq
import socket
import logging
import json
import errno
import os
import traceback
import tempfile
from zmq.auth.thread import ThreadAuthenticator

from ._version import __version__

class LoggingFunction:
    def out(self, x, exc_info=None):
        if exc_info:
            print (x, traceback.format_exc())
        else:
            print (x)

    def __init__(self):
        self.debug = lambda x, exc_info=None: self.out(x, exc_info)
        self.info = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning = lambda x, exc_info=None: self.out(x, exc_info)
        self.error = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


class NoLoggingFunction:
    def out(self, x, exc_info=None):
        pass

    def __init__(self):
        self.debug = lambda x, exc_info=None: self.out(x, exc_info)
        self.info = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning = lambda x, exc_info=None: self.out(x, exc_info)
        self.error = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


class NotSupported(Exception):
    pass


class UsageError(Exception):
    pass


class FormatError(Exception):
    pass


class ConnectionFailed(Exception):
    pass


class VersionError(Exception):
    pass


class AuthenticationFailed(Exception):
    pass


class CommunicationFailed(Exception):
    pass


class DataSavingError(Exception):
    pass


class Transfer():
    def __init__(self, connection_type, signal_host=None, use_log=False,
                 context=None):

        if use_log:
            self.log = logging.getLogger("Transfer")
        elif use_log is None:
            self.log = NoLoggingFunction()
        else:
            self.log = LoggingFunction()

        # ZMQ applications always start by creating a context,
        # and then using that for creating sockets
        # (source: ZeroMQ, Messaging for Many Applications by Pieter Hintjens)
        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.current_pid = os.getpid()

        self.signal_host = signal_host
        self.signal_port = "50000"
        self.request_port = "50001"
        self.file_op_port = "50050"
        self.data_host = None
        self.data_port = None
        self.ipc_path = os.path.join(tempfile.gettempdir(), "hidra")

        self.signal_socket = None
        self.request_socket = None
        self.file_op_socket = None
        self.data_socket = None
        self.control_socket = None

        self.poller = zmq.Poller()

        self.auth = None

        self.targets = None

        self.supported_connections = ["STREAM", "STREAM_METADATA",
                                     "QUERY_NEXT", "QUERY_METADATA",
                                     "NEXUS"]

        self.signal_exchanged = None

        self.stream_started = None
        self.query_next_started = None
        self.nexus_started = None

        self.socket_response_timeout = 1000

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

        if connection_type in self.supported_connections:
            self.connection_type = connection_type
        else:
            raise NotSupported("Chosen type of connection is not supported.")

    # targets: [host, port, prio] or [[host, port, prio], ...]
    def initiate(self, targets):
        if self.connection_type == "NEXUS":
            self.log.info("There is no need for a signal exchange for "
                          "connection type 'NEXUS'")
            return

        if type(targets) != list:
            self.stop()
            raise FormatError("Argument 'targets' must be list.")

        if not self.context:
            self.context = zmq.Context()
            self.ext_context = False

        signal = None
        # Signal exchange
        if self.connection_type == "STREAM":
            signal_port = self.signal_port
            signal = b"START_STREAM"
        elif self.connection_type == "STREAM_METADATA":
            signal_port = self.signal_port
            signal = b"START_STREAM_METADATA"
        elif self.connection_type == "QUERY_NEXT":
            signal_port = self.signal_port
            signal = b"START_QUERY_NEXT"
        elif self.connection_type == "QUERY_METADATA":
            signal_port = self.signal_port
            signal = b"START_QUERY_METADATA"

        self.log.debug("Create socket for signal exchange...")

        if self.signal_host:
            self.__create_signal_socket(signal_port)
        else:
            self.stop()
            raise ConnectionFailed("No host to send signal to specified.")

        self.__set_targets(targets)

        message = self.__send_signal(signal)

        if message and message == b"VERSION_CONFLICT":
            self.stop()
            raise VersionError("Versions are conflicting.")

        elif message and message == b"NO_VALID_HOST":
            self.stop()
            raise AuthenticationFailed("Host is not allowed to connect.")

        elif message and message == b"CONNECTION_ALREADY_OPEN":
            self.stop()
            raise CommunicationFailed("Connection is already open.")

        elif message and message == b"NO_VALID_SIGNAL":
            self.stop()
            raise CommunicationFailed("Connection type is not supported for "
                                      "this kind of sender.")

        # if there was no response or the response was of the wrong format,
        # the receiver should be shut down
        elif message and message.startswith(signal):
            self.log.info("Received confirmation ...")
            self.signal_exchanged = signal

        else:
            raise CommunicationFailed("Sending start signal ...failed.")

    def __create_signal_socket(self, signal_port):

        # To send a notification that a Displayer is up and running, a
        # communication socket is needed
        # create socket to exchange signals with Sender
        self.signal_socket = self.context.socket(zmq.REQ)

        # time to wait for the sender to give a confirmation of the signal
#        self.signal_socket.RCVTIMEO = self.socket_response_timeout
        connection_str = "tcp://{0}:{1}".format(self.signal_host, signal_port)
        try:
            self.signal_socket.connect(connection_str)
            self.log.info("signal_socket started (connect) for '{0}'"
                          .format(connection_str))
        except:
            self.log.error("Failed to start signal_socket (connect): '{0}'"
                           .format(connection_str))
            raise

        # using a Poller to implement the signal_socket timeout (in older
        # ZMQ version there is no option RCVTIMEO)
        self.poller.register(self.signal_socket, zmq.POLLIN)

    def __set_targets(self, targets):
        self.targets = []

        # [host, port, prio]
        if (len(targets) == 3
                and type(targets[0]) != list
                and type(targets[1]) != list
                and type(targets[2]) != list):
            host, port, prio = targets
            self.targets = [["{0}:{1}".format(host, port), prio, [""]]]

        # [host, port, prio, suffixes]
        elif (len(targets) == 4
                and type(targets[0]) != list
                and type(targets[1]) != list
                and type(targets[2]) != list
                and type(targets[3]) == list):
            host, port, prio, suffixes = targets
            self.targets = [["{0}:{1}".format(host, port), prio, suffixes]]

        # [[host, port, prio], ...] or [[host, port, prio, suffixes], ...]
        else:
            for t in targets:
                if type(t) == list and len(t) == 3:
                    host, port, prio = t
                    self.targets.append(
                        ["{0}:{1}".format(host, port), prio, [""]])
                elif type(t) == list and len(t) == 4 and type(t[3]):
                    host, port, prio, suffixes = t
                    self.targets.append(
                        ["{0}:{1}".format(host, port), prio, suffixes])
                else:
                    self.stop()
                    self.log.debug("targets={0}".format(targets))
                    raise FormatError("Argument 'targets' is of wrong format.")

    def __send_signal(self, signal):

        if not signal:
            return

        # Send the signal that the communication infrastructure should be
        # established
        self.log.info("Sending Signal")

        send_message = [__version__,  signal]

        trg = json.dumps(self.targets).encode('utf-8')
        send_message.append(trg)

#        send_message = [__version__, signal, self.data_host, self.data_port]

        self.log.debug("Signal: {0}".format(send_message))
        try:
            self.signal_socket.send_multipart(send_message)
        except:
            self.log.error("Could not send signal")
            raise

        message = None
        try:
            socks = dict(self.poller.poll(self.socket_response_timeout))
        except:
            self.log.error("Could not poll for new message")
            raise

        # if there was a response
        if (self.signal_socket in socks
                and socks[self.signal_socket] == zmq.POLLIN):
            try:
                #  Get the reply.
                message = self.signal_socket.recv()
                self.log.info("Received answer to signal: {0}"
                              .format(message))

            except:
                self.log.error("Could not receive answer to signal")
                raise

        return message

    def start(self, data_socket=False, whitelist=None):

        # Receive data only from whitelisted nodes
        if whitelist:
            if type(whitelist) == list:
                self.auth = ThreadAuthenticator(self.context)
                self.auth.start()
                for host in whitelist:
                    try:
                        if host == "localhost":
                            ip = [socket.gethostbyname(host)]
                        else:
                            hostname, tmp, ip = socket.gethostbyaddr(host)

                        self.log.debug("Allowing host {0} ({1})"
                                       .format(host, ip[0]))
                        self.auth.allow(ip[0])
                    except:
                        self.log.error("Error was: ", exc_info=True)
                        raise AuthenticationFailed(
                            "Could not get IP of host {0}".format(host))
            else:
                raise FormatError("Whitelist has to be a list of IPs")

        socket_id_to_bind = (
            self.stream_started
            or self.query_next_started
            or self.nexus_started
            )

        if socket_id_to_bind:
            self.log.info("Reopening already started connection.")
        else:

            ip = "0.0.0.0"           # TODO use IP of hostname?

            host = ""
            port = ""

            if data_socket:
                self.log.debug("Specified data_socket: {0}".format(data_socket))

                if type(data_socket) == list:
#                    socket_id_to_bind = "{0}:{1}".format(data_socket[0],
#                                                      data_socket[1])
                    host = data_socket[0]
                    ip = socket.gethostbyaddr(host)[2][0]
                    port = data_socket[1]
                else:
                    port = str(data_socket)

                    host = socket.gethostname()
                    socket_id = "{0}:{1}".format(host, port).encode("utf-8")
                    ip_from_host = socket.gethostbyaddr(host)[2]
                    if len(ip_from_host) == 1:
                        ip = ip_from_host[0]

            elif self.targets:
                if len(self.targets) == 1:
                    host, port = self.targets[0][0].split(":")
                    ip_from_host = socket.gethostbyaddr(host)[2]
                    if len(ip_from_host) == 1:
                        ip = ip_from_host[0]

                else:
                    raise FormatError("Multipe possible ports. "
                                      "Please choose which one to use.")
            else:
                    raise FormatError("No target specified.")

            socket_id = "{0}:{1}".format(host, port).encode("utf-8")

            try:
                socket.inet_aton(ip)
                self.log.info("IPv4 address detected: {0}.".format(ip))
                socket_id_to_bind = "{0}:{1}".format(ip, port)
                file_op_con_str = "tcp://{0}:{1}".format(ip, self.file_op_port)
                isIPv6 = False
            except socket.error:
                self.log.info("Address '{0}' is not a IPv4 address, "
                              "asume it is an IPv6 address.".format(ip))
#                socket_id_to_bind = "0.0.0.0:{0}".format(port)
                socket_id_to_bind = "[{0}]:{1}".format(ip, port)
#                file_op_con_str= "tcp://0.0.0.0:{0}".format(self.file_op_port)
                file_op_con_str = "tcp://[{0}]:{1}".format(ip, self.file_op_port)
                isIPv6 = True

            self.log.debug("socket_id_to_bind={0}".format(socket_id_to_bind))
            self.log.debug("file_op_con_str={0}".format(file_op_con_str))

#            socket_id_to_bind = "192.168.178.25:{0}".format(port)

        self.data_socket = self.context.socket(zmq.PULL)
        # An additional socket is needed to establish the data retriving
        # mechanism
        connection_str = "tcp://{0}".format(socket_id_to_bind)

        if whitelist:
            self.data_socket.zap_domain = b'global'

        if isIPv6:
            self.data_socket.ipv6 = True
            self.log.debug("Enabling IPv6 socket")

        try:
            self.data_socket.bind(connection_str)
#            self.data_socket.bind(
#                "tcp://[2003:ce:5bc0:a600:fa16:54ff:fef4:9fc0]:50102")
            self.log.info("Data socket of type {0} started (bind) for '{1}'"
                          .format(self.connection_type, connection_str))
        except:
            self.log.error("Failed to start Socket of type {0} (bind): '{1}'"
                           .format(self.connection_type, connection_str),
                           exc_info=True)
            raise

        self.poller.register(self.data_socket, zmq.POLLIN)

        if self.connection_type in ["QUERY_NEXT", "QUERY_METADATA"]:

            self.request_socket = self.context.socket(zmq.PUSH)
            # An additional socket is needed to establish the data retriving
            # mechanism
            connection_str = "tcp://{0}:{1}".format(self.signal_host,
                                                   self.request_port)
            try:
                self.request_socket.connect(connection_str)
                self.log.info("Request socket started (connect) for '{0}'"
                              .format(connection_str))
            except:
                self.log.error("Failed to start Socket of type {0} (connect):"
                               " '{1}'".format(self.connection_type,
                                               connection_str),
                               exc_info=True)
                raise

            self.query_next_started = socket_id

        elif self.connection_type in ["NEXUS"]:

            # An additional socket is needed to get signals to open and close
            # nexus files
            self.file_op_socket = self.context.socket(zmq.REP)

            if isIPv6:
                self.file_op_socket.ipv6 = True
                self.log.debug("Enabling IPv6 socket for file_op_socket")

            try:
                self.file_op_socket.bind(file_op_con_str)
                self.log.info("File operation socket started (bind) for '{0}'"
                              .format(file_op_con_str))
            except:
                self.log.error("Failed to start Socket of type {0} (bind): "
                               "'{1}'".format(self.connection_type,
                                              file_op_con_str),
                               exc_info=True)

            if not os.path.exists(self.ipc_path):
                os.makedirs(self.ipc_path)

            self.control_socket = self.context.socket(zmq.PULL)
            control_con_str = ("ipc://{0}/{1}_{2}"
                               .format(self.ipc_path,
                                       self.current_pid,
                                       "control_API"))
            try:
                self.control_socket.bind(control_con_str)
                self.log.info("Internal controlling socket started (bind) for"
                              " '{0}'".format(control_con_str))
            except:
                self.log.error("Failed to start internal controlling socket "
                               "(bind): '{0}'".format(control_con_str),
                               exc_info=True)

            self.poller.register(self.file_op_socket, zmq.POLLIN)
            self.poller.register(self.control_socket, zmq.POLLIN)

            self.nexus_started = socket_id
        else:
            self.stream_started = socket_id

    def read(self, callback_params, open_callback, read_callback,
             close_callback):

        if not self.connection_type == "NEXUS" or not self.nexus_started:
            raise UsageError("Wrong connection type (current: {0}) or session"
                             " not started.".format(self.connection_type))

        self.callback_params = callback_params
        self.open_callback = open_callback
        self.read_callback = read_callback
        self.close_callback = close_callback

        run_loop = True

        while run_loop:
            self.log.debug("polling")
            try:
                socks = dict(self.poller.poll())
            except:
                self.log.error("Could not poll for new message")
                raise

            if (self.file_op_socket in socks
                    and socks[self.file_op_socket] == zmq.POLLIN):
                self.log.debug("file_op_socket is polling")

                message = self.file_op_socket.recv_multipart()
                self.log.debug("file_op_socket recv: {0}".format(message))

                if message[0] == b"CLOSE_FILE":
                    if self.all_close_recvd:
                        self.file_op_socket.send_multipart(message)
                        logging.debug("file_op_socket send: {0}"
                                      .format(message))
                        self.all_close_recvd = False

                        self.close_callback(self.callback_params,
                                           message)
                        break
                    else:
                        self.reply_to_signal = message
                elif message[0] == b"OPEN_FILE":
                    self.file_op_socket.send_multipart(message)
                    self.log.debug("file_op_socket send: {0}".format(message))

                    try:
                        self.open_callback(self.callback_params, message[1])
                        self.file_opened = True
                    except:
                        self.file_op_socket.send_multipart([b"ERROR"])
                        self.log.error("Not supported message received")
#                    return message
                else:
                    self.file_op_socket.send_multipart([b"ERROR"])
                    self.log.error("Not supported message received")

            if (self.data_socket in socks
                    and socks[self.data_socket] == zmq.POLLIN):
                self.log.debug("data_socket is polling")

                try:
                    multipart_message = self.data_socket.recv_multipart()
#                    self.log.debug("multipart_message={0}"
#                                    .format(multipart_message[:100]))
                except:
                    self.log.error("Could not receive data due to unknown "
                                   "error.", exc_info=True)

                if multipart_message[0] == b"ALIVE_TEST":
                    continue

                if len(multipart_message) < 2:
                    self.log.error("Received mutipart-message is too short. "
                                   "Either config or file content is missing.")
#                    self.log.debug("multipart_message={0}"
#                                   .format(multipart_message[:100]))
                    # TODO return errorcode

                try:
                    run_loop = self.__react_on_message(multipart_message)
                except KeyboardInterrupt:
                    self.log.debug("Keyboard interrupt detected. "
                                   "Stopping to receive.")
                    raise
                except:
                    self.log.error("Unknown error while receiving files. "
                                   "Need to abort.", exc_info=True)
#                    raise Exception("Unknown error while receiving files. "
#                                   "Need to abort.")

            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):
                self.log.debug("control_socket is polling")
                self.control_socket.recv()
#                self.log.debug("Control signal received. Stopping.")
                raise Exception("Control signal received. Stopping.")

    def __react_on_message(self, multipart_message):

        if multipart_message[0] == b"CLOSE_FILE":
            try:
#                filename = multipart_message[1]
                id = multipart_message[2]
            except:
                self.log.error("Could not extract id from the "
                               "multipart-message", exc_info=True)
                self.log.debug("multipart-message: {0}"
                                .format(multipart_message),
                                exc_info=True)
                raise

            self.recvd_close_from.append(id)
            self.log.debug("Received close-file signal from "
                           "DataDispatcher-{0}".format(id))

            # get number of signals to wait for
            if not self.number_of_streams:
                self.number_of_streams = int(id.split("/")[1])

            # have all signals arrived?
            self.log.debug("self.recvd_close_from={0}, self.number_of_streams={1}"
                           .format(self.recvd_close_from, self.number_of_streams))
            if len(self.recvd_close_from) == self.number_of_streams:
                self.log.info("All close-file-signals arrived")
                if self.reply_to_signal:
                    self.file_op_socket.send_multipart(self.reply_to_signal)
                    self.log.debug("file_op_socket send: {0}"
                                   .format(self.reply_to_signal))
                    self.reply_to_signal = False
                    self.recvd_close_from = []

                    self.close_callback(self.callback_params, multipart_message)
                    return False
                else:
                    self.all_close_recvd = True

            else:
                self.log.info("self.recvd_close_from={0}, "
                              "self.number_of_streams={1}"
                              .format(self.recvd_close_from,
                                      self.number_of_streams))

        else:
            #extract multipart message
            try:
#                self.log.debug("multipart_message={0}".format(multipart_message))
                metadata = json.loads(multipart_message[0].decode("utf-8"))
            except:
                #json.dumps of None results in 'null'
                if multipart_message[0] != 'null':
                    self.log.error("Could not extract metadata from the "
                                   "multipart-message.", exc_info=True)
                    self.log.debug("multipartmessage[0] = {0}"
                                   .format(multipart_message[0]),
                                   exc_info=True)
                metadata = None

            # TODO validate multipart_message
            # (like correct dict-values for metadata)

            try:
                payload = multipart_message[1]
            except:
                self.log.warning("An empty file was received within the "
                                 "multipart-message", exc_info=True)
                payload = None

            self.read_callback(self.callback_params, [metadata, payload])

        return True

    def get(self, timeout=None):
        """
         Receives or queries for new files depending on the connection
         initialized

         returns either
            the newest file
                (if connection type "QUERY_NEXT" or "STREAM" was choosen)
            the path of the newest file
                (if connection type "QUERY_METADATA" or "STREAM_METADATA" was
                 choosen)

        """

        if not self.stream_started and not self.query_next_started:
            self.log.info("Could not communicate, no connection was "
                          "initialized.")
            return None, None

        if self.query_next_started:

            send_message = [b"NEXT", self.query_next_started]
            try:
                self.request_socket.send_multipart(send_message)
            except Exception:
                self.log.error("Could not send request to request_socket",
                               exc_info=True)
                return None, None

        while True:
            # receive data
            if timeout:
                try:
                    socks = dict(self.poller.poll(timeout))
                except:
                    self.log.error("Could not poll for new message")
                    raise
            else:
                try:
                    socks = dict(self.poller.poll())
                except:
                    self.log.error("Could not poll for new message")
                    raise

            # if there was a response
            if (self.data_socket in socks
                    and socks[self.data_socket] == zmq.POLLIN):

                try:
                    multipart_message = self.data_socket.recv_multipart()
                except:
                    self.log.error("Receiving data..failed.", exc_info=True)
                    return [None, None]

                if multipart_message[0] == b"ALIVE_TEST":
                    continue
                elif len(multipart_message) < 2:
                    self.log.error("Received mutipart-message is too short. "
                                   "Either config or file content is missing.")
                    self.log.debug("multipart_message={0}"
                                   .format(multipart_message[:100]))
                    return [None, None]

                # extract multipart message
                try:
                    metadata = json.loads(multipart_message[0].decode("utf-8"))
                except:
                    self.log.error("Could not extract metadata from the "
                                   "multipart-message.", exc_info=True)
                    metadata = None

                # TODO validate multipart_message
                # (like correct dict-values for metadata)

                try:
                    payload = multipart_message[1]
                except:
                    self.log.warning("An empty file was received within the "
                                     "multipart-message", exc_info=True)
                    payload = None

                return [metadata, payload]
            else:
#                self.log.warning("Could not receive data in the given time.")

                if self.query_next_started:
                    try:
                        self.request_socket.send_multipart(
                            [b"CANCEL", self.query_next_started])
                    except Exception:
                        self.log.error("Could not cancel the next query",
                                       exc_info=True)

                return [None, None]

    def store(self, target_base_path):

        # save all chunks to file
        while True:

            try:
                [payload_metadata, payload] = self.get()
            except KeyboardInterrupt:
                raise
            except:
                self.log.error("Getting data failed.", exc_info=True)
                raise

            if payload_metadata and payload:

                #generate target filepath
                target_filepath = self.generate_target_filepath(
                    target_base_path, payload_metadata)
                self.log.debug("New chunk for file {0} received."
                               .format(target_filepath))

                #append payload to file
                #TODO: save message to file using a thread (avoids blocking)
                try:
                    self.file_descriptors[target_filepath].write(payload)
                # File was not open
                except KeyError:
                    try:
                        self.file_descriptors[target_filepath] = (
                            open(target_filepath, "wb")
                            )
                        self.file_descriptors[target_filepath].write(payload)
                    except IOError as e:
                        # errno.ENOENT == "No such file or directory"
                        if e.errno == errno.ENOENT:
                            try:
                                # TODO do not create commissioning, current
                                # and local
                                target_path = self.__generate_target_path(
                                    target_base_path, payload_metadata)
                                os.makedirs(target_path)

                                self.file_descriptors[target_filepath] = (
                                    open(target_filepath, "wb")
                                    )
                                self.log.info("New target directory created: "
                                              "{0}".format(target_path))
                                self.file_descriptors[target_filepath].write(
                                    payload)
                            except:
                                self.log.error("Unable to save payload to "
                                               "file: '{0}'"
                                               .format(target_filepath),
                                               exc_info=True)
                                self.log.debug("target_path:{0}"
                                               .format(target_path))
                        else:
                            self.log.error("Failed to append payload to file:"
                                           " '{0}'".format(target_filepath),
                                           exc_info=True)
                except KeyboardInterrupt:
                    self.log.info("KeyboardInterrupt detected. Unable to "
                                  "append multipart-content to file.")
                    break
                except:
                    self.log.error("Failed to append payload to file: '{0}'"
                                   .format(target_filepath), exc_info=True)

                if len(payload) < payload_metadata["chunksize"]:
                    #indicated end of file. Leave loop
                    filename = self.generate_target_filepath(target_base_path,
                                                             payload_metadata)
                    file_mod_time = payload_metadata["file_mod_time"]

                    self.file_descriptors[target_filepath].close()
                    del self.file_descriptors[target_filepath]

                    self.log.info("New file with modification time {0} "
                                  "received and saved: {1}"
                                  .format(file_mod_time, filename))
                    break

    def generate_target_filepath(self, base_path, config_dict):
        """
        Generates full path where target file will saved to.

        """
        if not config_dict:
            return None

        filename = config_dict["filename"]
        # TODO This is due to Windows path names, check if there has do be
        # done anything additionally to work
        # e.g. check source_path if it's a windows path
        relative_path = config_dict["relative_path"].replace('\\', os.sep)

        if relative_path is '' or relative_path is None:
            target_path = base_path
        else:
            target_path = os.path.normpath(os.path.join(base_path, relative_path))

        filepath = os.path.join(target_path, filename)

        return filepath

    def __generate_target_path(self, base_path, config_dict):
        """
        generates path where target file will saved to.

        """
        # TODO This is due to Windows path names, check if there has do be
        # done anything additionally to work
        # e.g. check source_path if it's a windows path
        relative_path = config_dict["relative_path"].replace('\\', os.sep)

        # if the relative path starts with a slash path.join will consider it
        # as absolute path
        if relative_path.startswith("/"):
            relative_path = relative_path[1:]

        target_path = os.path.join(base_path, relative_path)

        return target_path

    def stop(self):
        """
        Send signal that the displayer is quitting, close ZMQ connections,
        destoying context

        """
        if self.signal_socket and self.signal_exchanged:
            self.log.info("Sending close signal")
            signal = None
            if self.stream_started or (b"STREAM" in self.signal_exchanged):
                signal = b"STOP_STREAM"
            elif self.query_next_started or (b"QUERY" in self.signal_exchanged):
                signal = b"STOP_QUERY_NEXT"

            self.__send_signal(signal)
            # TODO need to check correctness of signal?
#            message = self.__send_signal(signal)

            self.stream_started = None
            self.query_next_started = None

        try:
            if self.signal_socket:
                self.log.info("closing signal_socket...")
                self.signal_socket.close(linger=0)
                self.signal_socket = None
            if self.data_socket:
                self.log.info("closing data_socket...")
                self.data_socket.close(linger=0)
                self.data_socket = None
            if self.request_socket:
                self.log.info("closing request_socket...")
                self.request_socket.close(linger=0)
                self.request_socket = None
            if self.file_op_socket:
                self.log.info("closing file_op_socket...")
                self.file_op_socket.close(linger=0)
                self.file_op_socket = None
            if self.control_socket:
                self.log.info("closing control_socket...")
                self.control_socket.close(linger=0)
                self.control_socket = None

                control_con_str = ("{0}/{1}_{2}"
                                 .format(self.ipc_path,
                                         self.current_pid,
                                         "control_API"))
                try:
                    os.remove(control_con_str)
                    self.log.debug("Removed ipc socket: {0}"
                                   .format(control_con_str))
                except OSError:
                    self.log.warning("Could not remove ipc socket: {0}"
                                     .format(control_con_str))
                except:
                    self.log.warning("Could not remove ipc socket: {0}"
                                     .format(control_con_str), exc_info=True)
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        if self.auth:
            try:
                self.auth.stop()
                self.auth = None
                self.log.info("Stopping authentication thread...done.")
            except:
                self.log.error("Stopping authentication thread...done.",
                               exc_info=True)

        for target in self.file_descriptors:
            self.file_descriptors[target].close()
            self.log.warning("Not all chunks were received for file {0}"
                             .format(target))

        if self.file_descriptors:
            self.file_descriptors = dict()

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.ext_context and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)

    def force_stop(self, targets):

        if type(targets) != list:
            self.stop()
            raise FormatError("Argument 'targets' must be list.")

        if not self.context:
            self.context = zmq.Context()
            self.ext_context = False

        signal = None
        # Signal exchange
        if self.connection_type == "STREAM":
            signal_port = self.signal_port
            signal = b"STOP_STREAM"
        elif self.connection_type == "STREAM_METADATA":
            signal_port = self.signal_port
            signal = b"STOP_STREAM_METADATA"
        elif self.connection_type == "QUERY_NEXT":
            signal_port = self.signal_port
            signal = b"STOP_QUERY_NEXT"
        elif self.connection_type == "QUERY_METADATA":
            signal_port = self.signal_port
            signal = b"STOP_QUERY_METADATA"

        self.log.debug("Create socket for signal exchange...")

        if self.signal_host and not self.signal_socket:
            self.__create_signal_socket(signal_port)
        elif not self.signal_host:
            self.stop()
            raise ConnectionFailed("No host to send signal to specified.")

        self.__set_targets(targets)

        message = self.__send_signal(signal)

        if message and message == b"VERSION_CONFLICT":
            self.stop()
            raise VersionError("Versions are conflicting.")

        elif message and message == b"NO_VALID_HOST":
            self.stop()
            raise AuthenticationFailed("Host is not allowed to connect.")

        elif message and message == b"CONNECTION_ALREADY_OPEN":
            self.stop()
            raise CommunicationFailed("Connection is already open.")

        elif message and message == b"NO_VALID_SIGNAL":
            self.stop()
            raise CommunicationFailed("Connection type is not supported for "
                                      "this kind of sender.")

        # if there was no response or the response was of the wrong format,
        # the receiver should be shut down
        elif message and message.startswith(signal):
            self.log.info("Received confirmation ...")

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
