from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import zmq
import os
import time
import json
import signal

from __init__ import BASE_PATH  # noqa F401
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataDispatcher():

    def __init__(self,
                 dispatcher_id,
                 endpoints,
                 chunksize,
                 fixed_stream_addr,
                 config,
                 log_queue,
                 local_target=None,
                 context=None):

        self.dispatcher_id = dispatcher_id
        self.endpoints = endpoints
        self.chunksize = chunksize
        self.fixed_stream_addr = fixed_stream_addr
        self.config = config
        self.log_queue = log_queue
        self.local_target = local_target

        self.current_pid = None

        self.poller = None
        self.control_socket = None
        self.router_socket = None
        self.context = None
        self.ext_context = None
        self.open_connections = None
        self.datafetcher = None
        self.continue_run = None

        self.setup(context)

        try:
            self.run()
        except zmq.ZMQError:
            pass
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping DataDispatcher-{} due to unknown "
                           "error condition.".format(self.dispatcher_id),
                           exc_info=True)
        finally:
            self.stop()

    def setup(self, context):
        self.log = utils.get_logger("DataDispatcher-{}".format(self.dispatcher_id),
                                    self.log_queue)

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.current_pid = os.getpid()
        self.log.debug("DataDispatcher-{} started (PID {})."
                       .format(self.dispatcher_id, self.current_pid))

        formated_config = str(json.dumps(self.config,
                                         sort_keys=True,
                                         indent=4))
        self.log.info("Configuration for data fetcher: {}"
                      .format(formated_config))

        # dict with information of all open sockets to which a data stream is
        # opened (host, port,...)
        self.open_connections = dict()

        if context is not None:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False
            if ("context" in self.config
                    and not self.config["context"]):
                self.config["context"] = self.context

        self.log.info("Loading data fetcher: {}"
                      .format(self.config["data_fetcher_type"]))
        datafetcher_m = __import__(self.config["data_fetcher_type"])

        self.datafetcher = datafetcher_m.DataFetcher(self.config,
                                                     self.log_queue,
                                                     self.dispatcher_id,
                                                     self.context)

        self.continue_run = True

        try:
            self.create_sockets()
        except:
            self.log.error("Cannot create sockets", ext_info=True)
            self.stop()

    def create_sockets(self):

        # socket for control signals
        try:
            self.control_socket = self.context.socket(zmq.SUB)
            self.control_socket.connect(self.endpoints.control_sub_con)
            self.log.info("Start control_socket (connect): '{}'"
                          .format(self.endpoints.control_sub_con))
        except:
            self.log.error("Failed to start control_socket (connect): '{}'"
                           .format(self.endpoints.control_sub_con),
                           exc_info=True)
            raise

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "signal")

        # socket to get new workloads from
        try:
            self.router_socket = self.context.socket(zmq.PULL)
            self.router_socket.connect(self.endpoints.router_con)
            self.log.info("Start router_socket (connect): '{}'"
                          .format(self.endpoints.router_con))
        except:
            self.log.error("Failed to start router_socket (connect): '{}'"
                           .format(self.endpoints.router_con), exc_info=True)
            raise

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)
        self.poller.register(self.router_socket, zmq.POLLIN)

    def run(self):

        fixed_stream_addr = [self.fixed_stream_addr, 0, "data"]

        while self.continue_run:
            self.log.debug("DataDispatcher-{}: waiting for new job"
                           .format(self.dispatcher_id))
            socks = dict(self.poller.poll())

            ######################################
            #     messages from TaskProvider     #
            ######################################
            if (self.router_socket in socks
                    and socks[self.router_socket] == zmq.POLLIN):

                try:
                    message = self.router_socket.recv_multipart()
                    self.log.debug("DataDispatcher-{}: new job received"
                                   .format(self.dispatcher_id))
                    self.log.debug("message = {}".format(message))
                except:
                    self.log.error("DataDispatcher-{}: waiting for new job"
                                   "...failed".format(self.dispatcher_id),
                                   exc_info=True)
                    continue

                if len(message) >= 2:

                    metadata = json.loads(message[0].decode("utf-8"))
                    targets = json.loads(message[1].decode("utf-8"))

                    if self.fixed_stream_addr:
                        targets.insert(0, fixed_stream_addr)
                        self.log.debug("Added fixed_stream_addr {} to targets "
                                       "{}".format(fixed_stream_addr, targets))

                    # sort the target list by the priority
                    targets = sorted(targets, key=lambda target: target[1])

                else:
                    metadata = json.loads(message[0].decode("utf-8"))

                    if type(metadata) == list and metadata[0] == b"CLOSE_FILE":

                        # workaround for error
                        # "TypeError: Frame 0 (u'CLOSE_FILE') does not support
                        # the buffer interface."
                        metadata[0] = b"CLOSE_FILE"
                        for i in range(1, len(metadata)):
                            metadata[i] = (json.dumps(metadata[i])
                                           .encode("utf-8")
                                           )

                        if self.fixed_stream_addr:
                            self.log.debug("Router requested to send signal "
                                           "that file was closed.")
                            metadata.append(self.dispatcher_id)

                            # socket already known
                            if self.fixed_stream_addr in self.open_connections:
                                tracker = (
                                    self.open_connections[self.fixed_stream_addr]
                                    .send_multipart(metadata,
                                                    copy=False,
                                                    track=True)
                                )
                                self.log.info("Sending close file signal to "
                                              "'{}' with priority 0"
                                              .format(self.fixed_stream_addr))
                            else:
                                # open socket
                                socket = self.context.socket(zmq.PUSH)
                                connection_str = "tcp://{}".format(
                                    self.fixed_stream_addr)

                                socket.connect(connection_str)
                                self.log.info("Start socket (connect): '{}'"
                                              .format(connection_str))

                                # register socket
                                self.open_connections[self.fixed_stream_addr] = (
                                    socket
                                )

                                # send data
                                tracker = (
                                    self.open_connections[self.fixed_stream_addr]
                                    .send_multipart(metadata,
                                                    copy=False,
                                                    track=True)
                                )
                                self.log.info("Sending close file signal to "
                                              "'{}' with priority 0"
                                              .format(fixed_stream_addr))

                            # socket not known
                            if not tracker.done:
                                self.log.info("Close file signal has not "
                                              "been sent yet, waiting...")
                                tracker.wait()
                                self.log.info("Close file signal has not "
                                              "been sent yet, waiting...done")

                            time.sleep(2)
                            self.log.debug("Continue after sleeping")
                            continue
                        else:
                            self.log.warning("Router requested to send signal"
                                             "that file was closed, but no "
                                             "target specified")
                            continue

                    elif self.fixed_stream_addr:
                        targets = [fixed_stream_addr]
                        self.log.debug("Added fixed_stream_addr to targets {}."
                                       .format(targets))

                    else:
                        targets = []

                # get metadata and paths of the file
                try:
                    self.log.debug("Getting file paths and metadata")
                    # additional information is stored in the metadata dict
                    self.datafetcher.get_metadata(targets, metadata)

                except KeyboardInterrupt:
                    break
                except:
                    self.log.error("Building of metadata dictionary failed "
                                   "for metadata: {}".format(metadata),
                                   exc_info=True)
                    # skip all further instructions and
                    # continue with next iteration
                    continue

                # send data
                try:
                    self.datafetcher.send_data(targets, metadata,
                                               self.open_connections)
                except:
                    self.log.error("DataDispatcher-{}: Passing new file to "
                                   "data stream...failed".format(self.dispatcher_id),
                                   exc_info=True)

                # finish data handling
                self.datafetcher.finish(targets, metadata,
                                        self.open_connections)

            ######################################
            #         control commands           #
            ######################################
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):
                try:
                    message = self.control_socket.recv_multipart()
                    self.log.debug("DataDispatcher-{}: control signal "
                                   "received".format(self.dispatcher_id))
                    self.log.debug("message = {}".format(message))
                except:
                    self.log.error("DataDispatcher-{}: reiceiving control "
                                   "signal...failed".format(self.dispatcher_id),
                                   exc_info=True)
                    continue

                # remove subsription topic
                del message[0]

                if message[0] == b"EXIT":
                    self.log.debug("Received EXIT signal")
                    self.react_to_exit_signal()
                    break

                elif message[0] == b"CLOSE_SOCKETS":
                    self.react_to_close_sockets_signal(message)
                    continue

                elif message[0] == b"SLEEP":
                    self.log.debug("Router requested DataDispatcher-{} to "
                                   "wait.".format(self.dispatcher_id))
                    break_outer_loop = False

                    # if there are problems on the receiving side no data
                    # should be processed till the problem is solved
                    while True:
                        try:
                            message = self.control_socket.recv_multipart()
                        except KeyboardInterrupt:
                            self.log.error("Receiving control signal..."
                                           "failed due to KeyboardInterrupt")
                            break_outer_loop = True
                            break
                        except:
                            self.log.error("Receiving control signal...failed",
                                           exc_info=True)
                            continue

                        # remove subsription topic
                        del message[0]

                        if message[0] == b"SLEEP":
                            continue

                        elif message[0] == b"WAKEUP":
                            self.log.debug("Received wakeup signal")
                            if len(message) == 2 and message[1] == "RECONNECT":
                                # Reestablish all open data connections
                                for socket_id in self.open_connections:
                                    # close the connection
                                    self.open_connections[socket_id].close(0)
                                    # reopen it
                                    try:
                                        self.open_connections[socket_id] = (
                                            self.context.socket(zmq.PUSH))
                                        (self.open_connections[socket_id]
                                            .connect("tcp://" + socket_id))
                                        self.log.info("Restart connection "
                                                      "(connect): '{}'"
                                                      .format(socket_id))
                                    except:
                                        self.log.error("Failed to restart "
                                                       "connection (connect): "
                                                       "'{}'"
                                                       .format(socket_id),
                                                       exc_info=True)

                            # Wake up from sleeping
                            break

                        elif message[0] == b"EXIT":
                            self.react_to_exit_signal()
                            break_outer_loop = True
                            break

                        elif message[0] == b"CLOSE_SOCKETS":
                            self.react_to_close_sockets_signal(message)
                            continue

                        else:
                            self.log.error("Unhandled control signal received:"
                                           " {}".format(message))

                    # the exit signal should become effective
                    if break_outer_loop:
                        break
                    else:
                        continue

                elif message[0] == b"WAKEUP":
                    self.log.debug("Received wakeup signal without sleeping. "
                                   "Do nothing.")
                    continue

                else:
                    self.log.error("Unhandled control signal received: {}"
                                   .format(message))

    def react_to_exit_signal(self):
        self.log.debug("Router requested to shutdown DataDispatcher-{}."
                       .format(self.dispatcher_id))

    def react_to_close_sockets_signal(self, message):
        targets = json.loads(message[1].decode("utf-8"))
        try:
            for socket_id, prio, suffix in targets:
                if socket_id in self.open_connections:
                    self.log.info("Closing socket {}".format(socket_id))
                    if self.open_connections[socket_id]:
                        self.open_connections[socket_id].close(0)
                    del self.open_connections[socket_id]
        except:
            self.log.error("Request for closing sockets of wrong format",
                           exc_info=True)

    def stop(self):
        self.continue_run = False

        # to prevent the message two be logged multiple times
        if self.continue_run:
            self.log.debug("Closing sockets for DataDispatcher-{}"
                           .format(self.dispatcher_id))

        for connection in self.open_connections:
            if self.open_connections[connection]:
                self.log.info("Closing socket {}".format(connection))
                self.open_connections[connection].close(0)
                self.open_connections[connection] = None

        if self.control_socket is not None:
            self.log.info("Closing control_socket")
            self.control_socket.close(0)
            self.control_socket = None

        if self.router_socket is not None:
            self.log.info("Closing router_socket")
            self.router_socket.close(0)
            self.router_socket = None

        if self.datafetcher is not None:
            self.datafetcher.stop()
            self.datafetcher = None

        if not self.ext_context and self.context is not None:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

    def signal_term_handler(self, signal, frame):
        self.log.debug('got SIGTERM')
        self.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()

# testing was moved into test/unittests/core/test_datadispatcher.py
