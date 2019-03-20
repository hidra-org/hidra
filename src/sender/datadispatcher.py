from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import zmq
import os
import time
import json
import signal
import threading

from base_class import Base
import utils
from _version import __version__

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataHandler(Base, threading.Thread):
    def __init__(self,
                 dispatcher_id,
                 endpoints,
                 chunksize,
                 fixed_stream_addr,
                 config,
                 log_queue,
                 context):

        self.endpoints = endpoints
        self.context = context
        self.config = config
        self.dispatcher_id = dispatcher_id
        self.fixed_stream_addr = fixed_stream_addr
        self.log_queue = log_queue
        self.lock = threading.Lock()

        self.log = None
        self.datafetcher = None
        self.keep_running = True
        self.open_connections = None

        self.poller = None
        self.control_socket = None
        self.router_socket = None
        self.stopped = False

        threading.Thread.__init__(self)

        self._setup()

    def _setup(self):

        log_name = "DataHandler-{}".format(self.dispatcher_id)
        self.log = utils.get_logger(log_name, self.log_queue)

        # dict with information of all open sockets to which a data stream is
        # opened (host, port,...)
        self.open_connections = dict()

        self.log.info("Loading data fetcher: {}"
                      .format(self.config["data_fetcher_type"]))
        datafetcher_m = __import__(self.config["data_fetcher_type"])

        self.datafetcher = datafetcher_m.DataFetcher(self.config,
                                                     self.log_queue,
                                                     self.dispatcher_id,
                                                     self.context,
                                                     self.lock)

        try:
            self.create_sockets()
        except:
            self.log.error("Cannot create sockets", ext_info=True)
            self.stop()

    def create_sockets(self):

        # socket for control signals
        self.control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.control_sub_con
        )

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "signal")

        # socket to get new workloads from
        self.router_socket = self.start_socket(
            name="router_socket",
            sock_type=zmq.PULL,
            sock_con="connect",
            endpoint=self.endpoints.router_con
        )

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)
        self.poller.register(self.router_socket, zmq.POLLIN)

    def set_control_signal(self, message):
        self.lock.acquire()
        try:
            self.control_signal = message
            self.datafetcher.control_signal = message
        finally:
            self.lock.release()

    def run(self):

        fixed_stream_addr = [self.fixed_stream_addr, 0, "data"]
        self.stopped = False

        while self.keep_running:
            self.log.debug("Waiting for new job")
            socks = dict(self.poller.poll())

            # ----------------------------------------------------------------
            # messages from TaskProvider
            # ----------------------------------------------------------------
            if (self.router_socket in socks
                    and socks[self.router_socket] == zmq.POLLIN):

                try:
                    message = self.router_socket.recv_multipart()
                    self.log.debug("New job received")
                    self.log.debug("message = %s", message)
                except:
                    self.log.error("Waiting for new job...failed",
                                   exc_info=True)
                    continue

                if len(message) >= 2:

                    metadata = json.loads(message[0].decode("utf-8"))
                    targets = json.loads(message[1].decode("utf-8"))

                    if self.fixed_stream_addr:
                        targets.insert(0, fixed_stream_addr)
                        self.log.debug("Added fixed_stream_addr %s to targets"
                                       " %s", fixed_stream_addr, targets)

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
                            metadata[i] = (
                                json.dumps(metadata[i]).encode("utf-8")
                            )

                        if not self.fixed_stream_addr:
                            self.log.warning("Router requested to send signal"
                                             "that file was closed, but no "
                                             "target specified")
                            continue

                        self.log.debug("Router requested to send signal that "
                                       "file was closed.")
                        metadata.append(self.dispatcher_id)

                        # socket not known
                        if self.fixed_stream_addr not in self.open_connections:
                            endpt = "tcp://{}".format(self.fixed_stream_addr)

                            # open and register socket
                            self.open_connections[self.fixed_stream_addr] = (
                                self.start_socket(
                                    name="socket",
                                    sock_type=zmq.PUSH,
                                    sock_con="connect",
                                    endpoint=endpt
                                )
                            )

                        # send data
                        sckt = self.open_connections[self.fixed_stream_addr]
                        tracker = sckt.send_multipart(metadata,
                                                      copy=False,
                                                      track=True)
                        self.log.info("Sending close file signal to '%s' with "
                                      "priority 0", fixed_stream_addr)

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

                    elif self.fixed_stream_addr:
                        targets = [fixed_stream_addr]
                        self.log.debug("Added fixed_stream_addr to targets "
                                       "%s.", targets)

                    else:
                        targets = []

                metadata["version"] = __version__

                # get metadata and paths of the file
                try:
                    self.log.debug("Getting file paths and metadata")
                    # additional information is stored in the metadata dict
                    self.datafetcher.get_metadata(targets, metadata)

                except KeyboardInterrupt:
                    break
                except:
                    self.log.error("Building of metadata dictionary failed "
                                   "for metadata: %s", metadata, exc_info=True)
                    # skip all further instructions and
                    # continue with next iteration
                    continue

                # send data
                try:
                    self.datafetcher.send_data(targets, metadata,
                                               self.open_connections)
                except:
                    self.log.error("Passing new file to data stream...failed",
                                   exc_info=True)

                # finish data handling
                try:
                    self.datafetcher.finish(targets, metadata,
                                            self.open_connections)
                except Exception:
                    # datafetcher/datahandler was not stopped in the meantime
                    if self.datafetcher is not None:
                        self.log.error("Finishing file failed.", exc_info=True)

            # ----------------------------------------------------------------
            # control commands
            # ----------------------------------------------------------------
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):

                try:
                    message = self.control_socket.recv_multipart()
                    self.log.debug("Control signal received")
                    self.log.debug("message = %s", message)
                except:
                    self.log.error("Reiceiving control signal...failed",
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
                    self.log.debug("Router requested to wait.")
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
                                    self.stop_socket(
                                        name="connection",
                                        socket=self.open_connections[socket_id]
                                    )

                                    # reopen it
                                    endpoint = "tcp://" + socket_id
                                    self.open_connections[socket_id] = (
                                        self.start_socket(
                                            name="connection",
                                            sock_type=zmq.PUSH,
                                            sock_con="connect",
                                            endpoint=endpoint,
                                            message="Restart"
                                        )
                                    )

                            # Wake up from sleeping
                            break

                        elif message[0] == b"EXIT":
                            self.log.debug("Received exit signal while "
                                           "sleeping.")
                            self.react_to_exit_signal()
                            break_outer_loop = True
                            break

                        elif message[0] == b"CLOSE_SOCKETS":
                            self.react_to_close_sockets_signal(message)
                            continue

                        else:
                            self.log.error("Unhandled control signal received:"
                                           " %s", message)

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
                    self.log.error("Unhandled control signal received: %s",
                                   message)

        self.stopped = True

    def react_to_exit_signal(self):
        self.log.debug("Router requested to shutdown.")
        self.keep_running = False

    def react_to_close_sockets_signal(self, message):
        targets = json.loads(message[1].decode("utf-8"))
        try:
            for socket_id, prio, suffix in targets:
                if socket_id in self.open_connections:
                    self.stop_socket(name="socket{}".format(socket_id),
                                     socket=self.open_connections[socket_id])
                    del self.open_connections[socket_id]
        except:
            self.log.error("Request for closing sockets of wrong format",
                           exc_info=True)

    def stop(self):
        self.keep_running = False

        i = 0
        while not self.stopped:
            # if the socket is closed to early the thread will hang.
            self.log.debug("Waiting for run loop to stop (iter %s)", i)
            time.sleep(0.1)
            i += 1

        self.stop_socket(name="router_socket")
        self.stop_socket(name="control_socket")

        if self.datafetcher is not None:
            self.datafetcher.stop()
            self.datafetcher = None

        for connection in self.open_connections:
            self.stop_socket(
                name=connection,
                socket=self.open_connections[connection]
            )
        self.open_connections = {}

        # context is destroyed in outer process.

    def signal_term_handler(self, signal, frame):
        self.log.debug('got SIGTERM')
        self.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


class DataDispatcher(Base):

    def __init__(self,
                 dispatcher_id,
                 endpoints,
                 chunksize,
                 fixed_stream_addr,
                 config,
                 log_queue):

        self.dispatcher_id = dispatcher_id
        self.endpoints = endpoints
        self.chunksize = chunksize
        self.fixed_stream_addr = fixed_stream_addr
        self.config = config
        self.log_queue = log_queue

        self.poller = None
        self.control_socket = None
        self.context = None
        self.datahandler = None
        self.continue_run = None
        self.stopped = False

        self._setup()

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

    def _setup(self):
        log_name = "DataDispatcher-{}".format(self.dispatcher_id)
        self.log = utils.get_logger(log_name, self.log_queue)

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.log.debug("DataDispatcher-{} started (PID {})."
                       .format(self.dispatcher_id, os.getpid()))

        formated_config = str(json.dumps(self.config,
                                         sort_keys=True,
                                         indent=4))
        self.log.info("Configuration for data dispatcher: {}"
                      .format(formated_config))

        self.context = zmq.Context()
        if ("context" in self.config
                and not self.config["context"]):
            self.config["context"] = self.context

        self.continue_run = True

        try:
            self.create_sockets()
        except:
            self.log.error("Cannot create sockets", ext_info=True)
            self.stop()

        self.datahandler = DataHandler(self.dispatcher_id,
                                       self.endpoints,
                                       self.chunksize,
                                       self.fixed_stream_addr,
                                       self.config,
                                       self.log_queue,
                                       self.context)

        self.datahandler.start()

    def create_sockets(self):

        # socket for control signals
        self.control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.control_sub_con
        )

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "signal")

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)

    def run(self):

        while self.continue_run:
            socks = dict(self.poller.poll())

            # ----------------------------------------------------------------
            # control commands
            # ----------------------------------------------------------------
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):

                try:
                    message = self.control_socket.recv_multipart()
                    self.log.debug("Control signal received")
                    self.log.debug("message = %s", message)
                except:
                    self.log.error("Reiceiving control signal...failed.",
                                   exc_info=True)
                    continue

                # remove subsription topic
                del message[0]

                self.log.debug("Setting control signal for data handler.")
                self.datahandler.set_control_signal(message)

                if message[0] == b"EXIT":
                    self.log.debug("Received EXIT signal")
                    self.log.debug("Router requested to shutdown.")
                    break

                elif message[0] in [b"CLOSE_SOCKETS", b"SLEEP", b"WAKEUP"]:
                    self.log.debug("Received %s signal. Do nothing.",
                                   message[0])
                    continue

                else:
                    self.log.error("Unhandled control signal received: %s",
                                   message)

        self.stopped = True


    def stop(self):
        self.continue_run = False

        # to prevent the message two be logged multiple times
        if self.continue_run:
            self.log.debug("Closing sockets.")

        i = 0
        while not self.stopped:
            # if the socket is closed to early the thread will hang.
            self.log.debug("Waiting for run loop to stop (iter %s)", i)
            time.sleep(0.1)
            i += 1

        self.stop_socket(name="control_socket")

        if self.datahandler is not None:
            self.datahandler.stop()
            self.log.debug("Waiting for datahandler to join.")
            self.datahandler.join()
            self.log.debug("Datahandler joined.")
            self.datahandler = None

        if self.context is not None:
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
