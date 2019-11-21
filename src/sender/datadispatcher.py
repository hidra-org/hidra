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
This module implements the data dispatcher.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

from importlib import import_module
import json
import os
import signal
import threading
import time
import zmq

from base_class import Base
import hidra.utils as utils
from hidra import __version__

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataHandler(Base, threading.Thread):
    """
    Reads the data using the configured module type and send it to the targets.
    """
    def __init__(self,
                 dispatcher_id,
                 endpoints,
                 fixed_stream_addr,
                 config,
                 log_queue,
                 context,
                 stop_request):

        super().__init__()

        self.dispatcher_id = dispatcher_id
        self.endpoints = endpoints
        self.fixed_stream_addr = fixed_stream_addr
        self.config_all = config
        self.stop_request = stop_request
        self.context = context
        self.config = self.config_all["general"]
        self.config_df = self.config_all["datafetcher"]
        self.log_queue = log_queue
        self.lock = threading.Lock()

        self.log = None
        self.datafetcher = None
        self.keep_running = True
        self.open_connections = None
        self.control_signal = None

        self.poller = None
        self.control_socket = None
        self.router_socket = None

    def _setup(self):
        """Initializes parameters and creates sockets.
        """

        log_name = "DataHandler-{}".format(self.dispatcher_id)
        self.log = utils.get_logger(log_name, self.log_queue)

        # dict with information of all open sockets to which a data stream is
        # opened (host, port,...)
        self.open_connections = dict()

        self.log.info("Loading data fetcher: %s", self.config_df["type"])
        datafetcher_m = import_module(self.config_df["type"])

        datafetcher_base_config = {
            "config": self.config_all,
            "log_queue": self.log_queue,
            "fetcher_id": self.dispatcher_id,
            "context": self.context,
            "lock": self.lock,
            "stop_request": self.stop_request,
            "check_dep": True
        }

        self.datafetcher = datafetcher_m.DataFetcher(datafetcher_base_config)

        try:
            self.create_sockets()
        except Exception:
            self.log.error("Cannot create sockets", ext_info=True)
            self.stop()

    def create_sockets(self):
        """Create ZMQ sockets.
        """

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
        """Sets control signal attribute.

        Args:
            message: The value to set the attribute to.
        """

        self.lock.acquire()
        try:
            self.control_signal = message
            self.datafetcher.control_signal = message
        finally:
            self.lock.release()

    def run(self):
        """Setting up and running while catching all run errors.
        """

        try:
            self._setup()
        except Exception:
            # to make sure that all sockets are closed
            self.stop()
            self.cleanup()
            raise

        try:
            self._run()
        finally:
            self.stop()
            self.cleanup()

    def _run(self):
        """Reacting on jobs
        """

        fixed_stream_addr = [self.fixed_stream_addr, 0, "data"]

        while self.keep_running:
            self.log.debug("Waiting for new job")
            try:
                socks = dict(self.poller.poll())
            except zmq.ZMQError:
                # when stop is called without a control signal
                # -> suppress error message
                self.log.error("Error when polling", exc_info=True)
                break

            # ----------------------------------------------------------------
            # messages from TaskProvider
            # ----------------------------------------------------------------
            if (self.router_socket in socks
                    and socks[self.router_socket] == zmq.POLLIN):

                try:
                    message = self.router_socket.recv_multipart()
                    self.log.debug("New job received")
                    self.log.debug("message = %s", message)
                except Exception:
                    self.log.error("Waiting for new job...failed",
                                   exc_info=True)
                    continue

                if len(message) >= 2:

                    metadata = json.loads(message[0].decode("utf-8"))
                    targets = json.loads(message[1].decode("utf-8"))

                    if self.fixed_stream_addr:
                        targets.insert(0, fixed_stream_addr)
                        self.log.debug("Added fixed_stream_addr %s to targets "
                                       "%s", fixed_stream_addr, targets)

                    # sort the target list by the priority
                    targets = sorted(targets, key=lambda target: target[1])

                else:
                    metadata = json.loads(message[0].decode("utf-8"))

                    if (isinstance(metadata, list)
                            and metadata[0] == b"CLOSE_FILE"):

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
                        metadata.append(self.dispatcher_id.encode("ascii"))

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
                except Exception:
                    self.log.error("Building of metadata dictionary failed "
                                   "for metadata: %s", metadata,
                                   exc_info=True)
                    # skip all further instructions and
                    # continue with next iteration
                    continue

                # send data
                try:
                    self.datafetcher.send_data(targets, metadata,
                                               self.open_connections)
                except Exception:
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

                # the exit signal should become effective
                if self.check_control_signal():
                    break

    def _react_to_wakeup_signal(self, message):
        """Overwrite the base class reaction method to wakeup signal.
        """

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

    def _react_to_exit_signal(self):
        """Overwrite the base class reaction method to exit signal.

        Reaction to exit signal from control socket.
        """
        self.log.debug("Requested to shut down.")
        self.keep_running = False

    def _react_to_close_sockets_signal(self, message):
        """Overwrite the base class reaction method to close_socket signal.

        Closing socket specified.

        Args:
            message: JSON decoded message of the form:
                     <socket id>, <prio>, <suffix>
        """

        targets = json.loads(message[1].decode("utf-8"))
        try:
            for socket_id, _, _ in targets:
                if socket_id in self.open_connections:
                    self.stop_socket(name="socket{}".format(socket_id),
                                     socket=self.open_connections[socket_id])
                    del self.open_connections[socket_id]

        except Exception:
            self.log.error("Request for closing sockets of wrong format",
                           exc_info=True)

    def stop(self):
        """Stopping, closing sockets and clean up.
        """
        self.keep_running = False
        self.stop_request.set()

    def cleanup(self):
        """Closing sockets and clean up.
        """
        self.cleanup_base()

        if self.datafetcher is not None:
            self.datafetcher.stop()
            self.datafetcher = None

        self.stop_socket(name="router_socket")
        self.stop_socket(name="control_socket")

        for connection in self.open_connections:
            self.stop_socket(
                name=connection,
                socket=self.open_connections[connection]
            )
        self.open_connections = {}

        # context is destroyed in outer process.


class DataDispatcher(Base):
    """ Starts a data handling thread while listening to control signals.
    """

    def __init__(self,
                 dispatcher_id,
                 endpoints,
                 fixed_stream_addr,
                 config,
                 log_queue):

        super().__init__()

        self.dispatcher_id = dispatcher_id
        self.endpoints = endpoints
        self.fixed_stream_addr = fixed_stream_addr
        self.config = config
        self.log_queue = log_queue

        self.context = None
        self.poller = None
        self.control_socket = None
        self.datahandler = None
        self.stopped = None
        self.stop_request = threading.Event()

        self.run()

    def _setup(self):
        """Initializes parameters and creates sockets.
        """

        log_name = "DataDispatcher-{}".format(self.dispatcher_id)
        self.log = utils.get_logger(log_name, self.log_queue)

        signal.signal(signal.SIGTERM, self.signal_term_handler)
        signal.signal(signal.SIGINT, self.signal_term_handler)

        self.log.debug("DataDispatcher-%s started (PID %s).",
                       self.dispatcher_id, os.getpid())

        super().print_config(self.config)

        self.context = zmq.Context()
        if ("context" in self.config
                and not self.config["context"]):
            self.config["context"] = self.context

        try:
            self.create_sockets()
        except Exception:
            self.log.error("Cannot create sockets", ext_info=True)
            self.stop()

        self.datahandler = DataHandler(
            dispatcher_id=self.dispatcher_id,
            endpoints=self.endpoints,
            fixed_stream_addr=self.fixed_stream_addr,
            config=self.config,
            log_queue=self.log_queue,
            context=self.context,
            stop_request=self.stop_request
        )
        self.datahandler.start()

    def create_sockets(self):
        """Create ZMQ sockets.
        """

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
        """Setting up and running while catching all run errors.
        """

        try:
            self._setup()
        except Exception:
            # make sure all sockets are closed
            self.stop()
            raise

        try:
            self.stopped = False
            self._run()
        except zmq.ZMQError:
            self.log.debug("ZMQERROR, pass")
        except KeyboardInterrupt:
            self.log.debug("KEYBOARDINTERRUPT")
        except Exception:
            self.log.error("Stopping DataDispatcher-%s due to unknown "
                           "error condition.", self.dispatcher_id,
                           exc_info=True)
        finally:
            self.set_stop_request()
            # ensure that the stop method always knows that the run method
            # actually stopped.
            self.stopped = True
            self.stop()

    def _run(self):
        """React on control signals and inform DataHandler thread.
        """
        while not self.stop_request.is_set():
            # wake up to check for stopping requests
            socks = dict(self.poller.poll(1000))  # in ms

            # ----------------------------------------------------------------
            # control commands
            # ----------------------------------------------------------------

            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):

                # the exit signal should become effective
                if self.check_control_signal():
                    break

    def _forward_control_signal(self, message):
        """
        Overwrite the base class method and forward the control signal to the
        data handler.
        """
        self.log.debug("Setting control signal for data handler.")
        try:
            self.datahandler.set_control_signal(message)
        except AttributeError:
            # if data handler is not initialized or already stopped
            pass

    def _react_to_sleep_signal(self, message):
        """Overwrite the base class reaction method to sleep signal.
        """

        # Do not react on sleep signals.
        pass

    def _react_to_exit_signal(self):
        """Overwrite the base class reaction method to exit signal.

        Reaction to exit signal from control socket.
        """
        self.log.debug("Requested to shut down.")
        self.set_stop_request()

    def stop(self):
        """Stopping, closing sockets and clean up.
        """
        self.set_stop_request()
        self.wait_for_stopped()

        super().cleanup_base()
        self.stop_socket(name="control_socket")

        if self.datahandler is not None:
            self.datahandler.stop()
            self.log.debug("Waiting for datahandler to join.")
            self.datahandler.join()
            self.log.debug("DataHandler joined.")
            self.datahandler = None

        if self.context is not None:
            self.log.info("Destroying context")
            self.context.destroy()
            self.context = None

    # pylint: disable=unused-argument
    def signal_term_handler(self, signal_to_react, frame):
        """React on external SIGTERM signal.
        """

        signal_type = None
        if signal_to_react == 2:
            signal_type = "SIGINT"
        elif signal_to_react == 15:
            signal_type = "SIGTERM"

        self.log.debug('got %s', signal_type)
        self.set_stop_request()

    def set_stop_request(self):
        """Stop the class and request the data handler to stop as well.
        """
        self.stop_request.set()
