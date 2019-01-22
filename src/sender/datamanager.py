#!/usr/bin/env python

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

# pylint: disable=redefined-variable-type

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
from distutils.version import LooseVersion
import logging
from multiprocessing import Process, freeze_support, Queue
import os
import tempfile
import threading
import time
import signal
import socket
import sys
import zmq
import zmq.devices

import setproctitle

# to make windows freeze work (cx_Freeze 5.x)
try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except NameError:
    CURRENT_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))

if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# pylint: disable=wrong-import-position
from base_class import Base  # noqa E402
# pylint: disable=wrong-import-position
from signalhandler import SignalHandler  # noqa E402
# pylint: disable=wrong-import-position
from taskprovider import TaskProvider  # noqa E402
# pylint: disable=wrong-import-position
from datadispatcher import DataDispatcher  # noqa E402

from _environment import BASE_DIR  # noqa E402
import hidra.utils as utils # noqa E402
from hidra import __version__  # noqa E402

CONFIG_DIR = os.path.join(BASE_DIR, "conf")

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def argument_parsing():
    """Parses and checks the command line arguments used.
    """

    base_config_file = os.path.join(CONFIG_DIR, "base_sender.conf")
    default_config_file = os.path.join(CONFIG_DIR, "datamanager.conf")

    supported_ed_types = ["inotifyx_events",
                          "inotify_events",
                          "watchdog_events",
                          "zmq_events",
                          "http_events",
                          "hidra_events"]

    supported_df_types = ["file_fetcher",
                          "zmq_fetcher",
                          "http_fetcher",
                          "hidra_fetcher"]

    # ------------------------------------------------------------------------
    # Get command line arguments
    # ------------------------------------------------------------------------

    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",
                        type=str,
                        help="Location of the configuration file")

    parser.add_argument("--log_path",
                        type=str,
                        help="Path where the logfile will be created")
    parser.add_argument("--log_name",
                        type=str,
                        help="Filename used for logging")
    parser.add_argument("--log_size",
                        type=int,
                        help="File size before rollover in B (linux only)")
    parser.add_argument("--verbose",
                        help="More verbose output",
                        action="store_true")
    parser.add_argument("--onscreen",
                        type=str,
                        help="Display logging on screen "
                             "(options are CRITICAL, ERROR, WARNING, "
                             "INFO, DEBUG)",
                        default=False)

    parser.add_argument("--procname",
                        type=str,
                        help="Name with which the service should be running")

    parser.add_argument("--ext_ip",
                        type=str,
                        help="IP of the interface to bind to for external "
                             "communication")

    # SignalHandler config

    parser.add_argument("--com_port",
                        type=str,
                        help="Port number to receive signals")
    parser.add_argument("--whitelist",
                        nargs='+',
                        help="List of hosts allowed to connect")

    parser.add_argument("--request_port",
                        type=str,
                        help="ZMQ port to get new requests")
    parser.add_argument("--request_fw_port",
                        type=str,
                        help="ZMQ port to forward requests")
    parser.add_argument("--control_pub_port",
                        type=str,
                        help="Port number to publish control signals")
    parser.add_argument("--control_sub_port",
                        type=str,
                        help="Port number to receive control signals")

    # EventDetector config

    parser.add_argument("--eventdetector_type",
                        type=str,
                        help="Type of event detector to use")
    parser.add_argument("--fix_subdirs",
                        type=str,
                        help="Subdirectories to be monitored and to store the "
                             "data to (only needed if event detector is "
                             "inotifyx_events or watchdog_events "
                             "and data fetcher is file_fetcher)")
    parser.add_argument("--create_fix_subdirs",
                        type=str,
                        help="Flag describing if the subdirectories should be "
                             "created if they do not exist")

    parser.add_argument("--monitored_dir",
                        type=str,
                        help="Directory to be monitor for changes; inside "
                             "this directory only the specified "
                             "subdirectories are monitred (only needed if "
                             "event detector is inotifyx_events or "
                             "watchdog_events)")
    parser.add_argument("--monitored_events",
                        type=str,
                        help="Event type of files (options are: "
                             "IN_CLOSE_WRITE, IN_MOVED_TO, ...) and the "
                             "formats to be monitored, files in an other "
                             "format will be be neglected (needed if "
                             "event detector is inotifyx_events or "
                             "watchdog_events)")

    parser.add_argument("--history_size",
                        type=int,
                        help="Number of events stored to look for doubles "
                             "(needed if event detector is "
                             "inotifyx_events)")

    parser.add_argument("--use_cleanup",
                        help="Flag describing if a clean up thread which "
                             "regularly checks if some files were missed "
                             "should be activated (needed if event detector "
                             "is inotifyx_events)",
                        choices=["True", "False"])

    parser.add_argument("--action_time",
                        type=float,
                        help="Intervall time (in seconds) used for clea nup "
                             "(only needed if eventdetector_type is "
                             "inotifyx_events)")

    parser.add_argument("--time_till_closed",
                        type=float,
                        help="Time (in seconds) since last modification after "
                             "which a file will be seen as closed (only "
                             "needed if eventdetector_type is "
                             "inotifyx_events (for clean up) or "
                             "watchdog_events)")

    parser.add_argument("--event_det_port",
                        type=str,
                        help="ZMQ port to get events from (only needed if "
                             "eventdetector_type is zmq_events)")

    parser.add_argument("--det_ip",
                        type=str,
                        help="IP of the detector (only needed if "
                             "eventdetector_type is http_events)")
    parser.add_argument("--det_api_version",
                        type=str,
                        help="API version of the detector (only needed "
                             "if eventdetector_type is http_events)")

    # DataFetcher config

    parser.add_argument("--datafetcher_type",
                        type=str,
                        help="Module with methods specifying how to get the "
                             "data)")
    parser.add_argument("--datafetcher_port",
                        type=str,
                        help="If 'zmq_fetcher' is specified as "
                             "datafetcher_type it needs a port to listen to)")

    parser.add_argument("--use_data_stream",
                        help="Enable ZMQ pipe into storage system (if set to "
                             "false: the file is moved into the "
                             "local_target)",
                        choices=["True", "False"])
    parser.add_argument("--data_stream_target",
                        type=str,
                        help="Fixed host and port to send the data to with "
                             "highest priority (only active if "
                             "use_data_stream is set)")
    parser.add_argument("--number_of_streams",
                        type=int,
                        help="Number of parallel data streams)")
    parser.add_argument("--chunksize",
                        type=int,
                        help="Chunk size of file-parts getting send via ZMQ)")

    parser.add_argument("--router_port",
                        type=str,
                        help="ZMQ-router port which coordinates the "
                             "load-balancing to the worker-processes)")

    parser.add_argument("--local_target",
                        type=str,
                        help="Target to move the files into")

    parser.add_argument("--store_data",
                        help="Flag describing if the data should be stored in "
                             "local_target (needed if datafetcher_type is "
                             "file_fetcher or http_fetcher)",
                        choices=["True", "False"])
    parser.add_argument("--remove_data",
                        help="Flag describing if the files should be removed "
                             "from the source (needed if datafetcher_type is "
                             "file_fetcher or http_fetcher)",
                        choices=["True",
                                 "False",
                                 "stop_on_error",
                                 "with_confirmation"])

    arguments = parser.parse_args()
    arguments.config_file = arguments.config_file or default_config_file

    # check if config_file exist
    utils.check_existance(base_config_file)
    utils.check_existance(arguments.config_file)

    # ------------------------------------------------------------------------
    # Get arguments from config file and comand line
    # ------------------------------------------------------------------------

    from pprint import pprint

    config = utils.load_config(base_config_file)
    config_detailed = utils.load_config(arguments.config_file)

    # if config and yaml is mixed mapping has to take place before merging them
    config = utils.map_conf_format(config)
    config_detailed = utils.map_conf_format(config_detailed)
    arguments_dict = utils.map_conf_format(arguments, is_namespace=True)

    utils.update_dict(config_detailed, config)
    utils.update_dict(arguments_dict, config)

#    config = utils.set_parameters(base_config_file=base_config_file,
#                                  config_file=arguments.config_file,
#                                  arguments=arguments)

    # ------------------------------------------------------------------------
    # Check given arguments
    # ------------------------------------------------------------------------

    required_params = {
        "general": [
            "log_path",
            "log_name",
            "procname",
            "ext_ip"
        ],
        "eventdetector": [
            "eventdetector_type",
        ],
        "datafetcher": [
            "datafetcher_type",
            "chunksize",
            "use_data_stream",
            "store_data",
#            "local_target"
        ]
    }

    # Check format of config
    check_passed, _ = utils.check_config(required_params,
                                         config,
                                         logging)
    if not check_passed:
        logging.error("Wrong configuration")
        sys.exit(1)


    # for convenience
    config_gen = config["general"]
    config_ed = config["eventdetector"]
    config_df = config["datafetcher"]
    eventdetector_type = config_ed["eventdetector_type"]
    datafetcher_type = config_df["datafetcher_type"]

    # check if logfile is writable
    config_gen["log_file"] = os.path.join(config_gen["log_path"],
                                          config_gen["log_name"])
    utils.check_writable(config_gen["log_file"])

    # check if the eventdetector_type is supported
    utils.check_type(eventdetector_type,
                     supported_ed_types,
                     "Event detector")

    # check if the datafetcher_type is supported
    utils.check_type(datafetcher_type,
                     supported_df_types,
                     "Data fetcher")

    # check if directories exist
    utils.check_existance(config_gen["log_path"])

    # TODO move this into eventdetector directly
    if (eventdetector_type in config_ed
        and "monitored_dir" in config_ed[eventdetector_type]):
        # for convenience
        config_ed_type = config_ed[eventdetector_type]

        # get rid of formating errors
        config_ed_type["monitored_dir"] = os.path.normpath(
            config_ed_type["monitored_dir"]
        )

        utils.check_existance(config_ed_type["monitored_dir"])
        if ("create_fix_subdirs" in config_ed_type
            and config_ed_type["create_fix_subdirs"]):
            # create the subdirectories which do not exist already
            utils.create_sub_dirs(
                dir_path=config_ed_type["monitored_dir"],
                subdirs=config_ed_type["fix_subdirs"],
                dirs_not_to_create=config_ed_type["dirs_not_to_create"]
            )
        else:
            # the subdirs have to exist because handles can only be added to
            # directories inside a directory in which a handle was already set,
            # e.g. handlers set to current/raw, local:
            # - all subdirs created are detected + handlers are set
            # - new directory on the same as monitored dir
            #   (e.g. current/scratch_bl) cannot be detected
            utils.check_all_sub_dir_exist(config_ed_type["monitored_dir"],
                                          config_ed_type["fix_subdirs"])

    if config_df["store_data"]:
        # set process name
        check_passed, _ = utils.check_config(["local_target"],
                                             config_df,
                                             self.log)
        if not check_passed:
            raise Exception("Wrong configuration")

        utils.check_existance(config_df["local_target"])
        # check if local_target contains fixed_subdirs
        # e.g. local_target = /beamline/p01/current/raw and
        #      fix_subdirs contain current/raw
#        if not utils.check_sub_dir_contained(config_df["local_target"],
#                                             config_ed["fix_subdirs"]):
#            # not in Eiger mode
#            utils.check_all_sub_dir_exist(config_df["local_target"],
#                                          config_ed["fix_subdirs"])

    if config_df["use_data_stream"]:

        check_passed, _ = utils.check_config(["data_stream_targets"],
                                             config_df,
                                             self.log)
        if not check_passed:
            raise Exception("Wrong configuration")

        utils.check_ping(config_df["data_stream_targets"][0][0])

    return config


class DataManager(Base):
    """The main class.
    """

    def __init__(self, log_queue=None, config=None):

        super(DataManager, self).__init__()

        self.device = None
        self.control_pub_socket = None
        self.test_socket = None
        self.context = None

        self.log = None
        self.log_queue = None
        self.ext_log_queue = None
        self.log_queue_listener = None

        self.localhost = None
        self.ext_ip = None
        self.con_ip = None
        self.ipc_dir = None
        self.current_pid = None

        self.reestablish_time = None
        self.continue_run = None
        self.config = None
        self.use_cleaner = None

        self.whitelist = None
        self.ldapuri = None

        self.use_data_stream = None
        self.fixed_stream_addr = None
        self.status_check_id = None
        self.check_target_host = None

        self.number_of_streams = None
        self.chunksize = None

        self.endpoints = None

        self.local_target = None

        self.signalhandler_thr = None
        self.taskprovider_pr = None
        self.cleaner_pr = None
        self.datadispatcher_pr = []

        self.cleaner_m = None

        self.zmq_again_occured = None
        self.socket_reconnected = None

        self.context = None

        self.setup(config, log_queue)

#        self.run()

    def setup(self, config, log_queue):
        """Initializes parameters and creates sockets.

        Args:
            config (dict): All the configuration set either via config file or
                           command line parameter.
            log_queue: Logging queue used to synchronize log messages.
        """

        self.localhost = "127.0.0.1"

        self.current_pid = os.getpid()

        self.reestablish_time = 600  # in sec

        self.continue_run = True

        try:
            if config is None:
                self.config = argument_parsing()
            else:
                self.config = config
        except:
            self.log = logging
            self.ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
            raise

        config_gen = self.config["general"]
        config_ed = self.config["eventdetector"]
        config_df = self.config["datafetcher"]

        # change user
        user_info, user_was_changed = utils.change_user(config_gen)

        # set up logging
        if log_queue is not None:
            self.log_queue = log_queue
            self.ext_log_queue = True
        else:
            self.ext_log_queue = False

            # Get queue
            self.log_queue = Queue(-1)

            # Get the log Configuration for the lisener
            if config_gen["onscreen"]:
                handler1, handler2 = utils.get_log_handlers(
                    config_gen["log_file"],
                    config_gen["log_size"],
                    config_gen["verbose"],
                    config_gen["onscreen"]
                )

                # Start queue listener using the stream handler above.
                self.log_queue_listener = utils.CustomQueueListener(
                    self.log_queue, handler1, handler2
                )
            else:
                handler1 = utils.get_log_handlers(
                    config_gen["log_file"],
                    config_gen["log_size"],
                    config_gen["verbose"],
                    config_gen["onscreen"]
                )

                # Start queue listener using the stream handler above
                self.log_queue_listener = utils.CustomQueueListener(
                    self.log_queue, handler1
                )

            self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = utils.get_logger("DataManager", self.log_queue)

        utils.log_user_change(self.log, user_was_changed, user_info)

        self.ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
        self.log.info("Configured ipc_dir: {}".format(self.ipc_dir))

        # set process name
        # pylint: disable=no-member
        setproctitle.setproctitle(config_gen["procname"])
        self.log.info("Running as {}".format(config_gen["procname"]))

        self.log.info("DataManager started (PID {})."
                      .format(self.current_pid))

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        if not os.path.exists(self.ipc_dir):
            os.mkdir(self.ipc_dir)
            # the permission have to changed explicitly because
            # on some platform they are ignored when called within mkdir
            os.chmod(self.ipc_dir, 0o777)
            self.log.info("Creating directory for IPC communication: {}"
                          .format(self.ipc_dir))

        # Enable specification via IP and DNS name
        # TODO make this IPv6 compatible
        if config_gen["ext_ip"] == "0.0.0.0":
            self.ext_ip = config_gen["ext_ip"]
        else:
            self.ext_ip = socket.gethostbyaddr(config_gen["ext_ip"])[2][0]
        self.con_ip = socket.getfqdn()

        self.use_cleaner = (config_df["remove_data"] == "with_confirmation")

        ports = {
            "com": config_gen["com_port"],
            "request": config_gen["request_port"],
            "request_fw": config_gen["request_fw_port"],
            "control_pub": config_gen["control_pub_port"],
            "control_sub": config_gen["control_sub_port"],
            "router": config_df["router_port"],
            "cleaner": config_df["cleaner_port"],
            "cleaner_trigger": config_df["cleaner_trigger_port"],
            "confirmation": config_df["confirmation_port"],
        }

        self.ipc_addresses = utils.set_ipc_addresses(
            ipc_dir=self.ipc_dir,
            main_pid=self.current_pid,
            use_cleaner=self.use_cleaner
        )

        self.use_data_stream = config_df["use_data_stream"]
        self.log.info("Usage of data stream set to '{}'"
                      .format(self.use_data_stream))

        if self.use_data_stream:
            data_stream_target = config_df["data_stream_targets"][0][0]
            confirm_ips = [socket.gethostbyaddr(data_stream_target)[2][0],
                           data_stream_target]
        else:
            confirm_ips = [self.ext_ip, self.con_ip]

        self.endpoints = utils.set_endpoints(ext_ip=self.ext_ip,
                                             con_ip=self.con_ip,
                                             ports=ports,
                                             ipc_addresses=self.ipc_addresses,
                                             confirm_ips=confirm_ips,
                                             use_cleaner=self.use_cleaner)

        if utils.is_windows():
            self.log.info("Using tcp for internal communication.")
        else:
            self.log.info("Using ipc for internal communication.")

        # Make ipc_dir accessible for modules
        self.config["network"] = {
            "ext_ip": self.ext_ip,
            "con_ip": self.con_ip,
            "ipc_dir": self.ipc_dir,
            "main_pid": self.current_pid,
            "endpoints": self.endpoints,
            # TODO: this should not be set here (it belong to the modules)
            "context": None,
            "session": None
        }


        self.whitelist = config_gen["whitelist"]
        self.ldapuri = config_gen["ldapuri"]

        if self.use_data_stream:
            if len(config_df["data_stream_targets"]) > 1:
                self.log.error("Targets to send data stream to have more than "
                               "one entry which is not supported")
                self.log.debug("data_stream_targets: {}"
                               .format(config_df["data_stream_targets"]))
                sys.exit(1)

            self.fixed_stream_addr = (
                "{}:{}".format(config_df["data_stream_targets"][0][0],
                               config_df["data_stream_targets"][0][1]))

            if config_df["remove_data"] == "stop_on_error":
                self.status_check_id = (
                    "{}:{}".format(config_df["data_stream_targets"][0][0],
                                   config_df["status_check_port"]))

                self.log.info("Enabled receiver checking")
                self.check_target_host = self.check_status_receiver
            else:
                self.status_check_id = None

                self.log.info("Enabled alive test")
                self.check_target_host = self.test_fixed_streaming_host

        else:
            self.check_target_host = lambda enable_logging=False: True
            self.fixed_stream_addr = None
            self.status_check_id = None

        self.number_of_streams = config_df["number_of_streams"]
        self.chunksize = config_df["chunksize"]

        try:
            self.local_target = config_df["local_target"]
            self.log.info("Configured local_target: {}"
                          .format(self.local_target))
        except KeyError:
            config_df["local_target"] = None
            self.local_target = None

        self.signalhandler_thr = None
        self.taskprovider_pr = None
        self.cleaner_pr = None
        self.datadispatcher_pr = []

        self.log.info("Version: {}".format(__version__))

        # IP and DNS name should be both in the whitelist
        self.whitelist = utils.extend_whitelist(self.whitelist,
                                                self.ldapuri,
                                                self.log)

        self.zmq_again_occured = 0
        self.socket_reconnected = False

        # Create zmq context
        # there should be only one context in one process
        self.context = zmq.Context()
        self.log.debug("Registering global ZMQ context")

    def create_sockets(self):
        """Create ZMQ sockets.
        """

        # initiate forwarder for control signals (multiple pub, multiple sub)
        try:
            self.device = zmq.devices.ThreadDevice(zmq.FORWARDER,
                                                   zmq.SUB,
                                                   zmq.PUB)
            self.device.bind_in(self.endpoints.control_pub_bind)
            self.device.bind_out(self.endpoints.control_sub_bind)
            self.device.setsockopt_in(zmq.SUBSCRIBE, b"")
            self.device.start()
            self.log.info("Start thead device forwarding messages "
                          "from '{}' to '{}'"
                          .format(self.endpoints.control_pub_bind,
                                  self.endpoints.control_sub_bind))
        except:
            self.log.error("Failed to start thead device forwarding messages "
                           "from '{}' to '{}'"
                           .format(self.endpoints.control_pub_bind,
                                   self.endpoints.control_sub_bind),
                           exc_info=True)
            raise

        # socket for control signals
        self.control_pub_socket = self.start_socket(
            name="control_pub_socket",
            sock_type=zmq.PUB,
            sock_con="connect",
            endpoint=self.endpoints.control_pub_con
        )

    def check_status_receiver(self, enable_logging=False):
        """Communicate to the receiver and checks status.

        Args:
            enable_logging (optional, bool): if log messages should be
                                             generated.
        Returns:
            Boolean depending if the status was ok or not.
        """

        return self.communicate_with_receiver(
            test_signal=b"STATUS_CHECK",
            socket_conf=dict(
                name="test_socket",
                sock_type=zmq.REQ,
                sock_con="connect",
                endpoint="tcp://{}".format(self.status_check_id)
            ),
            addr=self.status_check_id,
            use_log=enable_logging
        )

    def test_fixed_streaming_host(self, enable_logging=False):
        """Comminicates with the receiver and checks if it is alive.

        Args:
            enable_logging (optional, bool): if log messages should be
                                             generated.
        Returns:
            Boolean depending if the receiver is alive or not.
        """

        return self.communicate_with_receiver(
            test_signal=b"ALIVE_TEST",
            socket_conf=dict(
                name="test_socket",
                sock_type=zmq.PUSH,
                sock_con="connect",
                endpoint="tcp://{}".format(self.fixed_stream_addr)
            ),
            addr=self.fixed_stream_addr,
            use_log=enable_logging
        )

    def communicate_with_receiver(self,
                                  test_signal,
                                  socket_conf,
                                  addr,
                                  use_log=False):
        """Communicates to the receiver and checks response.

        Args:
            test_signal (str): The signal to send to the receiver
            socket_conf (dict): The configuration of the socket to use for
                                communication.
            addr: The address of the socket.
            use_log (optional, bool): if log messages should be generated.
        """

        # no data stream used means that no receiver is used
        # -> status always is fine
        if not self.use_data_stream:
            return True

        is_req = socket_conf["sock_type"] == zmq.REQ
        action_name = test_signal.lower()

        # --------------------------------------------------------------------
        # create socket
        # --------------------------------------------------------------------
        if self.test_socket is None:
            # Establish the test socket as REQ/REP to an extra signal
            # socket
            try:
                self.test_socket = self.start_socket(**socket_conf)
            except Exception:
                return False

        if use_log:
            self.log.debug("ZMQ version used: {}".format(zmq.__version__))

        # --------------------------------------------------------------------
        # old zmq version
        # --------------------------------------------------------------------
        # With older ZMQ versions the tracker results in an ZMQError in
        # the DataDispatchers when an event is processed
        # (ZMQError: Address already in use)
        if LooseVersion(zmq.__version__) <= LooseVersion("14.5.0"):
            try:
                self.test_socket.send_multipart([test_signal])
                if use_log:
                    self.log.info("Sending {} to fixed streaming host {}..."
                                  "success".format(action_name, addr))

                if is_req:
                    status = self.test_socket.recv_multipart()
                    if use_log:
                        self.log.info("Received responce for {} of fixed "
                                      "streaming host {}"
                                      .format(action_name, addr))
            except KeyboardInterrupt:
                # nothing to log
                raise
            except Exception:
                self.log.error("Failed to {} of fixed streaming host {}"
                               .format(action_name, addr), exc_info=True)
                return False

        # --------------------------------------------------------------------
        # newer zmq version
        # --------------------------------------------------------------------
        else:
            self.socket_reconnected = False

            try:
                # after unsuccessfully sending a test messages try to
                # reestablish the connection (this should not be done in
                # every test iteration because of overhead but only once in
                # a while)
                if self.zmq_again_occured >= self.reestablish_time:
                    # close the socket
                    self.test_socket.close()

                    # reopen it
                    try:
                        socket_conf["message"] = "Restart"
                        self.test_socket = self.start_socket(**socket_conf)
                    except Exception:
                        # TODO is this right here?
                        pass

                    self.zmq_again_occured = 0

                # send test message
                try:
                    tracker = self.test_socket.send_multipart(
                        [test_signal],
                        zmq.NOBLOCK,
                        copy=False,
                        track=True
                    )

                    if is_req and use_log:
                        self.log.info("Sent {} to fixed streaming host {}"
                                      .format(action_name, addr))

                # The receiver may have dropped authentication or
                # previous status check was not answered
                # (req send after req, without rep inbetween)
                except (zmq.Again, zmq.error.ZMQError):
                    # returns a tuple (type, value, traceback)
                    exc_type, exc_value, _ = sys.exc_info()

                    if self.zmq_again_occured == 0:
                        self.log.error("Failed to send {} to fixed streaming "
                                       "host {}".format(action_name, addr))
                        self.log.debug("Error was: {}: {}"
                                       .format(exc_type, exc_value))

                    self.zmq_again_occured += 1
                    self.socket_reconnected = False

                    return False

                # test if someone picks up the test message in the next
                # 2 sec
                if not tracker.done:
                    tracker.wait(2)

                # no one picked up the test message
                if not tracker.done:
                    self.log.error("Failed to send {} of fixed streaming host "
                                   "{}".format(action_name, addr),
                                   exc_info=True)
                    return False

                # test message was successfully sent
                if use_log:
                    self.log.info("Sending {} to fixed streaming host {}..."
                                  "success".format(action_name, addr))
                    self.zmq_again_occured = 0

                if is_req:
                    if use_log:
                        self.log.debug("Receiving response...")

                    status = self.test_socket.recv_multipart()

                    if use_log:
                        self.log.debug("Received response: {}".format(status))

                    # responce to test message was successfully received
                    # TODO check status + react
                    if status[0] == b"ERROR":
                        self.log.error("Fixed streaming host is in error "
                                       "state: {}"
                                       .format(status[1].decode("utf-8")))
                        return False

                    elif use_log:
                        self.log.info("Responce for {} of fixed streaming "
                                      "host {}: {}"
                                      .format(action_name, addr, status))

            except KeyboardInterrupt:
                # nothing to log
                raise
            except Exception:
                self.log.error("Failed to send {} of fixed streaming host {}"
                               .format(action_name, addr), exc_info=True)
                return False

        return True

    def run(self):
        """Running while reacting to exceptions.
        """

        try:
            if self.check_target_host(enable_logging=True):
                self.create_sockets()

                self.exec_run()
        except KeyboardInterrupt:
            pass
        except Exception:
            self.log.error("Stopping due to unknown error condition",
                           exc_info=True)
        finally:
            self.stop()

    def exec_run(self):
        """Starting all thread and processes and checks if they are running.
        """

        # SignalHandler
        # "bug in pylint pylint: disable=bad-continuation
        self.signalhandler_thr = threading.Thread(target=SignalHandler,
                                                  args=(
                                                      self.config,
                                                      self.endpoints,
                                                      self.whitelist,
                                                      self.ldapuri,
                                                      self.log_queue
                                                      )
                                                  )

        self.signalhandler_thr.start()

        # needed, because otherwise the requests for the first files are not
        # forwarded properly
        time.sleep(0.5)

        if not self.signalhandler_thr.is_alive():
            self.log.error("Signalhandler did not start.")
            return

        # TaskProvider
        # "bug in pylint pylint: disable=bad-continuation
        self.taskprovider_pr = Process(target=TaskProvider,
                                       args=(
                                           self.config,
                                           self.endpoints,
                                           self.log_queue
                                           )
                                       )
        self.taskprovider_pr.start()

        # Cleaner
        if self.use_cleaner:
            self.log.info("Loading cleaner from data fetcher module: {}"
                          .format(self.config["datafetcher"]["datafetcher_type"]))
            self.cleaner_m = __import__(self.config["datafetcher"]["datafetcher_type"])

            self.cleaner_pr = Process(
                target=self.cleaner_m.Cleaner,
                args=(self.config,
                      self.log_queue,
                      self.endpoints)
            )
            self.cleaner_pr.start()

        self.log.info("Configured Type of data fetcher: {}"
                      .format(self.config["datafetcher"]["datafetcher_type"]))

        # DataDispatcher
        for i in range(self.number_of_streams):
            dispatcher_id = b"{}/{}".format(i, self.number_of_streams)
            # "bug in pylint # pylint: disable=bad-continuation
            proc = Process(target=DataDispatcher,
                           args=(
                               dispatcher_id,
                               self.endpoints,
                               self.chunksize,
                               self.fixed_stream_addr,
                               self.config,
                               self.log_queue,
                               self.local_target
                               )
                           )
            proc.start()
            self.datadispatcher_pr.append(proc)

        # indicates if the processed are sent to waiting mode
        sleep_was_sent = False
        run_loop = self.core_parts_status_check()

        while run_loop:

            if self.check_target_host():
                if sleep_was_sent:
                    msg = [b"control", b"WAKEUP"]
                    if self.socket_reconnected:
                        msg += [b"RECONNECT"]

                    self.log.info("Sending 'WAKEUP' signal")
                    self.control_pub_socket.send_multipart(msg)
                    sleep_was_sent = False

            else:
                # Due to an unforseeable event there is no active receiver on
                # the other side. Thus the processes should enter a waiting
                # mode and no data should be send.
                if not sleep_was_sent:
                    self.log.warning("Sending 'SLEEP' signal")
                    self.control_pub_socket.send_multipart([b"control",
                                                            b"SLEEP"])
                    sleep_was_sent = True

            time.sleep(1)

            run_loop = (self.continue_run
                        and self.core_parts_status_check())

        # notify which subprocess terminated
        if not self.continue_run:
            self.log.debug("Stopped run loop.")
        else:
            if not self.signalhandler_thr.is_alive():
                self.log.info("SignalHandler terminated.")
            if not self.taskprovider_pr.is_alive():
                self.log.info("TaskProvider terminated.")
            if self.use_cleaner and not self.cleaner_pr.is_alive():
                self.log.info("Cleaner terminated.")
            if not any(datadispatcher.is_alive()
                       for datadispatcher in self.datadispatcher_pr):
                self.log.info("One DataDispatcher terminated.")

    def core_parts_status_check(self):
        """Check if the core componets still are running.

        Returns:
            A boolean if the components are running or not.
        """

        if self.use_cleaner:
            status = (
                self.signalhandler_thr.is_alive()
                and self.taskprovider_pr.is_alive()
                and self.cleaner_pr.is_alive()
                and all(datadispatcher.is_alive()
                        for datadispatcher
                        in self.datadispatcher_pr)
            )
        else:
            status = (
                self.signalhandler_thr.is_alive()
                and self.taskprovider_pr.is_alive()
                and all(datadispatcher.is_alive()
                        for datadispatcher
                        in self.datadispatcher_pr)
            )
        return status

    def stop(self):
        """Close socket and clean up.
        """

        self.continue_run = False

        if self.log is None:
            self.log = logging

        if self.control_pub_socket is not None:
            self.log.info("Sending 'Exit' signal")
            self.control_pub_socket.send_multipart([b"control", b"EXIT"])

        # closing control fowarding
        if self.device is not None:
            self.log.info("Stopping forwarder device")
#            self.device.context_factory().term()
            self.device.join(0.5)
            self.device = None

        self.stop_socket(name="control_pub_socket")
        self.stop_socket(name="test_socket")

        # cleanup hanging processes
#        if self.signalhandler_thr.is_alive():
#            self.log.info("SignalHandler hangs. Terminated.")
#        if self.taskprovider_pr.is_alive():
#            self.log.info("TaskProvider hangs. Terminated.")
#        if self.use_cleaner and self.cleaner_pr.is_alive():
#            self.log.info("Cleaner hangs. Terminated.")
#        for datadispatcher in self.datadispatcher_pr:
#            if datadispatcher.is_alive():
#                self.log.info("DataDispatcher hangs. Terminated.")

        if self.context is not None:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

        if (self.endpoints is not None
                and self.endpoints.control_pub_bind.startswith("ipc")):

            # Clean up ipc communication files
            for path in self.ipc_addresses:

                if path is None:
                    continue

                try:
                    os.remove(path)
                    self.log.debug("Removed ipc socket: {}".format(path))
                except OSError:
                    pass
                except Exception:
                    self.log.warning("Could not remove ipc socket: {}"
                                     .format(path), exc_info=True)

            # Remove temp directory (if empty)
            try:
                os.rmdir(self.ipc_dir)
                self.log.debug("Removed IPC direcory: {}".format(self.ipc_dir))
            except OSError:
                self.log.debug("Could not remove IPC directory: {}"
                               .format(self.ipc_dir))
            except Exception:
                self.log.warning("Could not remove IPC directory: {}"
                                 .format(self.ipc_dir), exc_info=True)

        if not self.ext_log_queue and self.log_queue_listener:
            self.log.info("Stopping log_queue")
            self.log_queue.put_nowait(None)
            self.log_queue_listener.stop()
            self.log_queue_listener = None

    # pylint: disable=unused-argument
    def signal_term_handler(self, signal_to_react, frame):
        """React on external SIGTERM signal.
        """

        self.log.debug('got SIGTERM')
        self.stop()

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    # see https://docs.python.org/2/library/multiprocessing.html#windows
    freeze_support()

    sender = None  # pylint: disable=invalid-name
    try:
        sender = DataManager()  # pylint: disable=invalid-name
        sender.run()
    finally:
        if sender is not None:
            sender.stop()
