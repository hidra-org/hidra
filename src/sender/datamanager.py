#!/usr/bin/env python

from __future__ import unicode_literals

import argparse
import zmq
import zmq.devices
import os
import sys
import logging
import json
import time
from multiprocessing import Process, freeze_support, Queue
import threading
import signal
import setproctitle
import tempfile
import socket

from signalhandler import SignalHandler
from taskprovider import TaskProvider
from datadispatcher import DataDispatcher

from __init__ import BASE_PATH
from logutils.queue import QueueHandler
import utils
from _version import __version__

CONFIG_PATH = os.path.join(BASE_PATH, "conf")

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def argument_parsing():
    base_config_file = os.path.join(CONFIG_PATH, "base_sender.conf")
    default_config_file = os.path.join(CONFIG_PATH, "datamanager.conf")

    supported_ed_types = ["inotifyx_events",
                          "watchdog_events",
                          "zmq_events",
                          "http_events",
                          "hidra_events"]

    supported_df_types = ["file_fetcher",
                          "zmq_fetcher",
                          "http_fetcher",
                          "hidra_fetcher"]

    ##################################
    #   Get command line arguments   #
    ##################################

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

    parser.add_argument("--event_detector_type",
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
                             "(only needed if event_detector_type is "
                             "inotifyx_events)")

    parser.add_argument("--time_till_closed",
                        type=float,
                        help="Time (in seconds) since last modification after "
                             "which a file will be seen as closed (only "
                             "needed if event_detector_type is "
                             "inotifyx_events (for clean up) or "
                             "watchdog_events)")

    parser.add_argument("--event_det_port",
                        type=str,
                        help="ZMQ port to get events from (only needed if "
                             "event_detector_type is zmq_events)")

    parser.add_argument("--det_ip",
                        type=str,
                        help="IP of the detector (only needed if "
                             "event_detector_type is http_events)")
    parser.add_argument("--det_api_version",
                        type=str,
                        help="API version of the detector (only needed "
                             "if event_detector_type is http_events)")

    # DataFetcher config

    parser.add_argument("--data_fetcher_type",
                        type=str,
                        help="Module with methods specifying how to get the "
                             "data)")
    parser.add_argument("--data_fetcher_port",
                        type=str,
                        help="If 'zmq_fetcher' is specified as "
                             "data_fetcher_type it needs a port to listen to)")

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
                             "local_target (needed if data_fetcher_type is "
                             "file_fetcher or http_fetcher)",
                        choices=["True", "False"])
    parser.add_argument("--remove_data",
                        help="Flag describing if the files should be removed "
                             "from the source (needed if data_fetcher_type is "
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

    ##################################################
    # Get arguments from config file and comand line #
    ##################################################

    params = utils.set_parameters(base_config_file=base_config_file,
                                  config_file=arguments.config_file,
                                  arguments=arguments)

    ##################################
    #     Check given arguments      #
    ##################################

    required_params = ["log_path",
                       "log_name",
                       "procname",
                       "ext_ip",
                       "event_detector_type",
                       "data_fetcher_type",
                       "store_data",
                       "use_data_stream",
                       "chunksize"]
#                       "local_target"]

    # Check format of config
    check_passed, _ = utils.check_config(required_params,
                                         params,
                                         logging)

    if not check_passed:
        logging.error("Wrong configuration")
        sys.exit(1)

    # check if logfile is writable
    params["log_file"] = os.path.join(params["log_path"], params["log_name"])
    utils.check_writable(params["log_file"])

    # check if the event_detector_type is supported
    utils.check_type(params["event_detector_type"],
                     supported_ed_types,
                     "Event detector")

    # check if the data_fetcher_type is supported
    utils.check_type(params["data_fetcher_type"],
                     supported_df_types,
                     "Data fetcher")

    # check if directories exist
    utils.check_existance(params["log_path"])
    if "monitored_dir" in params:
        # get rid of formating errors
        params["monitored_dir"] = os.path.normpath(params["monitored_dir"])

        utils.check_existance(params["monitored_dir"])
        if "create_fix_subdirs" in params and params["create_fix_subdirs"]:
            # create the subdirectories which do not exist already
            utils.create_sub_dirs(params["monitored_dir"],
                                  params["fix_subdirs"])
        else:
            # the subdirs have to exist because handles can only be added to
            # directories inside a directory in which a handle was already set,
            # e.g. handlers set to current/raw, local:
            # - all subdirs created are detected + handlers are set
            # - new directory on the same as monitored dir
            #   (e.g. current/scratch_bl) cannot be detected
            utils.check_all_sub_dir_exist(params["monitored_dir"],
                                          params["fix_subdirs"])
    if params["store_data"]:
        utils.check_existance(params["local_target"])
        # check if local_target contains fixed_subdirs
        # e.g. local_target = /beamline/p01/current/raw and
        #      fix_subdirs contain current/raw
#        if not utils.check_sub_dir_contained(params["local_target"],
#                                             params["fix_subdirs"]):
#            # not in Eiger mode
#            utils.check_all_sub_dir_exist(params["local_target"],
#                                          params["fix_subdirs"])

    if params["use_data_stream"]:
        utils.check_ping(params["data_stream_targets"][0][0])

    return params


class DataManager():
    def __init__(self, log_queue=None):
        self.device = None
        self.control_pub_socket = None
        self.test_socket = None
        self.context = None
        self.ext_log_queue = True
        self.log = None
        self.log_queue_listener = None

        self.localhost = "127.0.0.1"

        self.current_pid = os.getpid()

        self.reestablish_time = 600  # in sec

        try:
            self.params = argument_parsing()
        except:
            self.log = logging
            self.ipc_path = os.path.join(tempfile.gettempdir(), "hidra")
            raise

        if log_queue:
            self.log_queue = log_queue
            self.ext_log_queue = True
        else:
            self.ext_log_queue = False

            # Get queue
            self.log_queue = Queue(-1)

            # Get the log Configuration for the lisener
            if self.params["onscreen"]:
                h1, h2 = utils.get_log_handlers(self.params["log_file"],
                                                self.params["log_size"],
                                                self.params["verbose"],
                                                self.params["onscreen"])

                # Start queue listener using the stream handler above.
                self.log_queue_listener = utils.CustomQueueListener(
                    self.log_queue, h1, h2)
            else:
                h1 = utils.get_log_handlers(self.params["log_file"],
                                            self.params["log_size"],
                                            self.params["verbose"],
                                            self.params["onscreen"])

                # Start queue listener using the stream handler above
                self.log_queue_listener = (
                    utils.CustomQueueListener(self.log_queue, h1))

            self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = utils.get_logger("DataManager", self.log_queue)

        self.ipc_path = os.path.join(tempfile.gettempdir(), "hidra")
        self.log.info("Configured ipc_path: {}".format(self.ipc_path))

        # Make ipc_path accessible for modules
        self.params["ipc_path"] = self.ipc_path

        # set process name
        check_passed, _ = utils.check_config(["procname"],
                                             self.params,
                                             self.log)
        if not check_passed:
            raise Exception("Configuration check failed")
        setproctitle.setproctitle(self.params["procname"])
        self.log.info("Running as {}".format(self.params["procname"]))

        self.log.info("DataManager started (PID {})."
                      .format(self.current_pid))

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        if not os.path.exists(self.ipc_path):
            os.mkdir(self.ipc_path)
            # the permission have to changed explicitly because
            # on some platform they are ignored when called within mkdir
            os.chmod(self.ipc_path, 0o777)
            self.log.info("Creating directory for IPC communication: {}"
                          .format(self.ipc_path))

        # Enable specification via IP and DNS name
        # TODO make this IPv6 compatible
        if self.params["ext_ip"] == "0.0.0.0":
            self.ext_ip = self.params["ext_ip"]
        else:
            self.ext_ip = socket.gethostbyaddr(self.params["ext_ip"])[2][0]

        self.use_cleaner = (self.params["remove_data"] == "with_confirmation")

        # Make ipc_path accessible for modules
        self.params["ext_ip"] = self.ext_ip
        self.params["ipc_path"] = self.ipc_path
        self.params["main_pid"] = self.current_pid
        # TODO: this should not be set here (it belong to the moduls)
        self.params["context"] = None
        self.params["session"] = None

        if self.use_cleaner:
            self.params["cleaner_conf_con_str"] = (
                "tcp://{}:{}".format(self.params["data_stream_targets"][0][0],
                                     self.params["confirmation_port"])
            )
        else:
            self.params["cleaner_conf_con_str"] = None

        self.com_port = self.params["com_port"]
        self.request_port = self.params["request_port"]

        self.com_con_str = ("tcp://{}:{}"
                            .format(self.ext_ip, self.params["com_port"]))
        self.request_con_str = ("tcp://{}:{}"
                                .format(self.ext_ip,
                                        self.params["request_port"]))

        if utils.is_windows():
            self.log.info("Using tcp for internal communication.")
            self.control_pub_con_str = (
                "tcp://{}:{}".format(self.localhost,
                                     self.params["control_pub_port"]))
            self.control_sub_con_str = (
                "tcp://{}:{}".format(self.localhost,
                                     self.params["control_sub_port"]))
            self.request_fw_con_str = (
                "tcp://{}:{}".format(self.localhost,
                                     self.params["request_fw_port"]))
            self.router_con_str = (
                "tcp://{}:{}".format(self.localhost,
                                     self.params["router_port"]))
            if self.use_cleaner:
                self.params["cleaner_job_con_str"] = (
                    "tcp://{}:{}".format(self.localhost,
                                         self.params["cleaner_port"]))
                self.params["cleaner_tigger_con_str"] = (
                    "tcp://{}:{}".format(self.localhost,
                                         self.params["cleaner_trigger_port"]))
            else:
                self.params["cleaner_job_con_str"] = None
                self.params["cleaner_trigger_con_str"] = None

        else:
            self.log.info("Using ipc for internal communication.")
            self.control_pub_con_str = ("ipc://{}/{}_{}"
                                        .format(self.ipc_path,
                                                self.current_pid,
                                                "controlPub"))
            self.control_sub_con_str = ("ipc://{}/{}_{}"
                                        .format(self.ipc_path,
                                                self.current_pid,
                                                "controlSub"))
            self.request_fw_con_str = ("ipc://{}/{}_{}"
                                       .format(self.ipc_path,
                                               self.current_pid,
                                               "requestFw"))
            self.router_con_str = ("ipc://{}/{}_{}"
                                   .format(self.ipc_path,
                                           self.current_pid,
                                           "router"))
            if self.use_cleaner:
                self.params["cleaner_job_con_str"] = (
                    "ipc://{}/{}_{}".format(self.ipc_path,
                                            self.current_pid,
                                            "cleaner"))
                self.params["cleaner_trigger_con_str"] = (
                    "ipc://{}/{}_{}".format(self.ipc_path,
                                            self.current_pid,
                                            "cleaner_trigger"))
            else:
                self.params["cleaner_job_con_str"] = None
                self.params["cleaner_trigger_con_str"] = None

        self.whitelist = self.params["whitelist"]
        self.ldapuri = self.params["ldapuri"]

        self.use_data_stream = self.params["use_data_stream"]
        self.log.info("Usage of data stream set to '{}'"
                      .format(self.use_data_stream))

        if self.use_data_stream:
            if len(self.params["data_stream_targets"]) > 1:
                self.log.error("Targets to send data stream to have more than "
                               "one entry which is not supported")
                self.log.debug("data_stream_targets: {}"
                               .format(self.params["data_stream_targets"]))
                sys.exit(1)

            self.fixed_stream_id = (
                "{}:{}".format(self.params["data_stream_targets"][0][0],
                               self.params["data_stream_targets"][0][1]))

            if self.params["remove_data"] == "stop_on_error":
                self.status_check_id = (
                    "{}:{}".format(self.params["data_stream_targets"][0][0],
                                   self.params["status_check_port"]))

                self.log.info("Enabled receiver checking")
                self.check_target_host = self.check_status_receiver
            else:
                self.status_check_id = None

                self.log.info("Enabled alive test")
                self.check_target_host = self.test_fixed_streaming_host

        else:
            self.check_target_host = lambda enable_logging=False: True
            self.fixed_stream_id = None
            self.status_check_id = None

        self.number_of_streams = self.params["number_of_streams"]
        self.chunksize = self.params["chunksize"]

        try:
            self.local_target = self.params["local_target"]
            self.log.info("Configured local_target: {}"
                          .format(self.local_target))
        except KeyError:
            self.params["local_target"] = None
            self.local_target = None

        self.signalhandler_pr = None
        self.taskprovider_pr = None
        self.cleaner_pr = None
        self.datadispatcher_pr = []

        self.log.info("Version: {}".format(__version__))

        # IP and DNS name should be both in the whitelist
        self.whitelist = utils.extend_whitelist(self.whitelist,
                                                self.ldapuri,
                                                self.log)

        # Create zmq context
        # there should be only one context in one process
#        self.context = zmq.Context.instance()
        self.context = zmq.Context()
        self.log.debug("Registering global ZMQ context")

        self.zmq_again_occured = 0
        self.socket_reconnected = False

        try:
            if self.check_target_host(enable_logging=True):
                self.create_sockets()

                self.run()
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping due to unknown error condition",
                           exc_info=True)
        finally:
            self.stop()

    def create_sockets(self):

        # initiate forwarder for control signals (multiple pub, multiple sub)
        try:
            self.device = zmq.devices.ThreadDevice(zmq.FORWARDER,
                                                   zmq.SUB,
                                                   zmq.PUB)
            self.device.bind_in(self.control_pub_con_str)
            self.device.bind_out(self.control_sub_con_str)
            self.device.setsockopt_in(zmq.SUBSCRIBE, b"")
            self.device.start()
            self.log.info("Start thead device forwarding messages "
                          "from '{}' to '{}'"
                          .format(self.control_pub_con_str,
                                  self.control_sub_con_str))
        except:
            self.log.error("Failed to start thead device forwarding messages "
                           "from '{}' to '{}'"
                           .format(self.control_pub_con_str,
                                   self.control_sub_con_str), exc_info=True)
            raise

        # socket for control signals
        try:
            self.control_pub_socket = self.context.socket(zmq.PUB)
            self.control_pub_socket.connect(self.control_pub_con_str)
            self.log.info("Start control_pub_socket (connect): '{}'"
                          .format(self.control_pub_con_str))
        except:
            self.log.error("Failed to start control_pub_socket (connect): "
                           "'{0}'".format(self.control_pub_con_str),
                           exc_info=True)
            raise

    def check_status_receiver(self, enable_logging=False):

        if self.use_data_stream:

            test_signal = b"STATUS_CHECK"

            if self.test_socket is None:
                # Establish the test socket as REQ/REP to an extra signal
                # socket
                try:
                    self.test_socket = self.context.socket(zmq.REQ)
                    con_str = "tcp://{}".format(self.status_check_id)

                    self.test_socket.connect(con_str)
                    self.log.info("Start test_socket (connect): '{}'"
                                  .format(con_str))
                except:
                    self.log.error("Failed to start test_socket "
                                   "(connect): '{}'".format(con_str),
                                   exc_info=True)
                    return False

            try:
                if enable_logging:
                    self.log.debug("ZMQ version used: {}"
                                   .format(zmq.__version__))

                # With older ZMQ versions the tracker results in an ZMQError in
                # the DataDispatchers when an event is processed
                # (ZMQError: Address already in use)
                if zmq.__version__ <= "14.5.0":

                    self.test_socket.send_multipart([test_signal])
                    if enable_logging:
                        self.log.info("Sending status check to fixed streaming"
                                      " host {} ... success"
                                      .format(self.status_check_id))

                    status = self.test_socket.recv_multipart()
                    if enable_logging:
                        self.log.info("Received responce for status check of "
                                      "fixed streaming host {}"
                                      .format(self.status_check_id))
                else:
                    self.socket_reconnected = False

                    # after unsuccessfully sending a test messages try to
                    # reestablish the connection (this should not be done in
                    # every test iteration because of overhead but only once in
                    # a while)
                    if self.zmq_again_occured >= self.reestablish_time:
                        # close the socket
                        self.test_socket.close()
                        # reopen it
                        try:
                            self.test_socket = self.context.socket(zmq.REQ)
                            con_str = "tcp://{}".format(self.status_check_id)

                            self.test_socket.connect(con_str)
                            self.log.info("Restart test_socket (connect): "
                                          "'{}'".format(con_str))
                            self.socket_reconnected = True
                        except:
                            self.log.error("Failed to restart test_socket "
                                           "(connect): '{}'".format(con_str),
                                           exc_info=True)

                        self.zmq_again_occured = 0

                    # send test message
                    try:
                        tracker = self.test_socket.send_multipart(
                            [test_signal],
                            zmq.NOBLOCK,
                            copy=False,
                            track=True
                        )

                        if enable_logging:
                            self.log.info("Sent status check to fixed "
                                          "streaming host {}"
                                          .format(self.status_check_id))

                    # The receiver may have dropped authentication or
                    # previous status check was not answered
                    # (req send after req, without rep inbetween)
                    except (zmq.Again, zmq.error.ZMQError):
                        # returns a tuple (type, value, traceback)
                        exc_type, exc_value, _ = sys.exc_info()

                        if self.zmq_again_occured == 0:
                            self.log.error("Failed to send test message to "
                                           "fixed streaming host {}"
                                           .format(self.status_check_id))
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
                        self.log.error("Failed check status of fixed"
                                       "streaming host {}"
                                       .format(self.status_check_id),
                                       exc_info=True)
                        return False

                    # test message was successfully sent
                    if enable_logging:
                        self.log.info("Sending status test to fixed "
                                      "streaming host {} ... success"
                                      .format(self.status_check_id))
                        self.zmq_again_occured = 0

                        self.log.debug("Receiving responce...")

                    status = self.test_socket.recv_multipart()

                    if enable_logging:
                        self.log.debug("Received responce: {}".format(status))

                    # responce to test message was successfully received
                    # TODO check status + react
                    if status[0] == b"ERROR":
                        self.log.error("Fixed streaming host is in error "
                                       "status: {}"
                                       .format(status[1].decode("utf-8")))
                        return False
                    elif enable_logging:
                        self.log.info("Responce for status check of fixed "
                                      "streaming host {}: {}"
                                      .format(self.status_check_id, status))

            except KeyboardInterrupt:
                raise
            except:
                self.log.error("Failed to check status of fixed "
                               "streaming host {}"
                               .format(self.status_check_id), exc_info=True)
                return False
        return True

    def test_fixed_streaming_host(self, enable_logging=False):
        if self.use_data_stream:

            test_signal = b"ALIVE_TEST"

            if self.test_socket is None:
                # Establish the test socket as PUSH/PULL sending test signals
                # to the normal data stream id
                try:
                    self.test_socket = self.context.socket(zmq.PUSH)
                    con_str = "tcp://{}".format(self.fixed_stream_id)

                    self.test_socket.connect(con_str)
                    self.log.info("Start test_socket (connect): '{}'"
                                  .format(con_str))
                except:
                    self.log.error("Failed to start test_socket "
                                   "(connect): '{}'".format(con_str),
                                   exc_info=True)
                    return False

            try:
                if enable_logging:
                    self.log.debug("ZMQ version used: {}"
                                   .format(zmq.__version__))

                # With older ZMQ versions the tracker results in an ZMQError in
                # the DataDispatchers when an event is processed
                # (ZMQError: Address already in use)
                if zmq.__version__ <= "14.5.0":

                    self.test_socket.send_multipart([test_signal])
                    if enable_logging:
                        self.log.info("Sending test message to fixed streaming"
                                      " host {} ... success"
                                      .format(self.fixed_stream_id))

                else:
                    self.socket_reconnected = False

                    # after unsuccessfully sending a test messages try to
                    # reestablish the connection (this should not be done in
                    # every test iteration because of overhead but only once in
                    # a while)
                    if self.zmq_again_occured >= self.reestablish_time:
                        # close the socket
                        self.test_socket.close()
                        # reopen it
                        try:
                            self.test_socket = self.context.socket(zmq.PUSH)
                            con_str = "tcp://{}".format(self.fixed_stream_id)

                            self.test_socket.connect(con_str)
                            self.log.info("Restart test_socket (connect): "
                                          "'{}'".format(con_str))
                            self.socket_reconnected = True
                        except:
                            self.log.error("Failed to restart test_socket "
                                           "(connect): '{}'".format(con_str),
                                           exc_info=True)

                        self.zmq_again_occured = 0

                    # send test message
                    try:
                        tracker = self.test_socket.send_multipart(
                            [test_signal], zmq.NOBLOCK,
                            copy=False, track=True)

                    # The receiver may have dropped authentication
                    except zmq.Again:
                        _, exc_value, _ = sys.exc_info()
                        if self.zmq_again_occured == 0:
                            self.log.error("Failed to send test message to "
                                           "fixed streaming host {}"
                                           .format(self.fixed_stream_id))
                            self.log.debug("Error was: zmq.Again: {}"
                                           .format(exc_value))
                        self.zmq_again_occured += 1
                        self.socket_reconnected = False
                        return False

                    # test if someone picks up the test message in the next
                    # 2 sec
                    if not tracker.done:
                        tracker.wait(2)

                    # no one picked up the test message
                    if not tracker.done:
                        self.log.error("Failed to send test message to fixed "
                                       "streaming host {}"
                                       .format(self.fixed_stream_id),
                                       exc_info=True)
                        return False

                    # test was successful
                    elif enable_logging:
                        self.log.info("Sending test message to fixed "
                                      "streaming host {} ... success"
                                      .format(self.fixed_stream_id))
                        self.zmq_again_occured = 0

            except KeyboardInterrupt:
                raise
            except:
                self.log.error("Failed to send test message to fixed "
                               "streaming host {}"
                               .format(self.fixed_stream_id), exc_info=True)
                return False
        return True

    def run(self):
        # SignalHandler
        self.signalhandler_pr = threading.Thread(target=SignalHandler,
                                                 args=(
                                                     self.params,
                                                     self.control_pub_con_str,
                                                     self.control_sub_con_str,
                                                     self.whitelist,
                                                     self.ldapuri,
                                                     self.com_con_str,
                                                     self.request_fw_con_str,
                                                     self.request_con_str,
                                                     self.log_queue,
                                                     self.context)
                                                 )
        self.signalhandler_pr.start()

        # needed, because otherwise the requests for the first files are not
        # forwarded properly
        time.sleep(0.5)

        if not self.signalhandler_pr.is_alive():
            return

        # TaskProvider
        self.taskprovider_pr = Process(target=TaskProvider,
                                       args=(
                                           self.params,
                                           self.control_sub_con_str,
                                           self.request_fw_con_str,
                                           self.router_con_str,
                                           self.log_queue)
                                       )
        self.taskprovider_pr.start()

        # Cleaner
        if self.use_cleaner:
            self.log.info("Loading cleaner from data fetcher module: {}"
                          .format(self.params["data_fetcher_type"]))
            self.cleaner_m = __import__(self.params["data_fetcher_type"])

            self.cleaner_pr = Process(
                target=self.cleaner_m.Cleaner,
                args=(self.params,
                      self.log_queue,
                      self.params["cleaner_job_con_str"],
                      self.params["cleaner_trigger_con_str"],
                      self.params["cleaner_conf_con_str"],
                      self.control_sub_con_str))
            self.cleaner_pr.start()

        self.log.info("Configured Type of data fetcher: {}"
                      .format(self.params["data_fetcher_type"]))

        # DataDispatcher
        for i in range(self.number_of_streams):
            id = b"{}/{}".format(i, self.number_of_streams)
            pr = Process(target=DataDispatcher,
                         args=(
                             id,
                             self.control_sub_con_str,
                             self.router_con_str,
                             self.chunksize,
                             self.fixed_stream_id,
                             self.params,
                             self.log_queue,
                             self.local_target)
                         )
            pr.start()
            self.datadispatcher_pr.append(pr)

        # indicates if the processed are sent to waiting mode
        sleep_was_sent = False

        if self.use_cleaner:
            run_loop = (self.signalhandler_pr.is_alive()
                        and self.taskprovider_pr.is_alive()
                        and self.cleaner_pr.is_alive()
                        and all(datadispatcher.is_alive()
                                for datadispatcher in self.datadispatcher_pr))
        else:
            run_loop = (self.signalhandler_pr.is_alive()
                        and self.taskprovider_pr.is_alive()
                        and all(datadispatcher.is_alive()
                                for datadispatcher in self.datadispatcher_pr))

        while run_loop:

            if self.check_target_host():
                if sleep_was_sent:
                    if self.socket_reconnected:
                        self.log.info("Sending 'WAKEUP' signal")
                        self.control_pub_socket.send_multipart([b"control",
                                                                b"WAKEUP",
                                                                b"RECONNECT"])
                        sleep_was_sent = False
                    else:
                        self.log.info("Sending 'WAKEUP' signal")
                        self.control_pub_socket.send_multipart([b"control",
                                                                b"WAKEUP"])
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

            if self.use_cleaner:
                run_loop = (self.signalhandler_pr.is_alive()
                            and self.taskprovider_pr.is_alive()
                            and self.cleaner_pr.is_alive()
                            and all(datadispatcher.is_alive()
                                    for datadispatcher
                                    in self.datadispatcher_pr))
            else:
                run_loop = (self.signalhandler_pr.is_alive()
                            and self.taskprovider_pr.is_alive()
                            and all(datadispatcher.is_alive()
                                    for datadispatcher
                                    in self.datadispatcher_pr))

        # notify which subprocess terminated
        if not self.signalhandler_pr.is_alive():
            self.log.info("SignalHandler terminated.")
        if not self.taskprovider_pr.is_alive():
            self.log.info("TaskProvider terminated.")
        if self.use_cleaner and not self.cleaner_pr.is_alive():
            self.log.info("Cleaner terminated.")
        if not any(datadispatcher.is_alive()
                   for datadispatcher in self.datadispatcher_pr):
            self.log.info("One DataDispatcher terminated.")

    def stop(self):

        if self.log is None:
            self.log = logging

        if self.control_pub_socket:
            self.log.info("Sending 'Exit' signal")
            self.control_pub_socket.send_multipart([b"control", b"EXIT"])

        # waiting till the other processes are finished
        time.sleep(0.5)

        if self.control_pub_socket:
            self.log.info("Closing control_pub_socket")
            self.control_pub_socket.close(0)
            self.control_pub_socket = None

        if self.test_socket:
            self.log.debug("Stopping test_socket")
            self.test_socket.close(0)
            self.test_socket = None

        if self.context:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

        control_pub_path = ("{}/{}_{}"
                            .format(self.ipc_path,
                                    self.current_pid,
                                    "controlPub"))
        control_sub_path = ("{}/{}_{}"
                            .format(self.ipc_path,
                                    self.current_pid,
                                    "controlSub"))

        # Clean up ipc communication files
        try:
            os.remove(control_pub_path)
            self.log.debug("Removed ipc socket: {}".format(control_pub_path))
        except OSError:
            self.log.debug("Could not remove ipc socket: {}"
                           .format(control_pub_path))
        except:
            self.log.warning("Could not remove ipc socket: {}"
                             .format(control_pub_path), exc_info=True)

        try:
            os.remove(control_sub_path)
            self.log.debug("Removed ipc socket: {}".format(control_sub_path))
        except OSError:
            self.log.debug("Could not remove ipc socket: {}"
                           .format(control_sub_path))
        except:
            self.log.warning("Could not remove ipc socket: {}"
                             .format(control_sub_path), exc_info=True)

        # Remove temp directory (if empty)
        try:
            os.rmdir(self.ipc_path)
            self.log.debug("Removed IPC direcory: {}".format(self.ipc_path))
        except OSError:
            self.log.debug("Could not remove IPC directory: {}"
                           .format(self.ipc_path))
        except:
            self.log.warning("Could not remove IPC directory: {}"
                             .format(self.ipc_path), exc_info=True)

        if not self.ext_log_queue and self.log_queue_listener:
            self.log.info("Stopping log_queue")
            self.log_queue.put_nowait(None)
            self.log_queue_listener.stop()
            self.log_queue_listener = None

    def signal_term_handler(self, signal, frame):
        self.log.debug('got SIGTERM')
        self.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class TestReceiverStream():
    def __init__(self, com_port, fixed_recv_port, receiving_port,
                 receiving_port2, log_queue):

        self.log = utils.get_logger("TestReceiverStream", log_queue)

        context = zmq.Context.instance()

        self.com_socket = context.socket(zmq.REQ)
        connection_str = "tcp://localhost:{}".format(com_port)
        self.com_socket.connect(connection_str)
        self.log.info("=== com_socket connected to {}".format(connection_str))

        self.fixedRecvSocket = context.socket(zmq.PULL)
        connection_str = "tcp://0.0.0.0:{}".format(fixed_recv_port)
        self.fixedRecvSocket.bind(connection_str)
        self.log.info("=== fixedRecvSocket connected to {}"
                      .format(connection_str))

        self.receiving_socket = context.socket(zmq.PULL)
        connection_str = "tcp://0.0.0.0:{}".format(receiving_port)
        self.receiving_socket.bind(connection_str)
        self.log.info("=== receiving_socket connected to {}"
                      .format(connection_str))

        self.receiving_socket2 = context.socket(zmq.PULL)
        connection_str = "tcp://0.0.0.0:{}".format(receiving_port2)
        self.receiving_socket2.bind(connection_str)
        self.log.info("=== receiving_socket2 connected to {}"
                      .format(connection_str))

        self.send_signal("START_STREAM", receiving_port, 1)
        self.send_signal("START_STREAM", receiving_port2, 0)

        self.run()

    def send_signal(self, signal, ports, prio=None):
        self.log.info("=== send_signal : {}, {}".format(signal, ports))
        send_message = [__version__, signal]
        targets = []
        if type(ports) == list:
            for port in ports:
                targets.append(["localhost:{}".format(port), prio])
        else:
            targets.append(["localhost:{}".format(ports), prio])

        targets = json.dumps(targets).encode("utf-8")
        send_message.append(targets)
        self.com_socket.send_multipart(send_message)
        received_message = self.com_socket.recv()
        self.log.info("=== Responce : {}".format(received_message))

    def run(self):
        try:
            while True:
                recv_message = self.fixedRecvSocket.recv_multipart()
                self.log.info("=== received fixed: {}"
                              .format(json.loads(recv_message[0])))
                recv_message = self.receiving_socket.recv_multipart()
                self.log.info("=== received: {}"
                              .format(json.loads(recv_message[0])))
                recv_message = self.receiving_socket2.recv_multipart()
                self.log.info("=== received 2: {}"
                              .format(json.loads(recv_message[0])))
        except KeyboardInterrupt:
            pass

    def __exit__(self):
        self.receiving_socket.close(0)
        self.receiving_socket2.close(0)
        self.context.destroy()


if __name__ == '__main__':
    # see https://docs.python.org/2/library/multiprocessing.html#windows
    freeze_support()

    test = False

    if test:
        from shutil import copyfile

        logfile = os.path.join(BASE_PATH, "logs", "datamanager_test.log")
        logsize = 10485760

        log_queue = Queue(-1)

        # Get the log Configuration for the lisener
        h1, h2 = utils.get_log_handlers(logfile,
                                        logsize,
                                        verbose=True,
                                        onscreen_log_level="debug")

        # Start queue listener using the stream handler above
        log_queue_listener = utils.CustomQueueListener(log_queue, h1, h2)
        log_queue_listener.start()

        # Create log and set handler to queue handle
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)  # Log level = DEBUG
        qh = QueueHandler(log_queue)
        root.addHandler(qh)

        com_port = "50000"
        fixed_recv_port = "50100"
        receiving_port = "50101"
        receiving_port2 = "50102"

        testPr = Process(target=TestReceiverStream,
                         args=(com_port,
                               fixed_recv_port,
                               receiving_port,
                               receiving_port2,
                               log_queue))
        testPr.start()
        logging.debug("test receiver started")

        source_file = os.path.join(BASE_PATH, "test_file.cbf")
        target_file_base = os.path.join(BASE_PATH,
                                        "data",
                                        "source",
                                        "local",
                                        "raw") + os.sep

        try:
            sender = DataManager(log_queue)
        except:
            sender = None

        if sender:
            time.sleep(0.5)
            i = 100
            try:
                while i <= 105:
                    target_file = "{}{}.cbf".format(target_file_base, i)
                    logging.debug("copy to {}".format(target_file))
                    copyfile(source_file, target_file)
                    i += 1

                    time.sleep(1)
            except Exception as e:
                logging.error("Exception detected: {}".format(e),
                              exc_info=True)
            finally:
                time.sleep(3)
                testPr.terminate()

                for number in range(100, i):
                    target_file = "{}{}.cbf".format(target_file_base, number)
                    try:
                        os.remove(target_file)
                        logging.debug("remove {}".format(target_file))
                    except:
                        pass

                sender.stop()
                log_queue.put_nowait(None)
                log_queue_listener.stop()

    else:
        sender = None
        try:
            sender = DataManager()
        finally:
            if sender is not None:
                sender.stop()
