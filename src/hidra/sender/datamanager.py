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

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import argparse
from distutils.version import LooseVersion
from importlib import import_module
import logging
import multiprocessing
import os
import tempfile
import time
import signal
import socket
import sys
import zmq
import zmq.devices

try:
    # python3
    from pathlib import Path
except ImportError:
    # python2
    from pathlib2 import Path

import setproctitle

# to make windows freeze work (cx_Freeze 5.x)
try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except NameError:
    CURRENT_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))

if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# pylint: disable=wrong-import-position
import hidra.utils as utils  # noqa E402
from hidra import __version__  # noqa E402

# pylint: disable=wrong-import-position
from base_class import Base  # noqa E402
from signalhandler import run_signalhandler  # noqa E402
from taskprovider import run_taskprovider  # noqa E402
from datadispatcher import run_datadispatcher  # noqa E402
from statserver import run_statserver  # noqa E402

from _environment import BASE_DIR  # noqa E402 # pylint: disable=unused-import

try:
    import hidra.conf  # pylint: disable=ungrouped-imports
    CONFIG_DIR = hidra.conf.__path__[0]  # pylint: disable=no-member
except ImportError:
    # when using the git repo
    CONFIG_DIR = os.path.join(BASE_DIR, "conf")

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def argument_parsing():
    """ Get command line arguments
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",
                        type=str,
                        help="Location of the configuration file")

    parser.add_argument("--verbose",
                        help="More verbose output",
                        action="store_true")
    parser.add_argument("--onscreen",
                        type=str,
                        help="Display logging on screen "
                             "(options are CRITICAL, ERROR, WARNING, "
                             "INFO, DEBUG)",
                        default=False)

    return parser.parse_args()


def load_config():
    """Parses the command line arguments and loads config files.
    """
    base_config_file = utils.determine_config_file(fname_base="base_sender")
    arguments = argument_parsing()

    if arguments.config_file is None:
        arguments.config_file = utils.determine_config_file(
            fname_base="datamanager"
        ).as_posix()
    else:
        arguments.config_file = Path(arguments.config_file).as_posix()

    # check if config_file exist
    utils.check_existence(base_config_file)
    utils.check_existence(arguments.config_file)

    # ------------------------------------------------------------------------
    # Get arguments from config file and command line
    # ------------------------------------------------------------------------

    config = utils.load_config(base_config_file)
    config_detailed = utils.load_config(arguments.config_file)

    # if config and yaml is mixed mapping has to take place before merging them
    config_type = "sender"
    config = utils.map_conf_format(config, config_type)
    config_detailed = utils.map_conf_format(config_detailed, config_type)
    arguments_dict = utils.map_conf_format(arguments,
                                           config_type,
                                           is_namespace=True)

    utils.update_dict(config_detailed, config)
    utils.update_dict(arguments_dict, config)

    return config


def get_config():
    """Parses and checks the command line arguments used.
    """

    config = load_config()

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
            "type",
        ],
        "datafetcher": [
            "type",
            "chunksize",
            "use_data_stream",
            "store_data",
            # "local_target"
        ]
    }

    # Check format of config
    check_passed, _ = utils.check_config(required_params, config, logging)
    if not check_passed:
        logging.error("Wrong configuration")
        raise utils.WrongConfiguration

    # for convenience
    config_gen = config["general"]
    config_ed = config["eventdetector"]
    config_df = config["datafetcher"]
    config_ed["type_module"] = "eventdetectors." + config_ed["type"]
    config_df["type_module"] = "datafetchers." + config_df["type"]

    # generate log file name
    config_gen["log_file"] = utils.format_log_filename(
        os.path.join(config_gen["log_path"], config_gen["log_name"])
    )

    # check if configured eventdetector and datafetcher modules really exist
    utils.check_module_exist(config_ed["type_module"])
    utils.check_module_exist(config_df["type_module"])

    # check if directories exist
    utils.check_existence(config_gen["log_path"])

    if config_df["store_data"]:
        # set process name
        check_passed, _ = utils.check_config(["local_target"],
                                             config_df,
                                             logging)
        if not check_passed:
            raise utils.WrongConfiguration

        utils.check_existence(config_df["local_target"])
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
                                             logging)
        if not check_passed:
            raise utils.WrongConfiguration

        utils.check_ping(config_df["data_stream_targets"][0][0])

    return config


class CheckReceiver(Base):
    """ Communication with receiver. """

    def __init__(self, config, context, log_queue):
        super().__init__()

        self.config = config["datafetcher"]
        self.context = context

        self.log = utils.get_logger(self.__class__.__name__, log_queue)

        self.test_socket = None
        self.action_name = None
        self.address = None
        self.socket_conf = None
        self.test_signal = None
        self.is_req = None

        self.reestablish_time = 600  # in sec
        self.zmq_again_occurred = 0

        self.show_check_warning = True

    def enable_status_check(self):
        """ Check the status of ther reciever (REQ-REP) """

        self.log.info("Enabled receiver checking")

        self.test_signal = b"STATUS_CHECK"
        self.action_name = self.test_signal.lower()
        self.address = "{}:{}".format(self.config["data_stream_targets"][0][0],
                                      self.config["status_check_port"])
        self.socket_conf = dict(
            name="test_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint="tcp://{}".format(self.address)
        )
        self.is_req = True
        self.check_target_host = self._check_target_host

    def enable_alive_test(self):
        """ Check if the receiver is alive by tracking the data socket"""

        self.log.info("Enabled alive test")

        self.test_signal = b"ALIVE_TEST"
        self.action_name = self.test_signal.lower()
        self.address = "{}:{}".format(self.config["data_stream_targets"][0][0],
                                      self.config["data_stream_targets"][0][1])
        self.socket_conf = dict(
            name="test_socket",
            sock_type=zmq.PUSH,
            sock_con="connect",
            endpoint="tcp://{}".format(self.address)
        )
        self.is_req = False
        self.check_target_host = self._check_target_host

    # pylint: disable=method-hidden
    # pylint: disable=unused-argument
    def check_target_host(self, use_log=False):
        """No checking is done if there was no test type activated.

        Args:
            use_log (optional, bool): if log messages should be generated.
        """

        # only show the warning once at the beginning
        if self.show_check_warning:
            self.log.warning("No communication type enabled.")
            self.show_check_warning = False

        return True

    def _check_target_host(self, use_log=False):
        """Communicates to the receiver and checks response.

        Args:
            use_log (optional, bool): if log messages should be generated.
        """

        # no data stream used means that no receiver is used
        # -> status always is fine
        if not self.config["use_data_stream"]:
            return True

        # --------------------------------------------------------------------
        # create socket
        # --------------------------------------------------------------------
        if self.test_socket is None:
            # Establish the test socket as REQ/REP to an extra signal socket
            try:
                self.test_socket = self.start_socket(**self.socket_conf)
            except Exception:
                return False

        if use_log:
            self.log.debug("ZMQ version used: %s", zmq.__version__)

        # --------------------------------------------------------------------
        # old zmq version
        # --------------------------------------------------------------------
        # With older ZMQ versions the tracker results in an ZMQError in the
        # DataDispatchers when an event is processed
        # (ZMQError: Address already in use)
        if LooseVersion(zmq.__version__) <= LooseVersion("14.5.0"):
            return self._send_message_old_zmq(use_log=use_log)

        # --------------------------------------------------------------------
        # newer zmq version
        # --------------------------------------------------------------------
        # due to optimizations done in newer zmq version track needs
        # additional parameter to work properly again. See pyzmq issue #1364
        if LooseVersion(zmq.__version__) > LooseVersion("17.0.0"):
            self.test_socket.copy_threshold = 0

        try:
            # after unsuccessfully sending a test messages try to reestablish
            # the connection (this should not be done in every test iteration
            # because of overhead but only once in a while)
            if self.zmq_again_occurred >= self.reestablish_time:
                self._reopen_socket()

            self._send_message(use_log=use_log)

            if self.is_req:
                self._receive_response(use_log)
            return True

        except KeyboardInterrupt:
            # nothing to log
            raise
        except (zmq.Again, zmq.error.ZMQError):
            # nothing to log
            return False
        except Exception:
            self.log.error("Failed to send %s of fixed streaming host %s",
                           self.action_name, self.address, exc_info=True)
            return False

    def _reopen_socket(self):
        # close the socket
        self.test_socket.close()

        # reopen it
        try:
            self.socket_conf["message"] = "Restart"
            self.test_socket = self.start_socket(**self.socket_conf)
        except Exception:
            # TODO is this right here?
            pass

        self.zmq_again_occurred = 0

    def _send_message(self, use_log):
        # send test message
        try:
            tracker = self.test_socket.send_multipart(
                [self.test_signal],
                zmq.NOBLOCK,
                copy=False,
                track=True
            )
        # The receiver may have dropped authentication or
        # previous status check was not answered
        # (req send after req, without rep in between)
        except (zmq.Again, zmq.error.ZMQError):
            # returns a tuple (type, value, traceback)
            exc_type, exc_value, _ = sys.exc_info()

            if self.zmq_again_occurred == 0:
                self.log.debug("Error was: %s: %s", exc_type, exc_value)

            self.zmq_again_occurred += 1
            raise

        if self.is_req and use_log:
            self.log.info("Sent %s to fixed streaming host %s",
                          self.action_name, self.address)

        # test if someone picks up the test message in the next
        # 2 sec
        if not tracker.done:
            tracker.wait(2)

        # no one picked up the test message
        if not tracker.done:
            raise utils.CommunicationFailed("Tracker not done")

        # test message was successfully sent
        if use_log:
            self.log.info("Sending %s to fixed streaming host %s...success",
                          self.action_name, self.address)
            self.zmq_again_occurred = 0

    def _receive_response(self, use_log):
        if use_log:
            self.log.debug("Receiving response...")

        status = self.test_socket.recv_multipart()

        if use_log:
            self.log.debug("Received response: %s", status)

        # response to test message was successfully received
        # TODO check status + react
        if status[0] == b"ERROR":
            raise utils.CommunicationFailed(
                "Fixed streaming host is in error state: {}"
                .format(status[1].decode("utf-8"))
            )

        if use_log:
            self.log.info("Response for %s of fixed streaming host %s: %s",
                          self.action_name, self.address, status)

    def _send_message_old_zmq(self,
                              use_log):
        # TODO deprecated warning

        try:
            self.test_socket.send_multipart([self.test_signal])
            if use_log:
                self.log.info("Sending %s to fixed streaming host %s..."
                              "success", self.action_name, self.address)

            if self.is_req:
                self.test_socket.recv_multipart()
                if use_log:
                    self.log.info("Received response for %s of fixed "
                                  "streaming host %s",
                                  self.action_name, self.address)

            return True
        except KeyboardInterrupt:
            # nothing to log
            raise
        except Exception:
            self.log.error("Failed to %s of fixed streaming host %s",
                           self.action_name, self.address, exc_info=True)
            return False

    def stop(self):
        """ Stop and clean up """
        self.stop_socket(name="test_socket")


# Needs to be defined at the top-level of the module to be picklable. This is
# needed for multiprocessing spawn to work.
def run_cleaner(df_type, conf):
    """ Wrapper to run in a process or thread"""

    proc = import_module(df_type).Cleaner(**conf)
    proc.run()


class DataManager(Base):
    """The main class.
    """

    def __init__(self, config=None):

        super().__init__()

        self.stop_request = None

        self.device = None
        self.control_pub_socket = None
        self.context = None

        self.log = None
        self.log_queue = None
        self.log_level = None
        self.log_queue_listener = None

        self.localhost = None
        self.ext_ip = None
        self.con_ip = None
        self.ipc_dir = None
        self.current_pid = None

        self.ipc_dir_permissions = 0o777

        self.config = None

        self.whitelist = None
        self.ldapuri = None

        self.use_data_stream = None
        self.fixed_stream_addr = None
        self.receiver_communication = None

        self.number_of_streams = None

        self.endpoints = None
        self.ipc_addresses = None

        self.local_target = None

        self.signalhandler_pr = None
        self.taskprovider_pr = None
        self.datadispatcher_pr = []

        self.use_cleaner = None
        self.cleaner_m = None
        self.cleaner_pr = None

        self.use_statserver = None
        self.statserver = None

        self.context = None

        self.setup(config)

    def setup(self, config):
        """Initializes parameters and creates sockets.

        Args:
            config (dict): All the configuration set either via config file or
                           command line parameter.
        """
        self.localhost = "127.0.0.1"
        self.current_pid = os.getpid()

        try:
            if config is None:
                self.config = get_config()
            else:
                self.config = config
        except Exception:
            self.log = logging
            self.ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
            raise

        config_gen = self.config["general"]
        config_df = self.config["datafetcher"]

        # change user
        # has to be done before logging is setup because otherwise the logfile
        # belongs to the wrong user
        user_info, user_was_changed = utils.change_user(config_gen)

        self.stop_request = multiprocessing.Event()

        # set up logging
        utils.check_writable(config_gen["log_file"])
        self._setup_logging()

        utils.log_user_change(self.log, user_was_changed, user_info)

        # set process name
        # pylint: disable=no-member
        setproctitle.setproctitle(config_gen["procname"])
        self.log.info("Running as %s", config_gen["procname"])

        self.log.info("DataManager started (PID %s).", self.current_pid)

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.use_cleaner = (
            config_df["use_data_stream"]
            and config_df["remove_data"] == "with_confirmation"
        )
        config_df["use_cleaner"] = self.use_cleaner

        self.use_statserver = config_gen["use_statserver"]
        self.number_of_streams = config_df["number_of_streams"]
        self.use_data_stream = config_df["use_data_stream"]
        self.log.info("Usage of data stream set to '%s'", self.use_data_stream)

        if "local_target" in config_df:
            self.log.info("Configured local_target: %s",
                          config_df["local_target"])
        else:
            config_df["local_target"] = None

        self.log.info("Version: %s", __version__)

        self.whitelist = config_gen["whitelist"]
        self.ldapuri = config_gen["ldapuri"]

        # IP and DNS name should be both in the whitelist
        self.whitelist = utils.extend_whitelist(self.whitelist,
                                                self.ldapuri,
                                                self.log)

        # Create zmq context
        # there should be only one context in one process
        self.context = zmq.Context()
        self.log.debug("Registering global ZMQ context")

        # set up endpoints and network config
        self._setup_network()
        self.receiver_communication = CheckReceiver(config=self.config,
                                                    context=self.context,
                                                    log_queue=self.log_queue)
        self._check_data_stream_targets()

    def _setup_logging(self):
        config_gen = self.config["general"]

        # Get queue
        self.log_queue = multiprocessing.Queue(-1)

        handler = utils.get_log_handlers(
            config_gen["log_file"],
            config_gen["log_size"],
            config_gen["verbose"],
            config_gen["onscreen"]
        )

        # Start queue listener using the stream handler above.
        self.log_queue_listener = utils.CustomQueueListener(
            self.log_queue, *handler
        )

        self.log_queue_listener.start()

        # the least sever log level to forward to the queuelistener
        file_log_level = "debug" if config_gen["verbose"] else "error"
        if config_gen["onscreen"]:
            self.log_level = utils.get_least_sever_log_level(
                log_levels=(config_gen["onscreen"], file_log_level)
            )
        else:
            self.log_level = file_log_level

        # Create log and set handler to queue handle
        self.log = utils.get_logger(self.__class__.__name__,
                                    queue=self.log_queue,
                                    log_level=self.log_level)
        self.log.info("Setting process log level to '%s'", self.log_level)

    def _setup_network(self):
        config_gen = self.config["general"]
        config_df = self.config["datafetcher"]

        self.ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
        self.log.info("Configured ipc_dir: %s", self.ipc_dir)

        if not os.path.exists(self.ipc_dir):
            os.mkdir(self.ipc_dir)
            # the permission have to be changed explicitly because
            # on some platform they are ignored when called within mkdir
            os.chmod(self.ipc_dir, self.ipc_dir_permissions)
            self.log.info("Creating directory for IPC communication: %s",
                          self.ipc_dir)

        # Enable specification via IP and DNS name
        # TODO make this IPv6 compatible
        if config_gen["ext_ip"] == "0.0.0.0":
            self.ext_ip = config_gen["ext_ip"]
        else:
            self.ext_ip = socket.gethostbyaddr(config_gen["ext_ip"])[2][0]
        self.con_ip = socket.getfqdn()

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

    def _check_data_stream_targets(self):

        if not self.use_data_stream:
            self.fixed_stream_addr = None
            return

        targets = self.config["datafetcher"]["data_stream_targets"]

        if len(targets) > 1:
            self.log.error("Targets to send data stream to have more than "
                           "one entry which is not supported")
            self.log.debug("data_stream_targets: %s", targets)
            sys.exit(1)

        self.fixed_stream_addr = "{}:{}".format(targets[0][0], targets[0][1])

        if self.config["datafetcher"]["remove_data"] == "stop_on_error":
            self.receiver_communication.enable_status_check()
        else:
            self.receiver_communication.enable_alive_test()

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
            self.log.info("Start thread device forwarding messages "
                          "from '%s' to '%s'",
                          self.endpoints.control_pub_bind,
                          self.endpoints.control_sub_bind)
        except Exception:
            self.log.error("Failed to start thread device forwarding messages "
                           "from '%s' to '%s'",
                           self.endpoints.control_pub_bind,
                           self.endpoints.control_sub_bind, exc_info=True)
            raise

        # socket for control signals
        self.control_pub_socket = self.start_socket(
            name="control_pub_socket",
            sock_type=zmq.PUB,
            sock_con="connect",
            endpoint=self.endpoints.control_pub_con
        )

    def run(self):
        """Running while reacting to exceptions.
        """

        try:
            if self.receiver_communication.check_target_host(use_log=True):
                self.create_sockets()

                self._exec_run()
        except KeyboardInterrupt:
            pass
        except Exception:
            self.log.error("Stopping due to unknown error condition",
                           exc_info=True)
        finally:
            self.stop()

    def _exec_run(self):
        """Starting all thread and processes and checks if they are running.
        """

        # StatServer
        if self.use_statserver:
            self.statserver = multiprocessing.Process(
                target=run_statserver,
                kwargs=dict(
                    config=self.config,
                    log_queue=self.log_queue,
                    log_level=self.log_level,
                    stop_request=self.stop_request
                )
            )
            self.statserver.start()

        # SignalHandler
        self.signalhandler_pr = multiprocessing.Process(
            target=run_signalhandler,
            kwargs=dict(
                config=self.config,
                endpoints=self.endpoints,
                whitelist=self.whitelist,
                ldapuri=self.ldapuri,
                log_queue=self.log_queue,
                log_level=self.log_level,
                stop_request=self.stop_request
            )
        )
        self.signalhandler_pr.start()

        # needed, because otherwise the requests for the first files are not
        # forwarded properly
        time.sleep(0.5)

        if not self.signalhandler_pr.is_alive():
            self.log.error("Signalhandler did not start.")
            return

        # TaskProvider
        self.taskprovider_pr = multiprocessing.Process(
            target=run_taskprovider,
            kwargs=dict(
                config=self.config,
                endpoints=self.endpoints,
                log_queue=self.log_queue,
                log_level=self.log_level,
                stop_request=self.stop_request
            )
        )
        self.taskprovider_pr.start()

        # Cleaner
        if self.use_cleaner:
            self.log.info("Loading cleaner from data fetcher module: %s",
                          self.config["datafetcher"]["type"])

            self.cleaner_pr = multiprocessing.Process(
                target=run_cleaner,
                kwargs=dict(
                    df_type=self.config["datafetcher"]["type"],
                    conf=dict(
                        config=self.config,
                        log_queue=self.log_queue,
                        log_level=self.log_level,
                        endpoints=self.endpoints,
                        stop_request=self.stop_request
                    )
                )
            )
            self.cleaner_pr.start()

        self.log.info("Configured type of data fetcher: %s",
                      self.config["datafetcher"]["type"])

        # DataDispatcher
        for i in range(self.number_of_streams):
            dispatcher_id = "{}/{}".format(i, self.number_of_streams)
            proc = multiprocessing.Process(
                target=run_datadispatcher,
                kwargs=dict(
                    dispatcher_id=dispatcher_id,
                    endpoints=self.endpoints,
                    fixed_stream_addr=self.fixed_stream_addr,
                    config=self.config,
                    log_queue=self.log_queue,
                    log_level=self.log_level,
                    stop_request=self.stop_request
                )
            )
            proc.start()
            self.datadispatcher_pr.append(proc)

        # indicates if the processed are sent to waiting mode
        sleep_was_sent = False
        run_loop = self.core_parts_status_check()

        while run_loop:

            if self.receiver_communication.check_target_host():
                if sleep_was_sent:
                    msg = [b"control", b"WAKEUP"]
                    msg += [b"RECONNECT"]

                    self.log.info("Sending 'WAKEUP' signal")
                    self.control_pub_socket.send_multipart(msg)
                    sleep_was_sent = False

            else:
                # Due to an unforeseeable event there is no active receiver on
                # the other side. Thus the processes should enter a waiting
                # mode and no data should be send.
                if not sleep_was_sent:
                    self.log.warning("Sending 'SLEEP' signal")
                    self.control_pub_socket.send_multipart([b"control",
                                                            b"SLEEP"])
                    sleep_was_sent = True

            time.sleep(1)

            run_loop = (not self.stop_request.is_set()
                        and self.core_parts_status_check())

        # notify which subprocess terminated
        if self.stop_request.is_set():
            self.log.debug("Stopped run loop.")
        else:
            if not self.signalhandler_pr.is_alive():
                self.log.info("SignalHandler terminated.")
            if not self.taskprovider_pr.is_alive():
                self.log.info("TaskProvider terminated.")
            if self.use_cleaner and not self.cleaner_pr.is_alive():
                self.log.info("Cleaner terminated.")
            if not any(datadispatcher.is_alive()
                       for datadispatcher in self.datadispatcher_pr):
                self.log.info("One DataDispatcher terminated.")

    def core_parts_status_check(self):
        """Check if the core components still are running.

        Returns:
            A boolean if the components are running or not.
        """

        if self.use_cleaner:
            status = (
                self.signalhandler_pr.is_alive()
                and self.taskprovider_pr.is_alive()
                and self.cleaner_pr.is_alive()
                and all(datadispatcher.is_alive()
                        for datadispatcher
                        in self.datadispatcher_pr)
            )
        else:
            status = (
                self.signalhandler_pr.is_alive()
                and self.taskprovider_pr.is_alive()
                and all(datadispatcher.is_alive()
                        for datadispatcher
                        in self.datadispatcher_pr)
            )
        return status

    def check_hanging(self, log=True):
        """Check if any subprocess or thread is hanging.

        Args:
            log (optional): if the information should be logged.

        Returns:
            A boolean with the information of any subprocess or thread is
            hanging.
        """

        is_hanging = False

        # detecting hanging processes
        if (self.signalhandler_pr is not None
                and self.signalhandler_pr.is_alive()):
            if log:
                self.log.error("SignalHandler hangs (PID %s).",
                               self.signalhandler_pr.pid)
            is_hanging = True

        if (self.taskprovider_pr is not None
                and self.taskprovider_pr.is_alive()):
            if log:
                self.log.error("TaskProvider hangs (PID %s).",
                               self.taskprovider_pr.pid)
            is_hanging = True

        if self.cleaner_pr is not None and self.cleaner_pr.is_alive():
            if log:
                self.log.error("Cleaner hangs (PID %s).", self.cleaner_pr.pid)
            is_hanging = True

        for i, datadispatcher in enumerate(self.datadispatcher_pr):
            if datadispatcher is not None and datadispatcher.is_alive():
                if log:
                    self.log.error("DataDispatcher-%s hangs (PID %s)",
                                   i, datadispatcher.pid)
                is_hanging = True

        return is_hanging

    def stop(self):
        """Close socket and clean up.
        """

        self.stop_request.set()

        if self.log is None:
            self.log = logging

        if self.control_pub_socket is not None:
            self.log.info("Sending 'Exit' signal")
            self.control_pub_socket.send_multipart([b"control", b"EXIT"])

            # check if the different processes where up and running (meaning
            # are able to receive signals) otherwise this would result in
            # hanging processes (zmq slow joiner problem)
            for i in range(5):
                if self.check_hanging(log=False):
                    self.log.debug("Waiting for processes to finish, "
                                   "resending 'EXIT' signal (try %s)", i)
                    time.sleep(1)
                    self.control_pub_socket.send_multipart(
                        [b"control", b"EXIT"]
                    )
                else:
                    break

        # closing control forwarding
        if self.device is not None:
            self.log.info("Stopping forwarder device")
#            self.device.context_factory().term()
            self.device.join(0.5)
            self.device = None

        self.stop_socket(name="control_pub_socket")

        # detecting hanging processes
        self.check_hanging(log=True)
        if (self.statserver is not None
                and self.statserver.is_alive()):
            self.log.error("StatServer hangs.")

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
                    self.log.debug("Removed ipc socket: %s", path)
                except OSError:
                    pass
                except Exception:
                    self.log.warning("Could not remove ipc socket: %s",
                                     path, exc_info=True)

            # Remove temp directory (if empty)
            try:
                os.rmdir(self.ipc_dir)
                self.log.debug("Removed IPC directory: %s", self.ipc_dir)
            except OSError:
                self.log.debug("Could not remove IPC directory: %s",
                               self.ipc_dir)
            except Exception:
                self.log.warning("Could not remove IPC directory: %s",
                                 self.ipc_dir, exc_info=True)

        if self.log_queue_listener:
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


def main():
    """Running the datamanager.
    """

    # see https://docs.python.org/2/library/multiprocessing.html#windows
    multiprocessing.freeze_support()
    frozen_not_win = (
        hasattr(sys, "frozen") and not sys.platform.startswith("win")
    )
    if (sys.version_info.major >= 3 and sys.version_info.minor >= 4
            and not frozen_not_win):
        # only availab since 3.4
        # and cannot be used for non win systems when freeze3 is used due
        # to a bug in multiprocessing.freeze_support()
        # https://bugs.python.org/issue32146
        multiprocessing.set_start_method('spawn')

    sender = None  # pylint: disable=invalid-name
    try:
        sender = DataManager()  # pylint: disable=invalid-name
        sender.run()
    finally:
        if sender is not None:
            sender.stop()


if __name__ == '__main__':
    main()
