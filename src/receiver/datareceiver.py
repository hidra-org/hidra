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
This module implements the receiver.
"""

# pylint: disable=global-variable-not-assigned
# pylint: disable=invalid-name

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import copy
import logging
import os
import signal
import threading
import time

import setproctitle

from __init__ import BASE_DIR

from hidra import Transfer, __version__
import hidra.utils as utils


__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

CONFIG_DIR = os.path.join(BASE_DIR, "conf")

_whitelist = []
_changed_netgroup = False


def argument_parsing():
    """Parses and checks the command line arguments used.
    """

    base_config_file = utils.determine_config_file(fname_base="base_receiver",
                                                   config_dir=CONFIG_DIR)

    # ------------------------------------------------------------------------
    # Get command line arguments
    # ------------------------------------------------------------------------

    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",
                        type=str,
                        help="Location of the configuration file")

    parser.add_argument("--log_path",
                        type=str,
                        help="Path where logfile will be created")
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

    parser.add_argument("--whitelist",
                        type=str,
                        help="List of hosts allowed to connect")
    parser.add_argument("--target_dir",
                        type=str,
                        help="Where incoming data will be stored to")
    parser.add_argument("--dirs_not_to_create",
                        type=str,
                        help="Subdirectories which should not be created when "
                             "data is stored")
    parser.add_argument("--data_stream_ip",
                        type=str,
                        help="Ip of dataStream-socket to pull new files from")
    parser.add_argument("--data_stream_port",
                        type=str,
                        help="Port number of dataStream-socket to pull new "
                             "files from")

    arguments = parser.parse_args()
    arguments.config_file = (
        arguments.config_file
        or utils.determine_config_file(fname_base="datareceiver",
                                       config_dir=CONFIG_DIR)
    )

    # check if config_file exist
    utils.check_existence(arguments.config_file)

    # ------------------------------------------------------------------------
    # Get arguments from config file
    # ------------------------------------------------------------------------

    config = utils.load_config(base_config_file)
    config_detailed = utils.load_config(arguments.config_file)

    # if config and yaml is mixed mapping has to take place before merging them
    config_type = "receiver"
    config = utils.map_conf_format(config, config_type)
    config_detailed = utils.map_conf_format(config_detailed, config_type)
    arguments_dict = utils.map_conf_format(arguments,
                                           config_type,
                                           is_namespace=True)

    utils.update_dict(config_detailed, config)

    utils.update_dict(arguments_dict, config)

#    params = utils.set_parameters(base_config_file=base_config_file,
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
            "procname",
            "ldapuri",
            "dirs_not_to_create",
            "whitelist"
        ],
        "datareceiver": [
            "target_dir",
            "data_stream_ip",
            "data_stream_port",
        ]
    }

    # Check format of config
    check_passed, _ = utils.check_config(required_params, config, logging)
    if not check_passed:
        logging.error("Configuration check failed")
        raise utils.WrongConfiguration

    # check target directory for existence
    utils.check_existence(config["datareceiver"]["target_dir"])

    # check if logfile is writable
    config["general"]["log_file"] = os.path.join(config["general"]["log_path"],
                                                 config["general"]["log_name"])

    return config


def reset_changed_netgroup():
    """helper because global variables can only be reset in same namespace.
    """
    global _changed_netgroup

    _changed_netgroup = False


class CheckNetgroup(threading.Thread):
    """A thread checking on a regular basis if the netgroup has changed.
    """

    def __init__(self, netgroup, lock, ldapuri, ldap_retry_time, check_time):
        self.log = logging.getLogger("CheckNetgroup")

        self.log.debug("init")
        self.netgroup = netgroup
        self.lock = lock
        self.run_loop = True
        self.ldapuri = ldapuri
        self.ldap_retry_time = ldap_retry_time
        self.check_time = check_time

        self.log.debug("threading.Thread init")
        threading.Thread.__init__(self)

    def run(self):
        global _whitelist
        global _changed_netgroup

        # wake up evey 2 seconds to see if there is a stopping signal
        sec_to_sleep = 2

        if self.ldap_retry_time < sec_to_sleep:
            ldap_sleep_intervals = 1
        else:
            ldap_sleep_intervals = int(self.ldap_retry_time // sec_to_sleep)

        if self.check_time < sec_to_sleep:
            check_sleep_intervals = 1
        else:
            check_sleep_intervals = int(self.check_time // sec_to_sleep)

        while self.run_loop:
            new_whitelist = utils.execute_ldapsearch(self.log,
                                                     self.netgroup,
                                                     self.ldapuri)

            # if there are problems with ldap the search returns an empty list
            # -> do nothing but wait till ldap is reachable again
            if not new_whitelist:
                self.log.info("LDAP search returned an empty list. Ignore.")
                for _ in range(ldap_sleep_intervals):
                    if self.run_loop:
                        time.sleep(sec_to_sleep)
                continue

            # new elements added to whitelist
            new_elements = [e for e in new_whitelist if e not in _whitelist]
            # elements which were removed from whitelist
            removed_elements = [e for e in _whitelist
                                if e not in new_whitelist]

            if new_elements or removed_elements:
                with self.lock:
                    # remember new whitelist
                    _whitelist = copy.deepcopy(new_whitelist)

                    # mark that there was a change
                    _changed_netgroup = True

                self.log.info("Netgroup has changed. New whitelist: %s",
                              _whitelist)

            for _ in range(check_sleep_intervals):
                if self.run_loop:
                    time.sleep(sec_to_sleep)

    def stop(self):
        """Stopping.
        """
        self.run_loop = False


class DataReceiver(object):
    """Receives data and stores it to disc usign the hidra API.
    """

    def __init__(self):

        self.transfer = None
        self.checking_thread = None
        self.timeout = None

        self.config = None

        self.log = None
        self.dirs_not_to_create = None
        self.lock = None
        self.target_dir = None
        self.data_ip = None
        self.data_port = None
        self.transfer = None
        self.checking_thread = None

        self.run_loop = True

        self.setup()

        self.exec_run()

    def setup(self):
        """Initializes parameters, logging and transfer object.
        """

        global _whitelist

        try:
            self.config = argument_parsing()
        except Exception:
            self.log = logging.getLogger("DataReceiver")
            raise

        config_gen = self.config["general"]
        config_recv = self.config["datareceiver"]

        # change user
        user_info, user_was_changed = utils.change_user(config_gen)

        # set up logging
        utils.check_writable(config_gen["log_file"])
        self._setup_logging()

        utils.log_user_change(self.log, user_was_changed, user_info)

        # set process name
        # pylint: disable=no-member
        setproctitle.setproctitle(config_gen["procname"])

        self.log.info("Version: %s", __version__)

        self.dirs_not_to_create = config_gen["dirs_not_to_create"]

        # for proper clean up if kill is called
        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.timeout = 2000
        self.lock = threading.Lock()

        try:
            ldap_retry_time = config_gen["ldap_retry_time"]
        except KeyError:
            ldap_retry_time = 10

        try:
            check_time = config_gen["netgroup_check_time"]
        except KeyError:
            check_time = 2

        if config_gen["whitelist"] is not None:
            self.log.debug("config_gen['whitelist']=%s",
                           config_gen["whitelist"])

            with self.lock:
                _whitelist = utils.extend_whitelist(config_gen["whitelist"],
                                                    config_gen["ldapuri"],
                                                    self.log)
            self.log.info("Configured whitelist: %s", _whitelist)
        else:
            _whitelist = None

        # only start the thread if a netgroup was configured
        if (config_gen["whitelist"] is not None
                and isinstance(config_gen["whitelist"], str)):
            self.log.debug("Starting checking thread")
            try:
                self.checking_thread = CheckNetgroup(
                    config_gen["whitelist"],
                    self.lock,
                    config_gen["ldapuri"],
                    ldap_retry_time,
                    check_time
                )
                self.checking_thread.start()
            except Exception:
                self.log.error("Could not start checking thread",
                               exc_info=True)
        else:
            self.log.debug("Checking thread not started: %s",
                           config_gen["whitelist"])

        self.target_dir = os.path.normpath(config_recv["target_dir"])
        self.data_ip = config_recv["data_stream_ip"]
        self.data_port = config_recv["data_stream_port"]

        self.log.info("Writing to directory '%s'", self.target_dir)

        self.transfer = Transfer(connection_type="STREAM",
                                 use_log=True,
                                 dirs_not_to_create=self.dirs_not_to_create)

    def _setup_logging(self):
        config_gen = self.config["general"]

        # enable logging
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)

        handlers = utils.get_log_handlers(
            config_gen["log_file"],
            config_gen["log_size"],
            config_gen["verbose"],
            config_gen["onscreen"]
        )

        if isinstance(handlers, tuple):
            for hdl in handlers:
                root.addHandler(hdl)
        else:
            root.addHandler(handlers)

        self.log = logging.getLogger("DataReceiver")

    def exec_run(self):
        """Wrapper around run to react to exceptions.
        """

        try:
            self.run()
        except KeyboardInterrupt:
            pass
        except Exception:
            self.log.error("Stopping due to unknown error condition",
                           exc_info=True)
            raise
        finally:
            self.stop()

    def run(self):
        """Start the transfer and stores the data.
        """

        global _whitelist
        global _changed_netgroup

        try:
            self.transfer.start([self.data_ip, self.data_port], _whitelist)
#            self.transfer.start(self.data_port)
        except Exception:
            self.log.error("Could not initiate stream", exc_info=True)
            self.stop(store=False)
            raise

        # enable status check requests from any sender
        self.transfer.setopt("status_check")
        # enable confirmation reply if this is requested in a received data
        # packet
        self.transfer.setopt("confirmation")

        self.log.debug("Waiting for new messages...")
        self.run_loop = True
        # run loop, and wait for incoming messages
        while self.run_loop:
            if _changed_netgroup:
                self.log.debug("Re-registering whitelist")
                self.transfer.register(_whitelist)

                # reset flag
                with self.lock:
                    _changed_netgroup = False

            try:
                self.transfer.store(self.target_dir, self.timeout)
            except KeyboardInterrupt:
                break
            except Exception:
                self.log.error("Storing data...failed.", exc_info=True)
                raise

    def stop(self, store=True):
        """Stop threads, close sockets and cleans up.

        Args:
            store (optional, bool): Run a little longer to store remaining
                                    data.
        """

        self.run_loop = False

        if self.transfer is not None:
            self.transfer.status = [b"ERROR", "receiver is shutting down"]

            if store:
                stop_timeout = 0.5
                start_time = time.time()
                diff_time = (time.time() - start_time) * 1000
                self.log.debug("Storing remaining data.")
                while diff_time < stop_timeout:
                    try:
                        self.log.debug("Storing remaining data...")
                        self.transfer.store(self.target_dir, self.timeout)
                    except Exception:
                        self.log.error("Storing data...failed.", exc_info=True)
                    diff_time = (time.time() - start_time) * 1000

            self.log.info("Shutting down receiver...")
            self.transfer.stop()
            self.transfer = None

        if self.checking_thread is not None:
            self.checking_thread.stop()
            self.checking_thread.join()
            self.log.debug("checking_thread stopped")
            self.checking_thread = None

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


if __name__ == "__main__":
    # start file receiver
    DataReceiver()
