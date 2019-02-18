#!/usr/bin/env python

from __future__ import print_function

import argparse
import logging
import os
import setproctitle
import signal
import threading
import copy
import time

from __init__ import BASE_DIR

import utils
from hidra import Transfer, __version__


__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

CONFIG_DIR = os.path.join(BASE_DIR, "conf")

whitelist = []
changed_netgroup = False


def argument_parsing():
    base_config_file = os.path.join(CONFIG_DIR, "base_receiver.conf")
    default_config_file = os.path.join(CONFIG_DIR, "datareceiver.conf")

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
    arguments.config_file = arguments.config_file or default_config_file

    # check if config_file exist
    utils.check_existance(arguments.config_file)

    # ------------------------------------------------------------------------
    # Get arguments from config file
    # ------------------------------------------------------------------------

    params = utils.set_parameters(base_config_file=base_config_file,
                                  config_file=arguments.config_file,
                                  arguments=arguments)

    # ------------------------------------------------------------------------
    # Check given arguments
    # ------------------------------------------------------------------------

    # check target directory for existance
    utils.check_existance(params["target_dir"])

    # check if logfile is writable
    params["log_file"] = os.path.join(params["log_path"], params["log_name"])
    utils.check_writable(params["log_file"])

    return params


def reset_changed_netgroup():
    """helper because global variables can only be reset in same namespace.
    """
    global changed_netgroup

    changed_netgroup = False


class CheckNetgroup (threading.Thread):
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
        global whitelist
        global changed_netgroup

        # wake up evey 2 seconds to see if there is a stopping signal
        sec_to_sleep = 2
        ldap_sleep_intervalls = int(self.ldap_retry_time // sec_to_sleep)
        check_sleep_intervalls = int(self.check_time // sec_to_sleep)

        while self.run_loop:
            new_whitelist = utils.execute_ldapsearch(self.log,
                                                     self.netgroup,
                                                     self.ldapuri)

            # if there are problems with ldap the search returns an empty list
            # -> do nothing but wait till ldap is reachable again
            if not new_whitelist:
                self.log.info("LDAP search returned an empty list. Ignore.")
                for i in range(ldap_sleep_intervalls):
                    if self.run_loop:
                        time.sleep(sec_to_sleep)
                continue

            # new elements added to whitelist
            new_elements = [e for e in new_whitelist if e not in whitelist]
            # elements which were removed from whitelist
            removed_elements = [e for e in whitelist if e not in new_whitelist]

            if new_elements or removed_elements:
                with self.lock:
                    # remember new whitelist
                    whitelist = copy.deepcopy(new_whitelist)

                    # mark that there was a change
                    changed_netgroup = True

                self.log.info("Netgroup has changed. New whitelist: {}"
                              .format(whitelist))

            for i in range(check_sleep_intervalls):
                if self.run_loop:
                    time.sleep(sec_to_sleep)

    def stop(self):
        self.run_loop = False


class DataReceiver:
    def __init__(self):

        self.transfer = None
        self.checking_thread = None
        self.timeout = None

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
        global whitelist

        try:
            params = argument_parsing()
        except:
            self.log = logging.getLogger("DataReceiver")
            raise

        # enable logging
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)

        handlers = utils.get_log_handlers(params["log_file"],
                                          params["log_size"],
                                          params["verbose"],
                                          params["onscreen"])

        if type(handlers) == tuple:
            for h in handlers:
                root.addHandler(h)
        else:
            root.addHandler(handlers)

        self.log = logging.getLogger("DataReceiver")

        # set process name
        check_passed, _ = utils.check_config(["procname"], params, self.log)
        if not check_passed:
            raise Exception("Configuration check failed")
        setproctitle.setproctitle(params["procname"])

        self.log.info("Version: {}".format(__version__))

        self.dirs_not_to_create = params["dirs_not_to_create"]

        # for proper clean up if kill is called
        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.timeout = 2000
        self.lock = threading.Lock()

        try:
            ldap_retry_time = params["ldap_retry_time"]
        except KeyError:
            ldap_retry_time = 10

        try:
            check_time = params["netgroup_check_time"]
        except KeyError:
            check_time = 2

        if params["whitelist"] is not None:
            self.log.debug("params['whitelist']={}"
                           .format(params["whitelist"]))

            with self.lock:
                whitelist = utils.extend_whitelist(params["whitelist"],
                                                   params["ldapuri"],
                                                   self.log)
            self.log.info("Configured whitelist: {}".format(whitelist))
        else:
            whitelist = None

        # only start the thread if a netgroup was configured
        if (params["whitelist"] is not None
                and type(params["whitelist"]) == str):
            self.log.debug("Starting checking thread")
            self.checking_thread = CheckNetgroup(params["whitelist"],
                                                 self.lock,
                                                 params["ldapuri"],
                                                 ldap_retry_time,
                                                 check_time)
            self.checking_thread.start()
        else:
            self.log.debug("Checking thread not started: {}"
                           .format(params["whitelist"]))

        self.target_dir = os.path.normpath(params["target_dir"])
        self.data_ip = params["data_stream_ip"]
        self.data_port = params["data_stream_port"]

        self.log.info("Writing to directory '{}'".format(self.target_dir))

        self.transfer = Transfer(connection_type="STREAM",
                                 use_log=True,
                                 dirs_not_to_create=self.dirs_not_to_create)

    def exec_run(self):

        try:
            self.run()
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping due to unknown error condition",
                           exc_info=True)
            raise
        finally:
            self.stop()

    def run(self):
        global whitelist
        global changed_netgroup

        try:
            self.transfer.start([self.data_ip, self.data_port], whitelist)
#            self.transfer.start(self.data_port)
        except:
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
            if changed_netgroup:
                self.log.debug("Reregistering whitelist")
                self.transfer.register(whitelist)

                # reset flag
                with self.lock:
                    changed_netgroup = False

            try:
                self.transfer.store(self.target_dir, self.timeout)
            except KeyboardInterrupt:
                break
            except:
                self.log.error("Storing data...failed.", exc_info=True)
                raise

    def stop(self, store=True):
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
                    except:
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

    def signal_term_handler(self, signal, frame):
        self.log.debug('got SIGTERM')
        self.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == "__main__":
    # start file receiver
    receiver = DataReceiver()
