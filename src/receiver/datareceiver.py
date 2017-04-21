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

from __init__ import BASE_PATH

import helpers
from hidra import Transfer


__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

CONFIG_PATH = os.path.join(BASE_PATH, "conf")

whitelist = None
changed_netgroup = False


def argument_parsing():
    default_config = os.path.join(CONFIG_PATH, "datareceiver.conf")

    ##################################
    #   Get command line arguments   #
    ##################################

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
    parser.add_argument("--data_stream_ip",
                        type=str,
                        help="Ip of dataStream-socket to pull new files from")
    parser.add_argument("--data_stream_port",
                        type=str,
                        help="Port number of dataStream-socket to pull new "
                             "files from")

    arguments = parser.parse_args()
    arguments.config_file = arguments.config_file or default_config

    # check if config_file exist
    helpers.check_existance(arguments.config_file)

    ##################################
    # Get arguments from config file #
    ##################################

    params = helpers.set_parameters(arguments.config_file, arguments)

    ##################################
    #     Check given arguments      #
    ##################################

    # check target directory for existance
    helpers.check_existance(params["target_dir"])

    # check if logfile is writable
    params["log_file"] = os.path.join(params["log_path"], params["log_name"])
    helpers.check_writable(params["log_file"])

    return params


def excecute_ldapsearch_test(netgroup):
    global whitelist

    import zmq
    import socket

    context = zmq.Context()
    my_socket = context.socket(zmq.PULL)
    ip = socket.gethostbyaddr("zitpcx19282")[2][0]
    my_socket.bind("tcp://" + ip + ":51000")

    poller = zmq.Poller()
    poller.register(my_socket, zmq.POLLIN)

    socks = dict(poller.poll(1000))
    if socks and socks.get(my_socket) == zmq.POLLIN:
        print("Waiting for new whitelist")
        new_whitelist = my_socket.recv_multipart(zmq.NOBLOCK)
        print("New whitelist received: {0}".format(whitelist))

        return new_whitelist
    else:
        return whitelist


class CheckNetgroup (threading.Thread):
    def __init__(self, netgroup, lock):
        self.log = logging.getLogger("CheckNetgroup")

        self.log.debug("init")
        self.netgroup = netgroup
        self.lock = lock
        self.run_loop = True

        self.log.debug("threading.Thread init")
        threading.Thread.__init__(self)

    def run(self):
        global whitelist
        global changed_netgroup

        while self.run_loop:
#            new_whitelist = excecute_ldapsearch_test(self.netgroup)
            new_whitelist = helpers.excecute_ldapsearch(self.netgroup)

            # new elements added to whitelist
            new_elements = [e for e in new_whitelist if e not in whitelist]
            # elements which were removed from whitelist
            removed_elements = [e for e in whitelist if e not in new_whitelist]

            if new_elements or removed_elements:
                self.lock.acquire()
                # remember new whitelist
                whitelist = copy.deepcopy(new_whitelist)

                # mark that there was a change
                changed_netgroup = True
                self.lock.release()

                self.log.info("Netgroup has changed. New whitelist: {0}"
                              .format(whitelist))

            time.sleep(2)

    def stop(self):
        self.run_loop = False


class DataReceiver:
    def __init__(self):
        global whitelist

        self.transfer = None

        try:
            params = argument_parsing()
        except:
            self.log = logging.getLogger("DataReceiver")
            raise

        # enable logging
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)

        handlers = helpers.get_log_handlers(params["log_file"],
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
        check_passed, _ = helpers.check_config(["procname"], params, self.log)
        if not check_passed:
            raise Exception("Configuration check failed")
        setproctitle.setproctitle(params["procname"])

        # for proper clean up if kill is called
        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.lock = threading.Lock()

        if (params["whitelist"] is not None
                and type(params["whitelist"]) == str):
            self.lock.acquire()
            whitelist = helpers.excecute_ldapsearch(params["whitelist"])
            self.log.info("Configured whitelist: {0}".format(whitelist))
            self.lock.release()

        self.target_dir = os.path.normpath(params["target_dir"])
        self.data_ip = params["data_stream_ip"]
        self.data_port = params["data_stream_port"]

        self.log.info("Writing to directory '{0}'".format(self.target_dir))

        self.transfer = Transfer("STREAM", use_log=True)

        # only start the thread if a netgroup was configured
        if (params["whitelist"] is not None
                and type(params["whitelist"]) is not list):
            self.log.debug("Starting checking thread")
            self.checking_thread = CheckNetgroup(params["whitelist"],
                                                 self.lock)
            self.checking_thread.start()
        else:
            self.log.debug("Checking thread not started: {0}"
                           .format(params["whitelist"]))
            self.checking_thread = None

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
            raise

        # enable status check requests from any sender
        self.transfer.setopt("status_check")
        # enable confirmation reply if this is requested in a received data
        # packet
        self.transfer.setopt("confirmation")

        self.log.debug("Waiting for new messages...")
        # run loop, and wait for incoming messages
        while True:
            if changed_netgroup:
                self.log.debug("Reregistering whitelist")
                self.transfer.register(whitelist)

                # reset flag
                self.lock.acquire()
                changed_netgroup = False
                self.lock.release()

            try:
                self.transfer.store(self.target_dir, 2000)
            except KeyboardInterrupt:
                break
            except:
                self.log.error("Storing data...failed.", exc_info=True)
                raise

    def stop(self):
        if self.transfer is not None:
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
