#!/usr/bin/env python

from __future__ import print_function

import argparse
import logging
import os
import setproctitle
import signal

from __init__ import BASE_PATH

import helpers
from hidra import Transfer


__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

CONFIG_PATH = os.path.join(BASE_PATH, "conf")


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

    if params["whitelist"] is not None and type(params["whitelist"]) == str:
        params["whitelist"] = helpers.excecute_ldapsearch(params["whitelist"])

    ##################################
    #     Check given arguments      #
    ##################################

    # check target directory for existance
    helpers.check_existance(params["target_dir"])

    # check if logfile is writable
    params["log_file"] = os.path.join(params["log_path"], params["log_name"])
    helpers.check_writable(params["log_file"])

    return params


class DataReceiver:
    def __init__(self):
        self.transfer = None

        try:
            params = argument_parsing()
        except:
            self.log = self.get_logger()
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

        self.log = self.get_logger()

        # set process name
        check_passed, _ = helpers.check_config(["procname"] , params, self.log)
        if not check_passed:
            raise Exception("Configuration check failed")
        setproctitle.setproctitle(params["procname"])

        # for proper clean up if kill is called
        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.whitelist = params["whitelist"]

        self.log.info("Configured whitelist: {0}".format(self.whitelist))

        self.target_dir = os.path.normpath(params["target_dir"])
        self.data_ip = params["data_stream_ip"]
        self.data_port = params["data_stream_port"]

        self.log.info("Writing to directory '{0}'".format(self.target_dir))

        self.transfer = Transfer("STREAM", use_log=True)

        try:
            self.run()
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping due to unknown error condition",
                           exc_info=True)
        finally:
            self.stop()

    def get_logger(self):
        logger = logging.getLogger("DataReceiver")
        return logger

    def run(self):

        try:
            self.transfer.start([self.data_ip, self.data_port], self.whitelist)
#            self.transfer.start(self.data_port)
        except:
            self.log.error("Could not initiate stream", exc_info=True)
            raise

        self.log.debug("Waiting for new messages...")
        # run loop, and wait for incoming messages
        while True:
            try:
                self.transfer.store(self.target_dir)
            except KeyboardInterrupt:
                break
            except:
                self.log.error("Storing data...failed.", exc_info=True)
                raise

    def stop(self):
        if self.transfer:
            self.log.info("Shutting down receiver...")
            self.transfer.stop()
            self.transfer = None

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
