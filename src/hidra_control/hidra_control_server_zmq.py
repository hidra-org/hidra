#!/usr/bin/env python
#
import time
import threading
import os
import sys
import socket
import subprocess
import logging
import argparse
import setproctitle
from multiprocessing import Queue
import tempfile
import json
import copy
import zmq

try:
    from logutils.queue import QueueHandler
    logutils_imported = True
except:
    logutils_imported = False

try:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.realpath(__file__))))
except:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.abspath(sys.argv[0]))))
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")
CONFIG_PATH = os.path.join(BASE_PATH, "conf")
API_PATH = os.path.join(BASE_PATH, "src", "APIs")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)
del SHARED_PATH
del CONFIG_PATH

try:
    # search in global python modules first
    import hidra
except:
    # then search in local modules
    if API_PATH not in sys.path:
        sys.path.append(API_PATH)
    del API_PATH

    import hidra

if not logutils_imported:
    from logutils.queue import QueueHandler  # noqa F811

import helpers  # noqa E402
from cfel_optarg import parse_parameters


BASEDIR = "/opt/hidra"

CONFIGPATH = "/opt/hidra/conf"

LOGPATH = os.path.join(tempfile.gettempdir(), "hidra", "logs")

beamline_config = dict()


class HidraController():
    '''
    this class holds getter/setter for all parameters
    and function members that control the operation.
    '''
    def __init__(self, beamline, log):

        # Beamline is read-only, determined by portNo
        self.beamline = beamline

        self.procname = "hidra_{0}".format(self.beamline)

        # Set log handler
        self.log = log

        self.supported_local_targets = ["current/raw",
                                        "current/scratch_bl",
                                        "commissioning/raw",
                                        "commissioning/scratch_bl",
                                        "local"]

        self.master_config = dict()

        # connection depending hidra configuration, master config one is
        # overwritten with these parameters when start is executed
        self.all_configs = dict()

        self.config_template = {
            "active": False,
            "beamline": self.beamline,
            "eiger_ip": None,
            "eiger_api_version": None,
            "history_size": None,
            "local_target": None,
            "store_data": None,
            "remove_data": None,
            "whitelist": None
        }


    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger(self.procname)
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def exec_msg(self, msg):
        '''
        set ID local_target /gpfs/current/raw
            returns DONE
        get ID local_target
            returns /gpfs/current/raw
        do ID start
            return DONE
        '''
        if len(msg) == 0:
            return "ERROR"

        if msg[0] == b"set":
            if len(msg) < 3:
                return "ERROR"

            return self.set(msg[1], msg[2], msg[3])

        elif msg[0] == b"get":
            if len(msg) != 3:
                return "ERROR"

            reply = json.dumps(self.get(msg[1], msg[2]))
            self.log.debug("reply is {0}".format(reply))

            if reply is None:
                self.log.debug("reply is None")
                reply = "None"

            return reply

        elif msg[0] == b"do":
            if len(msg) != 3:
                return "ERROR"

            return self.do(msg[1], msg[2])

        else:
            return "ERROR"

    def set(self, id, param, value):
        '''
        set a parameter, e.g.: set local_target /beamline/p11/current/raw/
        '''
        # identify the configuration for this connection
        if id in self.all_configs[id]:
            # This is a pointer
            current_config = self.all_config[id]
        else:
            self.all_confi[id] = copy.deepcopy(self.config_template)

        key = param.lower()

        # IP of the EIGER Detector
        if key == "eiger_ip":
            current_config["eiger_ip"] = value
            return_val = "DONE"

        # API version of the EIGER Detector
        elif key == "eiger_api_version":
            current_config["eiger_api_version"] = value
            return_val = "DONE"

        # Number of events stored to look for doubles
        elif key == "history_size":
            current_config["history_size"] = value
            return_val = "DONE"

        # Target to move the files into
        # e.g. /beamline/p11/current/raw
        elif key == "local_target" and value in self.supported_local_targets:
            current_config["local_target"] = os.path.join("/beamline",
                                                          self.beamline,
                                                          value)
            return_val = "DONE"

        # Flag describing if the data should be stored in local_target
        elif key == "store_data":
            current_config["store_data"] = value
            return_val = "DONE"

        # Flag describing if the files should be removed from the source
        elif key == "remove_data":
            current_config["remove_data"] = value
            return_val = "DONE"

        # List of hosts allowed to connect to the data distribution
        elif key == "whitelist":
            current_config["whitelist"] = value
            return_val = "DONE"

        else:
            self.log.debug("key={0}; value={1}".format(key, value))
            return_val = "ERROR"

        if return_val != "ERROR":
            current_config["active"] = True

        return return_val

    def get(self, id, param):
        '''
        return the value of a parameter, e.g.: get local_target
        '''
        # if the requesting client has set parameters before but has not
        # executed start yet, the previously set parameters should be
        # displayed (not the ones with which hidra was started the last time)
        # on the other hand if it is a client coming up to check with which
        # parameters the current hidra instance is running, these should be
        # shown
        if id in self.all_configs[id] and self.all_configs[id]["active"]:
            # This is a pointer
            current_config = self.all_config[id]
        else:
            current_config = self.master_config

        key = param.lower()

        if key == "eiger_ip":
            return current_config["eiger_ip"]

        elif key == "eiger_api_version":
            return current_config["eiger_api_version"]

        elif key == "history_size":
            return current_config["history_size"]

        elif key == "local_target":
            if current_config["local_target"] is None:
                return current_config["local_target"]
            return os.path.relpath(current_config["local_target"],
                                   os.path.join("/beamline", self.beamline))

        elif key == "store_data":
            return current_config["store_data"]

        elif key == "remove_data":
            return current_config["remove_data"]

        elif key == "whitelist":
            return current_config["whitelist"]

        else:
            return "ERROR"

    def do(self, id, cmd):
        '''
        executes commands
        '''
        key = cmd.lower()

        if key == "start":
            ret_val = self.start(id)
            return ret_val

        elif key == "stop":
            return self.stop()

        elif key == "restart":
            return self.restart()

        elif key == "status":
            return hidra_status(self.beamline)

        else:
            return "ERROR"

    def __write_config(self, id):
        global CONFIGPATH

        # identify the configuration for this connection
        if id in self.all_configs[id]:
            # This is a pointer
            current_config = self.all_config[id]
        else:
            self.log.debug("No current configuration found")
            return

        # if the requesting client has set parameters before these should be
        # taken. If this was not the case use the one from the previous
        # executed start
        if not current_config["active"]:
            self.log.debug("Config parameters did not change since last start")
            self.log.debug("No need to write new config file")
            return

        #
        # see, if all required params are there.
        #
        if (current_config["eiger_ip"]
                and current_config["eiger_api_version"]
                and current_config["history_size"]
                and current_config["local_target"]
                and current_config["store_data"] is not None
                and current_config["remove_data"] is not None
                and current_config["whitelist"]):

            external_ip = hidra.connection_list[self.beamline]["host"]

            # TODO set p00 to http
            if self.beamline == "p00":
                eventdetector = "inotifyx_events"
                datafetcher = "file_fetcher"
            else:
                eventdetector = "http_events"
                datafetcher = "http_fetcher"

            # write configfile
            # /etc/hidra/P01.conf
            config_file = os.path.join(CONFIGPATH, self.beamline + ".conf")
            self.log.info("Writing config file: {0}".format(config_file))

            with open(config_file, 'w') as f:
                f.write("log_path             = {0}\n".format(LOGPATH))
                f.write("log_name             = datamanager_{0}.log\n"
                        .format(self.beamline))
                f.write("log_size             = 10485760\n")
                f.write("procname             = {0}\n".format(self.procname))
                f.write("ext_ip               = {0}\n".format(external_ip))
                f.write("com_port             = 50000\n")
                f.write("request_port         = 50001\n")

                f.write("event_detector_type  = {0}\n".format(eventdetector))
                f.write('fix_subdirs          = ["commissioning", "current", '
                        '"local"]\n')
                f.write("monitored_dir        = {0}/data/source\n"
                        .format(BASEDIR))
                f.write('monitored_events     = {"IN_CLOSE_WRITE" : [".tif", '
                        '".cbf", ".nxs"]}\n')
                f.write("use_cleanup          = False\n")
                f.write("action_time          = 150\n")
                f.write("time_till_closed     = 2\n")

                f.write("data_fetcher_type    = {0}\n".format(datafetcher))

                f.write("number_of_streams    = 32\n")
                f.write("use_data_stream      = False\n")
                f.write("chunksize            = 10485760\n")

                f.write("eiger_ip             = {0}\n"
                        .format(current_config["eiger_ip"]))
                f.write("eiger_api_version    = {0}\n"
                        .format(current_config["eiger_api_version"]))
                f.write("history_size         = {0}\n"
                        .format(current_config["history_size"]))
                f.write("local_target         = {0}\n"
                        .format(current_config["local_target"]))
                f.write("store_data           = {0}\n"
                        .format(current_config["store_data"]))
                f.write("remove_data          = {0}\n"
                        .format(current_config["remove_data"]))
                f.write("whitelist            = {0}\n"
                        .format(current_config["whitelist"]))

                self.log.debug("Started with ext_ip: {0}".format(external_ip))
                self.log.debug("Started with event detector: {0}"
                               .format(eventdetector))
                self.log.debug("Started with data fetcher: {0}"
                               .format(datafetcher))

                # store the configuration parameters globally
                self.log.debug("config = {0}".format(current_config))
                for key in current_config:
                    if key != "active":
                        self.master_config[key] = (
                            copy.deepcopy(current_config[key]))

                # mark local_config as inactive
                current_config["active"] = False

        else:
            self.log.debug("eiger_ip: {0}"
                           .format(current_config["eiger_ip"]))
            self.log.debug("eiger_api_version: {0}"
                           .format(current_config["eiger_api_version"]))
            self.log.debug("history_size: {0}"
                           .format(current_config["history_size"]))
            self.log.debug("localTarge: {0}"
                           .format(current_config["local_target"]))
            self.log.debug("store_data: {0}"
                           .format(current_config["store_data"]))
            self.log.debug("remove_data: {0}"
                           .format(current_config["remove_data"]))
            self.log.debug("whitelist: {0}"
                           .format(current_config["whitelist"]))
            raise Exception("Not all required parameters are specified")

    def start(self, id):
        '''
        start ...
        '''

        # check if service is running
        if hidra_status(self.beamline) == "RUNNING":
            return "ALREADY_RUNNING"

        try:
            self.__write_config(id)
        except:
            self.log.error("Config file not written", exc_info=True)
            return "ERROR"

        # start service
        p = subprocess.call(["systemctl", "start",
                             "hidra@{0}.service".format(self.beamline)])

        if p != 0:
            return "ERROR"

        # Needed because status always returns "RUNNING" in the first second
        time.sleep(1)

        # check if really running before return
        if hidra_status(self.beamline) == "RUNNING":
            return "DONE"
        else:
            return "ERROR"

    def stop(self):
        # check if really running before return
        if hidra_status(self.beamline) != "RUNNING":
            return "ARLEADY_STOPPED"

        # stop service
        p = subprocess.call(["systemctl", "stop",
                             "hidra@{0}.service".format(self.beamline)])
        if p == 0:
            return "DONE"
        else:
            return "ERROR"

    def restart(self):
        # stop service
        reval = self.stop()

        if reval == "DONE":
            # start service
            return self.start()
        else:
            return "ERROR"


def hidra_status(beamline):
    try:
        p = subprocess.call(["systemctl", "is-active",
                             "hidra@{0}.service".format(beamline)])
    except:
        return "ERROR"

    if p == 0:
        return "RUNNING"
    else:
        return "NOT RUNNING"


class ControlServer():
    def __init__(self):
        global BASE_PATH

        arguments = self.argument_parsing()

        self.beamline = arguments.beamline

        setproctitle.setproctitle("hidra-control-server_{0}"
                                  .format(self.beamline))

        logfile = os.path.join(BASE_PATH, "logs",
                               "hidra-control-server_{0}.log"
                               .format(self.beamline))
        logsize = 10485760

        # Get queue
        self.log_queue = Queue(-1)

        # Get the log Configuration for the lisener
        if arguments.onscreen:
            h1, h2 = helpers.get_log_handlers(logfile, logsize,
                                              arguments.verbose,
                                              arguments.onscreen)

            # Start queue listener using the stream handler above.
            self.log_queue_listener = (
                helpers.CustomQueueListener(self.log_queue, h1, h2))
        else:
            h1 = helpers.get_log_handlers(logfile, logsize,
                                          arguments.verbose,
                                          arguments.onscreen)

            # Start queue listener using the stream handler above
            self.log_queue_listener = (
                helpers.CustomQueueListener(self.log_queue, h1))

        self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = self.get_logger(self.log_queue)

        self.log.info("Init")

        self.master_config = None

        # store in global variable to let other connections also
        # access the config (master_config has to contain all key
        # accessable with get
        self.__read_config()
        """
        if hidra_status(self.beamline) == "RUNNING":
            master_config = self.__read_config()
        else:
            master_config["beamline"] = self.beamline
            master_config["eiger_ip"] = "None"
            master_config["eiger_api_version"] = "None"
            master_config["history_size"] = 0
            master_config["local_target"] = None
            master_config["store_data"] = None
            master_config["remove_data"] = None
            master_config["whitelist"] = None
        """

        self.controller = HidraController(self.beamline, self.log)

        self.con_id = "tcp://{0}:{1}".format(
            socket.gethostbyaddr(
                hidra.connection_list[self.beamline]["host"])[2][0],
            hidra.connection_list[self.beamline]["port"])

        self.socket = None

        self.create_sockets()

        self.run()

    def argument_parsing(self):
        parser = argparse.ArgumentParser()

        parser.add_argument("--beamline",
                            type=str,
                            help="Beamline for which the HiDRA Server for the "
                                 "Eiger detector should be started",
                            default="p00")
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



    def __read_config(self):
        global CONFIGPATH

        # write configfile
        # /etc/hidra/P01.conf
        config_file = os.path.join(CONFIGPATH, self.beamline + ".conf")
        self.log.info("Reading config file: {0}".format(config_file))

        try:
            config = helpers.read_config(config_file)
            self.master_config = parse_parameters(config)["asection"]
        except IOError:
            self.log.debug("Configuration file available: {0}"
                           .format(config_file))
        self.log.debug("master_config={0}".format(self.master_config))

    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("ControlServer")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def create_sockets(self):

        #Create ZeroMQ context
        self.log.info("Registering ZMQ context")
        self.context = zmq.Context()

        # socket to get requests
        try:
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.con_id)
            self.log.info("Start socket (bind): '{0}'"
                          .format(self.con_id))
        except zmq.error.ZMQError:
            self.log.error("Failed to start socket (bind) zmqerror: '{0}'"
                           .format(self.con_id), exc_info=True)
            raise
        except:
            self.log.error("Failed to start socket (bind): '{0}'"
                           .format(self.con_id), exc_info=True)
            raise

    def run(self):

        while True:

            msg = self.socket.recv_multipart()
            self.log.debug("Recv {0}".format(msg))

            if len(msg) == 0:
                self.log.debug("Received empty msg")
                break

            elif msg[0] == b"bye":
                self.log.debug("Received 'bye'")
                # remove id from all_configs in HiDRAController

            elif msg[0] == b"exit":
                self.log.debug("Received 'exit'")
                self.close()
                sys.exit(1)

            reply = self.controller.exec_msg(msg)

            self.socket.send(reply)

    def stop(self):
        if self.socket:
            self.log.info("Closing Socket")
            self.socket.close()
            self.spcket = None
        if self.context:
            self.log.info("Destroying Context")
            self.context.destroy()
            self.context = None

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    ControlServer()
