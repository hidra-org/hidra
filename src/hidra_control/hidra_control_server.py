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

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)
del SHARED_PATH
del CONFIG_PATH

try:
    # search in global python modules first
    from hidra import connection_list
except:
    # then search in local modules
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.realpath(__file__))))
    API_PATH = os.path.join(BASE_PATH, "src", "APIs")

    if API_PATH not in sys.path:
        sys.path.append(API_PATH)
    del API_PATH

    from hidra import connection_list

from logutils.queue import QueueHandler
import helpers


BASEDIR = "/opt/hidra"

CONFIGPATH = "/opt/hidra/conf"

LOGPATH = os.path.join(tempfile.gettempdir(), "hidra", "logs")

beamline_config = dict()


class HidraController():
    '''
    this class holds getter/setter for all parameters
    and function members that control the operation.
    '''
    def __init__(self, beamline, lock, log):
        global beamline_config

        # Beamline is read-only, determined by portNo
        self.beamline = beamline

        self.procname = "hidra_{0}".format(self.beamline)

        # lock to access in global variable from multiple threads
        self.lock = lock

        # Set log handler
        self.log = log

        self.supported_local_targets = ["current/raw",
                                        "current/scratch_bl",
                                        "commissioning/raw",
                                        "commissioning/scratch_bl",
                                        "local"]

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
        set filedir /gpfs/current/raw
          returns DONE
        get filedir
          returns /gpfs/current/raw
        do reset
          return DONE
        '''
        tokens = msg.split(' ', 2)

        if len(tokens) == 0:
            return "ERROR"

        if tokens[0].lower() == 'set':
            if len(tokens) < 3:
                return "ERROR"

            return self.set(tokens[1], tokens[2])

        elif tokens[0].lower() == 'get':
            if len(tokens) != 2:
                return "ERROR"

            reply = json.dumps(self.get(tokens[1]))
            self.log.debug("reply is {0}".format(reply))

            if reply is None:
                self.log.debug("reply is None")
                reply = "None"

            return reply

        elif tokens[0].lower() == 'do':
            if len(tokens) != 2:
                return "ERROR"

            return self.do(tokens[1])

        else:
            return "ERROR"

    def set(self, param, value):
        '''
        set a parameter, e.g.: set local_target /beamline/p11/current/raw/
        '''
        global beamline_config

        key = param.lower()

        self.lock.acquire()
        # IP of the EIGER Detector
        if key == "eiger_ip":
            beamline_config["eiger_ip"] = value
            return_val = "DONE"

        # API version of the EIGER Detector
        elif key == "eiger_api_version":
            beamline_config["eiger_api_version"] = value
            return_val = "DONE"

        # Number of events stored to look for doubles
        elif key == "history_size":
            beamline_config["history_size"] = value
            return_val = "DONE"

        # Target to move the files into
        # e.g. /beamline/p11/current/raw
        elif key == "local_target" and value in self.supported_local_targets:
            beamline_config["local_target"] = os.path.join("/beamline",
                                                           self.beamline,
                                                           value)
            return_val = "DONE"

        # Flag describing if the data should be stored in local_target
        elif key == "store_data":
            beamline_config["store_data"] = value
            return_val = "DONE"

        # Flag describing if the files should be removed from the source
        elif key == "remove_data":
            beamline_config["remove_data"] = value
            return_val = "DONE"

        # List of hosts allowed to connect to the data distribution
        elif key == "whitelist":
            beamline_config["whitelist"] = value
            return_val = "DONE"

        else:
            self.log.debug("key={0}; value={1}".format(key, value))
            return_val = "ERROR"
        self.lock.release()

        return return_val

    def get(self, param):
        '''
        return the value of a parameter, e.g.: get local_target
        '''
        global beamline_config

        key = param.lower()

        if key == "eiger_ip":
            return beamline_config["eiger_ip"]

        elif key == "eiger_api_version":
            return beamline_config["eiger_api_version"]

        elif key == "history_size":
            return beamline_config["history_size"]

        elif key == "local_target":
            if beamline_config["local_target"] is None:
                return beamline_config["local_target"]
            return os.path.relpath(beamline_config["local_target"],
                                   os.path.join("/beamline", self.beamline))

        elif key == "store_data":
            return beamline_config["store_data"]

        elif key == "remove_data":
            return beamline_config["remove_data"]

        elif key == "whitelist":
            return beamline_config["whitelist"]

        else:
            return "ERROR"

    def do(self, cmd):
        '''
        executes commands
        '''
        key = cmd.lower()

        if key == "start":
            return self.start()

        elif key == "stop":
            return self.stop()

        elif key == "restart":
            return self.restart()

        elif key == "status":
            return hidra_status(self.beamline)

        else:
            return "ERROR"

    def __write_config(self):
        global CONFIGPATH
        global connection_list
        global beamline_config

        #
        # see, if all required params are there.
        #
        if (beamline_config["eiger_ip"]
                and beamline_config["eiger_api_version"]
                and beamline_config["history_size"]
                and beamline_config["local_target"]
                and beamline_config["store_data"] is not None
                and beamline_config["remove_data"] is not None
                and beamline_config["whitelist"]):

            external_ip = connection_list[self.beamline]["host"]

            # TODO set p00 to http
            if self.beamline == "p00":
                eventdetector = "inotifyx_events"
                datafetcher = "file_fetcher"
            else:
                eventdetector = "http_events"
                datafetcher = "http_fetcher"

            # write configfile
            # /etc/hidra/P01.conf
            config_file = CONFIGPATH + os.sep + self.beamline + ".conf"
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

                f.write("number_of_streams    = 1\n")
                f.write("use_data_stream      = False\n")
                f.write("chunksize            = 10485760\n")

                f.write("eiger_ip             = {0}\n"
                        .format(beamline_config["eiger_ip"]))
                f.write("eiger_api_version    = {0}\n"
                        .format(beamline_config["eiger_api_version"]))
                f.write("history_size         = {0}\n"
                        .format(beamline_config["history_size"]))
                f.write("local_target         = {0}\n"
                        .format(beamline_config["local_target"]))
                f.write("store_data           = {0}\n"
                        .format(beamline_config["store_data"]))
                f.write("remove_data          = {0}\n"
                        .format(beamline_config["remove_data"]))
                f.write("whitelist            = {0}\n"
                        .format(beamline_config["whitelist"]))

                self.log.debug("Started with ext_ip: {0}".format(external_ip))
                self.log.debug("Started with event detector: {0}"
                               .format(eventdetector))
                self.log.debug("Started with data fetcher: {0}"
                               .format(datafetcher))

        else:
            self.log.debug("eiger_ip: {0}".format(beamline_config["eiger_ip"]))
            self.log.debug("eiger_api_version: {0}"
                           .format(beamline_config["eiger_api_version"]))
            self.log.debug("history_size: {0}".format(beamline_config["history_size"]))
            self.log.debug("localTarge: {0}".format(beamline_config["local_target"]))
            self.log.debug("store_data: {0}".format(beamline_config["store_data"]))
            self.log.debug("remove_data: {0}".format(beamline_config["remove_data"]))
            self.log.debug("whitelist: {0}".format(beamline_config["whitelist"]))
            raise Exception("Not all required parameters are specified")

    def start(self):
        '''
        start ...
        '''

        try:
            self.__write_config()
        except:
            self.log.error("Config file not written", exc_info=True)
            return "ERROR"

        # check if service is running
        if hidra_status(self.beamline) == "RUNNING":
            return "ALREADY RUNNING"

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
            return "ARLEADY STOPPED"

        # stop service
        p = subprocess.call(["systemctl", "stop",
                             "hidra@{0}.service".format(self.beamline)])
        return "DONE"

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


class SocketServer(object):
    '''
    one socket for the port, accept() generates new sockets
    '''
    global connection_list

    def __init__(self, log_queue, beamline):
        global connection_list

        self.log_queue = log_queue

        self.log = self.get_logger(log_queue)

        self.beamline = beamline
        self.log.debug("SocketServer startet for beamline {0}"
                       .format(self.beamline))

        self.host = connection_list[self.beamline]["host"]
        self.port = connection_list[self.beamline]["port"]
        self.conns = []
        self.socket = None

        self.lock = threading.Lock()

        self.create_socket()

    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("SocketServer")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def create_socket(self):

        try:
            self.sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.log.debug("Socket created.")
        except Exception:
            self.log.error("Creation of socket failed", exc_info=True)
            sys.exit()

        try:
            self.sckt.bind((self.host, self.port))
            self.log.info("Start socket (bind):  {0}, {1}"
                          .format(self.host, self.port))
        except Exception:
            self.log.error("Failed to start socket (bind): {0}, {1}"
                           .format(self.host, self.port), exc_info=True)
            raise

        self.sckt.listen(5)

    def run(self):
        while True:
            try:
                conn, addr = self.sckt.accept()

                threading.Thread(
                    target=SocketCom,
                    args=(self.log_queue,
                          self.beamline,
                          conn,
                          addr,
                          self.lock)
                ).start()
            except KeyboardInterrupt:
                break
            except Exception:
                self.log.error("Stopped due to unknown error", exc_info=True)
                break

    def finish(self):
        if self.sckt:
            self.log.info("Closing Socket")
            self.sckt.close()
            self.sckt = None

    def __exit__(self):
        self.finish()

    def __del__(self):
        self.finish()


class SocketCom ():
    def __init__(self, log_queue, beamline, conn, addr, lock):
        self.id = threading.current_thread().name

        self.log = self.get_logger(log_queue)

        self.controller = HidraController(beamline, lock, self.log)
        self.conn = conn
        self.addr = addr

        self.run()

    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("SocketCom_{0}".format(self.id))
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def run(self):

        while True:

            msg = self.recv()

            # These calls return the number of bytes received, or -1 if an
            # error occurred.The return value will be 0 when the peer has
            # performed an orderly shutdown.
            # see: http://man7.org/linux/man-pages/man2/recv.2.html
            if len(msg) == 0:
                self.log.debug("Received empty msg")
                break

            elif msg.lower().find('bye') == 0:
                self.log.debug("Received 'bye'")
                self.close()
                break

            elif msg.find('exit') >= 0:
                self.log.debug("Received 'exit'")
                self.close()
                sys.exit(1)

            reply = self.controller.exec_msg(msg)

            if self.send(reply) == 0:
                self.close()
                break

    def recv(self):
        argout = None
        try:
            argout = self.conn.recv(1024)
        except Exception as e:
            print e
            argout = None

        self.log.debug("Recv (len {0: <2}): {1}"
                       .format(len(argout.strip()), argout.strip()))

        return argout.strip()

    def send(self, msg):
        try:
            argout = self.conn.send(msg)
        except:
            argout = ""

        self.log.debug("Send (len {0: <2}): {1}".format(argout, msg))

        return argout

    def close(self):
        #
        # close the 'accepted' socket only, not the main socket
        # because it may still be in use by another client
        #
        if self.conn:
            self.log.info("Closing connection")
            self.conn.close()
            self.conn = None

    def __exit__(self):
        self.close()

    def __del__(self):
        self.close()


def argument_parsing():
    parser = argparse.ArgumentParser()

    parser.add_argument("--beamline",
                        type=str,
                        help="Beamline for which the HiDRA Server for the "
                             "Eiger detector should be started",
                        default="p00")
    return parser.parse_args()


class HidraControlServer():
    def __init__(self):
        global BASE_PATH
        global beamline_config

        arguments = argument_parsing()

        self.beamline = arguments.beamline

        setproctitle.setproctitle("hidra-control-server_{0}"
                                  .format(self.beamline))

        onscreen = False
#        onscreen = "debug"
        verbose = True
        logfile = os.path.join(BASE_PATH, "logs",
                               "hidra-control-server_{0}.log"
                               .format(self.beamline))
        logsize = 10485760

        # Get queue
        self.log_queue = Queue(-1)

        # Get the log Configuration for the lisener
        if onscreen:
            h1, h2 = helpers.get_log_handlers(logfile, logsize,
                                              verbose, onscreen)

            # Start queue listener using the stream handler above.
            self.log_queue_listener = (
                helpers.CustomQueueListener(self.log_queue, h1, h2))
        else:
            h1 = helpers.get_log_handlers(logfile, logsize,
                                          verbose, onscreen)

            # Start queue listener using the stream handler above
            self.log_queue_listener = (
                helpers.CustomQueueListener(self.log_queue, h1))

        self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = self.get_logger(self.log_queue)

        self.log.info("Init")

        # store in global variable to let other connections also
        # access the config (beamline_config has to contain all key
        # accessable with get
        self.__read_config()
        """
        if hidra_status(self.beamline) == "RUNNING":
            beamline_config = self.__read_config()
        else:
            beamline_config["beamline"] = self.beamline
            beamline_config["eiger_ip"] = "None"
            beamline_config["eiger_api_version"] = "None"
            beamline_config["history_size"] = 0
            beamline_config["local_target"] = None
            beamline_config["store_data"] = None
            beamline_config["remove_data"] = None
            beamline_config["whitelist"] = None
        """

        # waits for new accepts on the original socket,
        # receives the newly created socket and
        # creates threads to handle each client separatly
        s = SocketServer(self.log_queue, self.beamline)

        s.run()

    def __read_config(self):
        global CONFIGPATH
        global beamline_config

        # write configfile
        # /etc/hidra/P01.conf
        config_file = CONFIGPATH + os.sep + self.beamline + ".conf"
        self.log.info("Reading config file: {0}".format(config_file))

        try:
            config = helpers.read_config(config_file)
            beamline_config = helpers.parse_config(config)["asection"]
        except IOError:
            self.log.debug("Configuration file available: {0}"
                           .format(config_file))
        self.log.debug("beamline_config={0}".format(beamline_config))

    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("HidraControlServer")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


if __name__ == '__main__':
    t = HidraControlServer()
