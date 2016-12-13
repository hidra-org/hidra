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

from logutils.queue import QueueHandler
import helpers


BASEDIR = "/opt/hidra"

CONFIGPATH = "/opt/hidra/conf"

LOGPATH = os.path.join(tempfile.gettempdir(), "hidra", "logs")


#
# assume that the server listening to 51000 serves p00
#
connection_list = {
    "p00": {
        "host": "asap3-p00",
        "port": 51000
        },
    "p01": {
        "host": "asap3-bl-prx07",
        "port": 51001
        },
    "p02.1": {
        "host": "asap3-bl-prx07",
        "port": 51002
        },
    "p02.2": {
        "host": "asap3-bl-prx07",
        "port": 51003
        },
    "p03": {
        "host": "asap3-bl-prx07",
        "port": 51004
        },
    "p04": {
        "host": "asap3-bl-prx07",
        "port": 51005
        },
    "p05": {
        "host": "asap3-bl-prx07",
        "port": 51006
        },
    "p06": {
        "host": "asap3-bl-prx07",
        "port": 51007
        },
    "p07": {
        "host": "asap3-bl-prx07",
        "port": 51008
        },
    "p08": {
        "host": "asap3-bl-prx07",
        "port": 51009
        },
    "p09": {
        "host": "asap3-bl-prx07",
        "port": 51010
        },
    "p10": {
        "host": "asap3-bl-prx07",
        "port": 51011
        },
    "p11": {
        "host": "asap3-bl-prx07",
        "port": 51012
        },
    }


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

        # TODO remove TangoDevices
        # TangoDevices to talk with
        self.detectorDevice = None
        self.filewriterDevice = None

        # TODO after removal of detectorDevice and filewriterDevice:
        # change default to None
        # IP of the EIGER Detector
        self.eiger_ip = "None"
        # API version of the EIGER Detector
        self.eiger_api_version = "None"

        # Number of events stored to look for doubles
        self.history_size = None

        # Target to move the files into
        # e.g. /beamline/p11/current/raw
        self.local_target = None
        self.supported_local_targets = ["current/raw",
                                        "current/scratch_bl",
                                        "commissioning/raw",
                                        "commissioning/scratch_bl",
                                        "local"]

        # Flag describing if the data should be stored in local_target
        self.store_data = None

        # Flag describing if the files should be removed from the source
        self.remove_data = None

        # List of hosts allowed to connect to the data distribution
        self.whitelist = None

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

            return self.get(tokens[1])

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

        key = param.lower()

        if key == "eigerip":
            self.eiger_ip = value
            return "DONE"

        elif key == "eigerapiversion":
            self.eiger_api_version = value
            return "DONE"

        elif key == "historysize":
            self.history_size = value
            return "DONE"

        elif key == "localtarget" and value in self.supported_local_targets:
            self.local_target = os.path.join("/beamline", self.beamline, value)
            return "DONE"

        elif key == "storedata":
            self.store_data = value
            return "DONE"

        elif key == "removedata":
            self.remove_data = value
            return "DONE"

        elif key == "whitelist":
            self.whitelist = value
            return "DONE"

        else:
            self.log.debug("key={0}; value={1}".format(key, value))
            return "ERROR"

    def get(self, param):
        '''
        return the value of a parameter, e.g.: get localtarget
        '''
        key = param.lower()

        if key == "eigerip":
            return self.eiger_ip

        elif key == "eigerapiversion":
            return self.eiger_api_version

        elif key == "historysize":
            return self.history_size

        elif key == "localtarget":
            return os.path.relpath(self.local_target,
                                   os.path.join("/beamline", self.beamline))

        elif key == "storedata":
            return self.store_data

        elif key == "removedata":
            return self.remove_data

        elif key == "whitelist":
            return self.whitelist

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
            return self.status()

        else:
            return "ERROR"

    def __write_config(self):
        global CONFIGPATH

        #
        # see, if all required params are there.
        #
        if (self.eiger_ip
                and self.eiger_api_version
                and self.history_size
                and self.local_target
                and self.store_data is not None
                and self.remove_data is not None
                and self.whitelist):

            # TODO correct IP
            if self.beamline == "p00":
                external_ip = "asap3-p00"
#                external_ip = "131.169.251.55" # asap3-p00
                eventdetector = "inotifyx_events"
                datafetcher = "file_fetcher"
            else:
                external_ip = "asap3-bl-prx07"
#                external_ip = "131.169.251.38" # asap3-bl-prx07
                eventdetector = "http_events"
                datafetcher = "http_fetcher"

            # write configfile
            # /etc/hidra/P01.conf
            config_file = CONFIGPATH + os.sep + self.beamline + ".conf"
            self.log.info("Writing config file: {0}".format(config_file))

            with open(config_file, 'w') as f:
                f.write("log_path             = {0}\n".format(LOGPATH))
                f.write("log_name             = dataManager_{0}.log\n"
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

                f.write("eiger_ip             = {0}\n".format(self.eiger_ip))
                f.write("eiger_api_version    = {0}\n"
                        .format(self.eiger_api_version))
                f.write("history_size         = {0}\n"
                        .format(self.history_size))
                f.write("local_target         = {0}\n"
                        .format(self.local_target))
                f.write("store_data           = {0}\n".format(self.store_data))
                f.write("remove_data          = {0}\n"
                        .format(self.remove_data))
                f.write("whitelist            = {0}\n".format(self.whitelist))

                self.log.debug("Started with ext_ip: {0}".format(external_ip))
                self.log.debug("Started with event detector: {0}"
                               .format(eventdetector))
                self.log.debug("Started with data fetcher: {0}"
                               .format(datafetcher))

        else:
            self.log.debug("eiger_ip: {0}".format(self.eiger_ip))
            self.log.debug("eiger_api_version: {0}"
                           .format(self.eiger_api_version))
            self.log.debug("history_size: {0}".format(self.history_size))
            self.log.debug("localTarge: {0}".format(self.local_target))
            self.log.debug("store_data: {0}".format(self.store_data))
            self.log.debug("remove_data: {0}".format(self.remove_data))
            self.log.debug("whitelist: {0}".format(self.whitelist))
            raise Exception("Not all required parameters are specified")

    def start(self):
        '''
        start ...
        '''

        try:
            self.__write_config()
        except:
            self.log.error("ConfigFile not written", exc_info=True)
            return "ERROR"

        # check if service is running
        if self.status() == "RUNNING":
            return "ERROR"

        # start service
        p = subprocess.call(["systemctl", "start",
                             "hidra@{0}.service".format(self.beamline)])

        if p != 0:
            return "ERROR"

        # Needed because status always returns "RUNNING" in the first second
        time.sleep(1)

        # check if really running before return
        if self.status() == "RUNNING":
            return "DONE"
        else:
            return "ERROR"

    def stop(self):
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

    def status(self):
        try:
            p = subprocess.call(["systemctl", "is-active",
                                 "hidra@{0}.service".format(self.beamline)])
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
            self.log.error("Failed to start socket (bind).", exc_info=True)
            raise

        self.sckt.listen(5)

    def run(self):
        while True:
            try:
                conn, addr = self.sckt.accept()

                threading.Thread(
                    target=SocketCom,
                    args=(self.log_queue, self.beamline, conn, addr)).start()
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
    def __init__(self, log_queue, beamline, conn, addr):
        self.id = threading.current_thread().name

        self.log = self.get_logger(log_queue)

        self.controller = HidraController(beamline, self.log)
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

        # waits for new accepts on the original socket,
        # receives the newly created socket and
        # creates threads to handle each client separatly
        s = SocketServer(self.log_queue, self.beamline)

        s.run()

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
