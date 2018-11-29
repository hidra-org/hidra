#!/usr/bin/env python
#
import time
import os
import sys
import socket
import subprocess
import argparse
import setproctitle
from multiprocessing import Queue
# import tempfile
import json
import copy
import zmq
import glob

try:
    from logutils.queue import QueueHandler
    logutils_imported = True
except:
    logutils_imported = False

try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except:
    CURRENT_DIR = os.path.dirname(os.path.realpath('__file__'))

BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
SHARED_DIR = os.path.join(BASE_DIR, "src", "shared")
CONFIG_DIR = os.path.join(BASE_DIR, "conf")
API_DIR = os.path.join(BASE_DIR, "src", "APIs")

if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)
del SHARED_DIR

try:
    # search in global python modules first
    import hidra
except:
    # then search in local modules
    if API_DIR not in sys.path:
        sys.path.insert(0, API_DIR)
    del API_DIR

    import hidra

if not logutils_imported:
    from logutils.queue import QueueHandler  # noqa F811

import utils  # noqa E402
from cfel_optarg import parse_parameters  # noqa E402

#CONFIG_DIR = "/opt/hidra/conf"
CONFIG_PREFIX = "datamanager_"

LOGDIR = os.path.join("/var", "log", "hidra")
#LOGDIR = os.path.join(BASE_DIR, "logs")

BACKUP_FILE = "/beamline/support/hidra/instances.txt"
#BACKUP_FILE = os.path.join(BASE_DIR, "src/hidra_control/instances.txt")

beamline_config = dict()


class InstanceTracking(object):
    """Handles instance tracking.
    """

    def __init__(self, beamline, log):
        self.beamline = beamline
        self.log = log

        self.instances = None
        self._set_instances()
        print(self.instances)

    def _set_instances(self):
        """Set all previously started instances.
        """

        try:
            with open(BACKUP_FILE, 'r') as f:
                self.instances = json.loads(f.read())
        except IOError:
            self.instances = {}

    def _update_instances(self):
        """Updates the instances file
        """

        with open(BACKUP_FILE, "w") as f:
            f.write(json.dumps(self.instances, sort_keys=True, indent=4))

    def add(self, det_id):
        """Mark instance as started.
        """

        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        if self.beamline in self.instances:
            self.instances[self.beamline][det_id] = timestamp
        else:
            self.instances[self.beamline] = {det_id: timestamp}

        self._update_instances()

    def remove(self, det_id):
        """Remove instance from tracking.
        """

        if self.beamline in self.instances:
            try:
                del self.instances[self.beamline][det_id]
            except KeyError:
                self.log.warning("detector {} was not found in instance "
                                 "list".format(det_id))
        else:
            self.log.warning("beamline {} was not found in instance list"
                             .format(self.beamline))

        self._update_instances()


    def restart_instances(self):
        """Restarts instances if needed.
        """

        if self.beamline not in self.instances:
            return

        for det_id in self.instances[self.beamline]:
            # check if running
            if hidra_status(self.beamline, det_id, self.log) == "RUNNING":
                self.log.info("Started hidra for {}_{}, already running"
                              .format(self.beamline, det_id))
                continue

            # restart
            if call_hidra_service("start", self.beamline, det_id, self.log) == 0:
                self.log.info("Started hidra for {}_{}"
                              .format(self.beamline, det_id))
            else:
                self.log.error("Could not start hidra for {}_{}"
                               .format(self.beamline, det_id))


class HidraController():
    """
    this class holds getter/setter for all parameters
    and function members that control the operation.
    """

    def __init__(self, beamline, log):

        # Beamline is read-only, determined by portNo
        self.beamline = beamline

        self.procname = "hidra_{}".format(self.beamline)
        self.username = "{}user".format(self.beamline)

        # Set log handler
        self.log = log

        self.fix_subdirs = ["current/raw",
                            "current/scratch_bl",
                            "commissioning/raw",
                            "commissioning/scratch_bl",
                            "local"]
        self.local_target = os.path.join("/beamline", self.beamline)

        self.master_config = dict()

        self.instances = InstanceTracking(self.beamline, self.log)
        self.instances.restart_instances()

        self.__read_config()

        # connection depending hidra configuration, master config one is
        # overwritten with these parameters when start is executed
        self.all_configs = dict()

        self.ctemplate = {
            "active": False,
            "beamline": self.beamline,
            "det_ip": None,
            "det_api_version": None,
            "history_size": None,
            "store_data": None,
            "remove_data": None,
            "whitelist": None,
            "ldapuri": None
        }

    def __read_config(self):
        global CONFIG_PREFIX
        global CONFIG_DIR

        # write configfile
        # /etc/hidra/P01.conf
        joined_path = os.path.join(CONFIG_DIR, CONFIG_PREFIX + self.beamline)
        config_files = glob.glob(joined_path + "_*.conf")
        self.log.info("Reading config files: {}".format(config_files))

        for cfile in config_files:
            # extract the detector id from the config file name (remove path,
            # prefix, beamline and ending)
            det_id = cfile.replace(joined_path + "_", "")[:-5]
            try:
                config = utils.read_config(cfile)
                self.master_config[det_id] = (
                    parse_parameters(config)["asection"])
            except IOError:
                self.log.debug("Configuration file not readable: {}"
                               .format(cfile))
        self.log.debug("master_config={0}".format(self.master_config))

    def exec_msg(self, msg):
        """
        [b"IS_ALIVE"]
            return "OK"
        [b"do", host_id, det_id, b"start"]
            return "DONE"
        [b"bye", host_id, detector]
        """
        if len(msg) == 0:
            return "ERROR"

        if msg[0] == b"IS_ALIVE":
            return b"OK"

        elif msg[0] == b"set":
            if len(msg) < 4:
                return "ERROR"

            return self.set(msg[1], msg[2], msg[3], json.loads(msg[4]))

        elif msg[0] == b"get":
            if len(msg) != 4:
                return "ERROR"

            reply = json.dumps(self.get(msg[1], msg[2], msg[3]))
            self.log.debug("reply is {0}".format(reply))

            if reply is None:
                self.log.debug("reply is None")
                reply = "None"

            return reply

        elif msg[0] == b"do":
            if len(msg) != 4:
                return "ERROR"

            return self.do(msg[1], msg[2], msg[3])

        elif msg[0] == b"bye":
            if len(msg) != 3:
                return "ERROR"

            self.log.debug("Received 'bye' from host {} for detector {}"
                           .format(msg[1], msg[2]))
            if msg[1] in self.all_configs:
                if msg[2] in self.all_configs[msg[1]]:
                    del self.all_configs[msg[1]][msg[2]]

                # no configs for this host left
                if not self.all_configs[msg[1]]:
                    del self.all_configs[msg[1]]

            return "DONE"
        else:
            return "ERROR"

    def set(self, host_id, det_id, param, value):
        """
        set a parameter
        """
        # identify the configuration for this connection
        if host_id not in self.all_configs:
            self.all_configs[host_id] = dict()
        if det_id not in self.all_configs[host_id]:
            self.all_configs[host_id][det_id] = copy.deepcopy(self.ctemplate)

        # This is a pointer
        current_config = self.all_configs[host_id][det_id]

        key = param.lower()

        supported_keys = [
            # IP of the detector
            "det_ip",
            # API version of the detector
            "det_api_version",
            # Number of events stored to look for doubles
            "history_size",
            # Flag describing if the data should be stored in local_target
            "store_data",
            # Flag describing if the files should be removed from the source
            "remove_data",
            # List of hosts allowed to connect to the data distribution
            "whitelist",
            # Ldap node and port
            "ldapuri"
        ]

        if key in supported_keys:
            current_config[key] = value
            return_val = "DONE"

        else:
            self.log.debug("key={}; value={}".format(key, value))
            return_val = "ERROR"

        if return_val != "ERROR":
            current_config["active"] = True

        return return_val

    def get(self, host_id, det_id, param):
        """
        return the value of a parameter
        """
        # if the requesting client has set parameters before but has not
        # executed start yet, the previously set parameters should be
        # displayed (not the ones with which hidra was started the last time)
        # on the other hand if it is a client coming up to check with which
        # parameters the current hidra instance is running, these should be
        # shown
        if host_id in self.all_configs \
                and det_id in self.all_configs[host_id] \
                and self.all_configs[host_id][det_id]["active"]:
            # This is a pointer
            current_config = self.all_configs[host_id][det_id]
        else:
            current_config = self.master_config[det_id]

        key = param.lower()

        supported_keys = ["det_ip",
                          "det_api_version",
                          "history_size",
                          "store_data",
                          "remove_data",
                          "whitelist",
                          "ldapuri"]

        print("key", key)
        if key == "fix_subdirs":
            return str(self.fix_subdirs)

        elif key in supported_keys:
            return current_config[key]

        else:
            return "ERROR"

    def do(self, host_id, det_id, cmd):
        """
        executes commands
        """
        key = cmd.lower()

        if key == "start":
            ret_val = self.start(host_id, det_id)
            return ret_val
        elif key == "stop":
            return self.stop(det_id)

        elif key == "restart":
            return self.restart(det_id)

        elif key == "status":
            return hidra_status(self.beamline, det_id, self.log)

        else:
            return "ERROR"

    def __write_config(self, host_id, det_id):
        global CONFIG_DIR
        global CONFIG_PREFIX

        # identify the configuration for this connection
        if host_id in self.all_configs and det_id in self.all_configs[host_id]:
            # This is a pointer
            current_config = self.all_configs[host_id][det_id]
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
        if (current_config["det_ip"]
                and current_config["det_api_version"]
                and current_config["history_size"]
                and current_config["store_data"] is not None
                and current_config["remove_data"] is not None
                and current_config["whitelist"]
                and current_config["ldapuri"]):

            external_ip = hidra.connection_list[self.beamline]["host"]

            # TODO set p00 to http
            if self.beamline == "p00":
                eventdetector = "inotifyx_events"
                datafetcher = "file_fetcher"
            else:
                eventdetector = "http_events"
                datafetcher = "http_fetcher"

            # write configfile
            # /etc/hidra/P01_eiger01.conf
            config_file = os.path.join(CONFIG_DIR,
                                       CONFIG_PREFIX + "{}_{}.conf"
                                       .format(self.beamline, det_id))
            self.log.info("Writing config file: {}".format(config_file))

            with open(config_file, 'w') as f:
                f.write("log_path = {}\n".format(LOGDIR))
                f.write("log_name = datamanager_{}.log\n"
                        .format(self.beamline))
                f.write("log_size = 10485760\n")
                f.write("procname = {}\n".format(self.procname))
                f.write("username = {}\n".format(self.username))
                f.write("ext_ip = {}\n".format(external_ip))
                f.write("com_port = 50000\n")
                f.write("request_port = 50001\n")

                f.write("event_detector_type = {}\n".format(eventdetector))
                f.write("fix_subdirs = {}\n".format(self.fix_subdirs))

                if eventdetector == "inotifyx_events":
                    f.write("monitored_dir = {}/data/source\n".format(BASE_DIR))
                    f.write('monitored_events = {"IN_CLOSE_WRITE" : '
                            '[".tif", ".cbf", ".nxs"]}\n')
                f.write("use_cleanup = False\n")
                f.write("action_time = 150\n")
                f.write("time_till_closed = 2\n")

                f.write("data_fetcher_type = {}\n".format(datafetcher))

                f.write("number_of_streams = 32\n")
                f.write("use_data_stream = False\n")
                f.write("chunksize = 10485760\n")

                f.write("local_target = {}\n".format(self.local_target))

                for key in current_config:
                    f.write(key + " = {}\n".format(current_config[key]))

                self.log.info("Started with ext_ip: {}, event detector: {},"
                              " data fetcher: {}".format(external_ip,
                                                         eventdetector,
                                                         datafetcher))

                # store the configuration parameters globally
                self.log.debug("config = {}".format(current_config))
                self.master_config[det_id] = dict()
                for key in current_config:
                    if key != "active":
                        self.master_config[det_id][key] = (
                            copy.deepcopy(current_config[key]))

                # mark local_config as inactive
                current_config["active"] = False

        else:
            for key in current_config:
                self.log.debug(key + ":" + current_config[key])
            raise Exception("Not all required parameters are specified")

    def start(self, host_id, det_id):
        """
        start ...
        """

        # check if service is running
        if hidra_status(self.beamline, det_id, self.log) == "RUNNING":
            return "ALREADY_RUNNING"

        try:
            self.__write_config(host_id, det_id)
        except:
            self.log.error("Config file not written", exc_info=True)
            return "ERROR"

        # start service
        if call_hidra_service("start", self.beamline, det_id, self.log) != 0:
            self.log.error("Could not start the service.")
            return "ERROR"

        # Needed because status always returns "RUNNING" in the first second
        time.sleep(1)

        # check if really running before return
        if hidra_status(self.beamline, det_id, self.log) != "RUNNING":
            self.log.error("Service is not running after triggering start.")
            return "ERROR"

        # remember that the instance was started
        self.instances.add(det_id)
        return "DONE"

    def stop(self, det_id):
        """
        stop ...
        """
        # check if really running before return
        if hidra_status(self.beamline, det_id, self.log) != "RUNNING":
            return "ARLEADY_STOPPED"

        # stop service
        if call_hidra_service("stop", self.beamline, det_id, self.log) != 0:
            self.log.error("Could not stop the service.")
            return "ERROR"

        self.instances.remove(det_id)
        return "DONE"

    def restart(self, det_id):
        """
        restart ...
        """
        # stop service
        reval = self.stop()

        if reval == "DONE":
            # start service
            return self.start()
        else:
            return "ERROR"


def call_hidra_service(cmd, beamline, det_id, log):
    SYSTEMD_PREFIX = "hidra@"
    SERVICE_NAME = "hidra"

#    sys_cmd = ["/home/kuhnm/Arbeit/projects/hidra/initscripts/hidra.sh",
#               "--beamline", "p00",
#               "--detector", "asap3-mon",
#               "--"+cmd]
#    return subprocess.call(sys_cmd)

    # systems using systemd
    if (os.path.exists("/usr/lib/systemd")
            and (os.path.exists("/usr/lib/systemd/{}.service"
                                .format(SYSTEMD_PREFIX))
                 or os.path.exists("/usr/lib/systemd/system/{}.service"
                                   .format(SYSTEMD_PREFIX))
                 or os.path.exists("/etc/systemd/system/{}.service"
                                   .format(SYSTEMD_PREFIX)))):

        svc = "{}{}_{}.service".format(SYSTEMD_PREFIX, beamline, det_id)
        log.debug("Call: systemctl {} {}".format(cmd, svc))
        if cmd == "status":
            return subprocess.call(["systemctl", "is-active", svc])
        else:
            return subprocess.call(["sudo", "-n", "systemctl", cmd, svc])

    # systems using init scripts
    elif os.path.exists("/etc/init.d") \
            and os.path.exists("/etc/init.d/" + SERVICE_NAME):
        log.debug("Call: service {} {}".format(cmd, svc))
        return subprocess.call(["service", SERVICE_NAME, cmd])
        # TODO implement beamline and det_id in hisdra.sh
        # return subprocess.call(["service", SERVICE_NAME, "status",
        #                         beamline, det_id])
    else:
        log.debug("Call: no service to call found")


def hidra_status(beamline, det_id, log):
    try:
        p = call_hidra_service("status", beamline, det_id, log)
    except:
        return "ERROR"

    if p == 0:
        return "RUNNING"
    else:
        return "NOT RUNNING"


class ControlServer():
    def __init__(self):

        arguments = self.argument_parsing()

        self.beamline = arguments.beamline

        setproctitle.setproctitle("hidra-control-server_{}"
                                  .format(self.beamline))

        logfile = os.path.join(LOGDIR, "hidra-control-server_{}.log"
                                       .format(self.beamline))
        logsize = 10485760

        # Get queue
        self.log_queue = Queue(-1)

        # Get the log Configuration for the lisener
        if arguments.onscreen:
            h1, h2 = utils.get_log_handlers(logfile, logsize,
                                            arguments.verbose,
                                            arguments.onscreen)

            # Start queue listener using the stream handler above.
            self.log_queue_listener = (
                utils.CustomQueueListener(self.log_queue, h1, h2))
        else:
            h1 = utils.get_log_handlers(logfile, logsize,
                                        arguments.verbose,
                                        arguments.onscreen)

            # Start queue listener using the stream handler above
            self.log_queue_listener = (
                utils.CustomQueueListener(self.log_queue, h1))

        self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = utils.get_logger("ControlServer", self.log_queue)

        self.log.info("Init")

        self.master_config = None

        self.controller = HidraController(self.beamline, self.log)

        host = hidra.connection_list[self.beamline]["host"]
        host = socket.gethostbyaddr(host)[2][0]
        port = hidra.connection_list[self.beamline]["port"]
        self.endpoint = "tcp://{}:{}".format(host, port)

        self.socket = None

        self.create_sockets()

        self.run()

    def argument_parsing(self):
        parser = argparse.ArgumentParser()

        parser.add_argument("--beamline",
                            type=str,
                            help="Beamline for which the HiDRA Server "
                                 "(detector mode) should be started",
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

    def create_sockets(self):
        # Create ZeroMQ context
        self.log.info("Registering ZMQ context")
        self.context = zmq.Context()

        # socket to get requests
        try:
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.endpoint)
            self.log.info("Start socket (bind): '{}'"
                          .format(self.endpoint))
        except zmq.error.ZMQError:
            self.log.error("Failed to start socket (bind) zmqerror: '{}'"
                           .format(self.endpoint), exc_info=True)
            raise
        except:
            self.log.error("Failed to start socket (bind): '{}'"
                           .format(self.endpoint), exc_info=True)
            raise

    def run(self):
        while True:
            try:
                msg = self.socket.recv_multipart()
                self.log.debug("Recv {}".format(msg))
            except KeyboardInterrupt:
                break

            if len(msg) == 0:
                self.log.debug("Received empty msg")
                break

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
