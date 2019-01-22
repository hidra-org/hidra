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
This server configures and starts up hidra.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import copy
import glob
import json
import os
import sys
import socket
import subprocess
import time
from multiprocessing import Queue

import setproctitle
from six import iteritems
import zmq

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
CONFIG_DIR = os.path.join(BASE_DIR, "conf")
API_DIR = os.path.join(BASE_DIR, "src", "APIs")

if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)
del API_DIR

# pylint: disable=wrong-import-position

import hidra  # noqa E402
import hidra.utils  as utils # noqa E402

CONFIG_PREFIX = "datamanager_"


class InstanceTracking(object):
    """Handles instance tracking.
    """

    def __init__(self, beamline, backup_file, log):
        self.beamline = beamline
        self.backup_file = backup_file
        self.log = log

        self.instances = None
        self._set_instances()

    def _set_instances(self):
        """Set all previously started instances.
        """

        try:
            with open(self.backup_file, 'r') as f:
                self.instances = json.loads(f.read())
        except IOError:
            self.instances = {}

    def _update_instances(self):
        """Updates the instances file
        """

        with open(self.backup_file, "w") as f:
            f.write(json.dumps(self.instances, sort_keys=True, indent=4))

    def get_instances(self):
        """Get all previously started instances

        Returns:
            A dictionary containing the instances of the form:
            { <beamline>: { <detector>: timestamp } }
        """

        return self.instances

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
                self.log.warning("detector %s was not found in instance "
                                 "list", det_id)
        else:
            self.log.warning("beamline %s was not found in instance list",
                             self.beamline)

        self._update_instances()

    def restart_instances(self):
        """Restarts instances if needed.
        """

        if self.beamline not in self.instances:
            return

        for det_id in self.instances[self.beamline]:
            # check if running
            if hidra_status(self.beamline, det_id, self.log) == "RUNNING":
                self.log.info("Started hidra for %s_%s, already running",
                              self.beamline, det_id)
                continue

            # restart
            if call_hidra_service("start",
                                  self.beamline,
                                  det_id,
                                  self.log) == 0:
                self.log.info("Started hidra for %s_%s",
                              self.beamline, det_id)
            else:
                self.log.error("Could not start hidra for %s_%s",
                               self.beamline, det_id)


class HidraController(object):
    """
    This class holds getter/setter for all parameters
    and function members that control the operation.
    """

    def __init__(self, beamline, config, log):

        # Beamline is read-only, determined by portNo
        self.beamline = beamline
        self.config = config

        # Set log handler
        self.log = log

        self.master_config = dict()

        backup_file = self.config["general"]["backup_file"]
        self.instances = InstanceTracking(self.beamline, backup_file, self.log)
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

        # write configfile
        # /etc/hidra/P01.conf
        joined_path = os.path.join(CONFIG_DIR, CONFIG_PREFIX + self.beamline)
        config_files = glob.glob(joined_path + "_*.conf")
        self.log.info("Reading config files: %s", config_files)

        for cfile in config_files:
            # extract the detector id from the config file name (remove path,
            # prefix, beamline and ending)
            det_id = cfile.replace(joined_path + "_", "")[:-5]
            try:
                config = utils.load_config(cfile)
                self.master_config[det_id] = (
                    utils.parse_parameters(config)["asection"])
            except IOError:
                self.log.debug("Configuration file not readable: %s", cfile)
        self.log.debug("master_config=%s", self.master_config)

    def exec_msg(self, msg):
        """
        [b"IS_ALIVE"]
            return "OK"
        [b"do", host_id, det_id, b"start"]
            return "DONE"
        [b"bye", host_id, detector]
        """
        if len(msg) == 0:
            return b"ERROR"

        if msg[0] == b"IS_ALIVE":
            return b"OK"

        elif msg[0] == b"set":
            if len(msg) < 4:
                return b"ERROR"
            _, host_id, det_id, param, value = msg

            return self.set(host_id,
                            socket.getfqdn(det_id),
                            param,
                            json.loads(value))

        elif msg[0] == b"get":
            if len(msg) != 4:
                return b"ERROR"
            _, host_id, det_id, param = msg

            reply = json.dumps(
                self.get(host_id,
                         socket.getfqdn(det_id),
                         param)
            ).encode("utf-8")
            self.log.debug("reply is %s", reply)

            if reply is None:
                self.log.debug("reply is None")
                reply = b"None"

            return reply

        elif msg[0] == b"do":
            if len(msg) != 4:
                return b"ERROR"

            _, host_id, det_id, cmd = msg
            return self.do(host_id, socket.getfqdn(det_id), cmd)

        elif msg[0] == b"bye":
            if len(msg) != 3:
                return b"ERROR"

            _, host_id, det_id = msg
            det_id = socket.getfqdn(det_id)

            self.log.debug("Received 'bye' from host %s for detector %s",
                           host_id, det_id)

            if host_id in self.all_configs:
                try:
                    del self.all_configs[host_id][det_id]
                except KeyError:
                    pass

                # no configs for this host left
                if not self.all_configs[host_id]:
                    del self.all_configs[host_id]

            return b"DONE"
        else:
            return b"ERROR"

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
            return_val = b"DONE"

        else:
            self.log.debug("key=%s; value=%s", key, value)
            return_val = b"ERROR"

        if return_val != b"ERROR":
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

        if key == "fix_subdirs":
            return str(self.config["hidraconfig_static"]["fix_subdirs"])

        elif key in supported_keys:
            return current_config[key]

        else:
            return b"ERROR"

    def do(self, host_id, det_id, cmd):  # pylint: disable=invalid-name
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
            return self.restart(host_id, det_id)

        elif key == "status":
            return hidra_status(self.beamline, det_id, self.log)

        elif key == "get_instances":
            return self.get_instances()

        else:
            return b"ERROR"

    def __write_config(self, host_id, det_id):
        # pylint: disable=global-variable-not-assigned
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

            # static config
            config_to_write = self.config["hidraconfig_static"]

            # add variable config
            config_var = self.config["hidraconfig_variable"]
            username = config_var["username"].format(bl=self.beamline)
            procname_prefix = config_var["procname"].format(bl=self.beamline)
            procname = "{}_{}".format(procname_prefix, det_id)
            log_name_prefix = config_var["log_name"].format(bl=self.beamline)
            log_name = "{}_{}.log".format(log_name_prefix, det_id)
            local_target = config_var["local_target"].format(bl=self.beamline)
            external_ip = hidra.CONNECTION_LIST[self.beamline]["host"]

            config_to_write["log_name"] = log_name
            config_to_write["procname"] = procname
            config_to_write["username"] = username
            config_to_write["ext_ip"] = external_ip
            config_to_write["local_target"] = local_target

            # dynamic config
            config_to_write.update(current_config)

            # write configfile
            # /etc/hidra/P01_eiger01.conf
            config_file = os.path.join(
                CONFIG_DIR,
                self.config["general"]["hidra_config_name"]
                .format(bl=self.beamline, det=det_id)
            )
            self.log.info("Writing config file: {}".format(config_file))

            with open(config_file, 'w') as f:
                for key, value in iteritems(config_to_write):
                    f.write("{} = {}\n".format(key, value))

            config_static = self.config["hidraconfig_static"]
            eventdetector = config_static["eventdetector_type"]
            datafetcher = config_static["datafetcher_type"]
            self.log.info(
                "Started withdd ext_ip: %s, event detector: %s, "
                "data fetcher: %s",
                external_ip, eventdetector, datafetcher
            )

            # store the dynamic config globally
            self.log.debug("config = {}", current_config)
            self.master_config[det_id] = copy.deepcopy(current_config)
            # this information shout not go into the master config
            del self.master_config[det_id]["active"]

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
        if hidra_status(self.beamline, det_id, self.log) == b"RUNNING":
            return b"ALREADY_RUNNING"

        try:
            self.__write_config(host_id, det_id)
        except Exception:
            self.log.error("Config file not written", exc_info=True)
            return b"ERROR"

        # start service
        if call_hidra_service("start", self.beamline, det_id, self.log) != 0:
            self.log.error("Could not start the service.")
            return b"ERROR"

        # Needed because status always returns "RUNNING" in the first second
        time.sleep(1)

        # check if really running before return
        if hidra_status(self.beamline, det_id, self.log) != b"RUNNING":
            self.log.error("Service is not running after triggering start.")
            return b"ERROR"

        # remember that the instance was started
        self.instances.add(det_id)
        return b"DONE"

    def get_instances(self):
        """Get the started hidra instances

        Returns:
            List of detectors started for this beamline as json dump.
        """

        bl_instances = self.instances.get_instances()[self.beamline]
        return json.dumps(list(bl_instances.keys())).encode("utf-8")

    def stop(self, det_id):
        """
        stop ...
        """
        # check if really running before return
        if hidra_status(self.beamline, det_id, self.log) != b"RUNNING":
            return b"ARLEADY_STOPPED"

        # stop service
        if call_hidra_service("stop", self.beamline, det_id, self.log) != 0:
            self.log.error("Could not stop the service.")
            return b"ERROR"

        self.instances.remove(det_id)
        return b"DONE"

    def restart(self, host_id, det_id):
        """
        restart ...
        """
        # stop service
        reval = self.stop(det_id)

        if reval == b"DONE":
            # start service
            return self.start(host_id, det_id)
        else:
            return b"ERROR"


def call_hidra_service(cmd, beamline, det_id, log):
    """Command hidra (e.g. start, stop, statu,...).

    Args:
        beamline: For which beamline to command hidra.
        det_id: Which detector to command hidra for.
        log: log handler.

    Returns:
        Return value of the systemd or service call.
    """

    systemd_prefix = "hidra@"
    service_name = "hidra"

#    sys_cmd = ["/home/kuhnm/Arbeit/projects/hidra/initscripts/hidra.sh",
#               "--beamline", "p00",
#               "--detector", "asap3-mon",
#               "--"+cmd]
#    return subprocess.call(sys_cmd)

    # systems using systemd
    if (os.path.exists("/usr/lib/systemd")
            and (os.path.exists("/usr/lib/systemd/{}.service"
                                .format(systemd_prefix))
                 or os.path.exists("/usr/lib/systemd/system/{}.service"
                                   .format(systemd_prefix))
                 or os.path.exists("/etc/systemd/system/{}.service"
                                   .format(systemd_prefix)))):

        svc = "{}{}_{}.service".format(systemd_prefix, beamline, det_id)
        log.debug("Call: systemctl %s %s", cmd, svc)
        if cmd == "status":
            return subprocess.call(["systemctl", "is-active", svc])
        else:
            return subprocess.call(["sudo", "-n", "systemctl", cmd, svc])

    # systems using init scripts
    elif os.path.exists("/etc/init.d") \
            and os.path.exists("/etc/init.d/" + service_name):
        log.debug("Call: service %s %s", cmd, svc)
        return subprocess.call(["service", service_name, cmd])
        # TODO implement beamline and det_id in hisdra.sh
        # return subprocess.call(["service", service_name, "status",
        #                         beamline, det_id])
    else:
        log.debug("Call: no service to call found")


def hidra_status(beamline, det_id, log):
    """Request hidra status.

    Args:
        beamline: For which beamline to command hidra.
        det_id: Which detector to command hidra for.
        log: log handler.

    Returns:
        A string describing the status:
            'RUNNING'
            'NOT RUNNING'
            'ERROR'
    """

    try:
        proc = call_hidra_service("status", beamline, det_id, log)
    except Exception:
        return b"ERROR"

    if proc == 0:
        return b"RUNNING"
    else:
        return b"NOT RUNNING"


def argument_parsing():
    """Parsing of command line arguments.
    """

    config_file = os.path.join(CONFIG_DIR, "control_server.yaml")

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

    arguments = parser.parse_args()

    # convert to dict and map to config section
    arguments = {"general": vars(arguments)}

    # ------------------------------------------------------------------------
    # Get arguments from config file and comand line
    # ------------------------------------------------------------------------
    utils.check_existance(config_file)

    config = utils.load_config(config_file)
    utils.update_dict(arguments, config)

    return config


class ControlServer(object):
    """The main server class.
    """

    def __init__(self):

        self.beamline = None
        self.context = None
        self.socket = None

        self.master_config = None
        self.controller = None
        self.endpoint = None

        self.log_queue = None
        self.log_queue_listener = None

        self._setup()

    def _setup(self):

        config = argument_parsing()

        # shortcut for simpler use
        config_g = config["general"]

        self.beamline = config_g["beamline"]

        setproctitle.setproctitle(config_g["procname"]
                                  .format(bl=self.beamline))

        logfile = os.path.join(
            config_g["log_path"],
            config_g["log_name"].format(bl=self.beamline)
        )

        # Get queue
        self.log_queue = Queue(-1)

        # Get the log Configuration for the lisener
        if config_g["onscreen"]:
            handler1, handler2 = utils.get_log_handlers(
                logfile,
                config_g["log_size"],
                config_g["verbose"],
                config_g["onscreen"]
            )

            # Start queue listener using the stream handler above.
            self.log_queue_listener = utils.CustomQueueListener(
                self.log_queue, handler1, handler2
            )
        else:
            handler1 = utils.get_log_handlers(
                logfile,
                config_g["log_size"],
                config_g["verbose"],
                config_g["onscreen"]
            )

            # Start queue listener using the stream handler above
            self.log_queue_listener = utils.CustomQueueListener(
                self.log_queue, handler1
            )

        self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = utils.get_logger("ControlServer", self.log_queue)

        self.log.info("Init")

        self.controller = HidraController(self.beamline, config, self.log)

        host = hidra.CONNECTION_LIST[self.beamline]["host"]
        host = socket.gethostbyaddr(host)[2][0]
        port = hidra.CONNECTION_LIST[self.beamline]["port"]
        self.endpoint = "tcp://{}:{}".format(host, port)

        self._create_sockets()

        self.run()

    def _create_sockets(self):

        # Create ZeroMQ context
        self.log.info("Registering ZMQ context")
        self.context = zmq.Context()

        # socket to get requests
        try:
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.endpoint)
            self.log.info("Start socket (bind): '%s'", self.endpoint)
        except zmq.error.ZMQError:
            self.log.error("Failed to start socket (bind) zmqerror: '%s'",
                           self.endpoint, exc_info=True)
            raise
        except Exception:
            self.log.error("Failed to start socket (bind): '%s'",
                           self.endpoint, exc_info=True)
            raise

    def run(self):
        """Waiting for new control commands and execute them.
        """

        while True:
            try:
                msg = self.socket.recv_multipart()
                self.log.debug("Recv %s", msg)
            except KeyboardInterrupt:
                break

            if len(msg) == 0:
                self.log.debug("Received empty msg")
                break

            elif msg[0] == b"exit":
                self.log.debug("Received 'exit'")
                self.stop()
                sys.exit(1)

            reply = self.controller.exec_msg(msg)

            self.socket.send(reply)

    def stop(self):
        """Clean up zmq sockets.
        """

        if self.socket:
            self.log.info("Closing Socket")
            self.socket.close()
            self.socket = None
        if self.context:
            self.log.info("Destroying Context")
            self.context.destroy()
            self.context = None

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    ControlServer()
