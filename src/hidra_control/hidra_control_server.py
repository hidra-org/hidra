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

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

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
import hidra.utils as utils  # noqa E402
from hidra.utils import FormatError  # noqa E402


REPLYCODES = utils.ReplyCodes(
    error=b"ERROR",
    ok=b"OK",
    done=b"DONE",
    running=b"RUNNING",
    not_running=b"NOT_RUNNING",
    already_running=b"ALREADY_RUNNING",
    already_stopped=b"ARLEADY_STOPPED"
)
CONFIG_PREFIX = "datamanager_"


class HidraServiceHandling(object):
    """
    Implements service handling.
    """

    def __init__(self, beamline, log):

        self.beamline = beamline
        self.log = log
        self.service_conf = {}
        self.reply_codes = REPLYCODES

        self.call_hidra_service = None

        self.__setup()

    # prevent overwriting from subclass
    def __setup(self):

        self.__set_service_conf()

        if self.service_conf["manager"] == "systemd":
            self.call_hidra_service = self._call_systemd

        elif self.service_conf["manager"] == "init":
            self.call_hidra_service = self._call_init_script

        else:
            self.log.debug("Call: no service to call found")

    def __set_service_conf(self):
        systemd_prefix = "hidra@"
        service_name = "hidra"
        service_manager = utils.get_service_manager(
            systemd_prefix=systemd_prefix,
            service_name=service_name
        )

        self.service_conf["manager"] = service_manager
        if service_manager == "systemd":
            self.service_conf["name"] = service_name
            self.service_conf["prefix"] = systemd_prefix
            self.service_conf["template"] = (
                "{}{}".format(self.service_conf["prefix"], self.beamline)
                + "_{}.service"
            )
        else:
            self.service_conf["name"] = service_name
            self.service_conf["prefix"] = None
            self.service_conf["template"] = None

    def hidra_status(self, det_id):
        """Request hidra status.

        Args:
            det_id: Which detector to command hidra for.

        Returns:
            A string describing the status:
                'RUNNING'
                'NOT RUNNING'
                'ERROR'
        """

        try:
            proc = self.call_hidra_service("status", det_id)
        except Exception:
            return self.reply_codes.error

        if proc == 0:
            return self.reply_codes.running
        else:
            return self.reply_codes.not_running

    def _call_systemd(self, cmd, det_id):
        """Command hidra (e.g. start, stop, status,...).

        Args:
            cmd: The command to call the service with
                 (e.g. start, stop, status,...).
            det_id: Which detector to command hidra for.

        Returns:
            Return value of the systemd call.
        """

        svc = self.service_conf["template"].format(det_id)
        status_call = ["/bin/systemctl", "is-active", svc]
        other_call = ["sudo", "-n", "/bin/systemctl", cmd, svc]

        if cmd == "status":
            self.log.debug("Call: %s", " ".join(status_call))
            return subprocess.call(status_call)

        self.log.debug("Call: %s", " ".join(other_call))
        ret_call = subprocess.call(other_call)

        if cmd != "start":
            return ret_call

        # Needed because status always returns "RUNNING" in the first
        # second
        # TODO exchange that with proper communication to statserver
        time.sleep(2)

        # the return value might still be 0 even if start did not work
        # -> check status again
        ret_status = subprocess.call(status_call)

        if ret_status != 0:
            self.log.error("Service is not running after triggering start.")

            status = utils.read_status(service=svc, log=self.log)["info"]
            self.log.debug("systemctl status: \n%s", status)

        return ret_status

    def _call_init_script(self, cmd, det_id):
        """Command hidra (e.g. start, stop, status,...).

        Args:
            cmd: The command to call the service with
                 (e.g. start, stop, status,...).
            det_id: Which detector to command hidra for.

        Returns:
            Return value of the service call.

        """
        # pylint: disable=unused-argument

        call = ["service", self.service_conf["name"], cmd]
        # TODO implement beamline and det_id in hisdra.sh
        # call = ["service", service_conf["name"], "status", beamline, det_id]

        self.log.debug("Call: %s", " ".join(call))
        return subprocess.call(call)


class InstanceTracking(HidraServiceHandling):
    """Handles instance tracking.
    """

    def __init__(self, beamline, backup_file, log_queue):

        self.log = utils.get_logger(self.__class__.__name__, log_queue)
        super().__init__(beamline, self.log)

        self.beamline = beamline
        self.backup_file = backup_file

        self.reply_codes = REPLYCODES

        self.instances = None
        self._set_instances()

    def _set_instances(self):
        """Set all previously started instances.
        """

        try:
            with open(self.backup_file, 'r') as f:
                self.instances = json.loads(f.read())
        except IOError:
            # file does not exist
            self.instances = {}
        except Exception:
            # file content ist not as expected
            self.log.error("File containing instances existed but error "
                           "occured when reading it", exc_info=True)
            self.instances = {}

    def _update_instances(self):
        """Updates the instances file
        """

        try:
            with open(self.backup_file, "w") as f:
                f.write(json.dumps(self.instances, sort_keys=True, indent=4))
        except Exception:
            self.log.error("File containing instances could not be written",
                           exc_info=True)

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
        # the instances backup file is shared between all control servers
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
            if self.hidra_status(det_id) == self.reply_codes.running:
                self.log.info("Started hidra for %s_%s, already running",
                              self.beamline, det_id)
                continue

            # restart
            if self.call_hidra_service("start", det_id) == 0:
                self.log.info("Started hidra for %s_%s",
                              self.beamline, det_id)
            else:
                self.log.error("Could not start hidra for %s_%s",
                               self.beamline, det_id)


class ConfigHandling(utils.Base):
    """
    Handler for all configuration related changed (read, write, modify, ...)
    """

    def __init__(self, context, beamline, det_id, config, log_queue):

        super().__init__()

        self.context = context
        self.beamline = beamline
        self.det_id = det_id
        self.log = utils.get_logger(self.__class__.__name__, log_queue)

        self.config = config
        self.config_static = None
        self.config_variable = None
        self.config_ending = ".yaml"
        self.config_remote = None

        # connection depending hidra configuration, master config one is
        # overwritten with these parameters when start is executed
        self.all_configs = {}
        self.master_config = {}
        self.ctemplate = {}
        self.required_params = {}

        self.use_statserver = None
        self.stats_expose_sockets = {}
        self.stats_expose_endpt_tmpl = None

        self.timeout = 1000

        self._setup()

    def _setup(self):
        self.config_static = self.config["hidraconfig_static"]
        self.config_variable = self.config["hidraconfig_variable"]

        self.config_ending = ".yaml"

        # connection depending hidra configuration, master config one is
        # overwritten with these parameters when start is executed
        self.all_configs = dict()

        # ctemplate should only contain the entries which should overwrite the
        # ones in the control server config file
        # e.g. fix_subdirs is not added because of that
        ed_type = "http_events"
        self.ctemplate = {
            "active": False,
            "beamline": self.beamline,
            "general": {
                "ldapuri": None,
                "whitelist": None,
            },
            "eventdetector": {
                "type": ed_type,
                ed_type: {
                    "det_ip": None,
                    "det_api_version": None,
                    "history_size": None,
                }
            },
            "datafetcher": {
                "store_data": None,
                "remove_data": None,
            }
        }
        self.required_params = {
            "general": ["ldapuri", "whitelist"],
            "eventdetector": [
                ["type", [ed_type]],
                {
                    ed_type: [
                        "det_ip",
                        "det_api_version",
                        "history_size",
                    ]
                }
            ],
            "datafetcher": ["store_data", "remove_data"]
        }

        try:
            self.use_statserver = (
                self.config_static["general"]["use_statserver"]
            )
        except KeyError:
            self.use_statserver = False
        self.stats_expose_sockets = {}
        self.stats_expose_endpt_tmpl = "ipc:///tmp/hidra/{}_stats_exposing"

        self._read_config()

    def set(self, host_id, param, value):
        """Set a configuration parameter to certain value.

        Args:
            host_id: Which host is setting the parameter
            param: The parameter to set
            value: The value to set the parameter to
        """

        # identify the configuration for this connection
        if host_id not in self.all_configs:
            self.all_configs[host_id] = copy.deepcopy(self.ctemplate)

        utils.set_flat_param(
            param=param,
            param_value=value,
            config=self.all_configs[host_id],
            config_type="sender",
            log=self.log
        )

    def activate(self, host_id):
        """Mark the configuration with which the hidra instance is running with

        Args:
            host_id: The host the configuration belongs to.
        """
        self.all_configs[host_id]["active"] = True

    def get(self, host_id, param):
        """ Get a parameter value

        if the requesting client has set parameters before but has not
        executed start yet, the previously set parameters should be
        displayed (not the ones with which hidra was started the last time)
        on the other hand if it is a client coming up to check with which
        parameters the current hidra instance is running, these should be
        shown

        Args:
            host_id: The host the configuration belongs to.
            param: The parameter for which the value should be get.

        Returns:
            The value the parameter is set to.
        """
        try:
            if self.all_configs[host_id]["active"]:
                # This is a pointer
                current_config = self.all_configs[host_id]
            else:
                raise KeyError
        except KeyError:
            current_config = self.master_config

        return utils.get_flat_param(param,
                                    current_config,
                                    "sender",
                                    log=self.log)

    def clear(self, host_id):
        """Clear the configuration.

        Args:
            host_id: The host for which the configuration should be cleared.
            det_id: the detector to clear the configuration for.
        """

        try:
            del self.all_configs[host_id]
        except KeyError:
            pass

    def _read_config(self):

        # write configfile
        # /etc/hidra/P01.conf
        joined_path = os.path.join(CONFIG_DIR, CONFIG_PREFIX + self.beamline)
        config_files = glob.glob(joined_path + "_*" + self.config_ending)
        self.log.info("Reading config files: %s", config_files)

        for cfile in config_files:
            # extract the detector id from the config file name (remove path,
            # prefix, beamline and ending)
            det_id = cfile.replace(joined_path + "_", "")[:-5]
            try:
                self.master_config[det_id] = utils.load_config(cfile,
                                                               log=self.log)
            except IOError:
                self.log.debug("Configuration file not readable: %s", cfile)
            except Exception:
                self.log.debug("cfile=%s", cfile)
                self.log.error("Error when trying to load config file",
                               exc_info=True)
                raise
        self.log.debug("master_config=%s", json.dumps(self.master_config,
                                                      sort_keys=True,
                                                      indent=4))

    def _check_config_complete(self, host_id):
        """
         Check if all required params are there.
        """

        # identify the configuration for this connection
        try:
            # This is a pointer
            current_config = self.all_configs[host_id]
        except KeyError:
            self.log.debug("No current configuration found")
            raise utils.NotFoundError()

        config_complete, _ = utils.check_config(self.required_params,
                                                current_config,
                                                self.log)

        if not config_complete:
            self.log.debug(
                json.dumps(current_config, sort_keys=True, indent=4)
            )
            raise utils.WrongConfiguration(
                "Not all required parameters are specified"
            )

    def get_config_file_name(self):
        """
        Get the configuration file name

        Returns:
            A absolute configuration file path as string.
        """

        # /etc/hidra/P01_eiger01.conf
        return os.path.join(
            CONFIG_DIR,
            self.config["controlserver"]["hidra_config_name"]
            .format(bl=self.beamline, det=self.det_id)
        )

    def write_config(self, host_id):
        """
        Write the configuration into a file.

        Args:
            host_id: the host id the config belongs to.
            det_id: the detector id the config belongs to.
        """
        # pylint: disable=global-variable-not-assigned
        global CONFIG_DIR
        global CONFIG_PREFIX

        try:
            self._check_config_complete(host_id)
        except utils.NotFoundError:
            return

        current_config = self.all_configs[host_id]

        # if the requesting client has set parameters before these should be
        # taken. If this was not the case use the one from the previous
        # executed start
        if not current_config["active"]:
            self.log.debug("Config parameters did not change since last start")
            self.log.debug("No need to write new config file")
            return

        # add variable config
        config_g = self.config_variable["general"]
        config_df = self.config_variable["datafetcher"]

        username = config_g["username"].format(bl=self.beamline)
        procname_prefix = config_g["procname"].format(bl=self.beamline)
        procname = "{}_{}".format(procname_prefix, self.det_id)
        log_name_prefix = config_g["log_name"].format(bl=self.beamline)
        log_name = "{}_{}.log".format(log_name_prefix, self.det_id)
        local_target = config_df["local_target"].format(bl=self.beamline)
        external_ip = hidra.CONNECTION_LIST[self.beamline]["host"]

        self.config_static["general"]["log_name"] = log_name
        self.config_static["general"]["procname"] = procname
        self.config_static["general"]["username"] = username
        self.config_static["general"]["ext_ip"] = external_ip
        df_type = self.config_static["datafetcher"]["type"]
        try:
            self.config_static["datafetcher"][df_type]["local_target"] = (
                local_target
            )
        except KeyError:
            self.config_static["datafetcher"][df_type] = {
                "local_target": local_target
            }

        # dynamic config
        utils.update_dict(current_config, self.config_static)

        # write configfile
        config_file = self.get_config_file_name()
        self.log.info("Writing config file: %s", config_file)
        utils.write_config(config_file, self.config_static, log=self.log)

        self.log.info(
            "Started with ext_ip: %s, event detector: %s, "
            "data fetcher: %s", external_ip,
            self.config_static["eventdetector"]["type"],
            self.config_static["datafetcher"]["type"]
        )

        # store the dynamic config globally
        self.log.debug("config = %s", self.config_static)
        self.master_config = copy.deepcopy(self.config_static)
        # this information shout not go into the master config
        del self.master_config["active"]

        # mark local_config as inactive
        current_config["active"] = False

    def remove_config(self):
        """
        Remove the config file and stop stats socket.
        """
        self._stop_stats_socket()

        config_file = self.get_config_file_name()

        try:
            self.log.debug("Removing config file '%s'", config_file)
            os.remove(config_file)
        except Exception:
            self.log.error("Could not remove config file %s", config_file,
                           exc_info=True)

    def acquire_remote_config(self, systemd_service_tmpl):
        """Communicate with hidra instance and get its configuration.
        """

        if not self.use_statserver:
            return

        pid = utils.read_status(
            service=systemd_service_tmpl.format(self.det_id),
            log=self.log
            )["pid"]
        self.log.debug("hidra is running with pid %s", pid)

        endpoint = self.stats_expose_endpt_tmpl.format(pid)

        # get hidra config instance
        self.stats_expose_socket = self._start_socket(
            name="stats_expose_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint=endpoint,
            socket_options=[
                [zmq.SNDTIMEO, self.timeout],
                [zmq.RCVTIMEO, self.timeout]
            ]
        )

        try:
            self.stats_expose_socket.send(json.dumps("config").encode())
            self.config_remote = json.loads(
                self.stats_expose_socket.recv().decode()
            )
        except zmq.error.Again:
            self.log.error("Getting remote config failed due to timeout",
                           exc_info=True)
            return

        #endpt = utils.Endpoints(*answer["network"]["endpoints"])
        #self.log.debug("com_con=%s", endpt.com_con)

    def _stop_stats_socket(self):
        try:
            self._stop_socket(
                name="stat_expose_socket_{}".format(self.det_id),
                socket=self.stats_expose_socket
            )
        except KeyError:
            self.log.debug("No stats_expose_socket to stop available "
                           "(det_id=%s)", self.det_id)
        except Exception:
            self.log.error("Could not stop stats_expose_socket for %s",
                           self.det_id)

    def _stop(self):
        """
        Clean up.
        """
        self._stop_stats_socket()

    def __exit__(self, exception_type, exception_value, traceback):
        self._stop()

    def __del__(self):
        self._stop()


class HidraController(HidraServiceHandling):
    """
    This class holds getter/setter for all parameters
    and function members that control the operation.
    """

    def __init__(self,
                 context,
                 beamline,
                 det_id,
                 config,
                 instances,
                 log_queue):

        self.log = utils.get_logger(self.__class__.__name__, log_queue)
        super().__init__(beamline, self.log)

        self.context = context
        # Beamline is read-only, determined by portNo
        self.beamline = beamline
        self.det_id = det_id
        self.config = config
        self.log_queue = log_queue

        self.reply_codes = REPLYCODES

        self.confighandling = None
        self.instances = instances

        self.supported_keys = []

        self._setup()

    def _setup(self):

        config_ctrl = self.config["controlserver"]

        self.confighandling = ConfigHandling(self.context,
                                             self.beamline,
                                             self.det_id,
                                             self.config,
                                             self.log_queue)

        self.supported_keys = [
            "ldapuri",
            "whitelist",
            "det_ip",
            "det_api_version",
            "history_size",
            "store_data",
            "remove_data",
        ]

#        self.supported_keys = [k for k in list(self.ctemplate.keys())
#                               if k not in ["active", "beamline"]]

    def set(self, host_id, param, value):
        """
        set a parameter
        """

        if param in self.supported_keys:
            self.confighandling.set(host_id, param, value)
            self.confighandling.activate(host_id, )
            return_val = self.reply_codes.done
        else:
            self.log.debug("param=%s; value=%s", param, value)
            return_val = self.reply_codes.done

        return return_val

    def get(self, host_id, param):
        """
        return the value of a parameter
        """

        if param in self.supported_keys:
            value = self.confighandling.get(host_id, param)

            if isinstance(value, list):
                reply = str(value)
            else:
                reply = value

        else:
            self.log.debug("param=%s", param)
            reply = self.reply_codes.error

        reply = json.dumps(reply).encode()
        self.log.debug("reply is %s", reply)

        # TODO is this really necessary?
        if reply is None:
            self.log.debug("reply is None")
            return b"None"

        return reply

    def do(self, host_id, cmd):  # pylint: disable=invalid-name
        """
        executes commands
        """
        if cmd == "start":
            ret_val = self.start(host_id)
            return ret_val
        elif cmd == "stop":
            return self.stop()

        elif cmd == "restart":
            return self.restart(host_id)

        elif cmd == "status":
            return self.hidra_status(self.det_id)

        elif cmd == "get_instances":
            return self.get_instances()

        else:
            return self.reply_codes.error

    def bye(self, host_id):
        """Disconnect host and clear config.

        Args:
            host_id: The host to disconnect.

        Returns:
            b"DONE" if everything worked.
        """

        self.log.debug("Received 'bye' from host %s for detector %s",
                       host_id, self.det_id)

        self.confighandling.clear(host_id)

        return self.reply_codes.done

    def start(self, host_id):
        """
        start ...
        """

        # check if service is running
        if self.hidra_status(self.det_id) == self.reply_codes.running:
            return self.reply_codes.already_running

        try:
            self.confighandling.write_config(host_id)
        except Exception:
            self.log.error("Config file not written", exc_info=True)
            return self.reply_codes.error

        # start service
        if self.call_hidra_service("start", self.det_id) != 0:
            self.log.error("Could not start the service.")
            return self.reply_codes.error

        self.confighandling.acquire_remote_config(
            self.service_conf["template"]
        )

        # remember that the instance was started
        self.instances.add(self.det_id)
        return self.reply_codes.done

    def get_instances(self):
        """Get the started hidra instances

        Returns:
            List of detectors started for this beamline as json dump.
        """

        try:
            bl_instances = self.instances.get_instances()[self.beamline]
        except KeyError:
            # something went wrong when trying to start the instance
            bl_instances = {}

        return json.dumps(list(bl_instances.keys())).encode()

    def stop(self):
        """
        stop ...
        """
        # check if really running before return
        if self.hidra_status(self.det_id) != self.reply_codes.running:
            return self.reply_codes.already_stopped

        # stop service
        if self.call_hidra_service("stop", self.det_id) != 0:
            self.log.error("Could not stop the service.")
            return self.reply_codes.error

        self.instances.remove(self.det_id)
        self.confighandling.remove_config()

        return self.reply_codes.done

    def restart(self, host_id):
        """
        restart ...
        """
        # stop service
        reval = self.stop()

        if reval == self.reply_codes.done:
            # start service
            return self.start(host_id)
        else:
            return self.reply_codes.done


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
    arguments = {"controlserver": vars(arguments)}

    # ------------------------------------------------------------------------
    # Get arguments from config file and comand line
    # ------------------------------------------------------------------------
    utils.check_existance(config_file)

    config = utils.load_config(config_file)
    utils.update_dict(arguments, config)

    # the configuration is now of the form:
    # {
    #   "controlserver": {...}
    #   "hidraconfig_static" : {...}
    #   "hidraconfig_variable" : {...}
    # }

    # TODO check config for required params

    return config


class ControlServer(utils.Base):
    """The main server class.
    """

    def __init__(self):

        super().__init__()

        self.beamline = None
        self.context = None
        self.socket = None
        self.config = None
        self.ldapuri = None
        self.netgroup_template = None

        self.controller = {}
        self.endpoint = None

        self.log_queue = None
        self.log_queue_listener = None

        self.error = b"ERROR"
        self.ok = b"OK"

        self.instances = None

        self._setup()

        self.run()

    def _setup(self):

        self.config = argument_parsing()

        config_ctrl = self.config["controlserver"]

        self.beamline = config_ctrl["beamline"]
        self.ldapuri = config_ctrl["ldapuri"]
        self.netgroup_template = config_ctrl["netgroup_template"]

        setproctitle.setproctitle(config_ctrl["procname"]
                                  .format(bl=self.beamline))

        self._setup_logging()

        host = hidra.CONNECTION_LIST[self.beamline]["host"]
        host = socket.gethostbyaddr(host)[2][0]
        port = hidra.CONNECTION_LIST[self.beamline]["port"]
        self.endpoint = "tcp://{}:{}".format(host, port)
        self._create_sockets()

        self.instances = InstanceTracking(self.beamline,
                                          config_ctrl["backup_file"],
                                          self.log_queue)
        self.instances.restart_instances()

    def _setup_logging(self):
        config_ctrl = self.config["controlserver"]

        logfile = os.path.join(
            config_ctrl["log_path"],
            config_ctrl["log_name"].format(bl=self.beamline)
        )

        # Get queue
        self.log_queue = Queue(-1)

        handler = utils.get_log_handlers(
            logfile,
            config_ctrl["log_size"],
            config_ctrl["verbose"],
            config_ctrl["onscreen"]
        )

        # Start queue listener using the stream handler above
        self.log_queue_listener = utils.CustomQueueListener(
            self.log_queue, *handler
        )

        self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = utils.get_logger("ControlServer", self.log_queue)
        self.log.info("Init")

    def _create_sockets(self):

        # Create ZeroMQ context
        self.log.info("Registering ZMQ context")
        self.context = zmq.Context()

        # socket to get requests
        self.socket = self._start_socket(
            name="socket",
            sock_type=zmq.REP,
            sock_con="bind",
            endpoint=self.endpoint
        )

    def run(self):
        """Waiting for new control commands and execute them.
        """

        while True:
            try:
                msg = self.socket.recv_multipart()
                self.log.debug("Recv %s", msg)
            except KeyboardInterrupt:
                break

            if not msg:
                self.log.debug("Received empty msg")
                break

            elif msg[0] == b"exit":
                self.log.debug("Received 'exit'")
                self.stop()
                sys.exit(1)

            reply = self.exec_msg(msg)

            self.socket.send(reply)

    def _decode_message(self, msg):
        """Decode the message
        """

        try:
            action = msg[0]
        except IndexError:
            raise FormatError

        if action == b"IS_ALIVE":
            return action, None, None, None, None

        try:
            action, host_id, det_id = msg[:3]
            host_id = host_id.decode()
            det_id = det_id.decode()
        except ValueError:
            self.log.error("No host_id and det_id defined")
            raise FormatError

        det_id = socket.getfqdn(det_id)

        if action == b"set":

            if len(msg) < 4:
                self.log.error("Not enough arguments")
                raise FormatError

            param, value = msg[3:]
            param = param.decode().lower()
            value = json.loads(value.decode())

        elif action in [b"get", b"do"]:
            if len(msg) != 4:
                self.log.error("Not enough arguments")
                raise FormatError

            param, value = msg[3].decode().lower(), None

        elif action == b"bye":
            param, value = None, None

        else:
            self.log.error("Unknown action")
            raise FormatError

        return action, host_id, det_id, param, value

    def exec_msg(self, msg):
        """
        [b"IS_ALIVE"]
            return "OK"
        [b"do", host_id, det_id, b"start"]
            return "DONE"
        [b"bye", host_id, detector]
        """

        # --------------------------------------------------------------------
        # decode message
        # --------------------------------------------------------------------
        try:
            action, host_id, det_id, param, value = self._decode_message(msg)
        except FormatError:
            self.log.error("Message of wrong format")
            return self.error

        if action == b"IS_ALIVE":
            return self.ok

        # --------------------------------------------------------------------
        # get hidra controller
        # --------------------------------------------------------------------
        try:
            controller = self.controller[det_id]
        except KeyError:
            self.controller[det_id] = HidraController(self.context,
                                                      self.beamline,
                                                      det_id,
                                                      self.config,
                                                      self.instances,
                                                      self.log_queue)
            controller = self.controller[det_id]

        # --------------------------------------------------------------------
        # check_netgroup
        # --------------------------------------------------------------------
        try:
            # check if host is allowed to execute commands
            check_res = utils.check_netgroup(
                host_id,
                self.beamline,
                self.ldapuri,
                self.netgroup_template.format(bl=self.beamline),
                log=self.log,
                raise_if_failed=False
            )
        except Exception:
            self.log.error("Error when checking netgroup", exc_info=True)
            self.log.debug("msg=%s", msg)
            return self.error

        if not check_res:
            return self.error

        # --------------------------------------------------------------------
        # react to message
        # --------------------------------------------------------------------
        if action == b"set":
            return controller.set(host_id, param, value)

        elif action == b"get":
            return self._get(controller, host_id, param)

        elif action == b"do":
            return controller.do(host_id, param)

        elif action == b"bye":
            return controller.bye(host_id)
        else:
            return self.error

    def _get(self, controller, host_id, param):
        reply = json.dumps(
            controller.get(host_id, param)
        ).encode()
        self.log.debug("reply is %s", reply)

        if reply is None:
            self.log.debug("reply is None")
            return b"None"

        return reply

    def stop(self):
        """Clean up zmq sockets.
        """
        self._stop_socket(name="socket")

        if self.context:
            self.log.info("Destroying Context")
            self.context.destroy()
            self.context = None

        if self.log_queue_listener:
            self.log.info("Stopping log_queue")
            self.log_queue.put_nowait(None)
            self.log_queue_listener.stop()
            self.log_queue_listener = None

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    ControlServer()
