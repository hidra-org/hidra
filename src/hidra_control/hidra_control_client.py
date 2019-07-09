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
This client to communicate with the server and configure and start up hidra.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import os
import sys

# search in local modules
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
API_DIR = os.path.join(BASE_DIR, "src", "APIs")
CONFIG_DIR = os.path.join(BASE_DIR, "conf")

if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)
del API_DIR

# pylint: disable=wrong-import-position
import hidra  # noqa E402
import hidra.utils as utils  # noqa E402

# the list transformation is needed for Python 3 compliance
ALLOWED_BEAMLINES = list(hidra.CONNECTION_LIST.keys())


def argument_parsing():
    """Parsing command line arguments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--beamline",
                        type=str,
                        choices=ALLOWED_BEAMLINES,
                        help="Beamline for which the HiDRA server (detector "
                             "mode) should be operated")

    parser.add_argument("--det",
                        type=str,
                        required=True,
                        help="IP (or DNS name) of the detector")
    parser.add_argument("--detapi",
                        type=str,
                        help="API version of the detector "
                             "(default: 1.6.0)")

    parser.add_argument("--start",
                        help="Starts the HiDRA server (detector mode)",
                        action="store_true")
#    parser.add_argument("--restart",
#                        help="Restarts the HiDRA Server (detector mode)",
#                        action="store_true")
    parser.add_argument("--status",
                        help="Displays the status of the HiDRA server "
                             "(detector mode)",
                        action="store_true")
    parser.add_argument("--stop",
                        help="Stops the HiDRA server (detector mode)",
                        action="store_true")

    parser.add_argument("--version",
                        help="Displays the used hidra_control version",
                        action="store_true")
    parser.add_argument("--getsettings",
                        help="Displays the settings of the HiDRA Server "
                             "(detector mode)",
                        action="store_true")
    parser.add_argument("--getinstances",
                        help="Displays the HiDRA instances",
                        action="store_true")

    args = parser.parse_args()
    config = _merge_with_config(args)

    check_config(config)

    return config


def _merge_with_config(args):
    """
    Takes the comand line arguments and overwrites the parameter of the confi
    file with it.
    """
    config_file = os.path.join(CONFIG_DIR, "control_client.yaml")

    # convert to dict and map to config section
    args_dict = vars(args)

    # hidra config
    eventdetector = {
        "http_events": {
                "det_ip": args_dict["det"],
            }
    }
    if args_dict["detapi"] is not None:
        eventdetector["http-events"]["det_api_version"] = args_dict["det_api"]

    arguments = {"hidra": {"eventdetector": eventdetector}}

    del args_dict["det"]
    del args_dict["detapi"]

    if args_dict["beamline"] is None:
        del args_dict["beamline"]

    # general config
    arguments["general"] = args_dict

    # ------------------------------------------------------------------------
    # Get arguments from config file and comand line
    # ------------------------------------------------------------------------
    utils.check_existance(config_file)

    config = utils.load_config(config_file)
    utils.update_dict(arguments, config)

    # check for beamline
    try:
        if config["general"]["beamline"] not in ALLOWED_BEAMLINES:
            raise parser.error(
                "argument --beamline: invalid choice: '{}' (choose from {})"
                .format(config["general"]["beamline"],
                        str(ALLOWED_BEAMLINES)[1:-1])
            )
    except KeyError:
        raise parser.error("the following arguments are required: --beamline")

    return config


def check_config(config):
    """Check if the configuration has the required structure.

    Args:
        config (dict): The configuration dictionary.
    """

    log = utils.LoggingFunction("debug")

    # general section
    required_params = ["beamline", "ldapuri", "netgroup_template"]
    check_passed, _ = utils.check_config(
        required_params,
        config["general"],
        log
    )

    if not check_passed:
        raise utils.WrongConfiguration(
            "The general section of configuration has missing or wrong "
            "parameteres."
        )

    # hidra section
    required_params_hidra = {
        "eventdetector": [
            {"http_events": ["history_size"]}
        ],
        "datafetcher": ["store_data", "remove_data"]
    }
    check_passed, _ = utils.check_config(
        required_params_hidra,
        config["hidra"],
        log
    )

    if not check_passed:
        raise utils.WrongConfiguration(
            "The hidra section of configuration has missing or wrong "
            "parameteres."
        )

    potential_whitelist = (
        config["general"]["netgroup_template"]
        .format(bl=config["general"]["beamline"])
    )

    try:
        whitelist = (config["hidra"]["general"]["whitelist"]
                     or potential_whitelist)
    except KeyError:
        if "general" in config["hidra"]:
            config["hidra"]["general"]["whitelist"] = whitelist
        else:
            config["hidra"]["general"] = {
                "whitelist": potential_whitelist
            }


class Client(object):
    """The hidra control client.
    """

    def __init__(self):

        self.config = argument_parsing()

        # for convenience
        self.config_g = self.config["general"]
        self.config_hidra = self.config["hidra"]
        self.config_ed = self.config_hidra["eventdetector"]["http_events"]
        self.config_df = self.config_hidra["datafetcher"]

        if self.config_g["version"]:
            print("Hidra version:", hidra.__version__)
            sys.exit(0)

        self.beamline = self.config_g["beamline"]
        self.ldapuri = self.config_g["ldapuri"]
        self.netgroup_template = self.config_g["netgroup_template"]

        try:
            self.control = hidra.Control(self.beamline,
                                         self.config_ed["det_ip"],
                                         self.ldapuri,
                                         self.netgroup_template,
                                         use_log="warning")
        except utils.NotAllowed as excp:
            print(excp)
            sys.exit(1)

    def run(self):
        """Communicate with hidra instance and send commands.
        """
        if self.config_g["start"]:
            self._start()

#        elif config_g["restart"]:
#            print ("Restarting HiDRA (detector mode):",
#                   self.control.do("restart"))

        elif self.config_g["status"]:
            try:
                res_start = self.control.do("status")
                print("Status of HiDRA (detector mode):", res_start)
            except utils.NotAllowed:
                print("except")

        elif self.config_g["stop"]:
            try:
                res_start = self.control.do("stop")
                print("Stopping HiDRA (detector mode):", res_start)
            except utils.NotAllowed:
                print("except")

        elif self.config_g["getsettings"]:
            self._getsettings()

        elif self.config_g["getinstances"]:
            self._getinstances()

    def _start(self):
        self.control.set("det_ip", self.config_ed["det_ip"])
        self.control.set("det_api_version", self.config_ed["det_api_version"])
        self.control.set("history_size", self.config_ed["history_size"])
        self.control.set("store_data", self.config_df["store_data"])
        self.control.set("remove_data", self.config_df["remove_data"])
        self.control.set("ldapuri", self.ldapuri)
        self.control.set("whitelist",
                         self.config_hidra["general"]["whitelist"])

        try:
            res_start = self.control.do("start")
            print("Starting HiDRA (detector mode):", res_start)
        except utils.NotAllowed:
            print("except")

        if res_start == "ERROR":
            instances = self.control.do("get_instances")
            if instances:
                print("Instances already running for:", instances)

    def _getsettings(self):
        if self.control.do("status") == "RUNNING":

            print("Configured settings:")
            print("Detector IP:                   {}"
                  .format(self.control.get("det_ip")))
            print("Detector API version:          {}"
                  .format(self.control.get("det_api_version")))
            print("History size:                  {}"
                  .format(self.control.get("history_size")))
            print("Store data:                    {}"
                  .format(self.control.get("store_data")))
            print("Remove data from the detector: {}"
                  .format(self.control.get("remove_data")))
            print("Whitelist:                     {}"
                  .format(self.control.get("whitelist")))
            print("Ldapuri:                       {}"
                  .format(self.control.get("ldapuri")))
            print("fix_subdirs:                   {}"
                  .format(self.control.get("fix_subdirs")))
        else:
            print("HiDRA is not running")

    def _getinstances(self):
        instances = self.control.do("get_instances")
        if instances:
            print("Beamline {} has running instances for the following "
                  "detectors: \n{}".format(self.beamline,
                                           "\n".join(instances)))
        else:
            print("No running hidra instances")

    def stop(self):
        """ Stop and clean up.
        """
        try:
            self.control.stop()
        except AttributeError:
            pass

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


def main():
    """The main program.
    """
    client = Client()
    client.run()


if __name__ == "__main__":
    main()
