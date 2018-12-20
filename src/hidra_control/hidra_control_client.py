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

    config_file = os.path.join(CONFIG_DIR, "control_client.yaml")

    parser = argparse.ArgumentParser()

    parser.add_argument("--beamline",
                        type=str,
                        required=True,
                        choices=ALLOWED_BEAMLINES,
                        help="Beamline for which the HiDRA server (detector "
                             "mode) should be operated")

    parser.add_argument("--det",
                        type=str,
                        required=True,
                        help="IP (or DNS name) of the detector")
    parser.add_argument("--detapi",
                        type=str,
                        default="1.6.0",
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

    args = parser.parse_args()

    # convert to dict and map to config section
    args_dict = vars(args)

    # hidra config
    arguments = {
        "hidra": {
            "det_ip": args_dict["det"],
            "det_api_version": args_dict["detapi"]
        }
    }
    del args_dict["det"]
    del args_dict["detapi"]

    # general config
    arguments["general"] = args_dict

    # ------------------------------------------------------------------------
    # Get arguments from config file and comand line
    # ------------------------------------------------------------------------
    utils.check_existance(config_file)

    config = utils.load_config(config_file)
    utils.update_dict(arguments, config)

    check_config(config)

    return config


def check_config(config):
    log = utils.LoggingFunction("debug")

    # general section
    required_params = ["beamline", "ldapuri", "netgroup_template"]
    check_passed, config_reduced = utils.check_config(
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
    required_params = ["history_size", "store_data", "remove_data"]
    check_passed, config_reduced = utils.check_config(
        required_params,
        config["hidra"],
        log
    )

    if not check_passed:
        raise utils.WrongConfiguration(
            "The hidra section of configuration has missing or wrong "
            "parameteres."
        )

    if "whitelist" not in config["hidra"]:
        config["hidra"]["whitelist"] = (
            config["general"]["netgroup_template"]
            .format(bl=config["general"]["beamline"])
        )


def client():
    """The hidra control client.
    """

    config = argument_parsing()

    config_g = config["general"]
    config_hidra = config["hidra"]

    if config_g["version"]:
        print("Hidra version: {}".format(hidra.__version__))
        sys.exit(0)

    beamline = config_g["beamline"]
    ldapuri = config_g["ldapuri"]
    netgroup_template = config_g["netgroup_template"]

    obj = hidra.Control(beamline,
                        config_hidra["det_ip"],
                        ldapuri,
                        netgroup_template,
                        use_log="warning")

    try:
        if config_g["start"]:
            # check if beamline is allowed to get data from this detector
            hidra.check_netgroup(config_hidra["det_ip"],
                                 beamline,
                                 ldapuri,
                                 netgroup_template.format(bl=beamline),
                                 log=hidra.LoggingFunction())

            obj.set("det_ip", config_hidra["det_ip"])
            obj.set("det_api_version", config_hidra["det_api_version"])
            obj.set("history_size", config_hidra["history_size"])
            obj.set("store_data", config_hidra["store_data"])
            obj.set("remove_data", config_hidra["remove_data"])
            obj.set("whitelist", config_hidra["whitelist"])
            obj.set("ldapuri", ldapuri)

            res_start = obj.do("start")
            print("Starting HiDRA (detector mode):", res_start)

            if res_start == b"ERROR":
                instances = obj.do("get_instances")
                if instances:
                    print("Instances already running for:", instances)

#        elif config_g["restart"]:
#            print ("Restarting HiDRA (detector mode):", obj.do("restart"))

        elif config_g["status"]:
            print("Status of HiDRA (detector mode):", obj.do("status"))

        elif config_g["stop"]:
            print("Stopping HiDRA (detector mode):", obj.do("stop"))

        elif config_g["getsettings"]:

            if obj.do("status") == b"RUNNING":
                print("Configured settings:")
                print("Detector IP:                   {}"
                      .format(obj.get("det_ip")))
                print("Detector API version:          {}"
                      .format(obj.get("det_api_version")))
                print("History size:                  {}"
                      .format(obj.get("history_size")))
                print("Store data:                    {}"
                      .format(obj.get("store_data")))
                print("Remove data from the detector: {}"
                      .format(obj.get("remove_data")))
                print("Whitelist:                     {}"
                      .format(obj.get("whitelist")))
                print("Ldapuri:                       {}"
                      .format(obj.get("ldapuri")))
                print("fix_subdirs:                   {}"
                      .format(obj.get("fix_subdirs")))
            else:
                print("HiDRA is not running")

    finally:
        obj.stop()


if __name__ == "__main__":
    client()
