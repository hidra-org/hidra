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

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import os
import sys

try:
    # search in global python modules first
    import hidra
except:
    # then search in local modules
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
    API_DIR = os.path.join(BASE_DIR, "src", "APIs")

    if API_DIR not in sys.path:
        sys.path.insert(0, API_DIR)
    del API_DIR

    import hidra

# the list transformation is needed for Python 3 compliance
ALLOWED_BEAMLINES = list(hidra.CONNECTION_LIST.keys())
# ALLOWED_BEAMLINES = ["p00", "p01", "p02.1", "p02.2", "p03", "p04", "p05",
#                      "p06", "p07", "p08", "p09", "p10", "p11"]

LDAPURI = "it-ldap-slave.desy.de:1389"
NETGROUP_TEMPLATE = "a3{bl}-hosts"


def argument_parsing():
    """Parsing command line arguments.
    """

    # pylint: disable=global-variable-not-assigned
    global ALLOWED_BEAMLINES

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

    return parser.parse_args()


def client():
    """The hidra control client.
    """

    arguments = argument_parsing()

    if arguments.version:
        print("Hidra version: {}".format(hidra.__version__))
        sys.exit(0)

    beamline = arguments.beamline

    obj = hidra.Control(beamline,
                        arguments.det,
                        LDAPURI,
                        NETGROUP_TEMPLATE,
                        use_log="warning")

    try:
        if arguments.start:
            # check if beamline is allowed to get data from this detector
            hidra.check_netgroup(arguments.det,
                                 beamline,
                                 LDAPURI,
                                 NETGROUP_TEMPLATE.format(bl=beamline),
                                 log=hidra.LoggingFunction())

            obj.set("det_ip", arguments.det)
            obj.set("det_api_version", arguments.detapi)
            obj.set("history_size", 2000)
            obj.set("store_data", False)
            obj.set("remove_data", False)
            obj.set("whitelist", NETGROUP_TEMPLATE.format(bl=beamline))
            obj.set("ldapuri", LDAPURI)

            print("Starting HiDRA (detector mode):", obj.do("start"))

#        elif arguments.restart:
#            print ("Restarting HiDRA (detector mode):", obj.do("restart"))

        elif arguments.status:
            print("Status of HiDRA (detector mode):", obj.do("status"))

        elif arguments.stop:
            print("Stopping HiDRA (detector mode):", obj.do("stop"))

        elif arguments.getsettings:

            if obj.do("status") == "RUNNING":
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


if __name__ == '__main__':
    client()
