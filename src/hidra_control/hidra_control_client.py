#!/usr/bin/env python
from __future__ import print_function

import os
import sys
import argparse

try:
    # search in global python modules first
    import hidra
except:
    # then search in local modules
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.realpath(__file__))))
    API_PATH = os.path.join(BASE_PATH, "src", "APIs")

    if API_PATH not in sys.path:
        sys.path.insert(0, API_PATH)
    del API_PATH

    import hidra

# the list transformation is needed for Python 3 compliance
ALLOWED_BEAMLINES = list(hidra.connection_list.keys())
# ALLOWED_BEAMLINES = ["p00", "p01", "p02.1", "p02.2", "p03", "p04", "p05",
#                      "p06", "p07", "p08", "p09", "p10", "p11"]

NETGROUP_TEMPLATE = Template("a3${bl}-hosts")  # noqa F821


def argument_parsing():
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
                        default="1.5.0",
                        help="API version of the detector "
                             "(default: 1.5.0)")

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
    parser.add_argument("--target",
                        type=str,
                        help="Where to write the data to "
                             "(default: current/raw; options are: "
                             "current/raw, current/scratch_bl, "
                             "commissioning/raw, commissioning/scratch_bl "
                             "or local)",
                        default="current/raw")

    parser.add_argument("--version",
                        help="Displays the used hidra_control version",
                        action="store_true")
    parser.add_argument("--getsettings",
                        help="Displays the settings of the HiDRA Server "
                             "(detector mode)",
                        action="store_true")

    return parser


if __name__ == '__main__':
    parser = argument_parsing()
    arguments = parser.parse_args()

    if arguments.version:
        print("Hidra version: {0}".format(hidra.__version__))
        sys.exit(0)

    beamline = arguments.beamline
    supported_targets = ["current/raw",
                         "current/scratch_bl",
                         "commissioning/raw",
                         "commissioning/scratch_bl",
                         "local"]

    if (arguments.target
            and os.path.normpath(arguments.target) not in supported_targets):
        print("ERROR: target not supported")
        sys.exit(1)

    obj = hidra.Control(beamline, arguments.det, use_log="warning")

    try:
        if arguments.start:
            # check if beamline is allowed to get data from this detector
            hidra.check_netgroup(arguments.det,
                                 beamline,
                                 log=hidra.LoggingFunction())

            obj.set("local_target", arguments.target)
            obj.set("det_ip", arguments.det)
            obj.set("det_api_version", arguments.detapi)
            obj.set("history_size", 2000)
            obj.set("store_data", True)
            obj.set("remove_data", True)
            obj.set("whitelist", NETGROUP_TEMPLATE)

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
                print("Data is written to:            {0}"
                      .format(obj.get("local_target")))
                print("Detector IP:                   {0}"
                      .format(obj.get("det_ip")))
                print("Detector API version:          {0}"
                      .format(obj.get("det_api_version")))
                print("History size:                  {0}"
                      .format(obj.get("history_size")))
                print("Store data:                    {0}"
                      .format(obj.get("store_data")))
                print("Remove data from the detector: {0}"
                      .format(obj.get("remove_data")))
                print("Whitelist:                     {0}"
                      .format(obj.get("whitelist")))
            else:
                print("HiDRA is not running")

    finally:
        obj.stop()
