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
        sys.path.append(API_PATH)
    del API_PATH

    import hidra


def argument_parsing():
    parser = argparse.ArgumentParser()

    parser.add_argument("--beamline",
                        type=str,
                        required=True,
                        choices=["p00", "p01", "p02.1", "p02.2", "p03", "p04",
                                 "p05", "p06", "p07", "p08", "p09", "p10",
                                 "p11"],
                        help="Beamline for which the HiDRA server for the "
                             "Eiger detector should be operated")

    parser.add_argument("--eigerip",
                        type=str,
                        help="IP (or DNS name) of the Eiger detector")
    parser.add_argument("--eigerapi",
                        type=str,
                        default="1.5.0",
                        help="API version of the Eiger detector (default: 1.5.0)")

    parser.add_argument("--start",
                        help="Starts the HiDRA Server for the Eiger detector",
                        action="store_true")
#    parser.add_argument("--restart",
#                        help="Restarts the HiDRA Server for the Eiger "
#                             "detector",
#                        action="store_true")
    parser.add_argument("--status",
                        help="Displays the Status of the HiDRA Server for "
                             "the Eiger detector",
                        action="store_true")
    parser.add_argument("--stop",
                        help="Stops the HiDRA Server for the Eiger detector",
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

    return parser


if __name__ == '__main__':
    parser = argument_parsing()
    arguments = parser.parse_args()

    if arguments.start and arguments.eigerip is None:
        print ("parser error")
        parser.error("--start requires --eigerip")
        sys.exit(1)

    if arguments.version:
        print ("Hidra version: {0}".format(hidra.__version__))
        sys.exit(0)


    beamline = arguments.beamline
    supported_targets = ["current/raw",
                         "current/scratch_bl",
                         "commissioning/raw",
                         "commissioning/scratch_bl",
                         "local"]

    if (arguments.target
            and os.path.normpath(arguments.target) not in supported_targets):
        print ("ERROR: target not supported")
        sys.exit(1)

    obj = hidra.Control(beamline, use_log=None)

    try:
        if arguments.start:
            # check if beamline is allowed to get data from this Eiger
            hidra.check_netgroup(arguments.eigerip,
                                 beamline,
                                 log=hidra.control.LoggingFunction())

            obj.set("local_target", arguments.target)
            obj.set("eiger_ip", arguments.eigerip)
            obj.set("eiger_api_version", arguments.eigerapi)
            obj.set("history_size", 2000)
            obj.set("store_data", False)
            obj.set("remove_data", False)
            obj.set("whitelist", "localhost")

            print ("Starting HiDRA for Eiger:", obj.do("start"))

#        elif arguments.restart:
#            print ("Restarting HiDRA for Eiger:", obj.do("restart"))

        elif arguments.status:
            print ("Status of HiDRA for Eiger:", obj.do("status"))

        elif arguments.stop:
            print ("Stopping HiDRA for Eiger:", obj.do("stop"))

    finally:
        obj.stop()
