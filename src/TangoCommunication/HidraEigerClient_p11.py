#!/usr/bin/env python
import os
import sys
import argparse
import HidraEigerAPI

def argumentParsing():
    parser = argparse.ArgumentParser()

    parser.add_argument("--start"   , help    = "Starts the Hidra Server for the Eiger detector",
                                      action = "store_true")
    parser.add_argument("--restart" , help    = "Restarts the Hidra Server for the Eiger detector",
                                      action = "store_true")
    parser.add_argument("--status"  , help    = "Displays the Status of the Hidra Server for the Eiger detector",
                                      action = "store_true")
    parser.add_argument("--stop"    , help    = "Stops the Hidra Server for the Eiger detector",
                                      action = "store_true")
    parser.add_argument("--target"  , type    = str,
                                      help    = "Where to write the data to (default: current/raw; options are: current/raw, current/processed, current/shared, current/scratch_bl, commissioning/raw, commissioning/processed, commissioning/shared,  commissioning/scratch_bl or local)",
                                      default = "local")
#                                      default = "current/scratch_bl")

    return parser.parse_args()


if __name__ == '__main__':
    arguments = argumentParsing()

    beamline = "p11"
    supportedLocalTargets = ["current/raw", "current/processed", "current/shared", "current/scratch_bl", "commissioning/raw", "commissioning/processed", "commissioning/shared", "commissioning/scratch_bl", "local"]

    if arguments.target and os.path.normpath(arguments.target) not in supportedLocalTargets:
        print "ERROR: target not supported"
        sys.exit(1)

    obj = HidraEigerAPI.HidraEigerAPI(beamline)

    try:
        if arguments.start:
            obj.set("localTarget", arguments.target)
            obj.set("eigerIp", "192.168.138.52")
            obj.set("eigerApiVersion", "1.5.0")
            obj.set("historySize", 0)
            obj.set("storeData", False)
            obj.set("removeData", False)
            obj.set("whitelist", "localhost", "zitpcx19282")

            obj.do("start")

        elif arguments.restart:
            obj.do("retart")

        elif arguments.status:
            obj.do("status")

        elif arguments.stop:
            obj.do("stop")

    finally:
        obj.stop()

