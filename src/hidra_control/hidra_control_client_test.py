#!/usr/bin/env python
from __future__ import print_function

import os
import sys

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


def do_set(obj):
    det = "lsdma-lab04"

    # check if beamline is allowed to get data from this Eiger
    hidra.check_netgroup(det,
                         "p00",
                         log=hidra.control.LoggingFunction())

    obj.set("local_target", "local")
    obj.set("eiger_ip", det)
    obj.set("eiger_api_version", "1.5.0")
    obj.set("history_size", 2000)
    obj.set("store_data", False)
    obj.set("remove_data", False)
    obj.set("whitelist", "localhost")


def do_start(obj):
    print ("Starting HiDRA for Eiger:", obj.do("start"))


def do_stop(obj):
    print ("Stopping HiDRA for Eiger:", obj.do("stop"))


def do_status(obj):
    print ("Status of HiDRA for Eiger:", obj.do("status"))


def do_getsettings(obj):
    if obj.do("status") == "RUNNING":
        print ("Configured settings:")
        print ("Data is written to:         {0}"
               .format(obj.get("local_target")))
#        print ("Eiger IP:                   {0}"
#               .format(obj.get("eiger_ip")))
#        print ("Eiger API version:          {0}"
#               .format(obj.get("eiger_api_version")))
#        print ("History size:               {0}"
#               .format(obj.get("history_size")))
#        print ("Store data:                 {0}"
#               .format(obj.get("store_data")))
#        print ("Remove data from the Eiger: {0}"
#               .format(obj.get("remove_data")))
#        print ("Whitelist:                  {0}"
#               .format(obj.get("whitelist")))
    else:
        print ("HiDRA is not running")


if __name__ == '__main__':
    obj = hidra.Control("p00", use_log=None)

    try:
        do_getsettings(obj)

        do_set(obj)
        do_start(obj)
        do_getsettings(obj)
        print ()

        new_target = "current/raw"
        print ("Setting local_target to {0}".format(new_target))
        obj.set("local_target", new_target)
        do_getsettings(obj)
        print ()

        obj.stop()

        obj = hidra.Control("p00", use_log=None)
        do_getsettings(obj)
        print ()

        do_stop(obj)
    finally:
        obj.stop()
