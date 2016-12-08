#!/usr/bin/env python
from __future__ import print_function

import time
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


obj = hidra.Control("p00", use_log=None)
#obj = hidra.Control("p00")

# where the data should be stored inside the beamline filesystem
# only the relative path is needed because the absolute path can be
# reconstucted with the beamline name
print ("Setting local_target:", obj.set("local_target", "commissioning/raw"))
#print ("Setting local_target:", obj.set("local_target", "current/raw"))

print (obj.get("local_target"))

# from which eiger should the data should be get
print ("Setting eiger_ip:", obj.set("eiger_ip", "192.168.138.52"))
# which simplon api version is running on the eiger
print ("Setting eiger_api_version:", obj.set("eiger_api_version", "1.5.0"))
# this should only be shown in expert view because it can be critical
# HiDRA gets a file list from the eiger and the downloads these files
# asynchoniously. If the work on file list is not finished when the next list
# is get there is an overlap. To saveguard this the last <history_size> files
# are remembered as currently processed and thus not handled if appearing
# again
print ("Setting history_size:", obj.set("history_size", 2000))
# if the data should be stored in the local_target
print ("Setting store_data:", obj.set("store_data", True))
# if the data should be removed from the eiger
print ("Setting remove_data:", obj.set("remove_data", True))
# which hosts are allowed to request the data
print ("Setting whitelist:", obj.set("whitelist", "localhost"))
#print ("Setting whitelist:", obj.set("whitelist", ["localhost"]))

print ("Starting:", obj.do("start"))
print ("Status:", obj.do("status"))
for i in range(0):
    time.sleep(1)
    print ("Status:", obj.do("status"))
print ("Stopping:", obj.do("stop"))

obj.stop()
