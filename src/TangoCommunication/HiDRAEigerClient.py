#!/usr/bin/env python
import HiDRAEigerAPI

obj = HiDRAEigerAPI.HiDRAEigerAPI("p00")

# where the data should be stored inside the beamline filesystem
# only the relative path is needed because the absolute path can be reconstucted with the beamline name
obj.set("localTarget", "current/raw")

obj.get("localTarget")

# from which eiger should the data should be get
obj.set("eigerIp", "192.168.138.52")
# which simplon api version is running on the eiger
obj.set("eigerApiVersion", "1.5.0")
# this should only be shown in expert view because it can be critical
# HiDRA gets a file list from the eiger and the downloads these files asynchoniously. If the work on file list is not finished when the next list ist get there is an overlap. To saveguard this the last <historySize> files are remembered as currently processed and thus not handled if appearing again
obj.set("historySize", 2000)
# if the data should be stored in the localTarget
obj.set("storeData", True)
# if the data should be removed from the eiger
obj.set("removeData", True)
# which hosts are allowed to request the data
obj.set("whitelist", "localhost", "zitpcx19282")

obj.do("start")
obj.do("status")
obj.do("stop")

obj.stop()

