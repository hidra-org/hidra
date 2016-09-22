#!/usr/bin/env python
import HiDRAControlAPI
import time

obj = HiDRAControlAPI.HiDRAControlAPI("p00", useLog = None)
#obj = HiDRAControlAPI.HiDRAControlAPI("p00")

# where the data should be stored inside the beamline filesystem
# only the relative path is needed because the absolute path can be reconstucted with the beamline name
print "Setting localTarget:", obj.set("localTarget", "commissioning/raw")
#print "Setting localTarget:", obj.set("localTarget", "current/raw")

print obj.get("localTarget")

# from which eiger should the data should be get
print "Setting eigerIp:", obj.set("eigerIp", "192.168.138.52")
# which simplon api version is running on the eiger
print "Setting eigerApiVersion:", obj.set("eigerApiVersion", "1.5.0")
# this should only be shown in expert view because it can be critical
# HiDRA gets a file list from the eiger and the downloads these files asynchoniously. If the work on file list is not finished when the next list ist get there is an overlap. To saveguard this the last <historySize> files are remembered as currently processed and thus not handled if appearing again
print "Setting historySize:", obj.set("historySize", 2000)
# if the data should be stored in the localTarget
print "Setting storeData:", obj.set("storeData", True)
# if the data should be removed from the eiger
print "Setting removeData:", obj.set("removeData", True)
# which hosts are allowed to request the data
print "Setting whitelist:", obj.set("whitelist", "localhost")
#print "Setting whitelist:", obj.set("whitelist", ["localhost"])

print "Starting:", obj.do("start")
print "Status:", obj.do("status")
for i in range(0):
    time.sleep(1)
    print "Status:", obj.do("status")
print "Stopping:", obj.do("stop")

obj.stop()

