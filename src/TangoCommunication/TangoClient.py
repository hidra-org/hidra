#!/usr/bin/env python
import TangoAPI

obj = TangoAPI.TangoAPI("p00")

obj.set("localTarget", "/root/zeromq-data-transfer/data/target")
#obj.set("localTarget", "/space/projects/zeromq-data-transfer/data/target")

obj.get("localTarget")

obj.set("eigerIp", "192.168.138.52")
obj.set("eigerApiVersion", "1.5.0")
obj.set("historySize", 0)
obj.set("storeData", True)
obj.set("removeData", True)
obj.set("whitelist", "localhost", "zitpcx19282")

obj.do("start")
obj.do("status")
obj.do("stop")

obj.stop()

