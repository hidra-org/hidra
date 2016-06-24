#!/usr/bin/env python
import socket
import sys
import TangoAPI

port = 51000

#host = socket.gethostname()
host = "asap3-bl-prx07"

obj = TangoAPI.TangoAPI(host, port)

obj.set('localTarget', '/root/zeromq-data-transfer/data/target')
#obj.set('localTarget', '/space/projects/zeromq-data-transfer/data/target')

obj.get('localTarget')

obj.set('detectorDevice', 'haspp06:10000/p06/eigerdectris/exp.01')
obj.set('filewriterDevice', 'haspp06:10000/p06/eigerfilewriter/exp.01')
obj.set('historySize', 0)
obj.set('storeData', True)
obj.set('removeData', True)
obj.set('whitelist', '["localhost","zitpcx19282"]')

obj.do('start')
obj.do('status')
obj.do('stop')

obj.stop()

