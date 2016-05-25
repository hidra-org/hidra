#!/usr/bin/env python
import socket
import sys

port = 50900

msgs = [
    "set localTarget /space/projects/zeromq-data-transfer/data/target",
    "get localTarget",
    "set detectorDevice haspp06:10000/p06/eigerdectris/exp.01",
    "set filewriterDevice haspp06:10000/p06/eigerfilewriter/exp.01",
    "set historySize 0",
    "set storeData True",
    "set removeData True",
    'set whitelist ["localhost","zitpcx19282"]',
    "do start",
    "exit",
]

host = socket.gethostname()
sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sckt.connect((host, port))
except Exception, e:
    print "connect() failed", e
    sckt.close()
    sys.exit()

for msg in msgs:
    sckt.send( msg)
    print "sent (len %2d): %s" % (len(msg), msg)
    reply = sckt.recv(1024)
    print "recv (len %2d): %s " % (len( reply), reply)

sckt.close()

