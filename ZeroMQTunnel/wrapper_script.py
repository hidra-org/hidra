__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import argparse
import subprocess
import os
import time
import zmq
import json
import logging
import helperScript


supportedFormats = [ "tif", "cbf", "hdf5"]
watchFolder = "/space/projects/Live_Viewer/source/"
logfile = "/space/projects/Live_Viewer/logs/wrapper_script.log"
verbose = True

#enable logging
helperScript.initLogging(logfile, verbose)


parser = argparse.ArgumentParser()
parser.add_argument("--mv_source", help = "Move source")
parser.add_argument("--mv_target", help = "Move target")

arguments = parser.parse_args()


source = os.path.normpath ( arguments.mv_source )
target = os.path.normpath ( arguments.mv_target )

( parentDir, filename )  = os.path.split ( source )
commonPrefix             = os.path.commonprefix ( [ watchFolder, source ] )
relativebasepath         = os.path.relpath ( source, commonPrefix )
( relativeParent, blub ) = os.path.split ( relativebasepath )

( name, postfix ) = filename.split( "." )
supported_file = postfix in supportedFormats

zmqIp   = "127.0.0.1"
zmqPort = "6080"

if supported_file:

    # set up ZeroMQ
    zmqContext = zmq.Context()

    socket = zmqContext.socket(zmq.PUSH)
    zmqSocketStr = 'tcp://' + zmqIp + ':' + zmqPort
    socket.connect(zmqSocketStr)
    logging.debug( "Connecting to ZMQ socket: " + str(zmqSocketStr))

    #send reply back to server
    workload = { "filepath": source, "targetPath": target }
    workload_json = json.dumps(workload)
    try:
        socket.send(workload_json)
    except:
        logging.debug( "Could not send message to ZMQ: " + str(workload))

    logging.debug( "Send message to ZMQ: " + str(workload))

    # We never get here but clean up anyhow
    try:
        socket.close()
        zmqContext.destroy()
    except KeyboardInterrupt:
        socket.close(0)
        zmqContext.destroy()
