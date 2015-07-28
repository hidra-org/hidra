import argparse
import subprocess
import os
import time
import zmq
import json
import logging


def initLogging(filenameFullPath, verbose):
    #@see https://docs.python.org/2/howto/logging-cookbook.html

    #more detailed logging if verbose-option has been set
    loggingLevel = logging.INFO
    if verbose:
        loggingLevel = logging.DEBUG

    #log everything to file
    logging.basicConfig(level=loggingLevel,
                        format='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s:%(lineno)d] [%(name)s] [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d_%H:%M:%S',
                        filename=filenameFullPath,
                        filemode="a")

    #log info to stdout, display messages with different format than the file output
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    formatter = logging.Formatter("%(asctime)s >  %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)




supportedFormats = [ "tif", "cbf", "hdf5"]
watchFolder = "/space/projects/Live_Viewer/source/"
logfile = "/space/projects/Live_Viewer/logs/wrapper_script.log"
verbose = True

#enable logging
initLogging(logfile, verbose)


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
    logging.debug( "Connecting to ZMQ socket: " + str(zmqSocketStr))
    socket.connect(zmqSocketStr)

    #send reply back to server
    workload = { "filepath": source, "targetPath": target }
    workload_json = json.dumps(workload)
    try:
        socket.send(workload_json)
    except:
        logging.debug( "Could not send message to ZMQ: " + str(workload))

    logging.debug( "Send message to ZMQ: " + str(workload))

#    my_cmd = 'echo "' +  source + '"  > /tmp/zeromqllpipe'
#    p = subprocess.Popen ( my_cmd, shell=True )
#    p.communicate()

    # wait to ZeroMQ to finish
#    time.sleep(10)

#p = subprocess.Popen ( [ 'mv', source, target ],
#                stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE,
#                universal_newlines = False )
#out, err = p.communicate()

    # We never get here but clean up anyhow
    socket.close()
    zmqContext.destroy()
