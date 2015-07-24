import argparse
import subprocess
import os
import time


supportedFormats = [ "tif", "cbf", "hdf5"]
watchFolder = "/space/projects/Live_Viewer/source/"


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

if supported_file:
    my_cmd = 'echo "' +  source + '"  > /tmp/zeromqllpipe'
    p = subprocess.Popen ( my_cmd, shell=True )
    p.communicate()

    # wait to ZeroMQ to finish
    time.sleep(10)

# get responce from zeromq
#pipe_path = "/tmp/zeromqllpipe_resp"

# Open the fifo. We need to open in non-blocking mode or it will stalls until
# someone opens it for writting
#pipe_fd = os.open(pipe_path, os.O_RDONLY | os.O_NONBLOCK)

#waitForAnswer = True

#wait for new files
#with os.fdopen(pipe_fd) as pipe:
#    while waitForAnswer:
#        message = pipe.read()
#        if message:
#            pathnames = message.splitlines()
#            for filepath in pathnames:
#                if filepath == source:
#                    waitForAnswer = False
#                    break
#        print "sleep"
#        time.sleep(0.1)




p = subprocess.Popen ( [ 'mv', source, target ],
                stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE,
                universal_newlines = False )
out, err = p.communicate()

